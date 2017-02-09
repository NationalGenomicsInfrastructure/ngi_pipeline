#!/bin/env python
""" Main entry point for the ngi_pipeline.

It can either start the Tornado server that will trigger analysis on the processing
cluster (UPPMAX for NGI), or trigger analysis itself.
"""
from __future__ import print_function

import argparse
import glob
import importlib
import inflect
import os
import shutil
import sys

from ngi_pipeline.conductor import flowcell
from ngi_pipeline.conductor import launchers
from ngi_pipeline.conductor.flowcell import organize_projects_from_flowcell, \
                                            setup_analysis_directory_structure
from ngi_pipeline.database.classes import CharonError
from ngi_pipeline.database.filesystem import create_charon_entries_from_project
from ngi_pipeline.engines import qc_ngi
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.server import main as server_main
from ngi_pipeline.utils.charon import find_projects_from_samples, \
                                      reset_charon_records_by_object, \
                                      reset_charon_records_by_name
from ngi_pipeline.utils.filesystem import locate_project, recreate_project_from_filesystem
from ngi_pipeline.utils.parsers import parse_samples_from_vcf

LOG = minimal_logger(os.path.basename(__file__))
inflector = inflect.engine()

def validate_dangerous_user_thing(action=("do SOMETHING that Mario thinks you "
                                          "should BE WARNED about"),
                                  setting_name=None,
                                  warning=None):
    if warning:
        print(warning, file=sys.stderr)
    else:
        print("WARNING: you have told this script to {action}! "
              "Are you sure??".format(action=action), file=sys.stderr)
    attempts = 0
    return_value = False
    while not return_value:
        if attempts < 3:
            attempts += 1
            user_input = raw_input("Confirm by typing 'yes' or 'no' "
                                   "({}): ".format(attempts)).lower()
            if user_input not in ('yes', 'no'):
                continue
            elif user_input == 'yes':
                return_value = True
            elif user_input == 'no':
                break
    if return_value:
        print("Confirmed!\n----", file=sys.stderr)
        return True
    else:
        message = "No confirmation received; "
        if setting_name:
            message += "setting {} to False.".format(setting_name)
        else:
            message += "not proceeding with action."
        message += "\n----"
        print(message, file=sys.stderr)
        return False


class ArgumentParserWithTheFlagsThatIWant(argparse.ArgumentParser):
    def __init__(self, *args, **kwargs):
        super(ArgumentParserWithTheFlagsThatIWant, self).__init__(*args,
                formatter_class=argparse.ArgumentDefaultsHelpFormatter, **kwargs)
        self.add_argument("-f", "--restart-failed", dest="restart_failed_jobs", action="store_true",
                help=("Restart jobs marked as 'FAILED' in Charon"))
        self.add_argument("-d", "--restart-done", dest="restart_finished_jobs", action="store_true",
                help=("Restart jobs marked as DONE in Charon."))
        self.add_argument("-r", "--restart-running", dest="restart_running_jobs", action="store_true",
                help=("Restart jobs marked as UNDER_ANALYSIS in Charon. Use with care."))
        self.add_argument("-a", "--restart-all", dest="restart_all_jobs", action="store_true",
                help=("Just start any kind of job you can get your hands on regardless of status."))
        self.add_argument("-s", "--sample", dest="restrict_to_samples", action="append",
                help=("Restrict analysis to these samples. "
                      "Use flag multiple times for multiple samples."))

if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Launch NGI pipeline")
    subparsers = parser.add_subparsers(help="Choose the mode to run")
    parser.add_argument("-v", "--verbose", dest="quiet", action="store_false",
            help=("Send mails (INFO/WARN/ERROR); default False."))

    # Add subparser for the server
    parser_server = subparsers.add_parser('server', help="Start ngi_pipeline server")
    parser_server.add_argument('-p', '--port', type=int,
            help="Port on which to listen for incoming connections")


    # Add subparser for organization
    parser_organize = subparsers.add_parser('organize',
            help="Organize one or more demultiplexed flowcells into project/sample/libprep/seqrun format.")
    subparsers_organize = parser_organize.add_subparsers(help='Choose unit to analyze')
    # Add sub-subparser for flowcell organization
    organize_flowcell = subparsers_organize.add_parser('flowcell',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            help='Organize one or more demultiplexed flowcells, populating Charon with relevant data.')
    organize_flowcell.add_argument("organize_fc_dirs", nargs="+",
            help=("The paths to the Illumina demultiplexed fc directories to organize"))
    organize_flowcell.add_argument("-l", "--fallback-libprep", default=None,
            help=("If no libprep is supplied in the SampleSheet.csv or in Charon, "
                  "use this value when creating records in Charon. (Optional)"))
    organize_flowcell.add_argument("-w", "--sequencing-facility", default="NGI-S", choices=('NGI-S', 'NGI-U'),
            help="The facility where sequencing was performed.")
    organize_flowcell.add_argument("-b", "--best_practice_analysis", default="whole_genome_reseq",
            help="The best practice analysis to run for this project or projects.")
    organize_flowcell.add_argument("-f", "--force", dest="force_update", action="store_true",
            help="Force updating Charon projects. Danger danger danger. This will overwrite things.")
    organize_flowcell.add_argument("-d", "--delete", dest="delete_existing", action="store_true",
            help="Delete existing projects in Charon. Similarly dangerous.")
    organize_flowcell.add_argument("--force-create-project", action="store_true",
            help="TESTING ONLY: Create a project if it does not exist in Charon "
                 "using the project name as the project id.")
    organize_flowcell.add_argument("-s", "--sample", dest="restrict_to_samples", action="append",
            help="Restrict processing to these samples. Use flag multiple times for multiple samples.")
    organize_flowcell.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help="Restrict processing to these projects. Use flag multiple times for multiple projects.")

    # Add subparser for deletion
    parser_delete = subparsers.add_parser('delete', help="Delete data systematically.")
    subparsers_delete = parser_delete.add_subparsers(help="Choose unit to delete.")
    delete_analysis = subparsers_delete.add_parser('analysis', help="Delete analysis files.")
    delete_analysis.add_argument("delete_proj_analysis", nargs="+",
            help=("The name of the project whose analysis you would delete.\n"
                  "NOTE: if no analysis engine is specified, entire project "
                  "analysis directory will be deleted."))
    delete_analysis.add_argument("-e", "--engine",
            help=("The engine whose  analysis data you would delete. Only required"
                  "for full project analysis deletion (not with specific samples."))
    delete_analysis.add_argument("-s", "--sample", dest="restrict_to_samples", action="append",
            help=("Restrict deletion to these samples. Use flag multiple times "
                  "for multiple samples.\nNOTE: requires engine has implemented "
                  "individual sample removal functionality."))
    delete_analysis.add_argument("-c", "--reset-charon", action="store_true",
            help=("Reset status values in Charon when deleting analyses for "
                  "a project/sample."))

    # Add subparser for analysis
    parser_analyze = subparsers.add_parser('analyze', help="Launch analysis.")
    subparsers_analyze = parser_analyze.add_subparsers(parser_class=ArgumentParserWithTheFlagsThatIWant,
            help='Choose unit to analyze')

    # Add sub-subparser for flowcell analysis
    analyze_flowcell = subparsers_analyze.add_parser('flowcell',
            help='Start analysis of raw flowcells')
    analyze_flowcell.add_argument("-k", "--keep-existing-data", action="store_true",
            help="Keep/re-use existing analysis data when launching new analyses.")
    analyze_flowcell.add_argument("--no-qc", action="store_true",
            help="Skip qc analysis.")
    analyze_flowcell.add_argument("--generate_bqsr_bam", action="store_true", dest="generate_bqsr_bam",
            default=False, help="Generate the recalibrated BAM file")
    analyze_flowcell.add_argument("analyze_fc_dirs", nargs="+",
            help=("The path to one or more demultiplexed Illumina flowcell "
                  "directories to process and analyze."))
    analyze_flowcell.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help=("Restrict analysis to these projects. "
                  "Use flag multiple times for multiple projects."))
    # Add sub-subparser for project analysis
    analyze_project = subparsers_analyze.add_parser('project',
            help='Start the analysis of a pre-parsed project.')
    analyze_project.add_argument("-k", "--keep-existing-data", action="store_true",
            help="Keep/re-use existing analysis data when launching new analyses.")
    analyze_project.add_argument("--no-qc", action="store_true",
            help="Skip qc analysis.")
    analyze_project.add_argument("--generate_bqsr_bam", action="store_true", dest="generate_bqsr_bam",
            default=False, help="Generate the recalibrated BAM file")
    analyze_project.add_argument('analyze_project_dirs', nargs='+',
            help='The path to the project folder to be analyzed.')

    # Add subparser for qc
    parser_qc = subparsers.add_parser('qc', help='Launch QC analysis.')
    subparsers_qc = parser_qc.add_subparsers(help='Choose unit to analyze')
    # Add sub-subparser for flowcell qc
    qc_flowcell = subparsers_qc.add_parser('flowcell',
            help='Start QC analysis of raw flowcells.')
    qc_flowcell.add_argument("-r", "--rerun", action="store_true",
            help='Force the rerun of the qc analysis if output files already exist.')
    qc_flowcell.add_argument("-l", "--fallback-libprep", default=None,
            help=("If no libprep is supplied in the SampleSheet.csv or in Charon, "
                  "use this value when creating records in Charon. (Optional)"))
    qc_flowcell.add_argument("-w", "--sequencing-facility", default="NGI-S", choices=('NGI-S', 'NGI-U'),
            help="The facility where sequencing was performed.")
    qc_flowcell.add_argument("-b", "--best_practice_analysis", default="whole_genome_reseq",
            help="The best practice analysis to run for this project or projects.")
    qc_flowcell.add_argument("-f", "--force", dest="force_update", action="store_true",
            help="Force updating Charon projects. Danger danger danger. This will overwrite things.")
    qc_flowcell.add_argument("-d", "--delete", dest="delete_existing", action="store_true",
            help="Delete existing projects in Charon. Similarly dangerous.")
    qc_flowcell.add_argument("-s", "--sample", dest="restrict_to_samples", action="append",
            help=("Restrict analysis to these samples. Use flag multiple times for multiple samples."))
    qc_flowcell.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help="Restrict processing to these projects. Use flag multiple times for multiple projects.")
    qc_flowcell.add_argument("qc_flowcell_dirs", nargs="+",
            help=("The path to one or more demultiplexed Illumina flowcell "
                  "directories to process and run through QC analysis."))
    # Add sub-subparser for project qc
    qc_project = subparsers_qc.add_parser('project',
            help='Start QC analysis of a pre-parsed project directory.')
    qc_project.add_argument("-f", "--force-rerun", action="store_true",
            help='Force the rerun of the qc analysis if output files already exist.')
    qc_project.add_argument("-s", "--sample", dest="restrict_to_samples", action="append",
            help=("Restrict analysis to these samples. Use flag multiple times for multiple samples."))
    qc_project.add_argument("qc_project_dirs", nargs="+",
            help=("The path to one or more pre-parsed project directories to "
                  "run through QC analysis."))


    # Add subparser for genotyping
    parser_genotype = subparsers.add_parser('genotype', help="Launch genotype concordance analysis.")
    subparsers_genotype = parser_genotype.add_subparsers(parser_class=ArgumentParserWithTheFlagsThatIWant,
            help="Choose unit to analyze.")
    # Add sub-subparser for project genotyping
    genotype_project = subparsers_genotype.add_parser('project',
            help="Start genotype analysis for all samples in a project")
    genotype_project.add_argument("genotype_project_dirs", nargs="*",
            help=("The path to one or more pre-parsed project directories to "
                  "run through genotype concordance analysis. If not specified, "
                  "all samples in vcf file are genotyped if possible. (Optional)"))
    genotype_project.add_argument("-g", "--genotype-file", action="store", required=True,
            help="The path to the genotype VCF file.")
    genotype_project.add_argument("-k", "--keep-existing-data", action="store_true",
            help="Keep/re-use existing analysis data when launching new analyses.")
    # Add sub-subparser for sample genotyping
    #genotype_sample = subparsers_genotype.add_parser('sample',
    #        help="Start genotype analysis for one specific sample in a project.")


    args = parser.parse_args()

    # These options are available only if the script has been called with the 'analyze' option
    restart_all_jobs = args.__dict__.get('restart_all_jobs')
    if restart_all_jobs:
        if not args.__dict__.get("keep_existing_data"):
            # Validate if not keep_existing_data
            restart_all_jobs = validate_dangerous_user_thing(action=("restart all FAILED, RUNNING, "
                                                                     "and FINISHED jobs, deleting "
                                                                     "previous analyses"))
        if restart_all_jobs: # 'if' b.c. there's no 'if STILL' operator (kludge kludge kludge)
            args.restart_failed_jobs = True
            args.restart_finished_jobs = True
            args.restart_running_jobs = True
    else:
        if not args.__dict__.get("keep_existing_data"):
            if args.__dict__.get("restart_failed_jobs"):
                args.restart_failed_jobs = \
                    validate_dangerous_user_thing(action=("restart FAILED jobs, deleting "
                                                          "previous analysies files"))
            if args.__dict__.get("restart_finished_jobs"):
                args.restart_finished_jobs = \
                    validate_dangerous_user_thing(action=("restart FINISHED jobs, deleting "
                                                          "previous analysis files"))
            if args.__dict__.get("restart_running_jobs"):
                args.restart_finished_jobs = \
                    validate_dangerous_user_thing(action=("restart RUNNING jobs, deleting "
                                                          "previous analysis files"))
    # Charon-specific arguments ('organize', 'analyze', 'qc')
    if args.__dict__.get("force_update"):
        args.force_update = \
                validate_dangerous_user_thing("overwrite existing data in Charon")
    if args.__dict__.get("delete_existing"):
        args.delete_existing = \
                validate_dangerous_user_thing("delete existing data in Charon")


    # Finally execute corresponding functions

    ## Analyze Flowcell
    if 'analyze_fc_dirs' in args:
        LOG.info('Starting flowcell analysis of flowcell {} '
                 '{}'.format(inflector.plural("directory", len(args.analyze_fc_dirs)),
                             ", ".join(args.analyze_fc_dirs)))
        flowcell.process_demultiplexed_flowcells(args.analyze_fc_dirs,
                                                 args.restrict_to_projects,
                                                 args.restrict_to_samples,
                                                 args.restart_failed_jobs,
                                                 args.restart_finished_jobs,
                                                 args.restart_running_jobs,
                                                 keep_existing_data=args.keep_existing_data,
                                                 no_qc=args.no_qc,
                                                 quiet=args.quiet,
                                                 manual=True,
                                                 generate_bqsr_bam=args.generate_bqsr_bam)

    ## Analyze Project
    elif 'analyze_project_dirs' in args:
        for analyze_project_dir in args.analyze_project_dirs:
            try:
                project_dir = locate_project(analyze_project_dir)
            except ValueError as e:
                LOG.error(e)
                continue
            project_obj = \
                    recreate_project_from_filesystem(project_dir=project_dir,
                                                     restrict_to_samples=args.restrict_to_samples)
            launchers.launch_analysis([project_obj],
                                      restart_failed_jobs=args.restart_failed_jobs,
                                      restart_finished_jobs=args.restart_finished_jobs,
                                      restart_running_jobs=args.restart_running_jobs,
                                      keep_existing_data=args.keep_existing_data,
                                      no_qc=args.no_qc,
                                      quiet=args.quiet,
                                      manual=True,
                                      generate_bqsr_bam=args.generate_bqsr_bam)

    elif 'delete_proj_analysis' in args:
        from ngi_pipeline.conductor.classes import get_engine_for_bp
        delete_proj_analysis_list = list(set(args.delete_proj_analysis))
        for delete_proj_analysis in delete_proj_analysis_list:
            if args.restrict_to_samples:
                try:
                    project_dir = locate_project(delete_proj_analysis)
                except ValueError as e:
                    LOG.error(e)
                    continue
                # Remove specific samples from the project if this code is implemented
                project_obj = \
                        recreate_project_from_filesystem(project_dir=project_dir,
                                                         restrict_to_samples=args.restrict_to_samples)
                if not project_obj.samples:
                    LOG.error("No samples found for project {}. Skipping.".format(project_obj))
                    continue
                try:
                    analysis_module = get_engine_for_bp(project_obj)
                except Exception as e:
                    if args.engine:
                        # Try to import using the user-supplied engine
                        analysis_engine = args.engine
                        if not analysis_engine.startswith("ngi_pipeline.engines."):
                            analysis_engine = "ngi_pipeline.engines." + analysis_engine
                        try:
                            analysis_module = importlib.import_module(analysis_engine)
                        except ImportError:
                            LOG.error('Analysis engine for project "{}" could not '
                                      'be determined from Charon and user-supplied '
                                      'engine "{}" was not importable.'.format(project_obj,
                                                                               analysis_engine))
                            continue
                    else:
                        LOG.error('Analysis engine for project "{}" could not '
                                  'be determined from Charon and no value was '
                                  'supplied. Skipping. ({})'.format(project_obj, e))
                        continue
                if args.engine and args.engine not in analysis_module.__name__:
                    LOG.error('Engine "{}" was specified for project "{}" but Charon '
                              'indicates engine is "{}". This parameter is not '
                              'required for individual sample deletion as it is '
                              'loaded from Charon. Skipping.'.format(args.engine,
                                                                     project_obj,
                                                                     analysis_module.__name__))
                    continue
                if validate_dangerous_user_thing( \
                        action=('delete the following sample analyses for engine "{}": '
                                 '{}'.format(analysis_module.__name__,
                                             ", ".join(
                                                 [s.name for s in project_obj.samples.values()])))):
                    try:
                        analysis_module.utils.remove_previous_sample_analyses(project_obj)
                    except AttributeError:
                        LOG.error('Analysis module "{}" has not implemented '
                                  'the function "utils.remove_previous_sample_analyses, '
                                  'so individual sample analyses cannot be removed. '
                                  'Skipping project analysis deletion for project '
                                  '"{}"'.format(analysis_module.__name__, project_obj))
                        continue
                    try:
                        reset_charon_records_by_object(project_obj)
                    except CharonError as e:
                        LOG.error("Error when resetting Charon records for project "
                                  "{}: {}".format(project_obj, e))
            else:
                try:
                    delete_tree_path = locate_project(delete_proj_analysis, subdir="ANALYSIS")
                except ValueError as e:
                    LOG.error(e)
                    continue
                delete_symlink_path = None
                for item in glob.glob(os.path.join(os.path.split(delete_tree_path)[0], "*")):
                    if os.path.islink(item):
                        if os.path.realpath(item) == delete_tree_path:
                            delete_symlink_path = item
                if args.engine:
                    delete_tree_path = os.path.join(delete_tree_path, args.engine)
                    if not os.path.exists(delete_tree_path):
                        LOG.error('User-specified engine analysis path does not '
                                  'exist; skipping: {}'.format(delete_tree_path))
                        continue
                    if delete_symlink_path:
                        delete_symlink_path = os.path.join(delete_symlink_path, args.engine)
                if validate_dangerous_user_thing(action=('delete ALL ANALYSIS files '
                                                         'under {} and RESET ALL '
                                                         'CHARON RECORDS for this '
                                                         'project'.format(delete_tree_path))):
                    try:
                        LOG.info("Deleting {}...".format(delete_tree_path))
                        shutil.rmtree(delete_tree_path)
                        LOG.info("Deleted {}".format(delete_tree_path))
                    except OSError as e:
                        LOG.error('Error when deleting {}: {}'.format(delete_tree_path, e))
                    if delete_symlink_path:
                        try:
                            LOG.info("Unlinking symlink {}...".format(delete_symlink_path))
                            os.unlink(delete_symlink_path)
                            LOG.info("Removed symlink {}".format(delete_symlink_path))
                        except OSError as e:
                            LOG.error("Error when unlinking {}: {}".format(delete_symlink_path, e))
                    try:
                        reset_charon_records_by_name(delete_proj_analysis)
                    except CharonError as e:
                        LOG.error("Error when resetting Charon records for project "
                                  "{}: {}".format(delete_proj_analysis, e))


    ## QC Flowcell
    elif 'qc_flowcell_dirs' in args:
        qc_flowcell_dirs_list = list(set(args.qc_flowcell_dirs))
        LOG.info("Organizing flowcell {} {}".format(inflector.plural("directory",
                                                                     len(qc_flowcell_dirs_list)),
                                                    ", ".join(qc_flowcell_dirs_list)))
        projects_to_analyze = \
                organize_projects_from_flowcell(demux_fcid_dirs=qc_flowcell_dirs_list,
                                                restrict_to_projects=args.restrict_to_projects,
                                                restrict_to_samples=args.restrict_to_samples,
                                                fallback_libprep=args.fallback_libprep,
                                                quiet=args.quiet)
        for project in projects_to_analyze:
            try:
                create_charon_entries_from_project(project=project,
                                                   best_practice_analysis=args.best_practice_analysis,
                                                   sequencing_facility=args.sequencing_facility,
                                                   force_overwrite=args.force_update,
                                                   delete_existing=args.delete_existing)
            except Exception as e:
                print(e, file=sys.stderr)
        LOG.info("Done with organization.")
        for project in projects_to_analyze:
            for sample in project:
                qc_ngi.launchers.analyze(project, sample, quiet=args.quiet)

    ## QC Project
    elif 'qc_project_dirs' in args:
        for qc_project_dir in args.qc_project_dirs:
            project = recreate_project_from_filesystem(project_dir=qc_project_dir,
                                                       restrict_to_samples=args.restrict_to_samples)
            if not project.samples:
                LOG.info('No samples found for project {} (path {})'.format(project.project_id,
                                                                            qc_project_dir))
            for sample in project:
                qc_ngi.launchers.analyze(project, sample, quiet=args.quiet)

    ## Organize Flowcell
    elif 'organize_fc_dirs' in args:
        organize_fc_dirs_list = list(set(args.organize_fc_dirs))
        LOG.info("Organizing flowcell {} {}".format(inflector.plural("directory",
                                                                     len(organize_fc_dirs_list)),
                                                    ", ".join(organize_fc_dirs_list)))
        projects_to_analyze = \
                organize_projects_from_flowcell(demux_fcid_dirs=organize_fc_dirs_list,
                                                restrict_to_projects=args.restrict_to_projects,
                                                restrict_to_samples=args.restrict_to_samples,
                                                fallback_libprep=args.fallback_libprep,
                                                quiet=args.quiet)
        for project in projects_to_analyze:
            try:
                create_charon_entries_from_project(project=project,
                                                   best_practice_analysis=args.best_practice_analysis,
                                                   sequencing_facility=args.sequencing_facility,
                                                   force_overwrite=args.force_update,
                                                   delete_existing=args.delete_existing)
            except Exception as e:
                LOG.error(e.message)
                print(e, file=sys.stderr)
        LOG.info("Done with organization.")

    elif 'genotype_project_dirs' in args:
        from ngi_pipeline.engines import piper_ngi
        genotype_file_path = args.genotype_file
        project_obj_list = []
        if not args.genotype_project_dirs:
            LOG.info('No projects specified; running genotype analysis for all '
                     'samples present in VCF file.')
            # User passed only the genotype file; try to determine samples/projects
            # from vcf file
            projects_samples_dict = \
                    find_projects_from_samples(parse_samples_from_vcf(genotype_file_path))
            for project_id, samples in projects_samples_dict.iteritems():
                try:
                    path_to_project = locate_project(project_id)
                except ValueError:
                    # Project has not yet been organized from flowcell level
                    LOG.warn('Project "{}" has not yet been organized from '
                             'flowcell to project level; skipping.'.format(project_id))
                    continue
                project = recreate_project_from_filesystem(project_dir=path_to_project,
                                                           restrict_to_samples=samples)
                project_obj_list.append(project)
        else:
            for genotype_project_dir in args.genotype_project_dirs:
                LOG.info("Starting genotype analysis of project {} with genotype "
                         "file {}".format(genotype_project_dir, genotype_file_path))
                project = recreate_project_from_filesystem(project_dir=genotype_project_dir,
                                                           restrict_to_samples=args.restrict_to_samples)
                project_obj_list.append(project)
        for project in project_obj_list:
            for sample in project:
                piper_ngi.launchers.analyze(project, sample,
                                            genotype_file=genotype_file_path,
                                            restart_finished_jobs=args.restart_finished_jobs,
                                            restart_running_jobs=args.restart_running_jobs,
                                            keep_existing_data=args.keep_existing_data,
                                            level="genotype")

    ## Server
    elif 'port' in args:
        LOG.info('Starting ngi_pipeline server at port {}'.format(args.port))
        server_main.start(args.port)
