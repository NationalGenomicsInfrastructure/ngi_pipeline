#!/bin/env python
""" Main entry point for the ngi_pipeline.

It can either start the Tornado server that will trigger analysis on the processing
cluster (UPPMAX for NGI), or trigger analysis itself.
"""
from __future__ import print_function

import argparse
import inflect
import os
import sys

from ngi_pipeline import __version__
from ngi_pipeline.conductor import flowcell
from ngi_pipeline.conductor import launchers
from ngi_pipeline.conductor.flowcell import organize_projects_from_flowcell
from ngi_pipeline.database.filesystem import create_charon_entries_from_project
from ngi_pipeline.engines import qc_ngi
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.filesystem import locate_project, recreate_project_from_filesystem
from six.moves import input

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
            user_input = input("Confirm by typing 'yes' or 'no' "
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
    parser.add_argument("-w", "--version", action='version', 
            version='NGI Pipeline version {version}'.format(version=__version__), help="Displays current version number")

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
    organize_flowcell.add_argument("-b", "--best_practice_analysis", default="wgs_germline",
            help="The best practice analysis to run for this project or projects.")
    organize_flowcell.add_argument("--pipeline", default="sarek",
            help="The pipeline to execute for this project or projects.")
    organize_flowcell.add_argument("--reference", default="GRCh38",
            help="The reference genome to use for this project or projects.")
    organize_flowcell.add_argument("-f", "--force", dest="force_update", action="store_true",
            help="Force updating Charon projects. Danger danger danger. This will overwrite things.")
    organize_flowcell.add_argument("-s", "--sample", dest="restrict_to_samples", action="append",
            help="Restrict processing to these samples. Use flag multiple times for multiple samples.")
    organize_flowcell.add_argument("-p", "--project", dest="restrict_to_projects", action="append",
            help="Restrict processing to these projects. Use flag multiple times for multiple projects.")

    # Add subparser for analysis
    parser_analyze = subparsers.add_parser('analyze', help="Launch analysis.")
    subparsers_analyze = parser_analyze.add_subparsers(parser_class=ArgumentParserWithTheFlagsThatIWant,
            help='Choose unit to analyze')

    # Add sub-subparser for project analysis
    analyze_project = subparsers_analyze.add_parser('project',
            help='Start the analysis of a pre-parsed project.')
    analyze_project.add_argument("--no-qc", action="store_true",
            help="Skip qc analysis.")
    analyze_project.add_argument("--batch-analysis", action="store_true",
            help="Batch analysis project-wise")
    analyze_project.add_argument('analyze_project_dirs', nargs='+',
            help='The path to the project folder to be analyzed.')

    # Add subparser for qc
    parser_qc = subparsers.add_parser('qc', help='Launch QC analysis.')
    subparsers_qc = parser_qc.add_subparsers(help='Choose unit to analyze')

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


    args = parser.parse_args()

    # These options are available only if the script has been called with the 'analyze' option
    restart_all_jobs = args.__dict__.get('restart_all_jobs')
    if restart_all_jobs:
        restart_all_jobs = validate_dangerous_user_thing(action=("restart all FAILED, RUNNING, "
                                                                     "and FINISHED jobs, deleting "
                                                                     "previous analyses"))
        if restart_all_jobs: # 'if' b.c. there's no 'if STILL' operator (kludge kludge kludge)
            args.restart_failed_jobs = True
            args.restart_finished_jobs = True
            args.restart_running_jobs = True
    else:
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

    # Finally execute corresponding functions

    ## Analyze Project
    if 'analyze_project_dirs' in args:
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
                                      batch_analysis=args.batch_analysis,
                                      no_qc=args.no_qc,
                                      quiet=args.quiet,
                                      manual=True)

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
                                                   pipeline=args.pipeline,
                                                   reference=args.reference,
                                                   sequencing_facility=args.sequencing_facility,
                                                   force_overwrite=args.force_update)
            except Exception as e:
                LOG.error(e.message)
                print(e, file=sys.stderr)
        LOG.info("Done with organization.")
