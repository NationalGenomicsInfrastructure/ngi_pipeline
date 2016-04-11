#!/usr/bin/env python
# -*- coding: utf-8 -*-


from __future__ import print_function

import glob
import os
import re
import sys

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.conductor.launchers import launch_analysis
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.database.communicate import get_project_id_from_name
from ngi_pipeline.database.filesystem import create_charon_entries_from_project
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.communication import mail_analysis
from ngi_pipeline.utils.filesystem import do_rsync, do_symlink, \
                                          locate_flowcell, safe_makedir
from ngi_pipeline.utils.parsers import determine_library_prep_from_fcid, \
                                       determine_library_prep_from_samplesheet, \
                                       parse_lane_from_filename

LOG = minimal_logger(__name__)

UPPSALA_PROJECT_RE = re.compile(r'(\w{2}-\d{4}|\w{2}\d{2,3})')
STHLM_PROJECT_RE = re.compile(r'[A-z]{1,2}[_.][A-z0-9]+_\d{2}_\d{2}')
STHLM_X_PROJECT_RE = re.compile(r'[A-z]{1,2}_[A-z0-9]+_\d{2}_\d{2}')


## TODO we should just remove this function
def process_demultiplexed_flowcell(demux_fcid_dir_path, restrict_to_projects=None,
                                   restrict_to_samples=None, restart_failed_jobs=False,
                                   restart_finished_jobs=False, restart_running_jobs=False,
                                   keep_existing_data=False, no_qc=False, quiet=False,
                                   manual=False, config=None, config_file_path=None, generate_bqsr_bam=False):
    """Call process_demultiplexed_flowcells, restricting to a single flowcell.
    Essentially a restrictive wrapper.

    :param str demux_fcid_dirs: The CASAVA-produced demux directory/directories.
    :param list restrict_to_projects: A list of projects; analysis will be
                                      restricted to these. Optional.
    :param list restrict_to_samples: A list of samples; analysis will be
                                     restricted to these. Optional.
    :param bool restart_failed_jobs: Restart jobs marked as "FAILED" in Charon.
    :param bool restart_finished_jobs: Restart jobs marked as "DONE" in Charon.
    :param bool restart_running_jobs: Restart jobs marked as running in Charon
    :param bool keep_existing_data: Keep existing analysis data when launching new jobs
    :param str config_file_path: The path to the NGI configuration file; optional.
    """
    if type(demux_fcid_dir_path) is not str:
        error_message = ("The path to a single demultiplexed flowcell should be "
                         "passed to this function as a string.")
        raise ValueError(error_message)
    process_demultiplexed_flowcells([demux_fcid_dir_path], restrict_to_projects,
                                    restrict_to_samples, restart_failed_jobs,
                                    restart_finished_jobs, restart_running_jobs,
                                    keep_existing_data=keep_existing_data,
                                    no_qc=no_qc, config_file_path=config_file_path,
                                    quiet=quiet, manual=manual, generate_bqsr_bam=generate_bqsr_bam)


@with_ngi_config
def process_demultiplexed_flowcells(demux_fcid_dirs, restrict_to_projects=None,
                                    restrict_to_samples=None, restart_failed_jobs=False,
                                    restart_finished_jobs=False, restart_running_jobs=False,
                                    fallback_libprep=None, keep_existing_data=False, no_qc=False,
                                    quiet=False, manual=False, config=None, config_file_path=None,
                                    generate_bqsr_bam=False):
    """Sort demultiplexed Illumina flowcells into projects and launch their analysis.

    :param list demux_fcid_dirs: The CASAVA-produced demux directory/directories.
    :param list restrict_to_projects: A list of projects; analysis will be
                                      restricted to these. Optional.
    :param list restrict_to_samples: A list of samples; analysis will be
                                     restricted to these. Optional.
    :param bool restart_failed_jobs: Restart jobs marked as "FAILED" in Charon.
    :param bool restart_finished_jobs: Restart jobs marked as "DONE" in Charon.
    :param bool restart_running_jobs: Restart jobs marked as running in Charon
    :param str fallback_libprep: If libprep cannot be determined, use this value if supplied (default None)
    :param bool keep_existing_data: Keep existing analysis data when launching new jobs
    :param bool quiet: Don't send notification emails; added to config
    :param bool manual: This is being run from a user script; added to config
    :param dict config: The parsed NGI configuration file; optional.
    :param str config_file_path: The path to the NGI configuration file; optional.
    """
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    projects_to_analyze = organize_projects_from_flowcell(demux_fcid_dirs=demux_fcid_dirs,
                                                          restrict_to_projects=restrict_to_projects,
                                                          restrict_to_samples=restrict_to_samples,
                                                          fallback_libprep=fallback_libprep,
                                                          quiet=quiet, config=config)
    for project in projects_to_analyze:
        if UPPSALA_PROJECT_RE.match(project.project_id):
            LOG.info('Creating Charon records for Uppsala project "{}" if they '
                     'are missing'.format(project))
            create_charon_entries_from_project(project, sequencing_facility="NGI-U")
    launch_analysis(projects_to_analyze, restart_failed_jobs, restart_finished_jobs,
                    restart_running_jobs, keep_existing_data=keep_existing_data,
                    no_qc=no_qc, config=config, generate_bqsr_bam=generate_bqsr_bam)


@with_ngi_config
def organize_projects_from_flowcell(demux_fcid_dirs, restrict_to_projects=None,
                                    restrict_to_samples=None,
                                    fallback_libprep=None, quiet=False,
                                    create_files=True,
                                    config=None, config_file_path=None):
    """Sort demultiplexed Illumina flowcells into projects and return a list of them,
    creating the project/sample/libprep/seqrun dir tree on disk via symlinks.

    :param list demux_fcid_dirs: The CASAVA-produced demux directory/directories.
    :param list restrict_to_projects: A list of projects; analysis will be
                                      restricted to these. Optional.
    :param list restrict_to_samples: A list of samples; analysis will be
                                     restricted to these. Optional.
    :param str fallback_libprep: If libprep cannot be determined, use this value if supplied (default None)
    :param bool quiet: Don't send notification emails
    :param bool create_files: Alter the filesystem (as opposed to just parsing flowcells) (default True)
    :param dict config: The parsed NGI configuration file; optional.
    :param str config_file_path: The path to the NGI configuration file; optional.

    :returns: A list of NGIProject objects.
    :rtype: list
    :raises RuntimeError: If no (valid) projects are found in the flowcell dirs
    """
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    demux_fcid_dirs_set = set(demux_fcid_dirs)
    # Sort/copy each raw demux FC into project/sample/fcid format -- "analysis-ready"
    projects_to_analyze = dict()
    for demux_fcid_dir in demux_fcid_dirs_set:
        try:
            # Get the full path to the flowcell if it was passed in as just a name
            demux_fcid_dir = locate_flowcell(demux_fcid_dir)
        except ValueError as e:
            # Flowcell path couldn't be found/doesn't exist; skip it
            LOG.error('Skipping flowcell "{}": {}'.format(demux_fcid_dir, e))
            continue
        # These will be a bunch of Project objects each containing Samples, FCIDs, lists of fastq files
        projects_to_analyze = \
                setup_analysis_directory_structure(fc_dir=demux_fcid_dir,
                                                   projects_to_analyze=projects_to_analyze,
                                                   restrict_to_projects=restrict_to_projects,
                                                   restrict_to_samples=restrict_to_samples,
                                                   create_files=create_files,
                                                   fallback_libprep=fallback_libprep,
                                                   config=config,
                                                   quiet=quiet)
    if not projects_to_analyze:
        if restrict_to_projects:
            error_message = ("No projects found to process: the specified flowcells "
                             "({fcid_dirs}) do not contain the specified project(s) "
                             "({restrict_to_projects}) or there was an error "
                             "gathering required information.").format(
                                    fcid_dirs=",".join(demux_fcid_dirs_set),
                                    restrict_to_projects=",".join(restrict_to_projects))
        else:
            error_message = ("No projects found to process in flowcells {} "
                             "or there was an error gathering required "
                             "information.".format(",".join(demux_fcid_dirs_set)))
        raise RuntimeError(error_message)
    else:
        projects_to_analyze = projects_to_analyze.values()
    return projects_to_analyze


@with_ngi_config
def setup_analysis_directory_structure(fc_dir, projects_to_analyze,
                                       restrict_to_projects=None, restrict_to_samples=None,
                                       create_files=True,
                                       fallback_libprep=None,
                                       quiet=False,
                                       config=None, config_file_path=None):
    """
    Copy and sort files from their CASAVA-demultiplexed flowcell structure
    into their respective project/sample/libPrep/FCIDs. This collects samples
    split across multiple flowcells.

    :param str fc_dir: The directory created by CASAVA for this flowcell.
    :param dict config: The parsed configuration file.
    :param set projects_to_analyze: A dict (of Project objects, or empty)
    :param bool create_files: Alter the filesystem (as opposed to just parsing flowcells) (default True)
    :param str fallback_libprep: If libprep cannot be determined, use this value if supplied (default None)
    :param list restrict_to_projects: Specific projects within the flowcell to process exclusively
    :param list restrict_to_samples: Specific samples within the flowcell to process exclusively

    :returns: A list of NGIProject objects that need to be run through the analysis pipeline
    :rtype: list

    :raises KeyError: If a required configuration key is not available.
    """
    LOG.info("Setting up analysis for demultiplexed data in source folder \"{}\"".format(fc_dir))
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    config["quiet"] = quiet # Hack because I enter here from a script sometimes
    pattern = "(.+(?:{}|{}))\/.+".format(
        config["analysis"].get("sthlm_root", "this_is_not_a_valid_path"),
        config["analysis"].get("upps_root", "this_is_not_a_valid_path"))
    matches=re.match(pattern, fc_dir)
    if matches:
        flowcell_root=matches.group(1)
    else:
        LOG.error("cannot guess which project the flowcell {} belongs to".format(fc_dir))
        raise RuntimeError

    analysis_top_dir = os.path.abspath(os.path.join(flowcell_root,config["analysis"]["top_dir"]))
    try:
        safe_makedir(analysis_top_dir)
    except OSError as e:
        LOG.error('Error: Analysis top directory {} does not exist and could not '
                  'be created.'.format(analysis_top_dir))
    fc_dir = fc_dir if os.path.isabs(fc_dir) else os.path.join(analysis_top_dir, fc_dir)
    if not os.path.exists(fc_dir):
        LOG.error("Error: Flowcell directory {} does not exist".format(fc_dir))
        return []
    # Map the directory structure for this flowcell
    try:
        fc_dir_structure = parse_flowcell(fc_dir)
    except (OSError, ValueError) as e:
        LOG.error("Error when processing flowcell dir \"{}\": {}".format(fc_dir, e))
        return []
    fc_full_id = fc_dir_structure['fc_full_id']
    if not fc_dir_structure.get('projects'):
        LOG.warn("No projects found in specified flowcell directory \"{}\"".format(fc_dir))
    # Iterate over the projects in the flowcell directory
    for project in fc_dir_structure.get('projects', []):
        project_name = project['project_name']
        project_original_name = project['project_original_name']
        samplesheet_path = fc_dir_structure.get("samplesheet_path")
        try:
            # Maps e.g. "Y.Mom_14_01" to "P123"
            project_id = get_project_id_from_name(project_name)
        except (CharonError, RuntimeError, ValueError) as e:
            LOG.warn('Could not retrieve project id from Charon (record missing?). '
                     'Using project name ("{}") as project id '
                     '(error: {})'.format(project_name, e))
            project_id = project_name
        # If specific projects are specified, skip those that do not match
        if restrict_to_projects and project_name not in restrict_to_projects and \
                                    project_id not in restrict_to_projects:
            LOG.debug("Skipping project {} (not in restrict_to_projects)".format(project_name))
            continue
        LOG.info("Setting up project {}".format(project.get("project_name")))
        # Create a project directory if it doesn't already exist, including
        # intervening "DATA" directory
        project_dir = os.path.join(analysis_top_dir, "DATA", project_id)
        project_sl_dir = os.path.join(analysis_top_dir, "DATA", project_name)
        project_analysis_dir = os.path.join(analysis_top_dir, "ANALYSIS", project_id)
        project_analysis_sl_dir = os.path.join(analysis_top_dir, "ANALYSIS", project_name)
        if create_files:
            safe_makedir(project_dir, 0o2770)
            safe_makedir(project_analysis_dir, 0o2770)
            if not project_dir == project_sl_dir and \
               not os.path.exists(project_sl_dir):
                os.symlink(project_dir, project_sl_dir)
            if not project_analysis_dir == project_analysis_sl_dir and \
               not os.path.exists(project_analysis_sl_dir):
                os.symlink(project_analysis_dir, project_analysis_sl_dir)
        try:
            project_obj = projects_to_analyze[project_dir]
        except KeyError:
            project_obj = NGIProject(name=project_name, dirname=project_id,
                                     project_id=project_id,
                                     base_path=analysis_top_dir)
            projects_to_analyze[project_dir] = project_obj
        # Iterate over the samples in the project
        for sample in project.get('samples', []):
            sample_name = sample['sample_name']
            # If specific samples are specified, skip those that do not match
            if restrict_to_samples and sample_name not in restrict_to_samples:
                LOG.debug("Skipping sample {}: not in specified samples "
                          "{}".format(sample_name, ", ".join(restrict_to_samples)))
                continue
            LOG.info("Setting up sample {}".format(sample_name))
            # Create a directory for the sample if it doesn't already exist
            sample_dir = os.path.join(project_dir, sample_name)
            if create_files: safe_makedir(sample_dir, 0o2770)
            # This will only create a new sample object if it doesn't already exist in the project
            sample_obj = project_obj.add_sample(name=sample_name, dirname=sample_name)
            # Get the Library Prep ID for each file
            pattern = re.compile(".*\.(fastq|fq)(\.gz|\.gzip|\.bz2)?$")
            fastq_files = filter(pattern.match, sample.get('files', []))
            # For each fastq file, create the libprep and seqrun objects
            # and add the fastq file to the seqprep object
            # Note again that these objects only get created if they don't yet exist;
            # if they do exist, the existing object is returned
            for fq_file in fastq_files:
                # Try to parse from SampleSheet
                try:
                    if not samplesheet_path: raise ValueError()
                    lane_num = re.match(r'[\w-]+_L\d{2}(\d)_\w+', fq_file).groups()[0]
                    libprep_name = determine_library_prep_from_samplesheet(samplesheet_path,
                                                                           project_original_name,
                                                                           sample_name,
                                                                           lane_num)
                except (IndexError, ValueError) as e:
                    LOG.debug('Unable to determine library prep from sample sheet file '
                              '("{}"); try to determine from Charon'.format(e))
                    try:
                        # Requires Charon access
                        libprep_name = determine_library_prep_from_fcid(project_id, sample_name, fc_full_id)
                        LOG.debug('Found libprep name "{}" in Charon'.format(libprep_name))
                    except ValueError:
                        charon_session = CharonSession()
                        libpreps = charon_session.sample_get_libpreps(project_id, sample_name).get('libpreps')
                        if len(libpreps) == 1:
                            libprep_name = libpreps[0].get('libprepid')
                            LOG.warn('Project "{}" / sample "{}" / seqrun "{}" / fastq "{}" '
                                     'has no libprep information in Charon, but only one '
                                     'library prep is present in Charon ("{}"). Using '
                                     'this as the library prep.'.format(project_name,
                                                                        sample_name,
                                                                        fc_full_id,
                                                                        fq_file,
                                                                        libprep_name))
                        elif fallback_libprep:
                            libprep_name = fallback_libprep
                            LOG.warn('Project "{}" / sample "{}" / seqrun "{}" / fastq "{}" '
                                     'has no libprep information in Charon, but a fallback '
                                     'libprep value of "{}" was supplied -- using this '
                                     'value.'.format(project_name,
                                                     sample_name,
                                                     fc_full_id,
                                                     fq_file,
                                                     libprep_name,
                                                     fallback_libprep))
                        else:
                            error_text = ('Project "{}" / sample "{}" / seqrun "{}" / fastq "{}" '
                                          'has no libprep information in Charon. Skipping '
                                          'analysis.'.format(project_name, sample_name,
                                                             fc_full_id, fq_file))
                            LOG.error(error_text)
                            if not config.get('quiet'):
                                mail_analysis(project_name=project_name,
                                              sample_name=sample_name,
                                              level="ERROR",
                                              info_text=error_text)
                            continue
                libprep_object = sample_obj.add_libprep(name=libprep_name,
                                                        dirname=libprep_name)
                libprep_dir = os.path.join(sample_dir, libprep_name)
                if create_files: safe_makedir(libprep_dir, 0o2770)
                seqrun_object = libprep_object.add_seqrun(name=fc_full_id,
                                                          dirname=fc_full_id)
                seqrun_dir = os.path.join(libprep_dir, fc_full_id)
                if create_files: safe_makedir(seqrun_dir, 0o2770)
                seqrun_object.add_fastq_files(fq_file)
            if fastq_files and create_files:
                src_sample_dir = os.path.join(fc_dir_structure['fc_dir'],
                                              project['data_dir'],
                                              project['project_dir'],
                                              sample['sample_dir'])
                for libprep_obj in sample_obj:
                    for seqrun_obj in libprep_obj:
                        src_fastq_files = [os.path.join(src_sample_dir, fastq_file) for
                                           fastq_file in seqrun_obj.fastq_files]
                        seqrun_dst_dir = os.path.join(project_obj.base_path, project_obj.dirname,
                                                      sample_obj.dirname, libprep_obj.dirname,
                                                      seqrun_obj.dirname)
                        LOG.info("Symlinking fastq files from {} to {}...".format(src_sample_dir, seqrun_dir))
                        try:
                            do_symlink(src_fastq_files, seqrun_dir)
                        except OSError:
                            error_text = ('Could not symlink files for project/sample'
                                          'libprep/seqrun {}/{}/{}/{}'.format(project_obj,
                                                                              sample_obj,
                                                                              libprep_obj,
                                                                              seqrun_obj))
                            LOG.error(error_text)
                            if not config.get('quiet'):
                                mail_analysis(project_name=project_name,
                                              sample_name=sample_name,
                                              level="ERROR",
                                              info_text=error_text)
                            continue
    return projects_to_analyze


def parse_flowcell(fc_dir):
    """
    Traverse a CASAVA-1.8 or 2.5 generated directory structure for the HiSeq 2500
    and return a dictionary of the elements it contains.

    :param str fc_dir: The directory created by CASAVA for this flowcell.

    :returns: A dict of information about the flowcell, including project/sample info
    :rtype: dict

    :raises OSError: If the fc_dir does not exist or cannot be accessed
    """
    projects = []
    fc_dir = os.path.abspath(fc_dir)
    if not os.access(fc_dir, os.F_OK): os_msg = "does not exist"
    if not os.access(fc_dir, os.R_OK): os_msg = "could not be read (permission denied)"
    if locals().get('os_msg'): raise OSError("Error with flowcell dir {}: directory {}".format(fc_dir, os_msg))
    LOG.info('Parsing flowcell directory "{}"...'.format(fc_dir))
    samplesheet_path = os.path.join(fc_dir, "SampleSheet.csv")
    if not os.path.exists(samplesheet_path):
        LOG.warn("Could not find samplesheet in directory {}".format(fc_dir))
        samplesheet_path = None
    else:
        LOG.debug("SampleSheet.csv found at {}".format(samplesheet_path))
    fc_full_id = os.path.basename(fc_dir)
    c2_5_path = os.path.join(fc_dir, "Demultiplexing")
    c1_8_path = os.path.join(fc_dir, "Unaligned*")
    if os.path.exists(c2_5_path):
        data_dir = c2_5_path
    else:
        data_dir = c1_8_path
    for project_dir in glob.glob(os.path.join(data_dir, "*")):
        path, base_dir = os.path.split(project_dir)
        if not base_dir: path, base_dir = os.path.split(path)
        project_original_name = os.path.basename(base_dir).replace('Project_', '')
        project_name = project_original_name.replace('__', '.')
        if not (os.path.isdir(project_dir) and \
                (UPPSALA_PROJECT_RE.match(project_name) or \
                   STHLM_PROJECT_RE.match(project_name))):
            continue
        LOG.info('Parsing project directory "{}"...'.format(
                            project_dir.split(os.path.split(fc_dir)[0] + "/")[1]))
        project_samples = []
        sample_dir_pattern = os.path.join(project_dir, "*")
        if STHLM_X_PROJECT_RE.match(project_name):
            project_name = project_name.replace('_', '.', 1)
        for sample_dir in glob.glob(sample_dir_pattern):
            LOG.info('Parsing samples directory "{}"...'.format(sample_dir.split(
                                                os.path.split(fc_dir)[0] + "/")[1]))
            sample_name = os.path.basename(sample_dir).replace('Sample_', '')
            fastq_file_pattern = os.path.join(sample_dir, "*.fastq.gz")
            fastq_files = [os.path.basename(fq) for fq in glob.glob(fastq_file_pattern)]
            project_samples.append({'sample_dir': os.path.basename(sample_dir),
                                    'sample_name': sample_name,
                                    'files': fastq_files})
        if not project_samples:
            LOG.warn('No samples found for project "{}" in fc "{}"'.format(project_name, fc_dir))
        else:
            projects.append({'data_dir': os.path.relpath(os.path.dirname(project_dir), fc_dir),
                             'project_dir': os.path.basename(project_dir),
                             'project_name': project_name,
                             'project_original_name': project_original_name,
                             'samples': project_samples})
    if not projects:
        raise ValueError('No projects or no projects with sample found in '
                         'flowcell directory {}'.format(fc_dir))
    else:
        return {'fc_dir'    : fc_dir,
                'fc_full_id': fc_full_id,
                'projects': projects,
                'samplesheet_path': samplesheet_path}
