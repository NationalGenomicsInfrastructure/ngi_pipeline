#!/usr/bin/env python
"""
This module organizes demultiplexed (CASAVA 1.8) sequencing data into the relevant
project/sample/flowcell directory structure.
"""

from __future__ import print_function

import glob
import importlib
import os
import re
import sys

from .classes import NGIProject
from ..log import minimal_logger
from ..utils.filesystem import do_rsync, safe_makedir
from ..utils.config import load_yaml_config
from ..utils.parsers import FlowcellRunMetricsParser

LOG = minimal_logger(__name__)


def process_demultiplexed_flowcells(demux_fcid_dirs, config_file_path=None, restrict_to_projects=None, restrict_to_samples=None):
    """
    The main launcher method.

    :param list demux_fcid_dirs: The CASAVA-produced demux directory/directories.
    :param str config_file_path: The path to the configuration file; can also be
                                 specified via environmental variable "NGI_CONFIG"
    :param list restrict_to_projects: A list of projects; analysis will be
                                      restricted to these. Optional.
    :param list restrict_to_samples: A list of samples; analysis will be
                                     restricted to these. Optional.
    """
    if not config_file_path:
        config_file_path = os.environ.get("NGI_CONFIG") or os.path.expandvars(os.path.join("$HOME/.ngipipeline/ngi_config.yaml"))
        if not os.path.isfile(config_file_path):
            error_msg = ("Configuration file \"{}\" does not exist or is not a "
                         "file. Cannot proceed.".format(config_file_path))
            LOG.error(error_msg)
            raise RuntimeError(error_msg)
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    demux_fcid_dirs_set = set(demux_fcid_dirs)
    projects_to_analyze = []
    config = load_yaml_config(config_file_path)

    # Sort/copy each raw demux FC into project/sample/fcid format -- "analysis-ready"
    projects_to_analyze = dict()
    for demux_fcid_dir in demux_fcid_dirs_set:
        # These will be a bunch of Project objects each containing Samples, FCIDs, lists of fastq files
        projects_to_analyze = setup_analysis_directory_structure(demux_fcid_dir,
                                                                 config,
                                                                 projects_to_analyze,
                                                                 restrict_to_projects,
                                                                 restrict_to_samples)
    if not projects_to_analyze:
        error_message = "No projects found to process."
        LOG.info(error_message)
        sys.exit("Quitting: " + error_message)
    else:
        # Don't need the dict functionality anymore; revert to list
        projects_to_analyze = projects_to_analyze.values()

    analysis_engine_module_name = config.get("analysis", {}).get("analysis_engine")
    if not analysis_engine_module_name:
        error_msg = "No analysis engine specified in configuration file. Exiting."
        LOG.error(error_msg)
        raise RuntimeError(error_msg)
    # Import the module specified in the config file (e.g. bcbio, piper)
    analysis_module = importlib.import_module(analysis_engine_module_name)
    analysis_module.main(projects_to_analyze=projects_to_analyze, config_file_path=config_file_path)


def setup_analysis_directory_structure(fc_dir, config, projects_to_analyze,
                                       restrict_to_projects=None, restrict_to_samples=None):
    """
    Copy and sort files from their CASAVA-demultiplexed flowcell structure
    into their respective project/sample/FCIDs. This collects samples
    split across multiple flowcells.

    :param str fc_dir: The directory created by CASAVA for this flowcell.
    :param dict config: The parsed configuration file.
    :param set projects_to_analyze: A dict (of Project objects, or empty)
    :param list restrict_to_projects: Specific projects within the flowcell to process exclusively
    :param list restrict_to_samples: Specific samples within the flowcell to process exclusively

    :returns: A list of NGIProject objects that need to be run through the analysis pipeline
    :rtype: list

    :raises OSError: If the analysis destination directory does not exist or if there are permissions errors.
    :raises KeyError: If a required configuration key is not available.
    """
    LOG.info("Setting up analysis for demultiplexed data in source folder \"{}\"".format(fc_dir))
    if not restrict_to_projects: restrict_to_projects = []
    if not restrict_to_samples: restrict_to_samples = []
    analysis_top_dir = os.path.abspath(config["analysis"]["top_dir"])
    if not os.path.exists(analysis_top_dir):
        error_msg = "Error: Analysis top directory {} does not exist".format(analysis_top_dir)
        LOG.error(error_msg)
        raise OSError(error_msg)
    if not os.path.exists(fc_dir):
        LOG.error("Error: Flowcell directory {} does not exist".format(fc_dir))
        return []
    # Map the directory structure for this flowcell
    try:
        fc_dir_structure = parse_casava_directory(fc_dir)
    except RuntimeError as e:
        LOG.error("Error when processing flowcell dir \"{}\": {}".format(fc_dir, e))
        return []
    # From RunInfo.xml
    fc_date = fc_dir_structure['fc_date']
    # From RunInfo.xml (name) & runParameters.xml (position)
    fc_name = fc_dir_structure['fc_name']
    fc_run_id = "{}_{}".format(fc_date, fc_name)

    ## This appears to be unneeded, at least for the moment.
    ##  When would these be required?
    ##  Where should they be copied to (not the top analysis directory -- inside the project? Why?)
    # Copy the basecall stats directory.
    # This will be causing an issue when multiple directories are present...
    # syncing should be done from archive, preserving the Unaligned* structures
    #LOG.info("Copying basecall stats for run {}".format(fc_dir))
    #_copy_basecall_stats([os.path.join(fc_dir_structure['fc_dir'], d) for d in
    #                                    fc_dir_structure['basecall_stats_dir']],
    #                                    analysis_top_dir)
    if not fc_dir_structure.get('projects'):
        LOG.warn("No projects found in specified flowcell directory \"{}\"".format(fc_dir))
    # Iterate over the projects in the flowcell directory
    for project in fc_dir_structure.get('projects', []):
        project_name = project['project_name']
        # If specific projects are specified, skip those that do not match
        if restrict_to_projects and project_name not in restrict_to_projects:
            LOG.debug("Skipping project {}".format(project_name))
            continue
        LOG.info("Setting up project {}".format(project.get("project_name")))
        # Create a project directory if it doesn't already exist
        project_dir = os.path.join(analysis_top_dir, project_name)
        if not os.path.exists(project_dir): safe_makedir(project_dir, 0770)
        try:
            project_obj = projects_to_analyze[project_dir]
        except KeyError:
            project_obj = NGIProject(name=project_name, dirname=project_name, base_path=analysis_top_dir)
            projects_to_analyze[project_dir] = project_obj
        # Iterate over the samples in the project
        for sample in project.get('samples', []):
            # If specific samples are specified, skip those that do not match
            sample_name = sample['sample_name'].replace('__','.')
            if restrict_to_samples and sample_name not in restrict_to_samples:
                LOG.debug("Skipping sample {}".format(sample_name))
                continue
            LOG.info("Setting up sample {}".format(sample_name))
            # Create a directory for the sample if it doesn't already exist
            sample_dir = os.path.join(project_dir, sample_name)
            if not os.path.exists(sample_dir): safe_makedir(sample_dir, 0770)
            # This will only create a new sample object if it doesn't already exist in the project
            sample_obj = project_obj.add_sample(name=sample_name, dirname=sample_name)
            # Create a directory for the flowcell if it does not exist
            dst_sample_fcid_dir = os.path.join(sample_dir, fc_run_id)
            if not os.path.exists(dst_sample_fcid_dir): safe_makedir(dst_sample_fcid_dir, 0770)
            # This will only create a new FCID object if it doesn't already exist in the sample
            fcid_obj = sample_obj.add_fcid(name=fc_run_id, dirname=fc_run_id)
            # rsync the source files to the sample directory
            #    src: flowcell/data/project/sample
            #    dst: project/sample/flowcell_run
            src_sample_dir = os.path.join(fc_dir_structure['fc_dir'],
                                          project['data_dir'],
                                          project['project_dir'],
                                          sample['sample_dir'])
            LOG.info("Copying sample files from \"{}\" to \"{}\"...".format(
                                            src_sample_dir, dst_sample_fcid_dir))
            sample_files = do_rsync([os.path.join(src_sample_dir, f) for f in
                                     sample.get('files', [])], dst_sample_fcid_dir)
            # Just want fastq files here
            pattern = re.compile(".*\.(fastq|fq)(\.gz|\.gzip|\.bz2)?$")
            fastq_files = filter(pattern.match, sample.get('files', []))
            fcid_obj.add_fastq_files(fastq_files)
    return projects_to_analyze


def parse_casava_directory(fc_dir):
    """
    Traverse a CASAVA-1.8-generated directory structure and return a dictionary
    of the elements it contains.
    The flowcell directory tree has (roughly) the structure:

    |-- Data
    |   |-- Intensities
    |       |-- BaseCalls
    |-- InterOp
    |-- Unaligned
    |   |-- Basecall_Stats_C2PUYACXX
    |-- Unaligned_16bp
        |-- Basecall_Stats_C2PUYACXX
        |   |-- css
        |   |-- Matrix
        |   |-- Phasing
        |   |-- Plots
        |   |-- SignalMeans
        |   |-- Temp
        |-- Project_J__Bjorkegren_13_02
        |   |-- Sample_P680_356F_dual56
        |   |   |-- <fastq files are here>
        |   |   |-- <SampleSheet.csv is here>
        |   |-- Sample_P680_360F_dual60
        |   |   ...
        |-- Undetermined_indices
            |-- Sample_lane1
            |   ...
            |-- Sample_lane8

    :param str fc_dir: The directory created by CASAVA for this flowcell.

    :returns: A dict of information about the flowcell, including project/sample info
    :rtype: dict

    :raises RuntimeError: If the fc_dir does not exist or cannot be accessed,
                          or if Flowcell RunMetrics could not be parsed properly.
    """
    projects = []
    fc_dir = os.path.abspath(fc_dir)
    LOG.info("Parsing flowcell directory \"{}\"...".format(fc_dir))
    parser = FlowcellRunMetricsParser(fc_dir)
    run_info = parser.parseRunInfo()
    runparams = parser.parseRunParameters()
    try:
        fc_name = run_info['Flowcell']
        fc_date = run_info['Date']
        fc_pos = runparams['FCPosition']
    except KeyError as e:
        raise RuntimeError("Could not parse flowcell information {} "
                           "from Flowcell RunMetrics in flowcell {}".format(e, fc_dir))
    # "Unaligned*" because SciLifeLab dirs are called "Unaligned_Xbp"
    # (where "X" is the index length) and there is also an "Unaligned" folder
    unaligned_dir_pattern = os.path.join(fc_dir,"Unaligned*")
    basecall_stats_dir_pattern = os.path.join(unaligned_dir_pattern,"Basecall_Stats_*")
    basecall_stats_dir = [os.path.relpath(d,fc_dir) for d in glob.glob(basecall_stats_dir_pattern)]
    # e.g. 131030_SN7001362_0103_BC2PUYACXX/Unaligned_16bp/Project_J__Bjorkegren_13_02/
    project_dir_pattern = os.path.join(unaligned_dir_pattern,"Project_*")
    for project_dir in glob.glob(project_dir_pattern):
        LOG.info("Parsing project directory \"{}\"...".format(project_dir.split(os.path.split(fc_dir)[0] + "/")[1]))
        project_samples = []
        sample_dir_pattern = os.path.join(project_dir,"Sample_*")
        # e.g. <Project_dir>/Sample_P680_356F_dual56/
        for sample_dir in glob.glob(sample_dir_pattern):
            LOG.info("Parsing samples directory \"{}\"...".format(sample_dir.split(os.path.split(fc_dir)[0] + "/")[1]))
            fastq_file_pattern = os.path.join(sample_dir,"*.fastq.gz")
            samplesheet_pattern = os.path.join(sample_dir,"*.csv")
            fastq_files = [os.path.basename(file) for file in glob.glob(fastq_file_pattern)]
            ## NOTE that we don't wind up using this SampleSheet for anything so far as I know
            ## TODO consider removing; however, some analysis engines that
            ##      we want to include in the future may need them.
            #samplesheet = glob.glob(samplesheet_pattern)
            #assert len(samplesheet) == 1, \
            #        "Error: could not unambiguously locate samplesheet in {}".format(sample_dir)
            sample_name = os.path.basename(sample_dir).replace("Sample_","").replace('__','.')
            project_samples.append({'sample_dir': os.path.basename(sample_dir),
                                    'sample_name': sample_name,
                                    'files': fastq_files,
            #                        'samplesheet': os.path.basename(samplesheet[0])})
                                   })
        project_name = os.path.basename(project_dir).replace("Project_","").replace('__','.')
        projects.append({'data_dir': os.path.relpath(os.path.dirname(project_dir),fc_dir),
                         'project_dir': os.path.basename(project_dir),
                         'project_name': project_name,
                         'samples': project_samples})
    return {'fc_dir': fc_dir,
            'fc_name': '{}{}'.format(fc_pos, fc_name),
            'fc_date': fc_date,
            'basecall_stats_dir': basecall_stats_dir,
            'projects': projects}


def _copy_basecall_stats(source_dirs, destination_dir):
    """Copy relevant files from the Basecall_Stats_FCID directory
       to the analysis directory
    """
    for source_dir in source_dirs:
        # First create the directory in the destination
        dirname = os.path.join(destination_dir,os.path.basename(source_dir))
        safe_makedir(dirname)
        # List the files/directories to copy
        files = glob.glob(os.path.join(source_dir,"*.htm"))
        files += glob.glob(os.path.join(source_dir,"*.metrics"))
        files += glob.glob(os.path.join(source_dir,"*.xml"))
        files += glob.glob(os.path.join(source_dir,"*.xsl"))
        for dir in ["Plots","css"]:
            d = os.path.join(source_dir,dir)
            if os.path.exists(d):
                files += [d]
        do_rsync(files,dirname)


# This isn't used at the moment
def copy_undetermined_index_files(casava_data_dir, destination_dir):
    """
    Copy fastq files with "Undetermined" index reads to the destination directory.
    :param str casava_data_dir: The Unaligned directory (e.g. "<FCID>/Unaligned_16bp")
    :param str destination_dir: Eponymous
    """
    # List of files to copy
    copy_list = []
    # List the directories containing the fastq files
    fastq_dir_pattern = os.path.join(casava_data_dir,"Undetermined_indices","Sample_lane*")
    # Pattern matching the fastq_files
    fastq_file_pattern = "*.fastq.gz"
    # Samplesheet name
    samplesheet_pattern = "SampleSheet.csv"
    samplesheets = []
    for dir in glob.glob(fastq_dir_pattern):
        copy_list += glob.glob(os.path.join(dir,fastq_file_pattern))
        samplesheet = os.path.join(dir,samplesheet_pattern)
        if os.path.exists(samplesheet):
            samplesheets.append(samplesheet)
    # Merge the samplesheets into one
    new_samplesheet = os.path.join(destination_dir,samplesheet_pattern)
    new_samplesheet = _merge_samplesheets(samplesheets,new_samplesheet)
    # Rsync the fastq files to the destination directory
    do_rsync(copy_list,destination_dir)

# Also not used at the moment
def _merge_samplesheets(samplesheets, merged_samplesheet):
    """
    Merge multiple Illumina SampleSheet.csv files into one.
    :param list samplesheets: A list of the paths to the SampleSheet.csv files to merge.
    :param str merge_samplesheet: The path <...>
    :returns: <...>
    :rtype: str
    """
    data = []
    header = []
    for samplesheet in samplesheets:
        with open(samplesheet) as fh:
            csvread = csv.DictReader(fh, dialect='excel')
            header = csvread.fieldnames
            for row in csvread:
                data.append(row)
    with open(merged_samplesheet, "w") as outh:
        csvwrite = csv.DictWriter(outh, header)
        csvwrite.writeheader()
        csvwrite.writerows(sorted(data, key=lambda d: (d['Lane'],d['Index'])))
    return merged_samplesheet