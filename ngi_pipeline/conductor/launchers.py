#!/usr/bin/env python

from __future__ import print_function

import importlib
import os

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.database.communicate import get_workflow_for_project
from ngi_pipeline.database.process_tracking import is_flowcell_analysis_running, \
                                                   is_sample_analysis_running, \
                                                   record_process_flowcell, \
                                                   record_process_sample
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config


LOG = minimal_logger(__name__)


# This flowcell-level analysis function is called automatically after newly-delivered flowcells are reorganized
# into projects. It runs only at the highest "flowcell" or "sequencing run" level, e.g. individual fastq files
# with none of their relationships considered (i.e. two fastq files from the same sample are analyzed independently).
@with_ngi_config
def launch_analysis_for_flowcells(projects_to_analyze, config=None, config_file_path=None):
    """Launch the appropriate flowcell-level analysis for each fastq file in the project.

    :param list projects_to_analyze: The list of projects (Project objects) to analyze
    :param dict config: The parsed NGI configuration file; optional/has default.
    :param str config_file_path: The path to the NGI configuration file; optional/has default.
    """
    

    for project in projects_to_analyze:
        # Get information from Charon regarding which workflows to run
        try:
            workflow = get_workflow_for_project(project.project_id)
        except CharonError as e:
            error_msg = ("Skipping project {} because of error: {}".format(project, e))
            LOG.error(error_msg)
            continue
        try:
            analysis_engine_module_name = config["analysis"]["workflows"][workflow]["analysis_engine"]
        except KeyError:
            error_msg = ("No analysis engine for workflow \"{}\" specified "
                         "in configuration file. Skipping this workflow "
                         "for project {}".format(workflow, project))
            LOG.error(error_msg)
            raise RuntimeError(error_msg)
        # Import the adapter module specified in the config file (e.g. piper_ngi)
        try:
            analysis_module = importlib.import_module(analysis_engine_module_name)
        except ImportError as e:
            error_msg = ("Couldn't import module {} for workflow {} "
                         "in project {}: \"{}\". Skipping.".format(analysis_engine_module_name,
                                                                   workflow, project, e))
            LOG.error(error_msg)
            # Next project
            continue

        for sample in project:
            for libprep in sample:
                for seqrun in libprep:
                    # Check Charon to ensure this hasn't already been processed
                    status = CharonSession().seqrun_get(project.project_id, sample, libprep, seqrun).get('alignment_status')
                    if status and status in ("RUNNING", "DONE"):
                        # If status is  RUNNING or DONE skip processing but check that logic is respected (spok roks!!!)
                        if status is "RUNNING":
                            if not is_flowcell_analysis_running(project, sample, libprep, seqrun, config):
                                error_msg = ('Charon and local db incongruency: project "{}" / sample "{}" / library "{}" / flowcell "{}": '
                                             'Charon reports it as running but not trace of it in local DB'.format(project, sample, libprep, seqrun))
                                LOG.error(error_msg)
                        else: #otherwise I am DONE
                            if is_flowcell_analysis_running(project, sample, libprep, seqrun, config):
                                error_msg = ('Charon and local db incongruency:  Project "{}", Sample "{}", Library "{}", flowcell "{}": '
                                             'Charon reports it is DONE but local db says it is RUNNING'.format(project, sample, libprep, seqrun))
                                LOG.error(error_msg)

                        continue
                    # if i am here the status on charon is either None, NEW, or FAILED
                    # Check the local jobs database to determine if this flowcell is already being analyzed
                    if status and status == "FAILED":
                        error_msg = ('FAILED:  Project "{}" / sample "{}" / library "{}" / flowcell "{}": '
                                     'Charon reports FAILURE, manual investigation needed!'.format(project, sample, libprep, seqrun))
                        ## TODO send an email or something -- this should be a Charon-related process, not done here
                        LOG.error(error_msg)
                        ### FIXME these next two line are a temp fix because Nestor is misbehaving and we just want to retry the analysis
                        ##        note that here it would be nice to differentiate between DATA_FAILURE and COMPUTE_FAILURE!!
                        LOG.warn("Continuing with project anyway (remove me later)")
                        #continue
                    # at this point the status is only None or NEW, I need only to check that the analysis is already running (which would be strange)
                    if not is_flowcell_analysis_running(project, sample, libprep, seqrun, config):
                        try:
                            # This workflow thing will be handled on the engine side. Here we'll just call like "piper_ngi.flowcell_level_analysis"
                            # or something and it will handle which workflows to execute (qc, alignment, ...)
                            workflow_name = "dna_alignonly"  #must be taken from somewhere, either config file or Charon

                            # Here we are not specifying any kind of output directory as I believe this will be pulled from
                            # the config file; however, we may have to adapt this as we add more engines.

                            ## TODO I think we need to detach these sessions or something as they will die
                            ##      when the main Python thread dies; however, this means ctrl-c will not kill them.
                            ##      This is probably alright as this will generally be run automatically.
                            p_handle = analysis_module.analyze_flowcell_run(project=project,
                                                                            sample=sample,
                                                                            libprep=libprep,
                                                                            seqrun=seqrun,
                                                                            workflow_name=workflow_name)

                            ## NOTE what happens when the process fails? we still get a Popen object I think?
                            if p_handle:
                                record_process_flowcell(p_handle, workflow_name, project, sample, libprep, seqrun, analysis_module, project.analysis_dir, config)
                        # TODO which exceptions can we expect to be raised here?
                        except Exception as e:
                            error_msg = ('Cannot process project "{}": {}'.format(project, e))
                            LOG.error(error_msg)
                            continue
                    else:
                        error_msg = ('Charon and local db incongruency:  Project "{}", Sample "{}", Library "{}", flowcell "{}": '
                                     'Charon reports it is "{}" but local db says it is RUNNING'.format(project, sample, libprep, seqrun, status))
                        LOG.error(error_msg)
                        continue


## FIRST PRIORITY
## MARIO FIXME adjust charon access etc.
## NOTE
## This function is responsable of trigger second level analyisis (i.e., sample level analysis)
## using the information available on the Charon.
## TOO MANY CALLS TO CHARON ARE MADE HERE: we need to restrict them
@with_ngi_config
def trigger_sample_level_analysis(config=None, config_file_path=None):
    """Triggers secondary analysis based on what is found on Charon
    for now this will work only with Piper/IGN

    :param dict config: The parsed NGI configuration file; optional.
    :param list config_file_path: The path to the NGI configuration file; optional.
    """
    LOG.info("Starting sample-level analysis routine.")
    #start by getting all projects, this will likely need a specific API
    
    charon_session = CharonSession()
    try:
        projects_dict = charon_session.projects_get_all()['projects']
    except (CharonError, KeyError) as e:
        raise RuntimeError("Unable to get list of projects from Charon and cannot continue: {}".format(e))
    for project in projects_dict:
        if project.get("status") in ("CLOSED", "ABORTED"):
            LOG.info("Skipping project {}: marked as {}".format(project, project.get("status")))
        project_id = project.get("projectid")
        try:
            workflow = get_workflow_for_project(project_id)
        except (RuntimeError) as e:
            error_msg = ("Skipping project {} because of error: {}".format(project_id, e))
            LOG.error(error_msg)
            # Next project
            continue
        try:
            analysis_engine_module_name = config["analysis"]["workflows"][workflow]["analysis_engine"]
        except KeyError:
            error_msg = ("No analysis engine for workflow \"{}\" specified "
                         "in configuration file. Skipping this workflow "
                         "for project {}".format(workflow, project))
            #LOG.error(error_msg)
            raise RuntimeError(error_msg)
        # Import the adapter module specified in the config file (e.g. piper_ngi)
        try:
            analysis_module = importlib.import_module(analysis_engine_module_name)
        except ImportError as e:
            error_msg = ("Couldn't import module {} for workflow {} "
                         "in project {}. Skipping.".format(analysis_module,
                                                            workflow,
                                                            project_id))
            LOG.error(error_msg)
            continue
        analysis_top_dir = os.path.abspath(config["analysis"]["top_dir"])
        proj_dir = os.path.join(analysis_top_dir, "DATA", project["name"])
        try:
            # Now recreate the project object
            ## NOTE this actually adds a "status" attribute to each object it recreates using info from Charon.
            ##      It's useful in this specific case but feels a little funky.
            ##      May rework this when restructuring the code later.
            project_obj = recreate_project_from_db(analysis_top_dir, project["name"],  project_id)
        except RuntimeError as e:
            LOG.error("Skipping project {}: could not recreate object from Charon: {}".format(project_id, e))
            continue
        analysis_dir = os.path.join(analysis_top_dir, "ANALYSIS", project["name"] )


        #I know which engine I need to use to process sample ready, however only the engine
        #knows that are the conditions that need to be made
        LOG.info('Finding samples read to be analyzed in project {} / workflow {}'.format(project_id, workflow))
        try:
            samples_dict = charon_session.project_get_samples(project_id)["samples"]
        except CharonError as e:
            raise RuntimeError("Could not access samples for project {}: {}".format(project_id, e))


        for sample_obj in project_obj: #sample_dict is a charon object
            # This status comes from the Charon database
            if sample_obj.status in ("DONE", "COMPUTATION_FAILED", "DATA_FAILED", "IGNORE"):
                LOG.info('Sample "{}" in project "{}" will not be processed at this time: status is "{}".'.format(sample_obj, project_obj, sample_obj.status))
            # Checks the local job-tracking database to determine if this sample analysis is currently ongoing
            elif not is_sample_analysis_running(project_obj, sample_obj, config):
                # Analysis not marked as "DONE" in Charon and also not yet running locally -- needs to be launched
                ## FIXME Mario this needs to be engine-agnostic
                try:
                    p_handle = analysis_module.analyze_sample_run(project=project_obj,
                                                                  sample=sample_obj)
                except RuntimeError as e:
                    # Some problem launching the job
                    error_msg = ('Cannot process sample "{}" in project "{}": {}'.format(sample_obj, project_obj, e))
                    LOG.error(error_msg)
                    continue
                if p_handle:    # p_handle is None when the engine decided that there is nothing to be done
                    record_process_sample(p_handle, workflow, project_obj, sample_obj,
                                          analysis_module, analysis_dir, config)


def recreate_project_from_db(analysis_top_dir, project_name, project_id):
    project_dir = os.path.join(analysis_top_dir, "DATA", project_name)
    project_obj = NGIProject(name=project_name,
                             dirname=project_name,
                             project_id=project_id,
                             base_path=analysis_top_dir)
    charon_session = CharonSession()
    try:
        samples_dict = charon_session.project_get_samples(project_id)["samples"]
    except CharonError as e:
        raise RuntimeError("Could not access samples for project {}: {}".format(project_id, e))
    for sample in samples_dict:
        sample_id = sample.get("sampleid")
        sample_dir = os.path.join(project_dir, sample_id)
        sample_obj = project_obj.add_sample(name=sample_id, dirname=sample_id)
        sample_obj.status = sample.get("status", "unknown")
        try:
            libpreps_dict = charon_session.sample_get_libpreps(project_id, sample_id)["libpreps"]
        except CharonError as e:
            raise RuntimeError("Could not access libpreps for project {} / sample {}: {}".format(project_id,sample_id, e))
        for libprep in libpreps_dict:
            libprep_id = libprep.get("libprepid")
            libprep_obj = sample_obj.add_libprep(name=libprep_id,  dirname=libprep_id)
            libprep_obj.status = libprep.get("status", "unknown")
            try:
                seqruns_dict = charon_session.libprep_get_seqruns(project_id, sample_id, libprep_id)["seqruns"]
            except CharonError as e:
                raise RuntimeError("Could not access seqruns for project {} / sample {} / "
                                   "libprep {}: {}".format(project_id, sample_id, libprep_id, e))
            for seqrun in seqruns_dict:
                # e.g. 140528_D00415_0049_BC423WACXX
                seqrun_id = seqrun.get("seqrunid")
                seqrun_obj = libprep_obj.add_seqrun(name=seqrun_id, dirname=seqrun_id)
                seqrun_obj.status = seqrun.get("status", "unknown")
    return project_obj
