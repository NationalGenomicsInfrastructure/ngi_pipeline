#!/usr/bin/env python

from __future__ import print_function

import importlib
import os

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.database.communicate import get_workflow_for_project
from ngi_pipeline.database.process_tracking import check_if_sample_analysis_is_running, \
                                                   is_flowcell_analysis_running, \
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
                         "in project {}. Skipping.".format(analysis_module,
                                                            workflow,
                                                           project))
            LOG.error(error_msg)
            # Next project
            continue

        for sample in project:
            for libprep in sample:
                for fcid in libprep:
                    # Check Charon to ensure this hasn't already been processed
                    status = CharonSession().seqrun_get(project.project_id, sample, libprep, fcid).get('alignment_status')
                    
                    if status and status not in ("NEW", "FAILED", "DONE"):
                        # If status is not NEW or FAILED (which means it is RUNNING or DONE), skip processing
                        if is_flowcell_analysis_running(project, sample, libprep, fcid, config):
                            continue
                    if status and status is "RUNNING":
                            if not is_flowcell_analysis_running(project, sample, libprep, fcid, config):
                                error_msg = ("Charon and local db incongruency:  Project {}, Sample {}, Library {}, flowcell {} "
                                        "Charon reports it as running but not trace of it in local DB ".format(project, sample, libprep, fcid))
                                LOG.error(error_msg)
                            continue
                    # Check the local jobs database to determine if this flowcell is already being analyzed
                    if not is_flowcell_analysis_running(project, sample, libprep, fcid, config):
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
                                                                            sample= sample,
                                                                            libprep = libprep,
                                                                            fcid = fcid,
                                                                            workflow_name=workflow_name,
                                                                            config_file_path=config_file_path)

                            record_process_flowcell(p_handle, workflow_name, project, sample, libprep, fcid, analysis_module, project.analysis_dir, config)
                        # TODO which exceptions can we expect to be raised here?
                        except Exception as e:
                            error_msg = ('Cannot process project "{}": {}'.format(project, e))
                            LOG.error(error_msg)
                            continue


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
    #start by getting all projects, this will likely need a specific API
    charon_session = CharonSession()
    url = charon_session.construct_charon_url("projects")
    projects_response = charon_session.get(url)
    if projects_response.status_code != 200:
        error_msg = ('Error accessing database: could not get all projects: {}'.format(project_response.reason))
        LOG.error(error_msg)
        raise RuntimeError(error_msg)

    projects_dict = projects_response.json()["projects"]

    for project in projects_dict:
        #check if the field Pipeline is set
        project_id = project["projectid"]

        try:
            workflow = get_workflow_for_project(project_id)
        except (RuntimeError) as e:
            error_msg = ("Skipping project {} because of error: {}".format(project_id, e))
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
                         "in project {}. Skipping.".format(analysis_module,
                                                            workflow,
                                                            project_id))
            LOG.error(error_msg)
            continue


        #I know which engine I need to use to process sample ready, however only the engine
        #knows that are the conditions that need to be made
        LOG.info('Checking for ready to be analysed samples in project {} with workflow {}'.format(project_id, workflow))
        #get all the samples from Charon
        url = charon_session.construct_charon_url("samples", project_id)
        samples_response = charon_session.get(url)
        if samples_response.status_code != 200:
            error_msg = ('Error accessing database: could not get samples for projects: {}'.format(project_id,
                            project_response.reason))
            LOG.error(error_msg)
            raise RuntimeError(error_msg)
        samples_dict = samples_response.json()["samples"]
        #now recreacte the project object
        analysis_top_dir = os.path.abspath(config["analysis"]["top_dir"])
        proj_dir = os.path.join(analysis_top_dir, "DATA", project["name"])
        projectObj = createIGNproject(analysis_top_dir, project["name"],  project_id)

        analysis_dir = os.path.join(analysis_top_dir, "ANALYSIS", project["name"] )
        #import pdb
        #pdb.set_trace()

        for sample in samples_dict: #sample_dict is a charon object
            sample_id = sample["sampleid"]
            #check that it is not already running
            analysis_running = check_if_sample_analysis_is_running(projectObj, projectObj.samples[sample_id], config)
            #check that this analysis is not already done
            if "status" in sample and sample["status"] == "DONE":
                analysis_done = True
            else:
                analysis_done = False
            
            if "status" in sample and sample["status"] == "RUNNING" and not analysis_running:
                error_msg = ("Charon and local db incongruency (sample process level):  Project {}, Sample {}  "
                                        "Charon reports it as running but not trace of it in local DB ".format(project_id, sample_id))
                LOG.error(error_msg)
                continue
            if not analysis_running and not analysis_done: #I need to avoid start process if things are done
                try:
                    # note here I do not know if I am going to start some anlaysis or not, depends on the Engine that is called
                    #I am here even with project that have no analysis ... maybe better to define a flag?
                    p_handle = analysis_module.analyse_sample_run(sample = sample , project = projectObj,
                                                              config_file_path=config_file_path )
                    #p_handle is None when the engine decided that there is nothing to be done
                    if p_handle != 1:
                        record_process_sample(p_handle, workflow, projectObj, sample_id, analysis_module,
                            analysis_dir, config)
                except Exception as e:
                    error_msg = ('Cannot process sample {} in project {}: {}'.format(sample_id, project_id, e))
                    LOG.error(error_msg)
                    continue
            elif analysis_done:
                LOG.info("Project {}, Sample {}  "
                     "have been succesfully processed.".format(project_id, sample_id))



def createIGNproject(analysis_top_dir, project_name, project_id):
    project_dir = os.path.join(analysis_top_dir, "DATA", project_name)
    project_obj = NGIProject(name=project_name, dirname=project_name,
            project_id=project_id,
            base_path=analysis_top_dir)
    #I use the DB to build the object
    #get the samples
    charon_session = CharonSession()
    url = charon_session.construct_charon_url("samples", project_id)
    samples_response = charon_session.get(url)
    if samples_response.status_code != 200:
        error_msg = ('Error accessing database: could not get samples for projects: {}'.format(project_id,
            project_response.reason))
        LOG.error(error_msg)
        raise RuntimeError(error_msg)
    #now I have all the samples
    samples_dict = samples_response.json()["samples"]
    for sample in samples_dict:
        sample_id = sample["sampleid"]
        sample_dir = os.path.join(project_dir, sample_id)
        sample_obj = project_obj.add_sample(name=sample_id, dirname=sample_id)
        #now get lib preps
        url = charon_session.construct_charon_url("libpreps", project_id, sample_id)
        libpreps_response = charon_session.get(url)
        if libpreps_response.status_code != 200:
            error_msg = ('Error accessing database: could not get lib preps for sample {}: {}'.format(sample_id,
                project_response.reason))
            LOG.error(error_msg)
            raise RuntimeError(error_msg)
        libpreps_dict = libpreps_response.json()["libpreps"]
        for libprep in libpreps_dict:
            libprep_id = libprep["libprepid"]
            libprep_object = sample_obj.add_libprep(name=libprep_id,  dirname=libprep_id)
            url = charon_session.construct_charon_url("seqruns", project_id, sample_id, libprep_id)
            seqruns_response = charon_session.get(url)
            if seqruns_response.status_code != 200:
                error_msg = ('Error accessing database: could not get lib preps for sample {}: {}'.format(sample_id,
                    seqruns_response.reason))
                LOG.error(error_msg)
                raise RuntimeError(error_msg)
            seqruns_dict = seqruns_response.json()["seqruns"]
            for seqrun in seqruns_dict:
                runid = seqrun["seqrunid"]
                #looks like 140528_D00415_0049_BC423WACXX
                
