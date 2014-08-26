"""Keeps track of running workflow processes"""
import contextlib
import glob
import json
import os
import re
import shelve

from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.database.classes import CharonSession
from ngi_pipeline.database.communicate import get_project_id_from_name
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.parsers import parse_qualimap_results

LOG = minimal_logger(__name__)


def get_all_tracked_processes(config=None):
    """Returns all the processes that are being tracked locally,
    which is to say all the processes that have a record in our local
    process_tracking database.

    :param dict config: The parsed configuration file (optional)

    :returns: The dict of the entire database
    :rtype: dict
    """
    with get_shelve_database(config) as db:
        db_dict = {job_name: job_obj for job_name, job_obj in db.iteritems()}
    return db_dict


# This can be run intermittently to track the status of jobs and update the database accordingly,
# as well as to remove entries from the local database if the job has completed (but ONLY ONLY ONLY
# once the status has been successfully written to Charon!!)
@with_ngi_config
def check_update_jobs_status(projects_to_check=None, config=None, config_file_path=None):
    """Check and update the status of jobs associated with workflows/projects;
    this goes through every record kept locally, and if the job has completed
    (either successfully or not) AND it is able to update Charon to reflect this
    status, it deletes the local record.

    :param list projects_to_check: A list of project names to check (exclusive, optional)
    :param dict config: The parsed NGI configuration file; optional.
    :param list config_file_path: The path to the NGI configuration file; optional.
    """
    db_dict = get_all_tracked_processes()
    for job_name, project_dict in db_dict.iteritems():
        LOG.info("Checking workflow {} for project {}...".format(project_dict["workflow"],
                                                                 job_name))
        return_code = project_dict["p_handle"].poll()
        if return_code is not None:
            # Job finished somehow or another; try to update database.
            LOG.info('Workflow "{}" for project "{}" completed '
                     'with return code "{}". Attempting to update '
                     'Charon database.'.format(project_dict['workflow'],
                                               job_name, return_code))
            # Only if we succesfully write to Charon will we remove the record
            # from the local db; otherwise, leave it and try again next cycle.
            try:
                project_id = project_dict['project_id']
                ## MARIO FIXME
                ### THIS IS NOT REALLY CORRECT, HERE TRIGGER KNOWS DETAILS ABOUT ENGINE!!!!
                if project_dict["workflow"] == "dna_alignonly":
                    #in this case I need to update the run level infomration
                    #I know that:
                    # I am running Piper at flowcell level, I need to know the folder where results are stored!!!
                    write_to_charon_alignment_results(job_name, return_code)
                elif project_dict["workflow"] == "NGI":
                    write_to_charon_NGI_results(job_name, return_code, project_dict["run_dir"])
                else:
                    write_status_to_charon(project_id, return_code)
                LOG.info("Successfully updated Charon database.")
                try:
                    # This only hits if we succesfully update Charon
                    remove_record_from_local_tracking(job_name)
                except RuntimeError:
                    LOG.error(e)
                    continue
            except RuntimeError as e:
                LOG.warn(e)
                continue
        else:
            ## FIXME
            #this code duplication can be avoided
            if project_dict["workflow"] == "dna_alignonly":
                #in this case I need to update the run level infomration
                write_to_charon_alignment_results(job_name, return_code)
            elif project_dict["workflow"] == "NGI":
                    write_to_charon_NGI_results(job_name, return_code, project_dict["run_dir"])

            LOG.info('Workflow "{}" for project "{}" (pid {}) '
                     'still running.'.format(project_dict['workflow'],
                                             job_name,
                                             project_dict['p_handle'].pid))


def remove_record_from_local_tracking(project, config=None):
    """Remove a record from the local tracking database.

    :param NGIProject project: The NGIProject object
    :param dict config: The parsed configuration file (optional)

    :raises RuntimeError: If the record could not be deleted
    """
    LOG.info('Attempting to remove local process record for '
             'project "{}"'.format(project))
    with get_shelve_database(config) as db:
        try:
            del db[project]
        except KeyError:
            error_msg = ('Project "{}" not found in local process '
                         'tracking database.'.format(project))
            LOG.error(error_msg)
            raise RuntimeError(error_msg)


def is_flowcell_analysis_running(project, sample, libprep, seqrun, config=None):
    """Determine if a flowcell is currently being analyzed."""
    LOG.info("Checking if sequencing run {}/{}/{}/{} is currently "
             "being analyzed.".format(project.project_id, sample, libprep, seqrun))
    return is_analysis_running(project, sample, libprep, seqrun, level="flowcell")

## MARIO FIXME
# This function should be used along with a Charon database check for sample status
# to ensure that there aren't flowcell analyses running
def is_sample_analysis_running(project, sample, config=None):
    """Determine if a sample is currently being analyzed."""
    raise NotImplementedError("Use the check_if_sample_analysis_is_running function for the moment.")
    LOG.info("Checking if sample {}/{} is currently "
             "being analyzed.".format(project.project_id, sample))
    return is_analysis_running(project, sample, level="sample")


## TODO this will be reworked once we change the way we access the database
def is_analysis_running(project, sample, libprep=None, seqrun=None, config=None, level=None):
    ## Something like this
    #database.check_local_jobs(project, sample, libprep, seqrun, table=level)
    ## For now:
    db_key = "{}_{}".format(project.project_id, sample)
    if libprep and seqrun:
        db_key = "{}_{}_{}".format(db_key, libprep, seqrun)
    with get_shelve_database(config) as db:
        return db_key in db


## MARIO FIXME
def check_if_sample_analysis_is_running( project, sample, config=None):
    """checks if a given project/sample is currently analysed. 
    Also check the no project working on that samples (maybe at flowcell level are running)

    :param Project project: the project object
    :param Sample sample:
    :param dict config: The parsed configuration file (optional)

    :raises RuntimeError: If the configuration file cannot be found

    :returns true/false
    """
    LOG.info("checking if Project {}, Sample {}   "
             "are currently being analysed ".format(project.name, sample))
    with get_shelve_database(config) as db:
        #get all keys, i.e., get all running projects
        running_processes = db.keys()
        #check that project_sample are not involved in any running process
        process_to_be_searched = "{}_{}".format(project, sample)
        information_to_extract = re.compile("([a-zA-Z]\.[a-zA-Z]*_\d*_\d*)_(P\d*_\d*)(.*)")

        for running_process in running_processes:
            projectName_of_running_process    = information_to_extract.match(running_process).group(1)
            sampleName_of_running_process     = information_to_extract.match(running_process).group(2)
            libprep_fcid_of_running_process   = information_to_extract.match(running_process).group(3)
            project_sample_of_running_process = "{}_{}".format(projectName_of_running_process,sampleName_of_running_process)
            if project_sample_of_running_process == process_to_be_searched:
                #match found, either this sample level analysis is already ongoing or I am still waiting for a fc level analysis to finish
                if libprep_fcid_of_running_process == "": #in this case I am running sample level analysis
                    warn_msg = ("Project {}, Sample {} "
                         "has an entry in the local db. Sample level analysis are ongoing.".format(project, sample))
                else:
                    warn_msg = ("Project {}, Sample {}  "
                         "has an entry in the local db. Waiting for a flowcell analysis to finish.".format(project, sample))
                LOG.warn(warn_msg)
                return True
        return False


#def check_if_flowcell_analysis_is_running( project, sample, libprep, fcid, config=None):
#    """checks if a given project/sample/library/flowcell is currently analysed.
#
#    :param Project project: the project object
#    :param Sample sample:
#    :param LibPrep libprep:
#    :param string fcid:
#    :param dict config: The parsed configuration file (optional)
#
#    :raises RuntimeError: If the configuration file cannot be found
#
#    :returns true/false
#    """
#    LOG.info("checking if Project {}, Sample {}, Library prep {}, fcid {} "
#             "are currently being analysed ".format(project, sample, libprep, fcid))
#    with get_shelve_database(config) as db:
#        if project.name in db:
#            error_msg = ("Project '{}' is already being analysed at project level "
#                         "This run should not exists!!! somethig terribly wrong is happening, "
#                         "Kill everything  and call Mario and Francesco".format(project))
#            LOG.info(error_msg)
#            raise RuntimeError(error_msg)
#
#        if "{}_{}".format(project.name, sample) in db:
#            error_msg = ("Sample '{}' of project '{}' is already being analysed at sample level "
#                         "This run should not exists!!! somethig terribly wrong is happening, "
#                         "Kill everything  and call Mario and Francesco".format(sample, project))
#            LOG.info(error_msg)
#            raise RuntimeError(error_msg)
#
#        db_key = "{}_{}_{}_{}".format(project, sample, libprep, fcid)
#        if db_key in db:
#            warn_msg = ("Project {}, Sample {}, Library prep {}, fcid {} "
#                         "has an entry in the local db. Analysis are ongoing.".format(project, sample, libprep, fcid))
#            LOG.warn(warn_msg)
#            return True
#        return False


def write_status_to_charon(project_id, return_code):
    """Update the status of a workflow for a project in the Charon database.

    :param NGIProject project_id: The name of the project
    :param int return_code: The return code of the workflow process

    :raises RuntimeError: If the Charon database could not be updated
    """
    ## Consider keeping on CharonSession open. What's the time savings?
    charon_session = CharonSession()
    ## Is "CLOSED" correct here?
    status = "CLOSED" if return_code is 0 else "FAILED"
    try:
        charon_session.project_update(project_id, status=status)
    except CharonError as e:
        error_msg = ('Failed to update project status to "{}" for "{}" '
                     'in Charon database: {}'.format(status, project_id, e))
        raise RuntimeError(error_msg)


## FIXME This needs to be moved to the engine_ngi or else some generic format needs to be created
##       and a dict passed back to this function. Something like that.
def write_to_charon_NGI_results(job_id, return_code, run_dir=None):
    """Update the status of a sequencing run after alignment.

    :param NGIProject project_id: The name of the project, sample, lib prep, flowcell id
    :param int return_code: The return code of the workflow process
    :param string run_dir: the directory where results are stored (I know that I am running piper)

    :raises RuntimeError: If the Charon database could not be updated
    """
    charon_session = CharonSession()
    # Consider moving this mapping to the CharonSession object or something
    if return_code is None:
        status = "RUNNING"
    elif return_code == 0:
        status = "DONE"
    else:
        status = "FAILED"
    try:
        ## Could move to ngi_pipeline.utils.parsers if it gets used a lot
        # e.g. A.Wedell_13_03_P567_102
        ## FIXME won't work for Uppsala project/sample names
        m = re.match(r'(?P<project_name>\w\.\w+_\d+_\d+)_(?P<sample_id>P\d+_\d+)', job_id)
        project_id   = get_project_id_from_name(m.group(1)) ## MARIO FIX THIS BEETER IF YOU WANT get_project_id_from_name(m.groupdict['project_name'])
        sample_id    = m.group(2) #m.groupdict['sample_id']
    except (TypeError, AttributeError):
        error_msg = "Could not parse project/sample ids from job id \"{}\"; cannot update Charon with results!".format(job_id)
        raise RuntimeError(error_msg)
    try:
        charon_session.sample_update(project_id, sample_id, status=status)
    except CharonError as e:
        error_msg = ('Failed to update project status to "{}" for "{}" sample {}'
                     'in Charon database: {}'.format(status, project_id, sample_id, e))
        raise RuntimeError(error_msg)


## MARIO FIXME Move to piper_ngi, see note above
def write_to_charon_alignment_results(job_id, return_code):
    """Update the status of a sequencing run after alignment.

    :param NGIProject project_id: The name of the project, sample, lib prep, flowcell id
    :param int return_code: The return code of the workflow process
    :param string run_dir: the directory where results are stored (I know that I am running piper)

    :raises RuntimeError: If the Charon database could not be updated
    """
    import pdb
    pdb.set_trace()
    try:
        # e.g. A.Wedell_13_03_P567_102_A_140528_D00415_0049_BC423WACXX
        m = re.match(('(?P<project_name>\w\.\w+_\d+_\d+)_(?P<sample_id>P\d+_\d+)_'
                      '(?P<libprep_id>\w)_(?P<seqrun_id>\d{6}_.+_\d{4}_.{10})'), job_id).groupdict()
    except AttributeError:
        raise ValueError("Could not parse job_id \"{}\"; does not match template.".format(job_id))
    project_name = m.groupdict["project_name"]
    project_id = get_project_id_from_name(project_name)
    sample_id = m.groupdict["sample_name"]
    libprep_id = m.groupdict["libprep"]
    seqrun_id = m.groupdict["seqrun"]
    piper_run_id = fcid.split("_")[3]

    charon_session = CharonSession()
    try:
        seqrun_dict = charon_session.seqrun_get(project_id, sample_id, library_id, fcid)
    except CharonError as e:
        raise RuntimeError('Error accessing database for project "{}", sample {}; '
                           'could not update Charon while performing best practice: '
                           '{}'.format(project_id, sample_id,  e))


    if return_code is None:     # Alignment is still running
        seqrun_dict["alignment_status"] = "RUNNING"
    elif return_code == 0:      # Alignment finished successfully
        # In this case I need to update the alignment statistics for each lane in this seq run
        if seqrun_dict.get("alignment_status") == "DONE":
            ## TODO Should we in fact overwrite the previous results?
            LOG.warn("Sequencing run \"{}\" marked as DONE but writing new alignment results; "
                     "this will overwrite the previous results.".format(fcid))

        try:
            # Find all the appropriate files
            piper_result_dir = os.path.join(run_dir, "02_preliminary_alignment_qc")
            if not os.path.isdir(piper_result_dir):
                raise ValueError("Piper result directory \"{}\" is missing.".format(piper_result_dir))
            piper_qc_dir_base = "{}.{}.{}".format(sample_id, piper_run_id, sample_id)
            piper_qc_path = "{}*/".format(os.path.join(piper_result_dir, piper_qc_dir_base))
            piper_qc_dirs = glob.glob(piper_qc_path)
            if not piper_qc_dirs: # Something went wrong in the alignment
                raise ValueError("Piper qc directories under \"{}\" are missing.".format(piper_qc_path))

            # Examine each lane and update the dict with its alignment metrics
            for qc_lane in piper_qc_dirs:
                genome_result = os.path.join(qc_lane, "genome_results.txt")
                # This means that if any of the lanes are missing results, the sequencing run is marked as a failure.
                # We should flag this somehow and send an email at some point.
                if not os.path.isfile(genome_result):
                    raise ValueError("File \"genome_results.txt\" is missing from Piper result directory \"{}\"".format(piper_result_dir))
                # Get the alignment results for this lane
                lane_alignment_metrics = parse_qualimap_results(genome_result)
                # Update the dict for this lane
                update_seq_run_for_lane(seqrun_dict, lane_alignment_metrics)
            seqrun_dict["alignment_status"] = "DONE"

        # This is hit if there is any problem parsing all the metrics in the try above
        except ValueError as e:
            error_msg = ("Could not parse alignment results when processing {}/{}/{}/{}: "
                         "{}".format(project_id, sample_id, library_id, seqrun_id, e))
            LOG.error(error_msg)
            ## Processing failed, unsure if alignment actually failed? Should we be resetting the seqrun in Charon?
            ## Consider just falling back to the final else bit below
            #charon_session.seqrun_update(project_id, sample_id, libprep_id, seqrun_id, status="FAILED")
            seqrun_dict["alignment_status"] = "FAILED"

    else:
        # Alignment failed (return code > 0), store it as aborted
        error_msg = ('Alignment ended with an error: Piper returned non 0 return code'
                      'currently processing runid {} for project {}, sample {}, libprep {}'.format(fc_id,
                      project_id, sample_id, library_id))
        LOG.error(error_msg)
        charon_session.seqrun_reset(project_id, sample_id, libprep_id, seqrun_id)
        #charon_session.seqrun_update(project_id, sample_id, libprep_id, seqrun_id, status="FAILED")
        seqrun_dict["alignment_status"] = "FAILED"

    try:
        # Update the seqrun in the Charon database
        charon_session.seqrun_update(project_id, sample_id, libprep_id, seqrun_id, **seqrun_dict)
    except CharonError as e:
        error_msg = ('Failed to update run alignment status for run "{}" in project {} '
                     'sample {}, library prep {} to  Charon database: {}'.format(fc_id,
                      project_id, sample_id, library_id, e))
        raise RuntimeError(error_msg)


# TODO rethink this possibly, works at the moment
def update_seq_run_for_lane(seqrun_dict, lane_alignment_metrics):
    num_lanes = seqrun_dict.get("lanes")    # This gives None the first time
    seqrun_dict["lanes"] += 1   # Increment
    ## FIXME Change this so the lane_alignment_metrics has a "lane" value
    current_lane = re.match(".+\.(\d)\.bam", lane_alignment_metrics["bam_file"]).group(1)

    fields_to_update = ('mean_coverage',
                        #'mean_autosomal_coverage',
                        'std_coverage',
                        'aligned_bases',
                        'mapped_bases',
                        'mapped_reads',
                        'reads_per_lane',
                        'sequenced_bases',
                        'bam_file',
                        'output_file',
                        'GC_percentage',
                        'mean_mapping_quality',
                        'bases_number',
                        'contigs_number'
                        )
    ## FIXME Change how Charon stores these things? A dict for each attribute seems a little funky
    for field in fields_to_update:
        if not lanes:
            seqrun_dict[field] = {current_lane : lane_alignment_metrics[field]}
        else:
            seqrun_dict[field][current_lane] =  lane_alignment_metrics[field]
    ## FIXME Are we just adding these all together? That doesn't really seem right for an average
    seqrun_dict[field] = seqrun_dict.get("mean_autosomal_coverage", 0) + lane_alignment_metrics["mean_autosomal_coverage"]


def record_process_flowcell(p_handle, workflow, project, sample, libprep, fcid,
                            analysis_module, analysis_dir, config=None):
    LOG.info("Recording process id {} for project {}, sample {}, fcid {} "
             "workflow {}".format(p_handle.pid, project, sample, fcid, workflow))
    project_dict = { "workflow": workflow,
                     "p_handle": p_handle,
                     "analysis_module": analysis_module.__name__,
                     "project_id": project.project_id,
                     "run_dir": analysis_dir
                   }
    with get_shelve_database(config) as db:
        db_key = "{}_{}_{}_{}".format(project, sample, libprep, fcid)
        if db_key in db:
            error_msg = ("Project {}, Sample {}, Library prep {}, fcid {} "
                         "has an entry in the local db. ".format(project, sample, libprep, fcid))
            LOG.warn(error_msg)
            return 

        db[db_key] = project_dict
        LOG.info("Successfully recorded process id {} for Project {}, Sample {}, Library prep {}, fcid {}, "
                 "workflow {}".format(p_handle.pid, project, sample, libprep, fcid,   workflow))


def record_process_sample(p_handle, workflow, project, sample, analysis_module, analysis_dir, config=None):
    LOG.info("Recording process id {} for project {}, sample {}, "
             "workflow {}".format(p_handle.pid, project, sample, workflow))
    project_dict = { "workflow": workflow,
                     "p_handle": p_handle,
                     "analysis_module": analysis_module.__name__,
                     "project_id": project.project_id,
                     "run_dir": analysis_dir
                   }
    with get_shelve_database(config) as db:
        db_key = "{}_{}".format(project, sample)
        if db_key in db:
            error_msg = ("Project {}, Sample {} "
                         "has an entry in the local db. ".format(project, sample))
            LOG.warn(error_msg)
            return 

        db[db_key] = project_dict
        LOG.info("Successfully recorded process id {} for Project {}, Sample {} "
                 "workflow {}".format(p_handle.pid, project, sample, workflow))


# Don't switch the order of these or you'll break everything
@contextlib.contextmanager
@with_ngi_config
def get_shelve_database(config=None, config_file_path=None):
    """Context manager for opening the local process tracking database.
    Closes the db automatically on exit.
    """
    try:
        database_path = config["database"]["record_tracking_db_path"]
    except KeyError as e:
        error_msg = ("Could not get path to process tracking database "
                     "from provided configuration: key missing: {}".format(e))
        raise KeyError(error_msg)
    db = shelve.open(database_path)
    try:
        yield db
    finally:
        db.close()
