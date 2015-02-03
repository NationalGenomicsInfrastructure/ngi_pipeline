import collections
import glob
import inspect
import os
import psutil
import re
import time

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.communication import mail_analysis
from ngi_pipeline.engines.piper_ngi.database import SampleAnalysis, get_db_session
from ngi_pipeline.engines.piper_ngi.utils import create_exit_code_file_path, \
                                                 create_project_obj_from_analysis_log, \
                                                 get_valid_seqruns_for_sample
from ngi_pipeline.utils.parsers import get_slurm_job_status, \
                                       parse_qualimap_results, \
                                       STHLM_UUSNP_SEQRUN_RE, \
                                       STHLM_UUSNP_SAMPLE_RE
from sqlalchemy.exc import IntegrityError, OperationalError


LOG = minimal_logger(__name__)


def update_charon_with_local_jobs_status():
    """Check the status of all locally-tracked jobs and update Charon accordingly.
    """
    LOG.info("Updating Charon with the status of all locally-tracked jobs...")
    with get_db_session() as session:
        charon_session = CharonSession()
        for sample_entry in session.query(SampleAnalysis).all():
            # Local names
            workflow = sample_entry.workflow
            project_name = sample_entry.project_name
            project_id = sample_entry.project_id
            project_base_path = sample_entry.project_base_path
            sample_id = sample_entry.sample_id
            engine=sample_entry.engine
            # Only one of these will have a value
            slurm_job_id = sample_entry.slurm_job_id
            process_id = sample_entry.process_id
            piper_exit_code = get_exit_code(workflow_name=workflow,
                                            project_base_path=project_base_path,
                                            project_name=project_name,
                                            project_id=project_id,
                                            sample_id=sample_id)
            label = "project/sample {}/{}".format(project_name, sample_id)

            try:
                project_obj = create_project_obj_from_analysis_log(project_name,
                                                                   project_id,
                                                                   project_base_path,
                                                                   sample_id,
                                                                   workflow)
            except IOError as e: # analysis log file is missing!
                error_text = ('Could not find analysis log file! Cannot update '
                              'Charon for sample run {}/{}: {}'.format(project_id,
                                                                   sample_id,
                                                                   e))
                LOG.error(error_text)
                mail_analysis(project_name=project_name, sample_name=sample_id,
                              engine_name=engine, info_text=error_text)
                continue
            try:
                if piper_exit_code == 0:
                    # 0 -> Job finished successfully
                    set_status = "ANALYZED"
                    info_text = ('Workflow "{}" for {} finished succesfully. '
                                 'Recording status {} in Charon'.format(workflow, label,
                                                                        set_status))
                    LOG.info(info_text)
                    mail_analysis(project_name=project_name, sample_name=sample_id,
                                  engine_name=engine, info_text=info_text)
                    # Parse seqrun output results / update Charon
                    piper_qc_dir = os.path.join(project_base_path, "ANALYSIS",project_id,
                                                "02_preliminary_alignment_qc")
                    update_coverage_for_sample_seqruns(project_id, sample_id, piper_qc_dir)

                    charon_session.sample_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 analysis_status=set_status)
                    recurse_status_for_sample(project_obj, "DONE")
                    # Job is only deleted if the Charon update succeeds
                    session.delete(sample_entry)
                elif piper_exit_code == 1:
                    # 1 -> Job failed
                    set_status = "FAILED"
                    info_text = ('Workflow "{}" for {} failed. Recording status '
                                 '{} in Charon.'.format(workflow, label, set_status))
                    LOG.info(info_text)
                    mail_analysis(project_name=project_name, sample_name=sample_id,
                                  engine_name=engine, info_text=info_text)
                    charon_session.sample_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 analysis_status=set_status)
                    recurse_status_for_sample(project_obj, set_status)
                    # Job is only deleted if the Charon update succeeds
                    session.delete(sample_entry)
                else:
                    # None -> Job still running OR exit code was never written (failure)
                    JOB_FAILED = None
                    if slurm_job_id:
                        try:
                            slurm_exit_code = get_slurm_job_status(slurm_job_id)
                        except ValueError as e:
                            slurm_exit_code = 1
                        if slurm_exit_code is not None: # "None" indicates job is still running
                            JOB_FAILED = True
                    else:
                        if not psutil.pid_exists(process_id):
                            # Job did not write an exit code and is also not running
                            JOB_FAILED = True
                    if JOB_FAILED:
                        set_status = "FAILED"
                        warn_text = ('No exit code found but job not running for '
                                     '{}: setting status to {} in Charon'.format(label, set_status))
                        LOG.warn(warn_text)
                        mail_analysis(project_name=project_name, sample_name=sample_id,
                                      engine_name=engine, info_text=warn_text)
                        charon_session.sample_update(projectid=project_id,
                                                     sampleid=sample_id,
                                                     analysis_status=set_status)
                        recurse_status_for_sample(project_obj, set_status)
                        # Job is only deleted if the Charon update succeeds
                        LOG.debug("Deleting local entry {}".format(sample_entry))
                        session.delete(sample_entry)
                    else: # Job still running
                        charon_status = charon_session.sample_get(projectid=project_id,
                                                                  sampleid=sample_id)['analysis_status']
                        if not charon_status == "UNDER_ANALYSIS":
                            set_status = "UNDER_ANALYSIS"
                            LOG.warn('Tracking inconsistency for {}: Charon status is "{}" but '
                                     'local process tracking database indicates it is running. '
                                     'Setting value in Charon to {}.'.format(label, charon_status,
                                                                             set_status))
                            charon_session.sample_update(projectid=project_id,
                                                         sampleid=sample_id,
                                                         analysis_status=set_status)
                            recurse_status_for_sample(project_obj, "RUNNING")
            except CharonError as e:
                error_text = ('Unable to update Charon status for "{}": {}'.format(label, e))
                LOG.error(error_text)
                mail_analysis(project_name=project_name, sample_name=sample_id,
                              engine_name=engine, info_text=error_text)
        session.commit()



def recurse_status_for_sample(project_obj, set_status, update_done=False):
    """Set seqruns under sample to have status "set_status"
    """

    charon_session = CharonSession()
    project_id = project_obj.project_id
    for sample_obj in project_obj:
        # There's only one sample but this is an iterator
        sample_id = sample_obj.name
    for libprep_obj in sample_obj:
        libprep_id = libprep_obj.name
        for seqrun_obj in libprep_obj:
            seqrun_id = seqrun_obj.name
            label = "{}/{}/{}/{}".format(project_id, sample_id, libprep_id, seqrun_id)
            LOG.info(('Updating status of project/sample/libprep/seqrun '
                      '"{}" to "{}" in Charon ').format(label, set_status))
            try:
                charon_session.seqrun_update(projectid=project_id,
                                             sampleid=sample_id,
                                             libprepid=libprep_id,
                                             seqrunid=seqrun_id,
                                             alignment_status=set_status)
            except CharonError as e:
                error_text =('Could not update status of project/sample/libprep/seqrun '
                             '"{}" in Charon to "{}": {}'.format(label, set_status, e))
                LOG.error(error_text)
                mail_analysis(project_name=project_id, sample_name=sample_obj.name,
                              info_text=error_text)



def update_coverage_for_sample_seqruns(project_id, sample_id, piper_qc_dir):
    """Find all the valid seqruns for a particular sample, parse their
    qualimap output files, and update Charon with the mean autosomal
    coverage for each.

    :param str piper_qc_dir: The path to the Piper qc dir (02_preliminary_alignment_qc at time of writing)
    :param str sample_id: The sample name (e.g. P1170_105)

    :raises OSError: If the qc path specified is missing or otherwise inaccessible
    :raises RuntimeError: If you specify both the seqrun_id and fcid and they don't match
    :raises ValueError: If arguments are incorrect
    """
    seqruns_by_libprep = get_valid_seqruns_for_sample(project_id, sample_id)

    charon_session = CharonSession()
    for libprep_id, seqruns in seqruns_by_libprep.iteritems():
        for seqrun_id in seqruns:
            label = "{}/{}/{}/{}".format(project_id, sample_id, libprep_id, seqrun_id)
            ma_coverage = _parse_mean_coverage_from_qualimap(piper_qc_dir, sample_id, seqrun_id)
            LOG.info('Updating project/sample/libprep/seqrun "{}" in '
                     'Charon with mean autosomal coverage "{}"'.format(label,  ma_coverage))
            try:
                charon_session.seqrun_update(projectid=project_id,
                                             sampleid=sample_id,
                                             libprepid=libprep_id,
                                             seqrunid=seqrun_id,
                                             mean_autosomal_coverage=ma_coverage)
            except CharonError as e:
                error_text = ('Could not update project/sample/libprep/seqrun "{}" '
                              'in Charon with mean autosomal coverage '
                              '"{}": {}'.format(label, ma_coverage, e))
                LOG.error(error_text)
                mail_analysis(project_name=project_id, sample_name=sample_id,
                              engine_name="piper_ngi", info_text=error_text)


def parse_mean_autosomal_coverage_for_sample(piper_qc_dir, sample_id):
    """This will return an integer value representing the total autosomal coverage
    for a particular sample as gleaned from the qualimapReport.html present in
    piper_qc_dir.

    :param str piper_qc_dir: The path to the Piper qc dir (02_preliminary_alignment_qc at time of writing)
    :param str sample_id: The sample name (e.g. P1170_105)

    :returns: The mean autosomal coverage
    :rtype: int
    :raises OSError: If the qc path specified is missing or otherwise inaccessible
    :raises ValueError: If arguments are incorrect
    """
    return _parse_mean_coverage_from_qualimap(piper_qc_dir, sample_id)


def _parse_mean_coverage_from_qualimap(piper_qc_dir, sample_id, seqrun_id=None, fcid=None):
    """This will return an integer value representing the total autosomal coverage
    for a particular sample OR seqrun (if seqrun_id is passed) as gleaned from
    the qualimapReport.html present in piper_qc_dir.

    :param str piper_qc_dir: The path to the Piper qc dir (02_preliminary_alignment_qc at time of writing)
    :param str sample_id: The sample name (e.g. P1170_105)
    :param str seqrun_id: The run id (e.g. 140821_D00458_0029_AC45JGANXX) (optional) (specify either this or fcid)
    :param str fcid: The FCID (optional) (specify either this or seqrun_id)

    :returns: The mean autosomal coverage
    :rtype: int

    :raises OSError: If the qc path specified is missing or otherwise inaccessible
    :raises ValueError: If arguments are incorrect
    """
    try:
        if seqrun_id and fcid and (fcid != seqrun_id.split("_")[3]):
            raise ValueError(('seqrun_id and fcid both passed as arguments but do not '
                              'match (seqrun_id: "{}", fcid: "{}")'.format(seqrun_id, fcid)))
        if seqrun_id:
            piper_run_id = seqrun_id.split("_")[3]
        elif fcid:
            piper_run_id = fcid
        else:
            piper_run_id = None
    except IndexError:
        raise ValueError('Can\'t parse FCID from run id ("{}")'.format(seqrun_id))
    seqrun_dict = {}
    seqrun_dict["lanes"] = 0
    # Find all the appropriate files
    try:
        os.path.isdir(piper_qc_dir) and os.listdir(piper_qc_dir)
    except OSError as e:
        raise OSError('Piper result directory "{}" inaccessible when updating stats to Charon: {}.'.format(piper_qc_dir, e))
    piper_qc_dir_base = "{}.{}.{}".format(sample_id, (piper_run_id or "*"), sample_id)
    piper_qc_path = "{}*/".format(os.path.join(piper_qc_dir, piper_qc_dir_base))
    piper_qc_dirs = glob.glob(piper_qc_path)
    if not piper_qc_dirs: # Something went wrong in the alignment or we can't parse the file format
        raise OSError('Piper qc directories under "{}" are missing or in an unexpected format when updating stats to Charon.'.format(piper_qc_path))
    # Examine each lane and update the dict with its alignment metrics
    for qc_lane in piper_qc_dirs:
        genome_result = os.path.join(qc_lane, "genome_results.txt")
        # This means that if any of the lanes are missing results, the sequencing run is marked as a failure.
        # We should flag this somehow and send an email at some point.
        if not os.path.isfile(genome_result):
            raise OSError('File "genome_results.txt" is missing from Piper result directory "{}"'.format(piper_qc_dir))
        # Get the alignment results for this lane
        lane_alignment_metrics = parse_qualimap_results(genome_result)
        # Update the dict for this lane
        update_seq_run_for_lane(seqrun_dict, lane_alignment_metrics)
    return seqrun_dict["mean_autosomal_coverage"]


def write_to_charon_alignment_results(base_path, project_name, project_id, sample_id, libprep_id, seqrun_id):
    """Update the status of a sequencing run after alignment.

    :param str project_name: The name of the project (e.g. T.Durden_14_01)
    :param str project_id: The id of the project (e.g. P1171)
    :param str sample_id: ...
    :param str libprep_id: ...
    :param str seqrun_id: ...

    :raises RuntimeError: If the Charon database could not be updated
    :raises ValueError: If the output data could not be parsed.
    """
    charon_session = CharonSession()
    try:
        seqrun_dict = charon_session.seqrun_get(project_id, sample_id, libprep_id, seqrun_id)
    except CharonError as e:
        raise CharonError('Error accessing database for project "{}", sample {}; '
                           'could not update Charon while performing best practice: '
                           '{}'.format(project_name, sample_id,  e))
    piper_run_id = seqrun_id.split("_")[3]
    seqrun_dict["lanes"] = 0
    if seqrun_dict.get("alignment_status") == "DONE":
        LOG.warn('Sequencing run "{}" marked as DONE but writing new alignment results; '
                 'this will overwrite the previous results.'.format(seqrun_id))
    # Find all the appropriate files
    piper_result_dir = os.path.join(base_path, "ANALYSIS", project_name,
                                    "02_preliminary_alignment_qc")
    try:
        os.path.isdir(piper_result_dir) and os.listdir(piper_result_dir)
    except OSError as e:
        raise ValueError('Piper result directory "{}" inaccessible when updating '
                         'itats to Charon: {}.'.format(piper_result_dir, e))
    piper_qc_dir_base = "{}.{}.{}".format(sample_id, piper_run_id, sample_id)
    piper_qc_path = "{}*/".format(os.path.join(piper_result_dir, piper_qc_dir_base))
    piper_qc_dirs = glob.glob(piper_qc_path)
    if not piper_qc_dirs: # Something went wrong in the alignment or we can't parse the file format
        raise ValueError('Piper qc directories under "{}" are missing or in an '
                         'unexpected format when updating stats to Charon.'.format(piper_qc_path))
    # Examine each lane and update the dict with its alignment metrics
    for qc_lane in piper_qc_dirs:
        genome_result = os.path.join(qc_lane, "genome_results.txt")
        # This means that if any of the lanes are missing results, the sequencing run is marked as a failure.
        # We should flag this somehow and send an email at some point.
        if not os.path.isfile(genome_result):
            raise ValueError('File "{}" is missing from Piper result directory '
                             '"{}"'.format(genome_result, piper_result_dir))
        # Get the alignment results for this lane
        lane_alignment_metrics = parse_qualimap_results(genome_result)
        # Update the dict for this lane
        update_seq_run_for_lane(seqrun_dict, lane_alignment_metrics)
    try:
        # Update the seqrun in the Charon database
        charon_session.seqrun_update(**seqrun_dict)
    except CharonError as e:
        error_msg = ('Failed to update run alignment status for project/sample/'
                     'libprep/seqrun {}/{}/{}/{} to Charon database: {}'.format(
                        project_name, sample_id, libprep_id, seqrun_id, e))
        raise CharonError(error_msg)


def update_seq_run_for_lane(seqrun_dict, lane_alignment_metrics):
    num_lanes = seqrun_dict.get("lanes")    # This gives 0 the first time
    seqrun_dict["lanes"] = seqrun_dict["lanes"] + 1   # Increment
    current_lane = re.match(".+\.(\d)\.bam", lane_alignment_metrics["bam_file"]).group(1)

    fields_to_update = ('mean_coverage',
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
    for field in fields_to_update:
        if not num_lanes:
            seqrun_dict[field] = {current_lane : lane_alignment_metrics[field]}
            seqrun_dict["mean_autosomal_coverage"] = 0
        else:
            seqrun_dict[field][current_lane] =  lane_alignment_metrics[field]
    seqrun_dict["mean_autosomal_coverage"] = seqrun_dict.get("mean_autosomal_coverage", 0) + lane_alignment_metrics["mean_autosomal_coverage"]


def record_process_sample(project, sample, workflow_subtask, analysis_module_name,
                          process_id=None, slurm_job_id=None, config=None):
    LOG.info('Recording slurm job id "{}" for project "{}", sample "{}", '
             'workflow "{}"'.format(slurm_job_id, project, sample, workflow_subtask))
    with get_db_session() as session:
        sample_db_obj = SampleAnalysis(project_id=project.project_id,
                                       project_name=project.name,
                                       project_base_path=project.base_path,
                                       sample_id=sample.name,
                                       engine=analysis_module_name,
                                       workflow=workflow_subtask,
                                       process_id=process_id,
                                       slurm_job_id=slurm_job_id)
        try:
            session.add(sample_db_obj)
            for attempts in range(3):
                try:
                    session.commit()
                    LOG.info('Successfully recorded slurm job id "{}" for project "{}", sample "{}", '
                             'workflow "{}"'.format(slurm_job_id, project, sample, workflow_subtask))
                    break
                except OperationalError as e:
                    LOG.warn('Database locked ("{}"). Waiting...'.format(e))
                    time.sleep(15)
            else:
                raise RuntimeError("Could not write to database after three attempts (locked?)")
        except (IntegrityError, RuntimeError):
            raise RuntimeError('Could not record slurm job id "{}" for project "{}", sample "{}", '
                               'workflow "{}": {}'.format(slurm_job_id, project, sample, workflow_subtask, e))
    try:
        set_status = "UNDER_ANALYSIS"
        LOG.info(('Updating Charon status for project/sample '
                  '{}/{} to {}').format(project, sample, set_status))
        CharonSession().sample_update(projectid=project.project_id,
                                      sampleid=sample.name,
                                      analysis_status=set_status)
        project_obj = create_project_obj_from_analysis_log(project.name,
                                                           project.project_id,
                                                           project.base_path,
                                                           sample.name,
                                                           workflow_subtask)
        recurse_status_for_sample(project_obj, "RUNNING")
    except CharonError as e:
        error_text = ('Could not update Charon status for project/sample '
                      '{}/{} due to error: {}'.format(project, sample, e))

        LOG.error(error_text)
        mail_analysis(project_name=project_id, sample_name=sample_id,
                      engine_name='piper_ngi', info_text=error_text)

def is_sample_analysis_running_local(workflow_subtask, project_id, sample_id):
    """Determine if a sample is currently being analyzed by accessing the local
    process tracking database."""
    sample_run_name = "{}/{}".format(project_id, sample_id)
    LOG.info('Checking if sample run "{}" is currently being analyzed '
             '(workflow "{}")...'.format(sample_run_name, workflow_subtask))
    with get_db_session() as session:
        db_q = session.query(SampleAnalysis).filter_by(workflow=workflow_subtask,
                                                       project_id=project_id,
                                                       sample_id=sample_id)
        if session.query(db_q.exists()).scalar():
            LOG.info('...sample run "{}" is currently being analyzed.'.format(sample_run_name))
            return True
        else:
            LOG.info('...sample run "{}" is not currently under analysis.'.format(sample_run_name))
            return False


def get_exit_code(workflow_name, project_base_path, project_name, project_id,
                  sample_id, libprep_id=None, seqrun_id=None):
    exit_code_file_path = create_exit_code_file_path(workflow_name,
                                                     project_base_path,
                                                     project_name,
                                                     project_id,
                                                     sample_id,
                                                     libprep_id,
                                                     seqrun_id)

    try:
        with open(exit_code_file_path, 'r') as f:
            exit_code = f.read().strip()
            if exit_code:
                exit_code = int(exit_code)
            return exit_code
    except IOError as e:
        if e.errno == 2:    # No such file or directory
            return None     # Process is not yet complete
    except ValueError as e:
        raise ValueError('Could not determine job exit status: not an integer ("{}")'.format(e))
