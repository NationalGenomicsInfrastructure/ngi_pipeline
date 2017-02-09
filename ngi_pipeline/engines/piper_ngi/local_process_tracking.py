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
                                                 get_finished_seqruns_for_sample
from ngi_pipeline.engines.piper_ngi.parsers import parse_genotype_concordance, \
                                                   parse_mean_coverage_from_qualimap, \
                                                   parse_deduplication_percentage,\
                                                   parse_qualimap_reads,\
                                                   parse_qualimap_coverage
from ngi_pipeline.utils.slurm import get_slurm_job_status, \
                                     kill_slurm_job_by_id
from ngi_pipeline.utils.parsers import STHLM_UUSNP_SEQRUN_RE, \
                                       STHLM_UUSNP_SAMPLE_RE
from sqlalchemy.exc import IntegrityError, OperationalError
from ngi_pipeline.utils.charon import recurse_status_for_sample
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.post_analysis import run_multiqc


LOG = minimal_logger(__name__)

@with_ngi_config
def update_charon_with_local_jobs_status(quiet=False, config=None, config_file_path=None):
    """Check the status of all locally-tracked jobs and update Charon accordingly.
    """
    if quiet and not config.get("quiet"):
        config['quiet'] = True
    LOG.info("Updating Charon with the status of all locally-tracked jobs...")
    multiqc_projects=set()
    with get_db_session() as session:
        charon_session = CharonSession()
        for sample_entry in session.query(SampleAnalysis).all():
            # Local names
            workflow = sample_entry.workflow
            project_name = sample_entry.project_name
            project_id = sample_entry.project_id
            project_base_path = sample_entry.project_base_path
            sample_id = sample_entry.sample_id
            engine = sample_entry.engine
            # Only one of these id fields (slurm, pid) will have a value
            slurm_job_id = sample_entry.slurm_job_id
            process_id = sample_entry.process_id
            piper_exit_code = get_exit_code(workflow_name=workflow,
                                            project_base_path=project_base_path,
                                            project_name=project_name,
                                            project_id=project_id,
                                            sample_id=sample_id)
            label = "project/sample {}/{}".format(project_name, sample_id)

            if workflow not in ("merge_process_variantcall", "genotype_concordance",):
                LOG.error('Unknown workflow "{}" for {}; cannot update '
                          'Charon. Skipping sample.'.format(workflow, label))
                continue

            try:
                project_obj = create_project_obj_from_analysis_log(project_name,
                                                                   project_id,
                                                                   project_base_path,
                                                                   sample_id,
                                                                   workflow)
            except IOError as e: # analysis log file is missing!
                error_text = ('Could not find analysis log file! Cannot update '
                              'Charon for {} run {}/{}: {}'.format(workflow,
                                                                   project_id,
                                                                   sample_id,
                                                                   e))
                LOG.error(error_text)
                if not config.get('quiet'):
                    mail_analysis(project_name=project_name,
                                  sample_name=sample_id,
                                  engine_name=engine,
                                  level="ERROR",
                                  info_text=error_text,
                                  workflow=workflow)
                continue
            try:
                if piper_exit_code == 0:
                    # 0 -> Job finished successfully
                    if workflow == "merge_process_variantcall":
                        sample_status_field = "analysis_status"
                        seqrun_status_field = "alignment_status"
                        set_status = "ANALYZED" # sample level
                    elif workflow == "genotype_concordance":
                        sample_status_field = seqrun_status_field = "genotype_status"
                        set_status = "DONE" # sample level
                    recurse_status = "DONE" # For the seqrun level
                    info_text = ('Workflow "{}" for {} finished succesfully. '
                                 'Recording status {} in Charon'.format(workflow,
                                                                        label,
                                                                        set_status))
                    LOG.info(info_text)
                    if not config.get('quiet'):
                        mail_analysis(project_name=project_name,
                                      sample_name=sample_id,
                                      engine_name=engine,
                                      level="INFO",
                                      info_text=info_text,
                                      workflow=workflow)
                    charon_session.sample_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 **{sample_status_field: set_status})
                    recurse_status_for_sample(project_obj,
                                              status_field=seqrun_status_field,
                                              status_value=recurse_status,
                                              config=config)
                    # Job is only deleted if the Charon status update succeeds
                    session.delete(sample_entry)
                    #add project to MultiQC
                    multiqc_projects.add((project_base_path, project_id, project_name))


                    if workflow == "merge_process_variantcall":
                        # Parse seqrun output results / update Charon
                        # This is a semi-optional step -- failure here will send an
                        # email but not more than once. The record is still removed
                        # from the local jobs database, so this will have to be done
                        # manually if you want it done at all.
                        piper_qc_dir = os.path.join(project_base_path, "ANALYSIS",
                                                    project_id, "piper_ngi",
                                                    "02_preliminary_alignment_qc")
                        update_coverage_for_sample_seqruns(project_id, sample_id,
                                                           piper_qc_dir)
                        update_sample_duplication_and_coverage(project_id, sample_id,
                                                           project_base_path)

                        
                    elif workflow == "genotype_concordance":
                        piper_gt_dir = os.path.join(project_base_path, "ANALYSIS",
                                                    project_id, "piper_ngi",
                                                    "03_genotype_concordance")
                        try:
                            update_gtc_for_sample(project_id, sample_id, piper_gt_dir)
                        except (CharonError, IOError, ValueError) as e:
                            LOG.error(e)
                elif type(piper_exit_code) is int and piper_exit_code > 0:
                    # 1 -> Job failed
                    set_status = "FAILED"
                    error_text = ('Workflow "{}" for {} failed. Recording status '
                                  '{} in Charon.'.format(workflow, label, set_status))
                    LOG.error(error_text)
                    if not config.get('quiet'):
                        mail_analysis(project_name=project_name,
                                      sample_name=sample_id,
                                      engine_name=engine,
                                      level="ERROR",
                                      info_text=error_text,
                                      workflow=workflow)
                    if workflow == "merge_process_variantcall":
                        sample_status_field = "analysis_status"
                        seqrun_status_field = "alignment_status"
                    elif workflow == "genotype_concordance":
                        sample_status_field = seqrun_status_field = "genotype_status"
                    charon_session.sample_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 **{sample_status_field: set_status})
                    recurse_status_for_sample(project_obj, status_field=seqrun_status_field,
                                              status_value=set_status, config=config)
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
                        error_text = ('No exit code found but job not running '
                                      'for {} / {}: setting status to {} in '
                                      'Charon'.format(label, workflow, set_status))
                        if slurm_job_id:
                            exit_code_file_path = \
                                create_exit_code_file_path(workflow_subtask=workflow,
                                                           project_base_path=project_base_path,
                                                           project_name=project_name,
                                                           project_id=project_id,
                                                           sample_id=sample_id)
                            error_text += (' (slurm job id "{}", exit code file path '
                                           '"{}")'.format(slurm_job_id, exit_code_file_path))
                        LOG.error(error_text)
                        if not config.get('quiet'):
                            mail_analysis(project_name=project_name,
                                          sample_name=sample_id,
                                          engine_name=engine, level="ERROR",
                                          info_text=error_text,
                                          workflow=workflow)
                        if workflow == "merge_process_variantcall":
                            sample_status_field = "analysis_status"
                            seqrun_status_field = "alignment_status"
                        elif workflow == "genotype_concordance":
                            sample_status_field = seqrun_status_field = "genotype_status"
                        charon_session.sample_update(projectid=project_id,
                                                     sampleid=sample_id,
                                                     **{sample_status_field: set_status})
                        recurse_status_for_sample(project_obj,
                                                  status_field=seqrun_status_field,
                                                  status_value=set_status,
                                                  config=config)
                        # Job is only deleted if the Charon update succeeds
                        LOG.debug("Deleting local entry {}".format(sample_entry))
                        session.delete(sample_entry)
                    else: # Job still running
                        set_status = "UNDER_ANALYSIS"
                        if workflow == "merge_process_variantcall":
                            sample_status_field = "analysis_status"
                            seqrun_status_field = "alignment_status"
                            recurse_status = "RUNNING"
                        elif workflow == "genotype_concordance":
                            sample_status_field = seqrun_status_field = "genotype_status"
                            recurse_status = "UNDER_ANALYSIS"
                        try:
                            remote_sample=charon_session.sample_get(projectid=project_id, sampleid=sample_id)
                            charon_status = remote_sample.get(sample_status_field)
                            if charon_status and not charon_status == set_status:
                                LOG.warn('Tracking inconsistency for {}: Charon status '
                                         'for field "{}" is "{}" but local process tracking '
                                         'database indicates it is running. Setting value '
                                         'in Charon to {}.'.format(label, sample_status_field,
                                                                   charon_status, set_status))
                                charon_session.sample_update(projectid=project_id,
                                                             sampleid=sample_id,
                                                             **{sample_status_field: set_status})
                                recurse_status_for_sample(project_obj,
                                                          status_field=seqrun_status_field,
                                                          status_value=recurse_status,
                                                          config=config)
                        except CharonError as e:
                            error_text = ('Unable to update/verify Charon '
                                          'for {}: {}'.format(label, e))
                            LOG.error(error_text)
                            if not config.get('quiet'):
                                mail_analysis(project_name=project_name, sample_name=sample_id,
                                              engine_name=engine, level="ERROR",
                                              workflow=workflow, info_text=error_text)
            except CharonError as e:
                error_text = ('Unable to update Charon for {}: '
                              '{}'.format(label, e))
                LOG.error(error_text)
                if not config.get('quiet'):
                    mail_analysis(project_name=project_name, sample_name=sample_id,
                                  engine_name=engine, level="ERROR",
                                  workflow=workflow, info_text=error_text)
            except OSError as e:
                error_text = ('Permissions error when trying to update Charon '
                              '"{}" status for "{}": {}'.format(workflow, label, e))
                LOG.error(error_text)
                if not config.get('quiet'):
                    mail_analysis(project_name=project_name, sample_name=sample_id,
                                  engine_name=engine, level="ERROR",
                                  workflow=workflow, info_text=error_text)
        session.commit()
    #Run Multiqc
    for pj_tuple in multiqc_projects:
        LOG.info("Running MultiQC on project {}".format(pj_tuple[1]))
        run_multiqc(pj_tuple[0], pj_tuple[1], pj_tuple[2])
        



@with_ngi_config
def update_gtc_for_sample(project_id, sample_id, piper_gtc_path, config=None, config_file_path=None):
    """Find the genotype concordance file for this sample, if it exists,
    and update the sample record in Charon with the value parsed from it.

    :param str project_id: The id of the project
    :param str sample_id: The id the sample
    :param str piper_gtc_path: The path to the piper genotype concordance directory

    :raises CharonError: If there is some Error -- with Charon
    :raises IOError: If the path specified is missing or inaccessible
    :raises ValueError: If the specified sample has no data in the gtc file
    """
    gtc_file = os.path.join(piper_gtc_path, "{}.gt_concordance".format(sample_id))
    try:
        concordance_value = parse_genotype_concordance(gtc_file)[sample_id]
    except KeyError:
        raise ValueError('Concordance data for sample "{}" not found in gt '
                         'concordance file "{}"'.format(sample_id, gtc_file))
    gtc_lower_bound = config.get("genotyping", {}).get("lower_bound_cutoff")
    status_dict = {}
    if gtc_lower_bound:
        if concordance_value < concordance_value:
            status_dict = {"genotype_status": "FAILED"}
        else:
            status_dict = {"genotype_status": "PASSED"}
    charon_session = CharonSession()
    charon_session.sample_update(projectid=project_id, sampleid=sample_id,
                                 genotype_concordance=concordance_value,
                                 **status_dict)

@with_ngi_config
def update_sample_duplication_and_coverage(project_id, sample_id, project_base_path,
                                       config=None, config_file_path=None):
    """Update Charon with the duplication rates for said sample.

    :param str project_base_path: The path to the project dir 
    :param str sample_id: The sample name (e.g. P1170_105)

    """
    
    dup_file_path=os.path.join(project_base_path, 'ANALYSIS', project_id, 'piper_ngi', '05_processed_alignments', "{}.metrics".format(sample_id))
    genome_results_file_path=os.path.join(project_base_path, 'ANALYSIS', project_id, 'piper_ngi', '06_final_alignment_qc', "{}.clean.dedup.qc".format(sample_id),"genome_results.txt")

    try:
        dup_pc=parse_deduplication_percentage(dup_file_path)
    except:
        dup_pc=0
        LOG.error("Cannot find {}.metrics file for duplication rate at {}. Continuing.".format(sample_id, dup_file_path))
    try:
        cov=parse_qualimap_coverage(genome_results_file_path)
        reads=parse_qualimap_reads(genome_results_file_path)
    except IOError as e:
        cov=0
        reads=0
        LOG.error("Cannot find genome_results.txt file for sample coverage at {}. Continuing.".format(genome_results_file_path))
    try:
        charon_session = CharonSession()
        charon_session.sample_update(projectid=project_id,
                                     sampleid=sample_id,
                                     duplication_pc=dup_pc,
                                     total_sequenced_reads=reads,
                                     total_autosomal_coverage=cov)
        LOG.info('Updating sample "{}" in '
                 'Charon with mean duplication_percentage"{}" and autosomal coverage "{}"'.format(sample_id, dup_pc, cov))
    except CharonError as e:
        error_text = ('Could not update project/sample "{}/{}" '
                    'in Charon with duplication rate : {}'
                      'and coverage {}'.format("{}/{}".format(project_id, sampleid, dup_pc, cov)))
        LOG.error(error_text)
        if not config.get('quiet'):
            mail_analysis(project_name=project_id, sample_name=sample_id,
                          engine_name="piper_ngi", level="ERROR", info_text=error_text)





@with_ngi_config
def update_coverage_for_sample_seqruns(project_id, sample_id, piper_qc_dir,
                                       config=None, config_file_path=None):
    """Find all the valid seqruns for a particular sample, parse their
    qualimap output files, and update Charon with the mean autosomal
    coverage for each.

    :param str piper_qc_dir: The path to the Piper qc dir (02_preliminary_alignment_qc at time of writing)
    :param str sample_id: The sample name (e.g. P1170_105)

    :raises OSError: If the qc path specified is missing or otherwise inaccessible
    :raises ValueError: If arguments are incorrect
    """
    seqruns_by_libprep = get_finished_seqruns_for_sample(project_id, sample_id)

    charon_session = CharonSession()
    for libprep_id, seqruns in seqruns_by_libprep.iteritems():
        for seqrun_id in seqruns:
            label = "{}/{}/{}/{}".format(project_id, sample_id, libprep_id, seqrun_id)
            genome_results_file_paths=glob.glob(os.path.join(piper_qc_dir, "{}.{}*.qc".format(sample_id, seqrun_id.split('_')[-1]),"genome_results.txt"))
            ma_coverage = parse_mean_coverage_from_qualimap(piper_qc_dir, sample_id, seqrun_id)

            reads=0
            for path in genome_results_file_paths:
                try:
                    reads += parse_qualimap_reads(path)
                except IOError as e :
                    LOG.error("Cannot find the genome_results.txt file to get the number of reads in {}".format(path))
                except :
                    LOG.error("Error in handling the genome_results.txt file located at {}".format(path))

            LOG.info('Updating project/sample/libprep/seqrun "{}" in '
                     'Charon with mean autosomal coverage "{}" and total reads {}'.format(label, ma_coverage, reads))
            try:
                charon_session.seqrun_update(projectid=project_id,
                                             sampleid=sample_id,
                                             libprepid=libprep_id,
                                             seqrunid=seqrun_id,
                                             total_reads=reads,
                                             mean_autosomal_coverage=ma_coverage)
            except CharonError as e:
                error_text = ('Could not update project/sample/libprep/seqrun "{}" '
                              'in Charon with mean autosomal coverage '
                              '"{}": {}'.format(label, ma_coverage, e))
                LOG.error(error_text)
                if not config.get('quiet'):
                    mail_analysis(project_name=project_id, sample_name=sample_id,
                                  engine_name="piper_ngi", level="ERROR", info_text=error_text)



@with_ngi_config
def record_process_sample(project, sample, workflow_subtask, analysis_module_name,
                          process_id=None, slurm_job_id=None, config=None, config_file_path=None):
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
        except (IntegrityError, RuntimeError) as e:
            raise RuntimeError('Could not record slurm job id "{}" for project "{}", '
                               'sample "{}", workflow "{}": {}'.format(slurm_job_id,
                                                                       project,
                                                                       sample,
                                                                       workflow_subtask,
                                                                       e.message))
        extra_args = None
        if workflow_subtask == "merge_process_variantcall":
            sample_status_field = "analysis_status"
            sample_status_value = "UNDER_ANALYSIS"
            sample_data_status_field = "status"
            sample_data_status_value = "STALE"
            seqrun_status_field = "alignment_status"
            seqrun_status_value = "RUNNING"
            extra_args = {"mean_autosomal_coverage": 0}
        elif workflow_subtask == "genotype_concordance":
            sample_status_field = seqrun_status_field = "genotype_status"
            sample_status_value = seqrun_status_value = "UNDER_ANALYSIS"
            sample_data_status_field = "status"
            sample_data_status_value = "STALE"
        else:
            raise ValueError('Charon field for workflow "{}" unknown; '
                             'cannot update Charon.'.format(workflow_subtask))
        try:
            LOG.info('Updating Charon status for project/sample '
                     '{}/{} key : {} value : {}'.format(project, sample, sample_status_field, sample_status_value))
            CharonSession().sample_update(projectid=project.project_id,
                                          sampleid=sample.name,
                                          **{sample_status_field: sample_status_value,
                                              sample_data_status_field: sample_data_status_value})
            project_obj = create_project_obj_from_analysis_log(project.name,
                                                               project.project_id,
                                                               project.base_path,
                                                               sample.name,
                                                               workflow_subtask)
            recurse_status_for_sample(project_obj,
                                      status_field=seqrun_status_field,
                                      status_value=seqrun_status_value,
                                      extra_args=extra_args,
                                      config=config)
        except CharonError as e:
            error_text = ('Could not update Charon status for project/sample '
                          '{}/{} due to error: {}'.format(project, sample, e))

            LOG.error(error_text)
            if not config.get('quiet'):
                mail_analysis(project_name=project.project_id,
                              sample_name=sample.name,
                              engine_name='piper_ngi',
                              level="ERROR",
                              info_text=error_text,
                              workflow=workflow_subtask)


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
            LOG.info('..."{}" for sample "{}" is currently being '
                     'analyzed.'.format(workflow_subtask, sample_run_name))
            return True
        else:
            LOG.info('..."{}" for sample "{}" is not currently under '
                     'analysis.'.format(workflow_subtask, sample_run_name))
            return False


def kill_running_sample_analysis(workflow_subtask, project_id, sample_id):
    """Determine if a sample is currently being analyzed by accessing the local
    process tracking database."""
    sample_run_name = "{}/{}".format(project_id, sample_id)
    LOG.info('Attempting to kill sample analysis run "{}"'.format(sample_run_name))
    LOG.info('Checking if sample run "{}" is currently being analyzed '
             '(workflow "{}")...'.format(sample_run_name, workflow_subtask))
    with get_db_session() as session:
        db_q = session.query(SampleAnalysis).filter_by(workflow=workflow_subtask,
                                                       project_id=project_id,
                                                       sample_id=sample_id)
        sample_run = db_q.first()
        if sample_run:
            try:
                slurm_job_id = sample_run.slurm_job_id
                LOG.info('...sample run "{}" is currently being analyzed '
                         '(workflow subtask "{}") and has slurm job id "{}"; '
                         'trying to kill it...'.format(sample_run_name,
                                                       workflow_subtask,
                                                       slurm_job_id))
                kill_slurm_job_by_id(slurm_job_id)
            except Exception as e:
                LOG.error('Could not kill sample run "{}": {}'.format(sample_run_name, e))
                return False
            try:
                project_obj = create_project_obj_from_analysis_log(sample_run.project_name,
                                                                   sample_run.project_id,
                                                                   sample_run.project_base_path,
                                                                   sample_run.sample_id,
                                                                   sample_run.workflow)
            except IOError as e: # analysis log file is missing!
                error_text = ('Could not find analysis log file! Cannot update '
                              'Charon for {} run {}/{}: {}'.format(sample_run.workflow,
                                                                   sample_run.project_id,
                                                                   sample_run.sample_id,
                                                                   e))
                LOG.error(error_text)
            else:
                try:
                    charon_session = CharonSession()
                    set_status = "FAILED"
                    if workflow_subtask == "genotype_concordance":
                        status_field = "genotype_status"
                    elif workflow_subtask == "merge_process_variantcall":
                        sample_status_field = "analysis_status"
                        seqrun_status_field = "alignment_status"
                    charon_session.sample_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 **{sample_status_field: set_status})
                    recurse_status_for_sample(project_obj,
                                              status_field=seqrun_status_field,
                                              status_value=set_status)
                except CharonError as e:
                    LOG.error('Couldn\'t update Charon field "{}" to "{} for '
                              'project/sample "{}/{}"'.format(status_field, set_status,
                                                              project_id, sample_id))
            try:
                LOG.info('Removing sample run "{}" from local jobs database...'.format(sample_run_name))
                # Remove from local jobs database
                session.delete(sample_run)
                session.commit()
                LOG.info("Deleted.")
            except Exception as e:
                LOG.error('Failed to remove entry for sample run "{}" from '
                          'local jobs database: {}'.format(sample_run_name, e))
        else:
            LOG.info('...sample run "{}" is not currently under analysis.'.format(sample_run_name))
    return True


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
        return None     # Process is not yet complete
    except ValueError as e:
        raise ValueError('Could not determine job exit status: not an integer ("{}")'.format(e))
    else:
        return None

