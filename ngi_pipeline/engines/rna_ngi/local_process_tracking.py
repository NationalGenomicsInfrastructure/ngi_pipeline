
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.engines.rna_ngi.database import get_session, ProjectAnalysis
from ngi_pipeline.utils.charon import recurse_status_for_sample
from ngi_pipeline.utils.communication import mail_analysis

import os
import shutil

LOG = minimal_logger(__name__)

def remove_analysis(projectid):
    job_id=None
    with get_session() as db_session:
        job=db_session.query(ProjectAnalysis).filter(ProjectAnalysis.project_id==projectid).one()
        job_id=job.job_id
        db_session.delete(job)
        db_session.commit()
    return job_id
    


@with_ngi_config
def update_charon_with_local_jobs_status(quiet=False, config=None, config_file_path=None):
    jobs=[]
    with get_session() as db_session:
        jobs=db_session.query(ProjectAnalysis).filter(ProjectAnalysis.engine=='rna_ngi').all()

    for job in jobs:
        #check if it's running
        try:
            os.kill(job.job_id, 0)
        except:
            #Process is not running anymore
            exit_code_path=os.path.join(job.project_base_path, "ANALYSIS", job.project_id, 'rna_ngi', 'nextflow_exit_code.out')
            if os.path.isfile(exit_code_path):
                with open(exit_code_path, 'r') as exit_file:
                    exit_code=exit_file.read()
                    if exit_code=='0':
                        update_analysis(job.project_id, True)
                        #clean work dir and merged fastqs
                        nextflow_work_path=os.path.join(job.project_base_path, "ANALYSIS", job.project_id, 'rna_ngi', 'work')
                        shutil.rmtree(nextflow_work_path)
                        merged_path=os.path.join(job.project_base_path, "ANALYSIS", job.project_id, 'rna_ngi', 'fastqs')
                        shutil.rmtree(merged_path)

                    else:
                        update_analysis(job.project_id, False)
            else:
                update_analysis(job.project_id, False)
            with get_session() as db_session:
                db_session.delete(job)
                db_session.commit()
            

            



def update_analysis(project_id, status):
    charon_session=CharonSession()
    mail_analysis(project_id, engine_name='rna_ngi', level='INFO' if status else 'ERROR')
    new_sample_status='ANALYZED' if status else 'FAILED'
    new_seqrun_status='DONE' if status else 'FAILED'
    for sample in charon_session.project_get_samples(project_id).get("samples", {}):
        if sample.get('analysis_status') == "UNDER_ANALYSIS":
            LOG.info("Marking analysis of sample {}/{} as {}".format(project_id, sample.get('sampleid'), new_sample_status))
            charon_session.sample_update(project_id, sample.get('sampleid'), analysis_status=new_sample_status)
            for libprep in charon_session.sample_get_libpreps(project_id, sample.get('sampleid')).get('libpreps', {}):
                if libprep.get('qc') != 'FAILED':
                    for seqrun in charon_session.libprep_get_seqruns(project_id, sample.get('sampleid'),libprep.get('libprepid')).get('seqruns', {}):
                        if seqrun.get('alignment_status')=="RUNNING":
                            LOG.info("Marking analysis of seqrun {}/{}/{}/{} as {}".format(project_id, sample.get('sampleid'),libprep.get('libprepid'), seqrun.get('seqrunid'), new_seqrun_status))
                            charon_session.seqrun_update(project_id, sample.get('sampleid'),libprep.get('libprepid'), seqrun.get('seqrunid'), alignment_status=new_seqrun_status)


@with_ngi_config
def record_project_job(project, job_id, analysis_dir, workflow=None, engine='rna_ngi', run_mode='local', config=None, config_file_path=None):
    with get_session() as db_session:
        project_db_obj=ProjectAnalysis(project_id=project.project_id,
                                        job_id=job_id,
                                        project_name=project.name,
                                        project_base_path=project.base_path,
                                        workflow=workflow,
                                        engine=engine,
                                        analysis_dir=analysis_dir,
                                        run_mode = run_mode)

        db_session.add(project_db_obj)
        db_session.commit()
        sample_status_value = "UNDER_ANALYSIS"
        for sample in project:
            if sample.being_analyzed:
                try:
                    LOG.info('Updating Charon status for project/sample '
                             '{}/{} : {}'.format(project, sample,  sample_status_value))
                    CharonSession().sample_update(projectid=project.project_id,
                                                  sampleid=sample.name,
                                                  analysis_status=sample_status_value)

                    for libprep in sample:
                        if CharonSession().libprep_get(project.project_id, sample.name, libprep.name).get('qc') != "FAILED":
                            for seqrun in libprep:
                                if seqrun.being_analyzed:
                                    CharonSession().seqrun_update(project.project_id, sample.name, libprep.name, seqrun.name, alignment_status="RUNNING")
                except Exception as e:
                    LOG.error("Could not update Charon for sample {}/{} : {}".format(project.project_id, sample.name, e))




