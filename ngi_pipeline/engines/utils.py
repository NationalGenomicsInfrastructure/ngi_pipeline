
from ngi_pipeline.utils.communication import mail_analysis
from ngi_pipeline.log.loggers import minimal_logger

LOG = minimal_logger(__name__)

def handle_seqrun_status(analysis_object, seqrun, charon_reported_status):
    if charon_reported_status == 'RUNNING' and not analysis_object.restart_running_jobs:
        LOG.info("seqrun {}/{}/{}/{} is being analyzed and no restart_running flag was given, skipping.".format(analysis_object.project.project_id, sample.name, libprep.name, seqrun.name))
        return False
    elif charon_reported_status == 'DONE' and not analysis_object.restart_finished_jobs:
        LOG.info("seqrun {}/{}/{}/{} has been analyzed and no restart_analyzed flag was given, skipping.".format(analysis_object.project.project_id, sample.name, libprep.name, seqrun.name))
        return False
    elif charon_reported_status == 'FAILED' and not analysis_object.restart_failed_jobs:
        LOG.info("seqrun {}/{}/{}/{} analysis has failed, but no restart_failed flag was given, skipping.".format(analysis_object.project.project_id, sample.name, libprep.name, seqrun.name))
        return False
    else:
        return True

def handle_libprep_status(analysis_object, libprep, charon_reported_status):
    if charon_reported_status == 'FAILED':
        LOG.info("libprep {}/{}/{} is marked as failed, skipping all of its seqruns.".format(analysis_object.project.project_id, sample.name, libprep.name))
        return False
    else:
        return True

def handle_sample_status(analysis_object, sample, charon_reported_status):
    """ returns true of false wether the sample should be analyzed"""
    if charon_reported_status == "UNDER_ANALYSIS":
        if not analysis_object.restart_running_jobs:
            error_text = ('Charon reports seqrun analysis for project "{}" '
                          '/ sample "{}" does not need processing (already '
                          '"{}")'.format(analysis_object.project, sample, charon_reported_status))
            LOG.error(error_text)
            if not analysis_object.config.get('quiet'):
                mail_analysis(project_name=analysis_object.project.name, sample_name=sample.name,
                              engine_name=analysis_module.__name__,
                              level="ERROR", info_text=error_text)
            return False 
        else:
            return True
    elif charon_reported_status == "ANALYZED":
        if not analysis_object.restart_finished_jobs:
            error_text = ('Charon reports seqrun analysis for project "{}" '
                          '/ sample "{}" does not need processing (already '
                          '"{}")'.format(analysis_object.project, sample, charon_reported_status))
            LOG.error(error_text)
            if not analysis_object.config.get('quiet') and not analysis_object.config.get('manual'):
                mail_analysis(project_name=analysis_object.project.name, sample_name=sample.name,
                              engine_name=analysis_module.__name__,
                              level="ERROR", info_text=error_text)
            return False 
        else:
            return True
    elif charon_reported_status == "FAILED":
        if not analysis_object.restart_failed_jobs:
            error_text = ('FAILED:  Project "{}" / sample "{}" Charon reports '
                          'FAILURE, manual investigation needed!'.format(analysis_object.project, sample))
            LOG.error(error_text)
            if not analysis_object.config.get('quiet'):
                mail_analysis(project_name=analysis_object.project.name, sample_name=sample.name,
                              engine_name=analysis_module.__name__,
                              level="ERROR", info_text=error_text)
            return False 
        else:
            return True
    else:
        return True
