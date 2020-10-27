from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.communication import mail_analysis

LOG = minimal_logger(__name__)

@with_ngi_config
def recurse_status_for_sample(project_obj, status_field, status_value, update_done=False,
                              extra_args=None, config=None, config_file_path=None):
    """Set seqruns under sample to have status for field <status_field> to <status_value>
    """

    if not extra_args:
        extra_args = {}
    extra_args.update({status_field: status_value})
    charon_session = CharonSession()
    project_id = project_obj.project_id
    for sample_obj in project_obj:
        # There's only one sample but this is an iterator so we iterate
        sample_id = sample_obj.name
        for libprep_obj in sample_obj:
            libprep_id = libprep_obj.name
            for seqrun_obj in libprep_obj:
                seqrun_id = seqrun_obj.name
                label = "{}/{}/{}/{}".format(project_id, sample_id, libprep_id, seqrun_id)
                LOG.info('Updating status for field "{}" of project/sample/libprep/seqrun '
                         '"{}" to "{}" in Charon '.format(status_field, label, status_value))
                try:
                    charon_session.seqrun_update(projectid=project_id,
                                                 sampleid=sample_id,
                                                 libprepid=libprep_id,
                                                 seqrunid=seqrun_id,
                                                 **extra_args)
                except CharonError as e:
                    error_text = ('Could not update {} for project/sample/libprep/seqrun '
                                  '"{}" in Charon to "{}": {}'.format(status_field,
                                                                      label,
                                                                      status_value,
                                                                      e))
                    LOG.error(error_text)
                    if not config.get('quiet'):
                        mail_analysis(project_name=project_id, sample_name=sample_obj.name,
                                      level="ERROR", info_text=error_text, workflow=status_field)
