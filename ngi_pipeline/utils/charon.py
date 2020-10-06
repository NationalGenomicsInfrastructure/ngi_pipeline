import collections
import re

from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.communication import mail_analysis

LOG = minimal_logger(__name__)


def reset_charon_records_by_object(project_obj):
    charon_session = CharonSession()
    LOG.info("Resetting Charon record for project {}".format(project_obj))
    charon_session.project_reset(projectid=project_obj.project_id)
    LOG.info("Charon record for project {} reset".format(project_obj))
    for sample_obj in project_obj:
        LOG.info("Resetting Charon record for project/sample {}/{}".format(project_obj,
                                                                           sample_obj))
        try:
            charon_session.sample_reset(projectid=project_obj.project_id,
                                        sampleid=sample_obj.name)
            LOG.info("Charon record for project/sample {}/{} reset".format(project_obj,
                                                                           sample_obj))
        except CharonError as e:
            LOG.error("Unable to reset Charon record for project/sample {}/{}: "
                      "{}".format(project_obj, sample_obj, e))
        for libprep_obj in sample_obj:
            LOG.info("Resetting Charon record for project/sample"
                     "libprep {}/{}/{}".format(project_obj, sample_obj, libprep_obj))
            try:
                charon_session.libprep_reset(projectid=project_obj.project_id,
                                             sampleid=sample_obj.name,
                                             libprepid=libprep_obj.name)
                LOG.info("Charon record for project/sample/libprep {}/{}/{} "
                         "reset".format(project_obj, sample_obj, libprep_obj))
            except CharonError as e:
                LOG.error("Unable to reset Charon record for project/sample/libprep "
                          "{}/{}/{}: {}".format(project_obj, sample_obj,
                                                libprep_obj, e))
            for seqrun_obj in libprep_obj:
                LOG.info("Resetting Charon record for project/sample/libprep/"
                         "seqrun {}/{}/{}/{}".format(project_obj, sample_obj,
                                                     libprep_obj, seqrun_obj))
                try:
                    charon_session.seqrun_reset(projectid=project_obj.project_id,
                                                sampleid=sample_obj.name,
                                                libprepid=libprep_obj.name,
                                                seqrunid=seqrun_obj.name)
                    LOG.info("Charon record for project/sample/libprep/seqrun "
                             "{}/{}/{}/{} reset".format(project_obj, sample_obj,
                                                        libprep_obj, seqrun_obj))
                except CharonError as e:
                    LOG.error("Unable to reset Charon record for project/sample/"
                              "libprep/seqrun {}/{}/{}/{}: {}".format(project_obj,
                                                                      sample_obj,
                                                                      libprep_obj,
                                                                      seqrun_obj,
                                                                      e))

def reset_charon_records_by_name(project_id, restrict_to_samples=None,
                                 restrict_to_libpreps=None, restrict_to_seqruns=None):
    if not restrict_to_samples: restrict_to_samples = []
    if not restrict_to_libpreps: restrict_to_libpreps = []
    if not restrict_to_seqruns: restrict_to_seqruns = []
    charon_session = CharonSession()
    LOG.info("Resetting Charon record for project {}".format(project_id))
    charon_session.project_reset(projectid=project_id)
    LOG.info("Charon record for project {} reset".format(project_id))
    for sample in charon_session.project_get_samples(projectid=project_id).get('samples', []):
        sample_id = sample['sampleid']
        if restrict_to_samples and sample_id not in restrict_to_samples:
            LOG.info("Skipping project/sample {}/{}: not in list of samples to use "
                     "({})".format(project_id, sample_id, ", ".join(restrict_to_samples)))
            continue
        LOG.info("Resetting Charon record for project/sample {}/{}".format(project_id,
                                                                           sample_id))
        charon_session.sample_reset(projectid=project_id, sampleid=sample_id)
        LOG.info("Charon record for project/sample {}/{} reset".format(project_id,
                                                                       sample_id))
        for libprep in charon_session.sample_get_libpreps(projectid=project_id,
                                                          sampleid=sample_id).get('libpreps', []):
            libprep_id = libprep['libprepid']
            if restrict_to_libpreps and libprep_id not in restrict_to_libpreps:
                LOG.info("Skipping project/sample/libprep {}/{}/{}: not in list "
                         "of libpreps to use ({})".format(project_id, sample_id,
                                                          libprep_id, ", ".join(restrict_to_libpreps)))
                continue
            LOG.info("Resetting Charon record for project/sample"
                     "libprep {}/{}/{}".format(project_id, sample_id, libprep_id))
            charon_session.libprep_reset(projectid=project_id, sampleid=sample_id,
                                         libprepid=libprep_id)
            LOG.info("Charon record for project/sample/libprep {}/{}/{} "
                     "reset".format(project_id, sample_id, libprep_id))
            for seqrun in charon_session.libprep_get_seqruns(projectid=project_id,
                                                             sampleid=sample_id,
                                                             libprepid=libprep_id).get('seqruns', []):
                seqrun_id = seqrun['seqrunid']
                if restrict_to_seqruns and seqrun_id not in restrict_to_seqruns:
                    LOG.info("Skipping project/sample/libprep/seqrun {}/{}/{}/{}: "
                             "not in list of seqruns to use ({})".format(project_id,
                                                                         sample_id,
                                                                         libprep_id,
                                                                         seqrun_id,
                                                                         ", ".join(restrict_to_seqruns)))
                    continue
                LOG.info("Resetting Charon record for project/sample/libprep/"
                         "seqrun {}/{}/{}/{}".format(project_id, sample_id,
                                                     libprep_id, seqrun_id))
                charon_session.seqrun_reset(projectid=project_id, sampleid=sample_id,
                                            libprepid=libprep_id, seqrunid=seqrun_id)
                LOG.info("Charon record for project/sample/libprep/seqrun "
                         "{}/{}/{}/{} reset".format(project_id, sample_id,
                                                    libprep_id, seqrun_id))


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
