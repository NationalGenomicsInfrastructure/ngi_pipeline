import json
import os

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger

LOG = minimal_logger(__name__)


def create_charon_entries_from_project(project, best_practice_analysis="wgs_germline",
                                       sequencing_facility="NGI-S",
                                       pipeline="sarek",
                                       reference="GRCh38",
                                       force_overwrite=False,
                                       retry_on_fail=True):
    """Given a project object, creates the relevant entries in Charon.
    This code is remarkably shoddy as I created it in a hurry and then later
    it became a part of the pipeline. Use at your own risk! Ha ha.

    :param NGIProject project: The NGIProject object
    :param str best_practice_analysis: The workflow to assign for this project (default "wgs_germline")
    :param str sequencing_facility: The facility that did the sequencing
    :param str pipeline: The pipeline to execute for this project (default "sarek")
    :param str reference: The reference genome to use for this project (default "GRCh38")
    :param bool force_overwrite: If this is set to true, overwrite existing entries in Charon (default false)
    """
    charon_session = CharonSession()
    update_failed=False
    try:
        status = "OPEN"
        LOG.info('Creating project "{}" with status "{}", best practice analysis "{}", '
                 'and sequencing_facility {}'.format(project, status,
                                                     best_practice_analysis,
                                                     sequencing_facility))
        charon_session.project_create(projectid=project.project_id,
                                      name=project.name,
                                      status=status,
                                      best_practice_analysis=best_practice_analysis,
                                      sequencing_facility=sequencing_facility,
                                      pipeline=pipeline,
                                      reference=reference)
        LOG.info('Project "{}" created in Charon.'.format(project))
    except CharonError as e:
        if e.status_code == 400:
            if force_overwrite:
                LOG.warning('Overwriting data for project "{}"'.format(project))
                charon_session.project_update(projectid=project.project_id,
                                              name=project.name,
                                              status=status,
                                              best_practice_analysis=best_practice_analysis,
                                              sequencing_facility=sequencing_facility,
                                              pipeline=pipeline,
                                              reference=reference)
                LOG.info('Project "{}" updated in Charon.'.format(project))
            else:
                LOG.info('Project "{}" already exists; moving to samples...'.format(project))
        else:
            raise
    for sample in project:
        try:
            analysis_status = "TO_ANALYZE"
            sample_data_status_value = "STALE"
            LOG.info('Creating sample "{}" with analysis_status "{}"'.format(sample, analysis_status))
            charon_session.sample_create(projectid=project.project_id,
                                         sampleid=sample.name,
                                         analysis_status=analysis_status)
            LOG.info('Project/sample "{}/{}" created in Charon.'.format(project, sample))
        except CharonError as e:
            if e.status_code == 400:
                if force_overwrite:
                    LOG.warning('Overwriting data for project "{}" / '
                             'sample "{}"'.format(project, sample))
                    charon_session.sample_update(projectid=project.project_id,
                                                 sampleid=sample.name,
                                                 analysis_status=analysis_status,
                                                 status=sample_data_status_value)
                    LOG.info('Project/sample "{}/{}" updated in Charon.'.format(project, sample))
                else:
                    #update the status of the sample to STALE
                    charon_session.sample_update(projectid=project.project_id,
                                                 sampleid=sample.name,
                                                 status=sample_data_status_value)
                    LOG.info('Project "{}" / sample "{}" already exists; moving '
                             'to libpreps'.format(project, sample))
            else:
                update_failed=True
                LOG.error(e)
                continue
        for libprep in sample:
            try:
                qc = "PASSED"
                LOG.info('Creating libprep "{}" with qc status "{}"'.format(libprep, qc))
                charon_session.libprep_create(projectid=project.project_id,
                                              sampleid=sample.name,
                                              libprepid=libprep.name,
                                              qc=qc)
                LOG.info(('Project/sample/libprep "{}/{}/{}" created in '
                          'Charon').format(project, sample, libprep))
            except CharonError as e:
                if e.status_code == 400:
                    if force_overwrite:
                        LOG.warning('Overwriting data for project "{}" / '
                                 'sample "{}" / libprep "{}"'.format(project, sample,
                                                                     libprep))
                        charon_session.libprep_update(projectid=project.project_id,
                                                      sampleid=sample.name,
                                                      libprepid=libprep.name,
                                                      qc=qc)
                        LOG.info(('Project/sample/libprep "{}/{}/{}" updated in '
                                  'Charon').format(project, sample, libprep))
                    else:
                        LOG.debug(e)
                        LOG.info('Project "{}" / sample "{}" / libprep "{}" already '
                                 'exists; moving to libpreps'.format(project, sample, libprep))
                else:
                    update_failed=True
                    LOG.error(e)
                    continue
            for seqrun in libprep:
                try:
                    alignment_status="NOT_RUNNING"
                    LOG.info('Creating seqrun "{}" with alignment_status "{}"'.format(seqrun, alignment_status))
                    charon_session.seqrun_create(projectid=project.project_id,
                                                 sampleid=sample.name,
                                                 libprepid=libprep.name,
                                                 seqrunid=seqrun.name,
                                                 alignment_status=alignment_status,
                                                 total_reads=0,
                                                 mean_autosomal_coverage=0)
                    LOG.info(('Project/sample/libprep/seqrun "{}/{}/{}/{}" '
                              'created in Charon').format(project, sample,
                                                          libprep, seqrun))
                except CharonError as e:
                    if e.status_code == 400:
                        if force_overwrite:
                            LOG.warning('Overwriting data for project "{}" / '
                                     'sample "{}" / libprep "{}" / '
                                     'seqrun "{}"'.format(project, sample,
                                                          libprep, seqrun))
                            charon_session.seqrun_update(projectid=project.project_id,
                                                         sampleid=sample.name,
                                                         libprepid=libprep.name,
                                                         seqrunid=seqrun.name,
                                                         alignment_status=alignment_status,
                                                         total_reads=0,
                                                         mean_autosomal_coverage=0)
                            LOG.info(('Project/sample/libprep/seqrun "{}/{}/{}/{}" '
                                      'updated in Charon').format(project, sample,
                                                                  libprep, seqrun))
                        else:
                            LOG.info('Project "{}" / sample "{}" / libprep "{}" / '
                                     'seqrun "{}" already exists; next...'.format(project, sample,
                                                                                  libprep, seqrun))
                    else:
                        update_failed=True
                        LOG.error(e)
                        continue

    if update_failed:
        if retry_on_fail:
            create_charon_entries_from_project(
                project, best_practice_analysis=best_practice_analysis,
                sequencing_facility=sequencing_facility, force_overwrite=force_overwrite,
                retry_on_fail=False)
        else:
            raise CharonError("A network error blocks Charon updating.")
