#!/usr/bin/env python

from __future__ import print_function

import importlib

from ngi_pipeline.conductor.classes import NGIProject, NGIAnalysis, get_engine_for_bp, load_engine_module
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.communication import mail_analysis


LOG = minimal_logger(__name__)

@with_ngi_config
def launch_analysis(projects_to_analyze, restart_failed_jobs=False,
                    restart_finished_jobs=False, restart_running_jobs=False,
                    keep_existing_data=False, no_qc=False, exec_mode="sbatch",
                    quiet=False, manual=False, config=None, config_file_path=None,
                    generate_bqsr_bam=False):
    """Launch the appropriate analysis for each fastq file in the project.

    :param list projects_to_analyze: The list of projects (Project objects) to analyze
    :param dict config: The parsed NGI configuration file; optional/has default.
    :param str config_file_path: The path to the NGI configuration file; optional/has default.
    """
    charon_session = CharonSession()
    for project in projects_to_analyze:
        analysis=NGIAnalysis(project=project, restart_failed_jobs=restart_failed_jobs,
                    restart_finished_jobs=restart_finished_jobs,
                    restart_running_jobs=restart_running_jobs,
                    keep_existing_data=keep_existing_data, no_qc=no_qc,
                    exec_mode=exec_mode, quiet=quiet, manual=manual,
                    config=config, config_file_path=config_file_path,
                    generate_bqsr_bam=generate_bqsr_bam, log=LOG)
        #update charon with the current analysis status
        analysis.engine.local_process_tracking.update_charon_with_local_jobs_status(config=config)
        try:
            project_status = charon_session.project_get(project.project_id)['status']
        except CharonError as e:
            LOG.error('Project {} could not be processed: {}'.format(project, e))
            continue
        if not project_status == "OPEN":
            error_text = ('Data found on filesystem for project "{}" but Charon '
                          'reports its status is not OPEN ("{}"). Not launching '
                          'analysis for this project.'.format(project, project_status))
            LOG.error(error_text)
            if not config.get('quiet'):
                mail_analysis(project_name=project.name, level="ERROR", info_text=error_text)
            continue
        try:
            analysis_module = get_engine_for_bp(project)
        except (RuntimeError, CharonError) as e: # BPA missing from Charon?
            LOG.error('Skipping project "{}" because of error: {}'.format(project, e))
            continue
        if not no_qc:
            try:
                qc_analysis_module = load_engine_module("qc", config)
            except RuntimeError as e:
                LOG.error("Could not launch qc analysis: {}".format(e))
        for sample in project:
            # Launch QC analysis
            if not no_qc:
                try:
                    LOG.info('Attempting to launch sample QC analysis '
                             'for project "{}" / sample "{}" / engine '
                             '"{}"'.format(project, sample, qc_analysis_module.__name__))
                    qc_analysis_module.analyze(project=project,
                                               sample=sample,
                                               config=config)
                except Exception as e:
                    error_text = ('Cannot process project "{}" / sample "{}" / '
                                  'engine "{}" : {}'.format(project, sample,
                                                            analysis_module.__name__,
                                                            e))
                    LOG.error(error_text)
                    if not config.get("quiet"):
                        mail_analysis(project_name=project.name, sample_name=sample.name,
                                      engine_name=analysis_module.__name__,
                                      level="ERROR", info_text=e)
            # Launch actual best-practice analysis
        analysis.engine.analyze(analysis)


