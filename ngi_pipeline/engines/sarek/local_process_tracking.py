import os

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.engines.sarek.database import CharonConnector, TrackingConnector
from ngi_pipeline.engines.sarek.models.sarek import SarekAnalysis
from ngi_pipeline.engines.sarek.models.sample import SarekAnalysisSample
from ngi_pipeline.engines.sarek.process import JobStatus, ProcessStatus, ProcessRunning, ProcessExitStatusSuccessful
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config


@with_ngi_config
def update_charon_with_local_jobs_status(
        config=None, log=None, tracking_connector=None, charon_connector=None, **kwargs):
    """
    Update Charon with the local changes in the SQLite tracking database.

    :param config: optional dict with configuration options. If not specified, the global configuration will be used
    instead
    :param log: optional log instance. If not specified, a new log instance will be created
    :param tracking_connector: optional connector to the tracking database. If not specified,
    a new connector will be created
    :param charon_connector: optional connector to the charon database. If not specified, a new connector will be
    created
    :param kwargs: placeholder for additional, unused options
    :return: None
    """
    log = log or minimal_logger(__name__, debug=True)

    tracking_connector = tracking_connector or TrackingConnector(config, log)
    charon_connector = charon_connector or CharonConnector(config, log)
    log.debug("updating Charon status for locally tracked jobs")
    # iterate over the analysis processes tracked in the local database
    for analysis in tracking_connector.tracked_analyses():
        try:
            log.debug("checking status for analysis of {}:{} with {}:{}, having {}".format(
                analysis.project_id,
                analysis.sample_id,
                analysis.engine,
                analysis.workflow,
                "pid {}".format(analysis.process_id) if analysis.process_id is not None else
                "sbatch job id {}".format(analysis.slurm_job_id)))
            # create an AnalysisTracker instance for the analysis
            analysis_tracker = AnalysisTracker(analysis, charon_connector, tracking_connector, log, config)
            # recreate the analysis_sample from disk/analysis
            analysis_tracker.recreate_analysis_sample()
            # poll the system for the analysis status
            analysis_tracker.get_analysis_status()
            # set the analysis status
            analysis_tracker.report_analysis_status()
            # set the analysis results
            analysis_tracker.report_analysis_results()
            # remove the analysis entry from the local db
            analysis_tracker.remove_analysis()
            # do cleanup
            analysis_tracker.cleanup()
        except Exception as e:
            log.error("exception raised when processing sample {} in project {}, please review: {}".format(
                analysis.sample_id, analysis.project_id, e))


class AnalysisTracker(object):
    """
    AnalysisTracker is a convenience class for operations related to checking the status of an analysis tracked
    in the local database and syncing the status and result metrics to the Charon database.
    """

    def __init__(self, analysis_entry, charon_connector, tracking_connector, log, config=None):
        """
        Create an AnalysisTracker instance

        :param analysis_entry: an analysis object
        (this is an instance of ngi_pipeline.engines.sarek.database.TrackingConnector._SampleAnalysis)
        :param charon_connector: a charon connector object
        :param tracking_connector: a tracking connector object
        :param log: a log instance
        :param config: optional dict with configuration options
        """
        self.analysis_entry = analysis_entry
        self.charon_connector = charon_connector
        self.tracking_connector = tracking_connector
        self.log = log
        self.config = config
        self.analysis_sample = None
        self.process_status = None

    def recreate_analysis_sample(self):
        """
        Recreate a SarekAnalysisSample instance for this analysis. This will also recreate a SarekAnalysis instance
        attached to the SarekanalysisSample instance. Behind the scenes, the structure of the sample input files will
        be parsed from the analysis tsv file.

        This method should be called before any other instance methods. After this method has been called, the
        `analysis_sample` attribute will be set.

        :return: None
        """
        # get an analysis instance representing the workflow
        analysis_instance = SarekAnalysis.get_analysis_instance_for_workflow(
            self.analysis_entry.workflow,
            self.config,
            self.log,
            charon_connector=self.charon_connector,
            tracking_connector=self.tracking_connector)
        # recreate a NGIProject object from the analysis
        project_obj = self.recreate_project_from_analysis(analysis_instance)
        # extract the sample object corresponding to the analysis entry
        sample_obj = list(filter(lambda x: x.name == self.analysis_entry.sample_id, project_obj)).pop()
        self.analysis_sample = SarekAnalysisSample(project_obj, sample_obj, analysis_instance)

    def recreate_project_from_analysis(self, analysis_instance):
        """
        Recreate a NGIProject object based on the analysis tsv file for this analysis.

        :param analysis_instance: a SarekAnalysis instance
        :return: a NGIProject object recreated from the information in the tsv file
        """
        tsv_file_path = analysis_instance.sample_analysis_tsv_file(
            self.analysis_entry.project_base_path, self.analysis_entry.project_id, self.analysis_entry.sample_id)
        runid_and_fastq_file_paths = analysis_instance.runid_and_fastq_files_from_tsv_file(tsv_file_path)
        # fetch just the fastq file paths
        fastq_file_paths = [
            fastq_path for runid_and_paths in runid_and_fastq_file_paths for fastq_path in runid_and_paths[1:]]
        return self._project_from_fastq_file_paths(fastq_file_paths)

    @staticmethod
    def _project_from_fastq_file_paths(fastq_file_paths):
        """
        recreate the project object from a list of fastq file paths
        :param fastq_file_paths: list of fastq file paths, expected to be arranged in subfolders according to
        [/]path/to/project name/sample name/libprep name/seqrun name/fastq_file_name.fastq.gz

        :return: a ngi_pipeline.conductor.classes.NGIProject object recreated from the directory tree and fastq files
        """
        project_obj = None
        for fastq_file_path in fastq_file_paths:
            seqrun_path, fastq_file_name = os.path.split(fastq_file_path)
            libprep_path, seqrun_name = os.path.split(seqrun_path)
            sample_path, libprep_name = os.path.split(libprep_path)
            project_path, sample_name = os.path.split(sample_path)
            project_data_path, project_name = os.path.split(project_path)
            project_base_path = os.path.dirname(project_data_path)

            project_obj = project_obj or NGIProject(project_name, project_name, project_name, project_base_path)
            sample_obj = project_obj.add_sample(sample_name, sample_name)
            libprep_obj = sample_obj.add_libprep(libprep_name, libprep_name)
            seqrun_obj = libprep_obj.add_seqrun(seqrun_name, seqrun_name)
            seqrun_obj.add_fastq_files(fastq_file_name)
        return project_obj

    def get_libpreps_and_seqruns(self):
        """
        Get the libpreps and seqruns included in this analysis. This information comes from the NGIProject object, i.e.
        it is recreated from the tsv file that was used to start this analysis.

        :return: a dict with the libprep ids as keys and the list of seqrun ids for each libprep as values
        """
        return {
            libprepid: self.analysis_sample.libprep_seqrun_ids(libprepid)
            for libprepid in self.analysis_sample.sample_libprep_ids()}

    def get_analysis_status(self):
        """
        Figure out the status of this analysis. If the process is not running, the exit code written to the
        exit code file will be checked.

        This method will set the `process_status` attribute to a subclass of ProcessStatus representing the status of
        this analysis.

        :return: None
        """
        status_type = ProcessStatus if self.analysis_entry.process_id is not None else JobStatus
        processid_or_jobid = self.analysis_entry.process_id or self.analysis_entry.slurm_job_id
        exit_code_path = self.analysis_sample.sample_analysis_exit_code_path()
        self.process_status = status_type.get_type_from_processid_and_exit_code_path(processid_or_jobid, exit_code_path)

    def report_analysis_status(self):
        """
        Update Charon with the analysis status for the sample in this analysis and the corresponding alignment status
        for the seqruns under the sample.

        :return: None
        """
        # set the status in charon, recursing into libpreps and seqruns
        libpreps_and_seqruns = self.get_libpreps_and_seqruns()
        self.log.debug("setting analysis status in charon to match '{}' for '{}' - '{}' - '{}' - '{}'".format(
            self.process_status,
            self.analysis_sample.projectid,
            self.analysis_sample.sampleid,
            ",".join(libpreps_and_seqruns.keys()),
            ",".join(set([seqrun for seqruns in libpreps_and_seqruns.values() for seqrun in seqruns]))
        ))
        self.charon_connector.set_sample_analysis_status(
            self.charon_connector.analysis_status_from_process_status(
                self.process_status),
            self.analysis_sample.projectid,
            self.analysis_sample.sampleid,
            recurse=True,
            restrict_to_libpreps=libpreps_and_seqruns.keys(),
            restrict_to_seqruns=libpreps_and_seqruns)

    def report_analysis_results(self):
        """
        Collect the analysis metrics from the SarekAnalysis instance and update the sample in Charon with the metrics.
        If the status of this analysis is not ProcessExitStatusSuccessful, this method does nothing.

        :return: None
        """
        # only report the analysis results if the process exited successfully
        if self.process_status != ProcessExitStatusSuccessful:
            return

        analysis_metrics = self.analysis_sample.analysis_object.collect_analysis_metrics(self.analysis_sample)
        self.log.debug("setting analysis metrics {} in charon for sample '{}' in project '{}'".format(
            analysis_metrics, self.analysis_sample.sampleid, self.analysis_sample.projectid))
        # for convenience, map the metrics to the corresponding setter in the charon_connector
        for metric, setter in {
            "total_reads": "set_sample_total_reads",
            "autosomal_coverage": "set_sample_autosomal_coverage",
            "percent_duplication": "set_sample_duplication"
        }.items():
            # set the status in charon, skip recursing into libpreps and seqruns
            self.log.debug("setting {} to {} with {}".format(metric, analysis_metrics[metric], setter))
            getattr(
                self.charon_connector,
                setter)(
                analysis_metrics[metric],
                self.analysis_sample.projectid,
                self.analysis_sample.sampleid)

    def remove_analysis(self, force=False):
        """
        Remove the analysis from the tracking database iff the process status is not ProcessRunning or the removal
        should be forced.

        :param force: if True, remove this analysis even if the process is still running (default is False)
        :return: None
        """
        msg = "removing from local tracking db"
        # only remove the analysis if the process is not running or it should be forced
        if self.process_status != ProcessRunning or force:
            self.tracking_connector.remove_analysis(self.analysis_entry)
        else:
            msg = "{} skipped".format(msg)
        self.log.debug(msg)

    def cleanup(self):
        # only cleanup if the process exited successfully
        if self.process_status != ProcessExitStatusSuccessful:
            return

        self.log.info("analysis of sample {} finished successfully, removing temporary work directory".format(
            self.analysis_sample.sampleid))
        self.analysis_sample.analysis_object.cleanup(self.analysis_sample)
