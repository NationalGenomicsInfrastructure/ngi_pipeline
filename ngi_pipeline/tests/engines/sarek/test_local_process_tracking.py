import mock
import os
import unittest

from ngi_pipeline.engines.sarek.local_process_tracking import AnalysisTracker
from ngi_pipeline.engines.sarek.database import TrackingConnector
from ngi_pipeline.engines.sarek.models.sample import SarekAnalysisSample
from ngi_pipeline.engines.sarek.models.sarek import SarekAnalysis
from ngi_pipeline.engines.sarek.process import ProcessRunning, ProcessStopped, ProcessExitStatusSuccessful, \
    ProcessExitStatusUnknown, ProcessExitStatusFailed
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.tests.engines.sarek.test_launchers import TestLaunchers


@mock.patch("ngi_pipeline.engines.sarek.database.TrackingConnector", autospec=True)
@mock.patch("ngi_pipeline.engines.sarek.database.CharonConnector", autospec=True)
class TestAnalysisTracker(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cls.log = minimal_logger(__name__, debug=True)

    def setUp(self):
        self.project_obj = TestLaunchers.get_NGIProject("1")
        self.process_id = 12345
        self.slurm_job_id = 98765
        self.seqruns = {
            "libprep-A": ["seqrun-1", "seqrun-2"],
            "libprep-B": ["seqrun-1", "seqrun-3"],
            "libprep-C": []}

    def get_analysis_entry(self):
        analysis_entry = TrackingConnector._SampleAnalysis()
        analysis_entry.project_id = self.project_obj.project_id
        analysis_entry.project_base_path = self.project_obj.base_path
        analysis_entry.sample_id = list(self.project_obj.samples.keys())[0]
        analysis_entry.workflow = "SarekGermlineAnalysis"
        return analysis_entry

    def get_tracker_instance(self, charon_connector_mock, tracking_connector_mock):
        tracker = AnalysisTracker(self.get_analysis_entry(), charon_connector_mock, tracking_connector_mock, self.log)
        analysis_sample = mock.Mock(spec=SarekAnalysisSample)
        analysis_sample.analysis_object = mock.Mock(spec=SarekAnalysis)
        analysis_sample.projectid = tracker.analysis_entry.project_id
        analysis_sample.sampleid = tracker.analysis_entry.sample_id
        tracker.analysis_sample = analysis_sample
        return tracker

    def test_recreate_analysis_sample(self, *mocks):
        tracker = self.get_tracker_instance(*mocks)
        with mock.patch(
                "ngi_pipeline.engines.sarek.local_process_tracking.SarekAnalysis.get_analysis_instance_for_workflow"
        ) as sarek_factory_mock, mock.patch.object(
            tracker, "recreate_project_from_analysis", return_value=self.project_obj
        ):
            sarek_factory_mock.return_value = tracker.analysis_sample.analysis_object
            tracker.recreate_analysis_sample()
            self.assertIsInstance(tracker.analysis_sample, SarekAnalysisSample)
            self.assertEqual(self.project_obj.project_id, tracker.analysis_sample.projectid)
            self.assertEqual(self.project_obj.base_path, tracker.analysis_sample.project_base_path)
            self.assertIn(tracker.analysis_sample.sampleid, self.project_obj.samples)

    def test_get_libpreps_and_seqruns(self, *mocks):
        def _seqruns_for_libprep(libprep):
            return self.seqruns[libprep]

        tracker = self.get_tracker_instance(*mocks)
        tracker.analysis_sample.sample_libprep_ids.return_value = list(self.seqruns.keys())
        tracker.analysis_sample.libprep_seqrun_ids.side_effect = _seqruns_for_libprep

        observed_libpreps_and_seqruns = tracker.get_libpreps_and_seqruns()
        self.assertDictEqual(self.seqruns, observed_libpreps_and_seqruns)

    def test__project_from_fastq_file_paths(self, *mocks):
        fastq_files = []
        for sample_obj in self.project_obj:
            for libprep_obj in sample_obj:
                for seqrun_obj in libprep_obj:
                    fastq_files.extend(
                        map(
                            lambda f: os.path.join(
                                self.project_obj.base_path,
                                "DATA",
                                self.project_obj.dirname,
                                sample_obj.dirname,
                                libprep_obj.dirname,
                                seqrun_obj.dirname,
                                f),
                            seqrun_obj.fastq_files))
        observed_project_obj = AnalysisTracker._project_from_fastq_file_paths(fastq_files)
        self.assertEqual(self.project_obj, observed_project_obj)

    def helper_analysis_status(self, process_mock, *mocks, **kwargs):
        tracker = self.get_tracker_instance(*mocks)
        for key, val in kwargs.items():
            setattr(tracker.analysis_entry, key, val)

        expected_exit_code_path = "this-is-a-path"
        tracker.analysis_sample.sample_analysis_exit_code_path.return_value = expected_exit_code_path

        expected_status = ProcessRunning
        process_mock.return_value = expected_status
        tracker.get_analysis_status()
        self.assertEqual(
            expected_status,
            tracker.process_status)
        process_mock.assert_called_once_with(list(kwargs.values())[0], expected_exit_code_path)

    def test_get_analysis_status_process(self, *mocks):
        with mock.patch(
            "ngi_pipeline.engines.sarek.local_process_tracking.ProcessStatus.get_type_from_processid_and_exit_code_path"
        ) as process_mock:
            self.helper_analysis_status(process_mock, *mocks, process_id=self.process_id)

    def test_get_analysis_status_job(self, *mocks):
        with mock.patch(
            "ngi_pipeline.engines.sarek.local_process_tracking.JobStatus.get_type_from_processid_and_exit_code_path"
        ) as job_mock:
            self.helper_analysis_status(job_mock, *mocks, slurm_job_id=self.slurm_job_id)

    def test_report_analysis_status(self, *mocks):
        tracker = self.get_tracker_instance(*mocks)
        tracker.process_status = ProcessStopped
        with mock.patch.object(tracker, "get_libpreps_and_seqruns") as seqrun_getter:
            seqrun_getter.return_value = self.seqruns
            tracker.report_analysis_status()
            tracker.charon_connector.analysis_status_from_process_status.assert_called_once_with(ProcessStopped)
            tracker.charon_connector.set_sample_analysis_status.assert_called_once()
            call_args, call_kwargs = tracker.charon_connector.set_sample_analysis_status.call_args
            for arg in [tracker.analysis_sample.projectid, tracker.analysis_sample.sampleid]:
                self.assertIn(arg, call_args)
            expected_kwargs = {
                "recurse": True,
                "restrict_to_libpreps": list(self.seqruns.keys()),
                "restrict_to_seqruns": self.seqruns
            }
            self.assertDictEqual(expected_kwargs, call_kwargs)

    @mock.patch("ngi_pipeline.engines.sarek.models.sarek.SarekAnalysis", autospec=True)
    @mock.patch("ngi_pipeline.engines.sarek.models.sarek.SarekAnalysisSample", autospec=True)
    def test_report_analysis_results(self, analysis_sample_mock, analysis_mock, *mocks):
        tracker = self.get_tracker_instance(*mocks)
        # set up some mocks
        expected_metrics = {
            "percent_duplication": 25.3,
            "autosomal_coverage": 35.8,
            "total_reads": 123456789
        }
        tracker.analysis_sample.analysis_object.collect_analysis_metrics.return_value = expected_metrics

        # if the exit status is not successful, results should not be reported
        tracker.process_status = ProcessExitStatusUnknown
        tracker.report_analysis_results()
        tracker.analysis_sample.analysis_object.collect_analysis_metrics.assert_not_called()

        # assert the expected setter methods are called
        tracker.process_status = ProcessExitStatusSuccessful
        tracker.report_analysis_results()
        expected_setters = {"total_reads": tracker.charon_connector.set_sample_total_reads,
                            "autosomal_coverage": tracker.charon_connector.set_sample_autosomal_coverage,
                            "percent_duplication": tracker.charon_connector.set_sample_duplication}
        for metric, setter in expected_setters.items():
            setter.assert_called_once_with(
                expected_metrics[metric],
                tracker.analysis_sample.projectid,
                tracker.analysis_sample.sampleid)

    def test_remove_analysis(self, *mocks):
        tracker = self.get_tracker_instance(*mocks)

        # do not remove running processes unless forced
        tracker.process_status = ProcessRunning
        tracker.remove_analysis()
        tracker.tracking_connector.remove_analysis.assert_not_called()
        tracker.remove_analysis(force=True)
        tracker.tracking_connector.remove_analysis.assert_called_once_with(tracker.analysis_entry)

        # all other process states should be removed
        for status in [ProcessStopped, ProcessExitStatusUnknown, ProcessExitStatusSuccessful, ProcessExitStatusFailed]:
            tracker.tracking_connector.remove_analysis.reset_mock()
            tracker.process_status = status
            tracker.remove_analysis()
            tracker.tracking_connector.remove_analysis.assert_called_once_with(tracker.analysis_entry)

    def test_cleanup(self, *mocks):
        tracker = self.get_tracker_instance(*mocks)
        cleanup_fn = tracker.analysis_sample.analysis_object.cleanup

        # do not cleanup for processes that have not finished successfully
        for status in [ProcessStopped, ProcessExitStatusUnknown, ProcessRunning, ProcessExitStatusFailed]:
            tracker.process_status = status
            tracker.cleanup()
            cleanup_fn.assert_not_called()

        # do cleanup if process has finished successfully
        tracker.process_status = ProcessExitStatusSuccessful
        tracker.cleanup()
        cleanup_fn.assert_called_once_with(tracker.analysis_sample)
