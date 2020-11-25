import mock
import unittest

from ngi_pipeline.engines.sarek.database import CharonConnector, CharonError, TrackingConnector
from ngi_pipeline.engines.sarek.exceptions import BestPracticeAnalysisNotSpecifiedError, \
    SampleAnalysisStatusNotFoundError, SampleLookupError, AnalysisStatusForProcessStatusNotFoundError, \
    AlignmentStatusForAnalysisStatusNotFoundError, SampleAnalysisStatusNotSetError, SampleUpdateError, \
    SeqrunUpdateError
from ngi_pipeline.engines.sarek.process import ProcessStopped
from ngi_pipeline.log.loggers import minimal_logger


@mock.patch("ngi_pipeline.database.classes.CharonSession", autospec=True)
class TestCharonConnector(unittest.TestCase):

    CONFIG = {}

    def setUp(self):
        self.log = minimal_logger(__name__, to_file=False, debug=True)
        self.config = TestCharonConnector.CONFIG
        self.project_id = "this-is-a-project-id"
        self.sample_id = "this-is-a-sample-id"
        self.libprep_id = "this-is-a-libprep-id"
        self.libpreps = [
            {"libprepid": "this-is-a-libprep-1"},
            {"libprepid": "this-is-a-libprep-2"},
            {"libprepid": "this-is-a-libprep-3"}]
        self.seqruns = [
            {"seqrunid": "this-is-a-seqrun-1"},
            {"seqrunid": "this-is-a-seqrun-2"},
            {"seqrunid": "this-is-a-seqrun-3"}]

    def _get_charon_connector(self, charon_session):
        self.charon_connector = CharonConnector(self.config, self.log, charon_session=charon_session)

    def test_best_practice_analysis(self, charon_session_mock):
        self._get_charon_connector(charon_session_mock.return_value)
        self.charon_connector.charon_session.project_get.return_value = {}
        with self.assertRaises(BestPracticeAnalysisNotSpecifiedError):
            self.charon_connector.best_practice_analysis(self.project_id)

        expected_bp_analysis = "this-is-a-best-practice-analysis"
        self.charon_connector.charon_session.project_get.return_value = {
            "best_practice_analysis": expected_bp_analysis}
        observed_bp_analysis = self.charon_connector.best_practice_analysis(self.project_id)

        self.assertEqual(expected_bp_analysis, observed_bp_analysis)

    def test_sample_analysis_status(self, charon_session_mock):
        self._get_charon_connector(charon_session_mock.return_value)
        self.charon_connector.charon_session.sample_get.return_value = {}
        with self.assertRaises(SampleAnalysisStatusNotFoundError):
            self.charon_connector.sample_analysis_status(self.project_id, self.sample_id)

        expected_sample_analysis_status = "this-is-a-sample-analysis-status"
        self.charon_connector.charon_session.sample_get.return_value = {
            "analysis_status": expected_sample_analysis_status}
        observed_sample_analysis_status = self.charon_connector.sample_analysis_status(
            self.project_id, self.sample_id)

        self.assertEqual(expected_sample_analysis_status, observed_sample_analysis_status)

    def libpreps_seqruns_helper(self, test_fn, test_args, expected_list, element_key):

        # simplest case, no restricting
        self.assertListEqual(
            expected_list,
            test_fn(*test_args))

        # filtering for a non-existing element should exclude everything
        self.assertListEqual(
            [],
            test_fn(*test_args, restrict_to=["this-element-does-not-exist"]))

        # filtering for existing elements should return only those
        self.assertListEqual(
            expected_list[0:2],
            test_fn(*test_args, restrict_to=map(lambda x: x[element_key], expected_list[0:2])))

        self.assertListEqual(
            expected_list[0:1],
            test_fn(*test_args, restrict_to=map(lambda x: x[element_key], expected_list[0:1])))

    def test_sample_libpreps(self, charon_session_mock):
        self._get_charon_connector(charon_session_mock.return_value)
        self.charon_connector.charon_session.sample_get_libpreps.return_value = {}

        with self.assertRaises(SampleLookupError) as sle:
            self.charon_connector.sample_libpreps(self.project_id, self.sample_id)

        expected_libpreps = self.libpreps
        self.charon_connector.charon_session.sample_get_libpreps.return_value = {"libpreps": expected_libpreps}
        self.libpreps_seqruns_helper(
            self.charon_connector.sample_libpreps, [self.project_id, self.sample_id], expected_libpreps, "libprepid")

    def test_libprep_seqruns(self, charon_session_mock):
        self._get_charon_connector(charon_session_mock.return_value)
        self.charon_connector.charon_session.libprep_get_seqruns.return_value = {}

        with self.assertRaises(SampleLookupError) as sle:
            self.charon_connector.libprep_seqruns(self.project_id, self.sample_id, self.libprep_id)

        expected_seqruns = self.seqruns
        self.charon_connector.charon_session.libprep_get_seqruns.return_value = {"seqruns": expected_seqruns}
        self.libpreps_seqruns_helper(
            self.charon_connector.libprep_seqruns,
            [self.project_id, self.sample_id, self.libprep_id],
            expected_seqruns,
            "seqrunid")

    def test_analysis_status_from_process_status(self, charon_session_mock):
        self._get_charon_connector(charon_session_mock.return_value)
        with self.assertRaises(AnalysisStatusForProcessStatusNotFoundError) as e:
            self.charon_connector.analysis_status_from_process_status(ProcessStopped)

        expected_status = CharonConnector._ANALYSIS_STATUS_FROM_PROCESS_STATUS.values()
        self.assertListEqual(
            expected_status,
            map(
                lambda p: self.charon_connector.analysis_status_from_process_status(p),
                CharonConnector._ANALYSIS_STATUS_FROM_PROCESS_STATUS.keys()))

    def test_alignment_status_from_analysis_status(self, charon_session_mock):
        self._get_charon_connector(charon_session_mock.return_value)
        with self.assertRaises(AlignmentStatusForAnalysisStatusNotFoundError) as e:
            self.charon_connector.alignment_status_from_analysis_status("this-status-does-not-exist")

        expected_status = CharonConnector._ALIGNMENT_STATUS_FROM_ANALYSIS_STATUS.values()
        self.assertListEqual(
            expected_status,
            map(
                lambda p: self.charon_connector.alignment_status_from_analysis_status(p),
                CharonConnector._ALIGNMENT_STATUS_FROM_ANALYSIS_STATUS.keys()))

    def _configure_sample_attribute_update(self, charon_session_mock):
        # set up some mocks
        self._get_charon_connector(charon_session_mock.return_value)
        self.charon_connector.charon_session.sample_get_libpreps.return_value = {"libpreps": self.libpreps}
        self.charon_connector.charon_session.libprep_get_seqruns.return_value = {"seqruns": self.seqruns}
        expected_libpreps = self.libpreps[-1].values()
        expected_seqruns = {lp.values()[0]: self.seqruns[1].values() for lp in self.libpreps}
        return expected_libpreps, expected_seqruns

    def test_set_sample_analysis_status(self, charon_session_mock):
        expected_libpreps, expected_seqruns = self._configure_sample_attribute_update(charon_session_mock)

        analysis_status = "FAILED"
        alignment_status = self.charon_connector.alignment_status_from_analysis_status(analysis_status)
        sample_update_kwargs = {"analysis_status": analysis_status}
        seqrun_update_kwargs = {"alignment_status": alignment_status}

        # set the analysis and alignment status for sample and seqrun, respectively
        update_args = (analysis_status, self.project_id, self.sample_id)
        update_kwargs = {
            "recurse": True,
            "restrict_to_libpreps": expected_libpreps,
            "restrict_to_seqruns": expected_seqruns}
        self.charon_connector.set_sample_analysis_status(*update_args, **update_kwargs)
        self.charon_connector.charon_session.sample_update.assert_called_once_with(
            self.project_id, self.sample_id, **sample_update_kwargs)
        self.charon_connector.charon_session.seqrun_update.assert_called_once_with(
            self.project_id,
            self.sample_id,
            expected_libpreps[0],
            expected_seqruns.values()[0][0],
            **seqrun_update_kwargs)

        # have exceptions raised
        for update_fn in [self.charon_connector.charon_session.sample_update,
                          self.charon_connector.charon_session.seqrun_update]:
            update_fn.side_effect = CharonError("raised CharonError")
            with self.assertRaises(SampleAnalysisStatusNotSetError):
                self.charon_connector.set_sample_analysis_status(*update_args, **update_kwargs)
            update_fn.side_effect = None

    def test_set_sample_attribute(self, charon_session_mock):
        expected_libpreps, expected_seqruns = self._configure_sample_attribute_update(charon_session_mock)
        expected_return_value = "this-is-the-return-value"
        self.charon_connector.charon_session.sample_update = mock.MagicMock()
        self.charon_connector.charon_session.sample_update.return_value = expected_return_value

        sample_update_kwargs = {"sample_attribute": "this-is-the-attribute-value"}
        seqrun_update_kwargs = {"seqrun_attribute": "this-is-the-attribute-value"}
        update_args = (self.project_id, self.sample_id)
        update_kwargs = {
            "recurse": False,
            "sample_update_kwargs": sample_update_kwargs,
            "seqrun_update_kwargs": seqrun_update_kwargs,
            "restrict_to_libpreps": expected_libpreps,
            "restrict_to_seqruns": expected_seqruns}
        # update a sample attribute
        observed_return_value = self.charon_connector.set_sample_attribute(*update_args, **update_kwargs)
        self.assertEqual(
            expected_return_value,
            observed_return_value)
        self.charon_connector.charon_session.seqrun_update.assert_not_called()
        self.charon_connector.charon_session.sample_update.assert_called_once_with(
            *update_args, **sample_update_kwargs)
        self.charon_connector.charon_session.sample_update.reset_mock()

        # update a sample attribute and seqrun recursively
        update_kwargs["recurse"] = True
        observed_return_value = self.charon_connector.set_sample_attribute(*update_args, **update_kwargs)
        self.assertEqual(
            expected_return_value,
            observed_return_value)
        self.charon_connector.charon_session.sample_update.assert_called_once_with(
            *update_args, **sample_update_kwargs)
        self.charon_connector.charon_session.seqrun_update.assert_called_once_with(
            update_args[0],
            update_args[1],
            expected_libpreps[0],
            expected_seqruns.values()[0][0],
            **seqrun_update_kwargs)

        # exception encountered during sample update
        self.charon_connector.charon_session.sample_update.side_effect = CharonError("raised CharonError")
        with self.assertRaises(SampleUpdateError):
            self.charon_connector.set_sample_attribute(
                self.project_id, self.sample_id, sample_update_kwargs=sample_update_kwargs)

        # exception encountered during seqrun update
        self.charon_connector.charon_session.seqrun_update.side_effect = CharonError("raised CharonError")
        with self.assertRaises(SeqrunUpdateError):
            self.charon_connector.set_sample_attribute(
                self.project_id,
                self.sample_id,
                sample_update_kwargs=sample_update_kwargs,
                seqrun_update_kwargs=seqrun_update_kwargs,
                recurse=True)

    def _set_metric_helper(self, charon_session_mock, update_fn, update_attribute, attribute_value):
        self._configure_sample_attribute_update(charon_session_mock)
        update_args = [self.project_id, self.sample_id]
        getattr(self.charon_connector, update_fn)(attribute_value, *update_args)
        self.charon_connector.charon_session.sample_update.assert_called_once_with(
            *update_args,
            **{update_attribute: attribute_value})

    def test_set_sample_duplication(self, charon_session_mock):
        self._set_metric_helper(
            charon_session_mock, "set_sample_duplication", "duplication_pc", 12.45)

    def test_set_sample_autosomal_coverage(self, charon_session_mock):
        self._set_metric_helper(
            charon_session_mock, "set_sample_autosomal_coverage", "total_autosomal_coverage", 30.9)

    def test_set_sample_total_reads(self, charon_session_mock):
        self._set_metric_helper(
            charon_session_mock, "set_sample_total_reads", "total_sequenced_reads", 123456789)

    def test_libprep_qc_status(self, charon_session_mock):
        self._get_charon_connector(charon_session_mock.return_value)
        self.charon_connector.charon_session.libprep_get.return_value = {'qc': 'qc_status'}
        qc_status = self.charon_connector.libprep_qc_status(self.project_id, 
                                                            self.sample_id, 
                                                            self.libprep_id)
        self.assertEqual(qc_status, 'qc_status')

        self.charon_connector.charon_session.libprep_get.return_value = {}
        with self.assertRaises(SampleLookupError):
            qc_status = self.charon_connector.libprep_qc_status(self.project_id,
                                                                self.sample_id,
                                                                self.libprep_id)

    def test_seqrun_alignment_status(self, charon_session_mock):
        self._get_charon_connector(charon_session_mock.return_value)
        self.charon_connector.charon_session.seqrun_get.return_value = {'alignment_status': 'alignment_status'}
        alignment_status = self.charon_connector.seqrun_alignment_status(self.project_id,
                                                                         self.sample_id,
                                                                         self.libprep_id,
                                                                         'seqrun_id')
        self.assertEqual(alignment_status, 'alignment_status')

        self.charon_connector.charon_session.seqrun_get.return_value = {}
        with self.assertRaises(SampleLookupError):
            alignment_status = self.charon_connector.seqrun_alignment_status(self.project_id,
                                                                             self.sample_id,
                                                                             self.libprep_id,
                                                                             'seqrun_id')


class TestTrackingConnector(unittest.TestCase):

    def test_pidfield_from_process_connector_type(self):
        self.assertListEqual(
            TrackingConnector.PIDFIELD_FROM_PROCESS_CONNECTOR_TYPE.values(),
            map(
                lambda c: TrackingConnector.pidfield_from_process_connector_type(c),
                TrackingConnector.PIDFIELD_FROM_PROCESS_CONNECTOR_TYPE.keys()))