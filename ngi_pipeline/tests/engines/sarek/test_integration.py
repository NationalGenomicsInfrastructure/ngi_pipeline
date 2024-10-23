import mock
import os
import shutil
import tempfile
import unittest

from ngi_pipeline.conductor.launchers import launch_analysis
from ngi_pipeline.tests.engines.sarek.test_launchers import TestLaunchers
from ngi_pipeline.utils.filesystem import recreate_project_from_filesystem


class TestIntegration(unittest.TestCase):
    RUNFOLDER_NAME = "180223_A00123_0012_AABC123DXX"

    @staticmethod
    def create_data_file_structure(project_objs, data_path):
        for project_obj in project_objs:
            project_obj.base_path = os.path.dirname(data_path)
            project_path = os.path.join(data_path, project_obj.dirname)
            for sample_obj in project_obj:
                sample_path = os.path.join(project_path, sample_obj.dirname)
                for libprep_obj in sample_obj:
                    libprep_path = os.path.join(sample_path, libprep_obj.dirname)
                    for seqrun_obj in libprep_obj:
                        seqrun_path = os.path.join(libprep_path, seqrun_obj.dirname)
                        os.makedirs(seqrun_path)
                        for fastq_file in seqrun_obj.fastq_files:
                            open(os.path.join(seqrun_path, fastq_file), "w").close()

    def setUp(self):
        self.config = TestLaunchers.CONFIG
        self.testdir = tempfile.mkdtemp(prefix="sarek_integration_test_")
        self.datadir = os.path.join(self.testdir, "DATA")
        self.projects = [TestLaunchers.get_NGIProject(1)]
        TestIntegration.create_data_file_structure(self.projects, self.datadir)

    def tearDown(self):
        shutil.rmtree(self.testdir, ignore_errors=True)

    def test_recreate_project_from_filesystem(self):
        project_obj = self.projects[0]
        recreated_project = recreate_project_from_filesystem(
            project_dir=os.path.join(self.datadir, project_obj.dirname)
        )
        self.assertEqual(project_obj, recreated_project)

    @mock.patch("ngi_pipeline.conductor.launchers.CharonSession", autospec=True)
    @mock.patch("ngi_pipeline.conductor.classes.CharonSession", autospec=True)
    @mock.patch(
        "ngi_pipeline.engines.sarek."
        "local_process_tracking.update_charon_with_local_jobs_status",
        autospec=True,
    )
    def test_launch_analysis(
        self,
        process_tracking_mock,
        charon_session_classes_mock,
        charon_session_launchers_mock,
    ):
        charon_classes_mock = charon_session_classes_mock.return_value
        charon_classes_mock.project_get.return_value = {
            "best_practice_analysis": list(
                self.config["analysis"]["best_practice_analysis"].keys()
            )[0]
        }

        charon_launchers_mock = charon_session_launchers_mock.return_value
        charon_launchers_mock.project_get.return_value = {"status": "OPEN"}

        with mock.patch("ngi_pipeline.engines.sarek.analyze") as analyze_mock:
            project_obj = self.projects[0]
            launch_analysis([project_obj], config=self.config, no_qc=True)
            process_tracking_mock.assert_called_once()
            analyze_mock.assert_called_once()
            called_args = analyze_mock.call_args[0]
            self.assertEqual(project_obj, called_args[0].project)
