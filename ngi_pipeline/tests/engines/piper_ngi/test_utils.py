import unittest
import mock
import tempfile
import os
import shutil
import yaml

import ngi_pipeline.engines.piper_ngi.utils as utils
from ngi_pipeline.conductor.classes import NGIProject, NGISample


class TestPiperUtils(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.tmp_dir = tempfile.mkdtemp()
        self.workflow_subtask = "subtask"
        self.project_base_path = self.tmp_dir
        self.project_name = "S.One_20_12"
        self.project_id = "P123"
        self.sample_id = "P123_1001"
        self.libprep_id = "A"
        self.seqrun_id = "seqrun"

        self.project_obj = NGIProject(
            self.project_name, self.project_id, self.project_id, self.project_base_path
        )
        self.sample_obj = self.project_obj.add_sample(self.sample_id, self.sample_id)

    @classmethod
    def tearDownClass(self):
        shutil.rmtree(self.tmp_dir)

    def test_find_previous_genotype_analyses(self):
        project_dir = os.path.join(
            self.tmp_dir, "ANALYSIS", "P123", "piper_ngi", "01_genotype_concordance"
        )
        os.makedirs(project_dir)
        sample_file = os.path.join(project_dir, "P123_1001.gtc")
        open(sample_file, "w").close()

        previous_analysis_not_done = utils.find_previous_genotype_analyses(
            self.project_obj, self.sample_obj
        )
        self.assertFalse(previous_analysis_not_done)

        sample_done_file = os.path.join(project_dir, ".P123_1001.gtc.done")
        open(sample_done_file, "w").close()

        previous_analysis_done = utils.find_previous_genotype_analyses(
            self.project_obj, self.sample_obj
        )
        self.assertTrue(previous_analysis_done)
        shutil.rmtree(project_dir)  # Remove dir or it will interfere with other tests

    @mock.patch("ngi_pipeline.engines.piper_ngi.utils.os.remove")
    def test_remove_previous_genotype_analyses(self, mock_remove):
        project_dir = os.path.join(
            self.tmp_dir, "ANALYSIS", "P123", "piper_ngi", "02_genotype_concordance"
        )
        os.makedirs(project_dir)
        sample_file = os.path.join(project_dir, "P123-1001.gtc")
        open(sample_file, "w").close()
        utils.remove_previous_genotype_analyses(self.project_obj)
        mock_remove.assert_called_once_with(sample_file)

    @mock.patch("ngi_pipeline.engines.piper_ngi.utils.find_previous_sample_analyses")
    @mock.patch("ngi_pipeline.engines.piper_ngi.utils.os.remove")
    def test_remove_previous_sample_analyses(self, mock_remove, mock_find):
        file_to_remove = os.path.join(self.tmp_dir, "a_file")
        open(file_to_remove, "w").close()
        mock_find.return_value = [file_to_remove]

        utils.remove_previous_sample_analyses(self.project_obj)
        mock_remove.assert_called_once_with(file_to_remove)

    def test_find_previous_sample_analyses(self):
        project_dir = os.path.join(
            self.tmp_dir, "ANALYSIS", "P123", "piper_ngi", "01_files"
        )
        os.makedirs(project_dir)
        sample_file = os.path.join(project_dir, "P123_1001.out")
        open(sample_file, "w").close()

        got_sample_files = utils.find_previous_sample_analyses(self.project_obj)
        self.assertEqual(got_sample_files, [sample_file])

    @mock.patch("ngi_pipeline.engines.piper_ngi.utils.datetime")
    @mock.patch("ngi_pipeline.engines.piper_ngi.utils.shutil.move")
    def test_rotate_previous_analysis(self, mock_move, mock_datetime):
        mock_datetime.datetime.now().strftime.return_value = (
            "2020-11-13_09:30:12:640314"
        )
        analysis_dir = os.path.join(
            self.tmp_dir, "ANALYSIS", "P123", "piper_ngi", "03_raw_alignments"
        )
        os.makedirs(analysis_dir)
        sample_file = os.path.join(analysis_dir, "P123-1001.bam")
        open(sample_file, "w").close()

        utils.rotate_previous_analysis(self.project_obj)
        rotated_file = "{}/ANALYSIS/P123/piper_ngi/previous_analyses/2020-11-13_09:30:12:640314/03_raw_alignments".format(
            self.tmp_dir
        )

        mock_move.assert_called_once_with(sample_file, rotated_file)

    @mock.patch("ngi_pipeline.engines.piper_ngi.utils.CharonSession")
    def test_get_finished_seqruns_for_sample(self, mock_charon):
        mock_charon().sample_get_libpreps.return_value = {
            "libpreps": [{"qc": "PASS", "libprepid": "A"}]
        }
        mock_charon().libprep_get_seqruns.return_value = {
            "seqruns": [{"seqrunid": "B"}]
        }
        mock_charon().seqrun_get.return_value = {"alignment_status": "DONE"}

        got_libpreps = utils.get_finished_seqruns_for_sample(
            self.project_id, self.sample_id
        )
        expected_libpreps = {"A": ["B"]}

        self.assertEqual(got_libpreps, expected_libpreps)

    @mock.patch("ngi_pipeline.engines.piper_ngi.utils.CharonSession")
    def test_get_valid_seqruns_for_sample(self, mock_charon):
        mock_charon().sample_get_libpreps.return_value = {
            "libpreps": [{"qc": "PASS", "libprepid": "A"}]
        }
        mock_charon().libprep_get_seqruns.return_value = {
            "seqruns": [{"seqrunid": "B"}]
        }

        got_libpreps = utils.get_valid_seqruns_for_sample(
            self.project_id, self.sample_id
        )
        expected_libpreps = {"A": ["B"]}

        self.assertEqual(got_libpreps, expected_libpreps)

    def test_record_analysis_details(self):
        job_identifier = "job_id"
        utils.record_analysis_details(self.project_obj, job_identifier)
        output_file_path = os.path.join(
            self.tmp_dir, "ANALYSIS", "P123", "piper_ngi", "logs", "job_id.files"
        )
        with open(output_file_path, "r") as f:
            got_content = yaml.load(f, Loader=yaml.FullLoader)
        expected_content = {"P123": {"P123_1001": {}}}
        self.assertEqual(got_content, expected_content)

    def test_create_project_obj_from_analysis_log(self):
        log_path = os.path.join(
            self.project_base_path, "ANALYSIS", self.project_id, "piper_ngi", "logs"
        )
        os.makedirs(log_path)
        log_file = os.path.join(log_path, "P123-P123_1001-workflow.files")
        log_content = ["{P123: {P123_1001: {}}}"]
        with open(log_file, "w") as f:
            f.write("\n".join(log_content))

        got_project_obj = utils.create_project_obj_from_analysis_log(
            self.project_name,
            self.project_id,
            self.project_base_path,
            self.sample_id,
            "workflow",
        )
        self.assertEqual(got_project_obj, self.project_obj)

    @mock.patch("ngi_pipeline.engines.piper_ngi.utils.CharonSession")
    def test_check_for_preexisting_sample_runs(self, mock_charon):
        mock_charon().sample_get_libpreps.return_value = {
            "libpreps": [{"libprepid": "A"}]
        }
        mock_charon().libprep_get_seqruns.return_value = {
            "seqruns": [{"seqrunid": "B"}]
        }
        mock_charon().seqrun_get.return_value = {"alignment_status": "RUNNING"}

        restart_running_jobs = False
        restart_finished_jobs = False

        with self.assertRaises(RuntimeError):
            utils.check_for_preexisting_sample_runs(
                self.project_obj,
                self.sample_obj,
                restart_running_jobs,
                restart_finished_jobs,
            )

    def test_create_sbatch_header(self):
        got_header = utils.create_sbatch_header(
            "slurm_project_id",
            "slurm_queue",
            17,
            "slurm_time",
            "job_name",
            "slurm_out_log",
            "slurm_err_log",
        )
        expected_header = """#!/bin/bash -l

#SBATCH -A slurm_project_id
#SBATCH -p slurm_queue
#SBATCH -n 16
#SBATCH -t slurm_time
#SBATCH -J job_name
#SBATCH -o slurm_out_log
#SBATCH -e slurm_err_log
"""
        self.assertEqual(got_header, expected_header)

    def test_add_exit_code_recording(self):
        cl = ["echo", "Hello!"]
        exit_code_path = "/some/path"
        got_cl = utils.add_exit_code_recording(cl, exit_code_path)
        expected_cl = "echo Hello!; echo $? > /some/path"
        self.assertEqual(got_cl, expected_cl)

    def test_create_log_file_path(self):
        got_path = utils.create_log_file_path(
            self.workflow_subtask,
            self.project_base_path,
            self.project_name,
            self.project_id,
            sample_id=self.sample_id,
            libprep_id=self.libprep_id,
            seqrun_id=self.seqrun_id,
        )
        expected_path = "{}/ANALYSIS/P123/piper_ngi/logs/P123-P123_1001-A-seqrun-subtask.log".format(
            self.tmp_dir
        )

        self.assertEqual(got_path, expected_path)

    def test_create_exit_code_file_path(self):
        got_path = utils.create_exit_code_file_path(
            self.workflow_subtask,
            self.project_base_path,
            self.project_name,
            self.project_id,
            sample_id=self.sample_id,
            libprep_id=self.libprep_id,
            seqrun_id=self.seqrun_id,
        )
        expected_path = "{}/ANALYSIS/P123/piper_ngi/logs/P123-P123_1001-A-seqrun-subtask.exit".format(
            self.tmp_dir
        )

        self.assertEqual(got_path, expected_path)

    def test__create_generic_output_file_path(self):
        got_path = utils._create_generic_output_file_path(
            self.workflow_subtask,
            self.project_base_path,
            self.project_name,
            self.project_id,
            sample_id=self.sample_id,
            libprep_id=self.libprep_id,
            seqrun_id=self.seqrun_id,
        )
        expected_path = (
            "{}/ANALYSIS/P123/piper_ngi/logs/P123-P123_1001-A-seqrun-subtask".format(
                self.tmp_dir
            )
        )

        self.assertEqual(got_path, expected_path)
