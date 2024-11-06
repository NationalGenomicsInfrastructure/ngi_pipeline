import unittest
import mock

import ngi_pipeline.utils.slurm as slurm


class TestSlurm(unittest.TestCase):
    @classmethod
    def setUpClass(self):
        self.slurm_job_id = 12345

    @mock.patch("ngi_pipeline.utils.slurm.subprocess.check_call")
    def test_kill_slurm_job_by_id(self, mock_subprocess):
        self.assertTrue(slurm.kill_slurm_job_by_id(self.slurm_job_id))
        mock_subprocess.assert_called_once_with(["scancel", "12345"])

    @mock.patch("ngi_pipeline.utils.slurm.subprocess.check_call")
    def test_kill_slurm_job_by_id_error(self, mock_subprocess):
        mock_subprocess.side_effect = OSError("Error")
        with self.assertRaises(RuntimeError):
            slurm.kill_slurm_job_by_id(self.slurm_job_id)

    @mock.patch("ngi_pipeline.utils.slurm.subprocess.check_output")
    def test_get_slurm_job_status(self, mock_subprocess):
        mock_subprocess.side_effect = ["PENDING", "COMPLETED", "CANCELLED", "", "Whut"]

        got_job_status = slurm.get_slurm_job_status(self.slurm_job_id)
        self.assertIsNone(got_job_status)

        got_job_status = slurm.get_slurm_job_status(self.slurm_job_id)
        self.assertEqual(got_job_status, 0)

        got_job_status = slurm.get_slurm_job_status(self.slurm_job_id)
        self.assertEqual(got_job_status, 1)

        with self.assertRaises(ValueError):
            got_job_status = slurm.get_slurm_job_status(self.slurm_job_id)

        with self.assertRaises(RuntimeError):
            got_job_status = slurm.get_slurm_job_status(self.slurm_job_id)

    def test_slurm_time_to_seconds(self):
        slurm_time_str = "1-3:46:40"
        got_sec = slurm.slurm_time_to_seconds(slurm_time_str)
        self.assertEqual(got_sec, 100000)
