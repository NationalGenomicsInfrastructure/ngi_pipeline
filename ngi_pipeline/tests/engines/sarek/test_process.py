import errno
import mock
import os
import shutil
import tempfile
import unittest

from ngi_pipeline.engines.sarek.process import ProcessConnector, ProcessExitStatus, ProcessExitStatusUnknown, \
    ProcessExitStatusSuccessful, ProcessExitStatusFailed, ProcessStatus, ProcessRunning, JobStatus, ProcessStopped, \
    SlurmConnector


class TestProcessConnector(unittest.TestCase):

    def setUp(self):
        self.tdir = tempfile.mkdtemp(prefix="test_process_connector_")
        self.process_connector = ProcessConnector(cwd=self.tdir)

    def tearDown(self):
        try:
            shutil.rmtree(self.tdir)
        except OSError:
            pass

    def test_execute_process(self):
        expected_pid = 12345
        with mock.patch('ngi_pipeline.engines.sarek.process.safe_makedir') as mkdir_mock, \
                mock.patch('ngi_pipeline.engines.sarek.process.execute_command_line') as execute_mock:

            # simplest case
            execute_mock.return_value.pid = expected_pid
            self.assertEqual(expected_pid, self.process_connector.execute_process("this-is-a-command-line"))

            # a valid working directory other than the cwd is used
            working_dir = os.path.join(self.tdir, "this-folder-exists")
            os.mkdir(working_dir)
            self.assertEqual(
                expected_pid,
                self.process_connector.execute_process("this-is-a-command-line", working_dir=working_dir))

            # extra keyword arguments cause no problems
            self.assertEqual(
                expected_pid,
                self.process_connector.execute_process(
                    "this-is-a-command-line", extra_kwarg_1="some-value", extra_kwarg_2=98765))

            # assume the execution raises an error
            execute_mock.side_effect = RuntimeError("raised runtime error")
            with self.assertRaises(RuntimeError):
                self.process_connector.execute_process("this-is-a-command-line")

            # if the desired working directory can not be accessed an OSError should be raised
            working_dir = os.path.join(self.tdir, "this-folder-does-not-exist")
            with self.assertRaises(OSError) as ose:
                self.process_connector.execute_process("this-is-a-command-line", working_dir=working_dir)
                self.assertEqual(errno.ENOENT, ose.exception.error_code)

            # if the desired working directory can not be created
            working_dir = os.path.join(self.tdir, "this-folder-can-not-be-created")
            expected_exception = OSError(errno.EPERM, "raised OSError")
            mkdir_mock.side_effect = expected_exception
            with self.assertRaises(OSError) as ose:
                self.process_connector.execute_process("this-is-a-command-line", working_dir=working_dir)
                self.assertEqual(expected_exception, ose.exception)

    def test_cleanup(self):
        self.assertTrue(os.path.exists(self.tdir))
        # create a file in the dir so it's not empty
        with tempfile.TemporaryFile(dir=self.tdir):
            self.process_connector.cleanup(self.tdir)
            self.assertFalse(os.path.exists(self.tdir))


class TestProcessExitStatus(unittest.TestCase):

    @staticmethod
    def _create_temp_file_with_contents(contents=None):
        fd, exit_code_path = tempfile.mkstemp(prefix="test_process_exit_status_", suffix=".exit_code")
        if contents is not None:
            os.write(fd, contents)
        os.close(fd)
        return exit_code_path

    def test_get_exit_code_missing_file(self):
        exit_code_path = os.path.join(tempfile.tempdir, "this-file-does-not-exist")
        self.assertIsNone(ProcessExitStatus.get_exit_code(exit_code_path))

    def test_get_exit_code_empty_file(self):
        exit_code_path = TestProcessExitStatus._create_temp_file_with_contents()
        self.assertIsNone(ProcessExitStatus.get_exit_code(exit_code_path))
        os.unlink(exit_code_path)

    def test_get_exit_code_nonsense_file(self):
        exit_code_path = TestProcessExitStatus._create_temp_file_with_contents(contents=b"nonsense-contents")
        self.assertIsNone(ProcessExitStatus.get_exit_code(exit_code_path))
        os.unlink(exit_code_path)

    def test_get_exit_code_0(self):
        exit_code_path = TestProcessExitStatus._create_temp_file_with_contents(contents=b"0")
        self.assertEquals(0, ProcessExitStatus.get_exit_code(exit_code_path))
        os.unlink(exit_code_path)

    def test_get_exit_code_1(self):
        exit_code_path = TestProcessExitStatus._create_temp_file_with_contents(contents=b"1")
        self.assertEquals(1, ProcessExitStatus.get_exit_code(exit_code_path))
        os.unlink(exit_code_path)

    @mock.patch("ngi_pipeline.engines.sarek.process.ProcessExitStatus.get_exit_code")
    def test_get_type_from_exit_code_path(self, exit_code_mock):
        exit_code_mock.return_value = None
        self.assertIs(ProcessExitStatusUnknown, ProcessExitStatus.get_type_from_exit_code_path("exit-code-path"))
        exit_code_mock.return_value = 0
        self.assertIs(ProcessExitStatusSuccessful, ProcessExitStatus.get_type_from_exit_code_path("exit-code-path"))
        exit_code_mock.return_value = 12
        self.assertIs(ProcessExitStatusFailed, ProcessExitStatus.get_type_from_exit_code_path("exit-code-path"))
        exit_code_mock.return_value = "0"
        self.assertIs(ProcessExitStatusFailed, ProcessExitStatus.get_type_from_exit_code_path("exit-code-path"))


class TestProcessStatus(unittest.TestCase):

    @staticmethod
    def helper_fn(test_case, test_class, process_running_mock, exit_status_mock):
        process_running_mock.return_value = True
        expected_type = ProcessRunning
        test_case.assertIs(
            test_class.get_type_from_processid_and_exit_code_path(123456, 'this-is-an-exit-code-path'),
            expected_type)
        process_running_mock.return_value = False
        for expected_type in [ProcessExitStatusFailed, ProcessExitStatusSuccessful, ProcessExitStatusUnknown]:
            exit_status_mock.return_value = expected_type
            test_case.assertIs(
                test_class.get_type_from_processid_and_exit_code_path(123456, 'this-is-an-exit-code-path'),
                expected_type)

    @mock.patch('os.kill')
    def test_is_process_running(self, kill_mock):
        self.assertTrue(ProcessStatus.is_process_running(0))
        kill_mock.side_effect = OSError(errno.ESRCH, "raised OSError")
        self.assertFalse(ProcessStatus.is_process_running(0))
        expected_exception = OSError(errno.EPERM, "raised OSError")
        kill_mock.side_effect = expected_exception
        with self.assertRaises(OSError) as ose:
            ProcessStatus.is_process_running(0)
            self.assertEqual(expected_exception, ose.exception)

    @mock.patch('ngi_pipeline.engines.sarek.process.ProcessExitStatus.get_type_from_exit_code_path')
    @mock.patch('ngi_pipeline.engines.sarek.process.ProcessStatus.is_process_running')
    def test_get_type_from_processid_and_exit_code_path(self, process_running_mock, exit_status_mock):
        TestProcessStatus.helper_fn(self, ProcessStatus, process_running_mock, exit_status_mock)


class TestJobStatus(unittest.TestCase):

    @mock.patch('ngi_pipeline.engines.sarek.process.ProcessExitStatus.get_type_from_exit_code_path')
    @mock.patch('ngi_pipeline.engines.sarek.process.JobStatus.is_process_running')
    def test_get_type_from_processid_and_exit_code_path(self, job_running_mock, exit_status_mock):
        TestProcessStatus.helper_fn(self, JobStatus, job_running_mock, exit_status_mock)
        job_running_mock.assert_called()

    @mock.patch('ngi_pipeline.engines.sarek.process.SlurmConnector')
    def test_is_process_running(self, connector_mock):
        connector_mock.get_slurm_job_status.return_value = ProcessRunning
        self.assertTrue(JobStatus.is_process_running(12345))
        connector_mock.get_slurm_job_status.return_value = ProcessStopped
        self.assertFalse(JobStatus.is_process_running(12345))


class TestSlurmConnector(unittest.TestCase):

    def setUp(self):
        self.slurm_project = "this-is-a-slurm-project"
        self.slurm_mail_user = "this-is-a-slurm-mail-user"
        self.slurm_cores = 16
        self.slurm_extra_args = [
            "--extra-slurm-arg-1 extra-slurm-arg-value-1",
            "--extra-slurm-arg-2 extra-slurm-arg-value-2"]
        self.cwd = tempfile.mkdtemp(prefix="test_slurm_connector_")

    def _create_slurm_connector(self):
        self.slurm_connector = SlurmConnector(
            self.slurm_project,
            self.slurm_mail_user,
            cwd=self.cwd,
            slurm_cores=self.slurm_cores,
            slurm_extra_args=self.slurm_extra_args)

    def tearDown(self):
        shutil.rmtree(self.cwd)

    def test_init(self):
        self._create_slurm_connector()
        self.assertEqual(self.slurm_project, self.slurm_connector.slurm_parameters["slurm_project"])
        self.assertEqual(self.slurm_mail_user, self.slurm_connector.slurm_parameters["slurm_mail_user"])
        self.assertEqual(self.slurm_cores, self.slurm_connector.slurm_parameters["slurm_cores"])
        self.assertEqual(self.slurm_extra_args, self.slurm_connector.slurm_parameters["slurm_extra_args"])
        self.assertEqual(self.cwd, self.slurm_connector.cwd)

    def test__slurm_script_from_command_line(self):
        self._create_slurm_connector()
        expected_command_line = "this-is-a-command-line"
        expected_exit_code_path = "this-is-the-exit-code-path"
        expected_job_name = "test_slurm_job"
        expected_script_dir = os.path.join(self.cwd, "sbatch")

        expected_prefix = "mock_prefix"
        expected_script_name = "{}.{}.sbatch".format(expected_job_name, expected_prefix)
        with mock.patch('ngi_pipeline.engines.sarek.process.datetime.datetime') as dt_mock:
            dt_mock.now.return_value.strftime.return_value = expected_prefix
            observed_script = self.slurm_connector._slurm_script_from_command_line(
                expected_command_line, self.cwd, expected_exit_code_path, expected_job_name)
            self.assertTrue(os.path.exists(observed_script))
            self.assertEqual(expected_script_dir, os.path.dirname(observed_script))
            self.assertEqual(expected_script_name, os.path.basename(observed_script))
