import datetime
import errno
import os
import re
import shutil
import subprocess

from ngi_pipeline.engines.sarek.exceptions import SlurmStatusNotRecognizedError
from ngi_pipeline.utils.filesystem import execute_command_line, safe_makedir, chdir
from ngi_pipeline.utils.slurm import get_slurm_job_status as core_get_slurm_job_status


class ProcessConnector(object):
    """
    The ProcessConnector class provides an interface for executing and managing processes in a local environment.
    Subclasses can provide interfaces for other environments, see e.g. the SlurmConnector
    """

    def __init__(self, cwd=None):
        """
        Create a ProcessConnector instance to manage locally running processes

        :param cwd: if specified, this directory will be used as the working directory by this ProcessConnector. Default
        is to use the current working directory
        """
        self.cwd = cwd or os.curdir

    def execute_process(self, command_line, working_dir=None, **extra_args):
        """
        Execute the supplied command line. If the working directory that should be used does not exist, it will be
        created.

        :param command_line: command line to be executed, can be a string or a list
        :param working_dir: directory to use as working directory when executing the command line. Default is to use the
        current working directory used by this ProcessConnector. Will be created if it does not exist
        :param extra_args: any additional parameters passed will be ignored
        :return: the process id (pid) of the launched process
        """
        working_dir = working_dir or self.cwd
        safe_makedir(working_dir)
        with chdir(working_dir):
            try:
                proc = execute_command_line(command_line, shell=False, cwd=working_dir)
                return proc.pid
            except RuntimeError:
                raise

    @staticmethod
    def cleanup(target_dir):
        """
        Remove a specified target directory, regardless whether it's empty or not.

        :param target_dir: the full path to the target directory
        :return: None
        """
        shutil.rmtree(target_dir)


class ProcessStatus(object):
    """
    Base class for process status indicators and related operations. Subclasses represent various process statuses.
    """

    @classmethod
    def get_type_from_processid_and_exit_code_path(cls, processid, exit_code_path):
        """
        Get the process status type from the supplied process id and path to a file where the exit status for the
        process will be stored.

        :param processid: process id for the process
        :param exit_code_path: path to the file where the exit code of the process is expected to be stored
        :return: the type of a subclass of ProcessStatus which represents the status of the process
        """
        return ProcessRunning if cls.is_process_running(processid) else \
            ProcessExitStatus.get_type_from_exit_code_path(exit_code_path)

    @staticmethod
    def is_process_running(processid):
        try:
            os.kill(processid, 0)
            return True
        except OSError as ose:
            if ose.errno != errno.ESRCH:
                raise
        return False


class ProcessRunning(ProcessStatus):
    """
    Class representing a process that is running
    """
    pass


class ProcessStopped(ProcessStatus):
    """
    Class representing a process that has stopped
    """
    pass


class JobStatus(ProcessStatus):
    """
    Class representing slurm jobs
    """
    @staticmethod
    def is_process_running(jobid):
        return SlurmConnector.get_slurm_job_status(jobid) == ProcessRunning


class ProcessExitStatus(ProcessStatus):
    """
    A class representing exit statuses for a process and related operations. Subclasses represent various exit statuses
    """

    @staticmethod
    def get_type_from_exit_code_path(exit_code_path):
        """
        Checks the supplied file for a stored exit code and returns a ProcessExitStatus type representing the exit code

        :param exit_code_path: path to the file where the exit code is expected to be stored
        :return: ProcessExitStatusUnknown if the exit_code_path could not be found or if an exit code value could not
        be parsed from the file. ProcessExitStatusFailed if the exit code is not 0. ProcessExitStatusSuccessful if the
        exit code is 0.
        """
        exit_code = ProcessExitStatus.get_exit_code(exit_code_path)
        if exit_code is None:
            return ProcessExitStatusUnknown
        if exit_code != 0:
            return ProcessExitStatusFailed
        return ProcessExitStatusSuccessful

    @staticmethod
    def get_exit_code(exit_code_path):
        """
        Parse the first line of the exit_code_path file and return the contents as an integer. Raises an IOError if
        an exception occurred when opening the file that was not caused by the file being missing.

        :param exit_code_path: the path to the file expected to have the exit code of the process on the first line
        :return: the exit code as an integer or None if the file is missing or the contents could not be parsed
        """
        try:
            with open(exit_code_path, "r") as fh:
                return int(fh.readline().strip())
        except IOError as e:
            if e.errno != errno.ENOENT:
                raise
        except (ValueError, TypeError):
            pass
        return None


class ProcessExitStatusSuccessful(ProcessExitStatus):
    """
    Class representing a successful exit status
    """
    pass


class ProcessExitStatusFailed(ProcessExitStatus):
    """
    Class representing a failed exit status
    """
    pass


class ProcessExitStatusUnknown(ProcessExitStatus):
    """
    Class representing an unknown exit status
    """
    pass


class SlurmConnector(ProcessConnector):
    """
    A ProcessConnector for interfacing with a SLURM job.
    """

    # a boilerplate template for the SLURM job header
    JOB_HEADER_TEMPLATE = """#! /bin/bash -l

#SBATCH -A {slurm_project}
#SBATCH -J "{slurm_job_name}"
#SBATCH -p {slurm_partition}
#SBATCH -N {slurm_nodes}
#SBATCH -n {slurm_cores}
#SBATCH -t {slurm_job_time}
#SBATCH -o "{slurm_stdout}"
#SBATCH -e "{slurm_stderr}"
#SBATCH -D "{slurm_working_directory}"
#SBATCH --mail-user {slurm_mail_user}
#SBATCH --mail-type {slurm_mail_events}
"""

    # sensible defaults for some of the slurm parameters
    SLURM_DEFAULTS = {
        "slurm_partition": "core",
        "slurm_nodes": 1,
        "slurm_cores": 8,
        "slurm_job_time": "72:00:00",
        "slurm_mail_events": "NONE"
    }

    def __init__(self, slurm_project, slurm_mail_user, cwd=None, **slurm_args):
        """
        Creates a SlurmConnector for executing and interacting with slurm jobs.

        :param slurm_project: the slurm project under which to run the job
        :param slurm_mail_user: an email address where slurm notifications will be sent
        :param cwd: working directory that will be passed to the parent constructor
        :param slurm_args: additional slurm arguments passed will be used for the job submission
        """
        super(SlurmConnector, self).__init__(cwd=cwd)
        slurm_args["slurm_project"] = slurm_project
        slurm_args["slurm_mail_user"] = slurm_mail_user
        # initialize the slurm parameters with the defaults
        self.slurm_parameters = self.SLURM_DEFAULTS.copy()
        # update the slurm parameters with the passed slurm arguments, overriding the defaults
        self.slurm_parameters.update(slurm_args)

    def _slurm_script_from_command_line(
            self,
            command_line,
            working_dir,
            exit_code_path,
            job_name):
        """
        Create a SLURM script ready for submission based on the supplied command line and the parameters in this
        SlurmConnector instance.

        The created SLURM script will contain a job header as created by this SlurmConnector. The exit code of the
        command to be executed as a SLURM job will be written to a file. This file will be truncated or created once
        the job starts. The SLURM script will include the current time, so the file name should be unique between
        subsequent calls to this function.

        :param command_line: command line to execute in the SLURM job, formatted as a string
        :param working_dir: the directory in which to create the SLURM script (expected to exist)
        :param exit_code_path: path to the file where the exit code from the command should be stored
        :param job_name: the job name to use for the SLURM submission
        :return: the path to the created SLURM script
        """
        # create the script in the passed working directory
        slurm_script_dir = os.path.join(working_dir, "sbatch")
        safe_makedir(slurm_script_dir)
        slurm_script = os.path.join(
            slurm_script_dir, "{}.{}.sbatch".format(job_name, datetime.datetime.now().strftime("%s")))
        slurm_stdout = "{}.out".format(slurm_script)
        slurm_stderr = "{}.err".format(slurm_script)

        with open(slurm_script, "w") as fh:
            fh.write(
                SlurmConnector.JOB_HEADER_TEMPLATE.format(
                    slurm_job_name=job_name,
                    slurm_stdout=slurm_stdout,
                    slurm_stderr=slurm_stderr,
                    slurm_working_directory=self.slurm_parameters.get("slurm_working_directory", working_dir),
                    **self.slurm_parameters))
            # append any extra SLURM arguments passed
            for slurm_extra_arg in self.slurm_parameters.get("slurm_extra_args", []):
                fh.write("#SBATCH {}\n".format(slurm_extra_arg))

            fh.write("\necho \"\" > \"{}\"\n".format(exit_code_path))
            fh.write("{}\n".format(command_line))
            fh.write("echo \"$?\" > \"{}\"\n".format(exit_code_path))

        return slurm_script

    def execute_process(self, command_line, working_dir=None, exit_code_path=None, job_name=None):
        """
        Wrap the supplied command line in a SLURM script and submit it to the job queue.

        :param command_line: command line to execute in the SLURM job, formatted as a string
        :param working_dir: the directory in which to create the SLURM script and use as working directory for the job.
        If it does not already exist, it will be created.
        :param exit_code_path: path to the file where the exit code from the command should be stored. If not specified,
        the exit code will be sent to /dev/null
        :param job_name: the job name to use when submitting to the cluster. If not specified, it will be constructed
        from the command line
        :return: the slurm job id
        """
        exit_code_path = exit_code_path or os.devnull
        job_name = job_name or command_line.replace(" ", "_")[0:20]
        # create the working dir if it does not exist already
        working_dir = working_dir or self.cwd
        safe_makedir(working_dir)
        with chdir(working_dir):
            slurm_script = self._slurm_script_from_command_line(
                command_line,
                working_dir,
                exit_code_path,
                job_name)
            # submit the sbatch file
            sbatch_command_line = "sbatch {}".format(slurm_script)
            proc = execute_command_line(
                sbatch_command_line,
                shell=False,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE)
            stdout, stderr = proc.communicate()
            try:
                # parse the slurm job id from the sbatch stdout
                slurm_job_id = re.match(r'Submitted batch job (\d+)', stdout).groups()[0]
                return slurm_job_id
            except AttributeError:
                raise RuntimeError(
                    'Could not submit sbatch job for workflow "{}": {}'.format(job_name, stderr))

    @staticmethod
    def get_slurm_job_status(slurm_job_id):
        """
        :param slurm_job_id: the slurm job id
        :return: a ProcessStatus type indicating the status
        """
        try:
            return ProcessRunning if core_get_slurm_job_status(slurm_job_id) is None else ProcessStopped
        except RuntimeError as e:
            raise SlurmStatusNotRecognizedError(slurm_job_id, e.message)
