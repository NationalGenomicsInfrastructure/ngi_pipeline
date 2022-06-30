"""Various utilities for interacting with SLURM"""

import shlex
import subprocess

from ngi_pipeline.log.loggers import minimal_logger
from six.moves import map


LOG = minimal_logger(__name__)

def kill_slurm_job_by_id(slurm_job_id):
    """Try to kill a slurm job based on its job ID.

    :param int slurm_job_id: The id of the slurm job to kill
    :returns: True if the kill succeeded
    :rtype: bool
    :raises RuntimeError: If the kill did not succed
    """
    LOG.info("Attempting to kill slurm job id {}".format(slurm_job_id))
    try:
        subprocess.check_call(shlex.split('scancel {}'.format(slurm_job_id)))
        LOG.info('slurm job id "{}" killed.'.format(slurm_job_id))
        return True
    except (OSError, subprocess.CalledProcessError) as e:
        raise RuntimeError('Could not kill job "{}": {}"'.format(slurm_job_id, e))

SLURM_EXIT_CODES = {"PENDING": None,
                    "RUNNING": None,
                    "RESIZING": None,
                    "SUSPENDED": None,
                    "COMPLETED": 0,
                    "CANCELLED": 1,
                    "FAILED": 1,
                    "TIMEOUT": 1,
                    "PREEMPTED": 1,
                    "BOOT_FAIL": 1,
                    "NODE_FAIL": 1,
                   }

def get_slurm_job_status(slurm_job_id):
    """Gets the State of a SLURM job and returns it as an integer (or None).

    :param int slurm_job_id: An integer of your choosing

    :returns: The status of the job (None == Queued/Running, 0 == Success, 1 == Failure)
    :rtype: None or int

    :raises TypeError: If the input is not/cannot be converted to an int
    :raises ValueError: If the slurm job ID is not found
    :raises RuntimeError: If the slurm job status is not understood
    """
    try:
        check_cl = "sacct -n -j {:d} -o STATE".format(slurm_job_id)
        # If the sbatch job has finished, this returns two lines. For example:
        # $ sacct -j 3655032
        #       JobID         JobName    Partition   Account  AllocCPUS    State    ExitCode
        #       ------------ ---------- ---------- ---------- ---------- ---------- --------
        #       3655032      test_sbat+       core   a2010002          1  COMPLETED      0:0
        #       3655032.bat+      batch              a2010002          1  COMPLETED      0:0
        #
        # In this case I think we want the first one but I'm actually still not
        # totally clear on this point -- the latter may be the return code of the
        # actual sbatch command for the bash interpreter? Unclear.
    except ValueError:
        raise TypeError("SLURM Job ID not an integer: {}".format(slurm_job_id))
    LOG.debug('Checking slurm job status with cl "{}"...'.format(check_cl))
    job_status = subprocess.check_output(shlex.split(check_cl)).decode("utf-8")
    LOG.debug('job status for job {} is "{}"'.format(slurm_job_id, job_status.strip()))
    if not job_status:
        raise ValueError("No such slurm job found: {}".format(slurm_job_id))
    else:
        try:
            return SLURM_EXIT_CODES[job_status.split()[0].strip("+")]
        except (IndexError, KeyError, TypeError) as e:
            raise RuntimeError("SLURM job status not understood: {}".format(job_status))


def slurm_time_to_seconds(slurm_time_str):
    """Convert a time in a normal goddamned format into seconds.
    Must follow the format:
        days-hours:minutes:seconds
    e.g.
        0-12:34:56
    or else I will just return 4 days and that's what you get for getting
    cute about the formatting.
    """
    try:
        days, time = slurm_time_str.split("-")
        hours, minutes, seconds = list(map(int, time.split(":")))
        hours += int(days) * 24
        minutes += hours * 60
        seconds += minutes * 60
    except Exception as e:
        LOG.error('Couldn\'t parse passed time "{}": {}'.format(slurm_time_str, e))
        return 345600
    return seconds
