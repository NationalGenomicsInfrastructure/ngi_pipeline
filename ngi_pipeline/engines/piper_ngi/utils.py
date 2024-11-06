import collections
import datetime
import fnmatch
import glob
import os
import shutil
import subprocess
import yaml

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.database.classes import CharonSession
from ngi_pipeline.log.loggers import log_process_non_blocking, minimal_logger
from ngi_pipeline.utils.filesystem import (
    execute_command_line,
    rotate_file,
    safe_makedir,
)

LOG = minimal_logger(__name__)


def launch_piper_job(command_line, project, log_file_path=None):
    """Launch the Piper command line.

    :param str command_line: The command line to execute
    :param Project project: The Project object (needed to set the CWD)

    :returns: The subprocess.Popen object for the process
    :rtype: subprocess.Popen
    """
    working_dir = os.path.join(project.base_path, "ANALYSIS", project.dirname)
    file_handle = None
    if log_file_path:
        try:
            file_handle = open(log_file_path, "w")
        except Exception as e:
            LOG.error(
                'Could not open log file "{}"; reverting to standard '
                "logger (error: {})".format(log_file_path, e)
            )
            log_file_path = None
    popen_object = execute_command_line(
        command_line,
        cwd=working_dir,
        shell=True,
        stdout=(file_handle or subprocess.PIPE),
        stderr=(file_handle or subprocess.PIPE),
    )
    if not log_file_path:
        log_process_non_blocking(popen_object.stdout, LOG.info)
        log_process_non_blocking(popen_object.stderr, LOG.warn)
    return popen_object


def find_previous_genotype_analyses(project_obj, sample_obj):
    project_dir_path = os.path.join(
        project_obj.base_path, "ANALYSIS", project_obj.project_id, "piper_ngi"
    )
    project_dir_pattern = os.path.join(project_dir_path, "??_genotype_concordance")
    LOG.debug(
        "Searching for previous genotype analysis output files in " "{}".format(
            project_dir_path
        )
    )
    piper_sample_name = sample_obj.name.replace("_", "-", 1)
    sample_files = glob.glob(
        os.path.join(project_dir_pattern, "{}*".format(sample_obj.name))
    )
    # P123_456 is renamed by Piper to P123-456? Sometimes? Always?
    sample_files.extend(
        glob.glob(os.path.join(project_dir_pattern, "{}*".format(piper_sample_name)))
    )

    sample_done_files = glob.glob(
        os.path.join(project_dir_pattern, ".{}*.done".format(sample_obj.name))
    )
    sample_done_files.extend(
        glob.glob(
            os.path.join(project_dir_pattern, ".{}*.done".format(piper_sample_name))
        )
    )
    sample_failed_files = glob.glob(
        os.path.join(project_dir_pattern, ".{}*.failed".format(sample_obj.name))
    )
    sample_failed_files.extend(
        glob.glob(
            os.path.join(project_dir_pattern, ".{}*.failed".format(piper_sample_name))
        )
    )
    sample_valid_files = []
    for sample_file in sample_files:
        sample_dirname, sample_basename = os.path.split(sample_file)
        sample_done_name = os.path.join(
            sample_dirname, ".{}.done".format(sample_basename)
        )
        if sample_done_name in sample_done_files:
            sample_valid_files.append(sample_file)
    if sample_valid_files:
        return True
    else:
        return False


def remove_previous_genotype_analyses(project_obj):
    """Remove genotype concordance analysis results for a sample, including
    .failed and .done files.
    Doesn't throw an error if it can't read a directory, but does if it can't
    delete a file it knows about.

    :param NGIProject project_obj: The NGIProject object with relevant NGISamples

    :returns: Nothing
    :rtype: None
    """
    project_dir_path = os.path.join(
        project_obj.base_path, "ANALYSIS", project_obj.project_id, "piper_ngi"
    )
    project_dir_pattern = os.path.join(project_dir_path, "??_genotype_concordance")
    LOG.info("deleting previous analysis in {}".format(project_dir_path))
    for sample in project_obj:
        piper_sample_name = sample.name.replace("_", "-", 1)
        sample_files = glob.glob(
            os.path.join(project_dir_pattern, "{}*".format(sample.name))
        )
        # P123_456 is renamed by Piper to P123-456? Sometimes? Always?
        sample_files.extend(
            glob.glob(
                os.path.join(project_dir_pattern, "{}*".format(piper_sample_name))
            )
        )
        sample_files.extend(
            glob.glob(
                os.path.join(project_dir_pattern, ".{}*.done".format(piper_sample_name))
            )
        )
        sample_files.extend(
            glob.glob(
                os.path.join(
                    project_dir_pattern, ".{}*.failed".format(piper_sample_name)
                )
            )
        )
    if sample_files:
        LOG.info(
            "Deleting genotype files for samples {} under " "{}".format(
                ", ".join(project_obj.samples), project_dir_path
            )
        )
        errors = []
        for sample_file in sample_files:
            LOG.debug("Deleting file {}".format(sample_file))
            try:
                if os.path.isdir(sample_file):
                    shutil.rmtree(sample_file)
                else:
                    os.remove(sample_file)
            except OSError as e:
                errors.append("{}: {}".format(sample_file, e))
        if errors:
            LOG.warning(
                "Error when removing one or more files: {}".format("\n".join(errors))
            )
    else:
        LOG.debug(
            "No genotype analysis files found to delete for project {} "
            "/ samples {}".format(project_obj, ", ".join(project_obj.samples))
        )


def remove_previous_sample_analyses(project_obj, sample_obj=None):
    """Remove analysis results for a sample, including .failed and .done files.
    Doesn't throw an error if it can't read a directory, but does if it can't
    delete a file it knows about.

    :param NGIProject project_obj: The NGIProject object with relevant NGISamples
    :param NGISample sample_obj: The relevant NGISample object

    :returns: Nothing
    :rtype: None
    """
    sample_files = find_previous_sample_analyses(
        project_obj, sample_obj=sample_obj, include_genotype_files=False
    )
    if sample_files:
        LOG.info(
            "Deleting files for samples {}".format(sample_obj or project_obj.samples)
        )
        errors = []
        for sample_file in sample_files:
            LOG.info("Deleting file {}".format(sample_file))
            try:
                if os.path.isdir(sample_file):
                    shutil.rmtree(sample_file)
                else:
                    os.remove(sample_file)
            except OSError as e:
                errors.append("{}: {}".format(sample_file, e))
        if errors:
            LOG.warning(
                "Error when removing one or more files: {}".format("\n".join(errors))
            )
    else:
        LOG.debug(
            "No sample analysis files found to delete for project {} "
            "/ samples {}".format(project_obj, ", ".join(project_obj.samples))
        )


def find_previous_sample_analyses(
    project_obj, sample_obj=None, include_genotype_files=False
):
    """Find analysis results for a sample, including .failed and .done files.
    Doesn't throw an error if it can't read a directory.

    :param NGIProject project_obj: The NGIProject object with relevant NGISamples
    :param bool include_genotype_files: Include genotyping files (default False)

    :returns: A list of files
    :rtype: list
    """
    sample_files = (
        set()
    )  # This isn't really necessary but scoping makes me want to do it
    project_dir_path = os.path.join(
        project_obj.base_path, "ANALYSIS", project_obj.project_id, "piper_ngi"
    )
    project_dir_pattern = os.path.join(project_dir_path, "??_*")
    for sample in project_obj:
        if sample_obj and sample.name != sample_obj.name:
            continue
        sample_files.update(
            glob.glob(os.path.join(project_dir_pattern, "{}.*".format(sample.name)))
        )
        # P123_456 is renamed by Piper to P123-456? Sometimes? Always?
        piper_sample_name = sample.name.replace("_", "?", 1)
        sample_files.update(
            glob.glob(
                os.path.join(project_dir_pattern, "{}.*".format(piper_sample_name))
            )
        )
        sample_files.update(
            glob.glob(
                os.path.join(
                    project_dir_pattern, ".{}.*.done".format(piper_sample_name)
                )
            )
        )
        sample_files.update(
            glob.glob(
                os.path.join(
                    project_dir_pattern, ".{}.*.fail".format(piper_sample_name)
                )
            )
        )
    # Include genotype files?
    if not include_genotype_files:
        sample_files = [
            x for x in sample_files if not fnmatch.fnmatch(x, "*genotype_concordance*")
        ]

    return sample_files


def rotate_previous_analysis(project_obj):
    """Rotates the files from the existing analysis starting at 03_merged_aligments"""
    project_dir_path = os.path.join(
        project_obj.base_path, "ANALYSIS", project_obj.project_id, "piper_ngi"
    )
    # analysis_move = glob.glob(os.path.join(project_dir_path, '0[3-9]_*'))
    for sample in project_obj:
        # P123_456 is renamed by Piper to P123-456
        piper_sample_name = sample.name.replace("_", "-", 1)
        sample_files = glob.glob(
            os.path.join(project_dir_path, "0[3-9]_*", "{}.*".format(piper_sample_name))
        )
        if sample_files:
            LOG.info(
                "Rotating files for sample {} under {} to "
                '"previous_analyses" folder'.format(sample, project_dir_path)
            )
            current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S:%f")
            for sample_file in sample_files:
                # This will be the project_dir_path, so I guess I'm just being paranoid
                common_prefix = os.path.commonprefix(
                    [os.path.abspath(project_dir_path), os.path.abspath(sample_file)]
                )
                # This part of the directory tree we need to recreate under previous_analyses
                # So e.g. with
                #       /proj/a2015001/Y.Mom_15_01/01_raw_alignments/P123_456.bam
                # we'd get
                #       01_raw_alignments/P123_456.bam
                # and we'd then create
                #       /proj/a2015001/Y.Mom_15_01/previous_analyses/2015-02-19_16:24:12:640314/01_raw_alignments/
                # and move the file to this directory.
                leaf_path = os.path.relpath(sample_file, common_prefix)
                leaf_base, filename = os.path.split(leaf_path)
                previous_analysis_dirpath = os.path.join(
                    common_prefix, "previous_analyses", current_datetime, leaf_base
                )
                safe_makedir(previous_analysis_dirpath, mode=0o2770)
                LOG.debug(
                    "Moving file {} to directory {}".format(
                        sample_file, previous_analysis_dirpath
                    )
                )
                shutil.move(sample_file, previous_analysis_dirpath)


def get_finished_seqruns_for_sample(
    project_id, sample_id, include_failed_libpreps=False
):
    """Find all the finished seqruns for a particular sample.

    :param str project_id: The id of the project
    :param str sample_id: The id of the sample

    :returns: A dict of {libprep_01: [seqrun_01, ..., seqrun_nn], ...}
    :rtype: dict
    """
    charon_session = CharonSession()
    sample_libpreps = charon_session.sample_get_libpreps(
        projectid=project_id, sampleid=sample_id
    )
    libpreps = collections.defaultdict(list)
    for libprep in sample_libpreps["libpreps"]:
        if libprep.get("qc") != "FAILED" or include_failed_libpreps:
            libprep_id = libprep["libprepid"]
            for seqrun in charon_session.libprep_get_seqruns(
                projectid=project_id, sampleid=sample_id, libprepid=libprep_id
            )["seqruns"]:
                seqrun_id = seqrun["seqrunid"]
                aln_status = charon_session.seqrun_get(
                    projectid=project_id,
                    sampleid=sample_id,
                    libprepid=libprep_id,
                    seqrunid=seqrun_id,
                ).get("alignment_status")
                if aln_status == "DONE":
                    libpreps[libprep_id].append(seqrun_id)
                else:
                    LOG.debug(
                        'Skipping seqrun "{}" due to alignment_status ' '"{}"'.format(
                            seqrun_id, aln_status
                        )
                    )
        else:
            LOG.info(
                'Skipping libprep "{}" due to qc status ' '"{}"'.format(
                    libprep, libprep.get("qc")
                )
            )
    return dict(libpreps)


def get_valid_seqruns_for_sample(
    project_id,
    sample_id,
    include_failed_libpreps=False,
    include_done_seqruns=False,
    status_field="alignment_status",
):
    """Find all the valid seqruns for a particular sample.

    :param str project_id: The id of the project
    :param str sample_id: The id of the sample
    :param bool include_failed_libpreps: Include seqruns for libreps that have failed QC
    :param bool include_done_seqruns: Include seqruns that are already marked DONE

    :returns: A dict of {libprep_01: [seqrun_01, ..., seqrun_nn], ...}
    :rtype: dict

    :raises ValueError: If status_field is not a valid value
    """
    valid_status_values = (
        "alignment_status",
        "genotype_status",
    )
    if status_field not in valid_status_values:
        raise ValueError(
            '"status_field" argument must be one of {} '
            '(value passed was "{}")'.format(
                ", ".join(valid_status_values), status_field
            )
        )
    charon_session = CharonSession()
    sample_libpreps = charon_session.sample_get_libpreps(
        projectid=project_id, sampleid=sample_id
    )
    libpreps = collections.defaultdict(list)
    for libprep in sample_libpreps["libpreps"]:
        if libprep.get("qc") != "FAILED" or include_failed_libpreps:
            libprep_id = libprep["libprepid"]
            for seqrun in charon_session.libprep_get_seqruns(
                projectid=project_id, sampleid=sample_id, libprepid=libprep_id
            )["seqruns"]:
                seqrun_id = seqrun["seqrunid"]
                try:
                    aln_status = charon_session.seqrun_get(
                        projectid=project_id,
                        sampleid=sample_id,
                        libprepid=libprep_id,
                        seqrunid=seqrun_id,
                    )[status_field]
                except KeyError:
                    LOG.error(
                        'Field "{}" not available for seqrun "{}" in Charon '
                        'for project "{}" / sample "{}". Including as '
                        "valid.".format(status_field, seqrun_id, project_id, sample_id)
                    )
                    aln_status = None
                if aln_status != "DONE" or include_done_seqruns:
                    libpreps[libprep_id].append(seqrun_id)
                else:
                    LOG.info(
                        'Skipping seqrun "{}" due to {}' '"{}"'.format(
                            seqrun_id, status_field, aln_status
                        )
                    )
        else:
            LOG.info(
                'Skipping libprep "{}" due to qc status ' '"{}"'.format(
                    libprep, libprep.get("qc")
                )
            )
    return dict(libpreps)


def record_analysis_details(project, job_identifier):
    """Write a yaml file enumerating exactly which fastq files we've started
    analyzing.
    """
    output_file_path = os.path.join(
        project.base_path,
        "ANALYSIS",
        project.dirname,
        "piper_ngi",
        "logs",
        "{}.files".format(job_identifier),
    )
    analysis_dict = {}
    proj_dict = analysis_dict[project.dirname] = {}
    for sample in project:
        samp_dict = proj_dict[sample.name] = {}
        for libprep in sample:
            lib_dict = samp_dict[libprep.name] = {}
            for seqrun in libprep:
                lib_dict[seqrun.name] = seqrun.fastq_files
    rotate_file(output_file_path)
    safe_makedir(os.path.dirname(output_file_path))
    with open(output_file_path, "w") as f:
        f.write(yaml.dump(analysis_dict))


def create_project_obj_from_analysis_log(
    project_name, project_id, project_base_path, sample_id, workflow
):
    """Using the log of seqruns used for a sample analysis, recreate a project
    object with relevant sample, libprep, and seqrun objects.
    """
    analysis_log_filename = "{}-{}-{}.files".format(project_id, sample_id, workflow)
    analysis_log_path = os.path.join(
        project_base_path,
        "ANALYSIS",
        project_id,
        "piper_ngi",
        "logs",
        analysis_log_filename,
    )
    with open(analysis_log_path, "r") as f:
        analysis_dict = yaml.load(f)
    project_obj = NGIProject(
        name=project_name,
        dirname=project_id,
        project_id=project_id,
        base_path=project_base_path,
    )
    sample_obj = project_obj.add_sample(sample_id, sample_id)
    for libprep_name, seqrun_dict in analysis_dict[project_id][sample_id].items():
        libprep_obj = sample_obj.add_libprep(libprep_name, libprep_name)
        for seqrun_name in seqrun_dict.keys():
            libprep_obj.add_seqrun(seqrun_name, seqrun_name)
    return project_obj


def check_for_preexisting_sample_runs(
    project_obj,
    sample_obj,
    restart_running_jobs,
    restart_finished_jobs,
    status_field="alignment_status",
):
    """If any analysis is undergoing or has completed for this sample's
    seqruns, raise a RuntimeError.

    :param NGIProject project_obj: The project object
    :param NGISample sample_obj: The sample object
    :param boolean restart_running_jobs: command line parameter
    :param boolean restart_finished_jobs: command line parameter
    :param str status_field: The field to check in Charon (seqrun level)

    :raise RuntimeError if the status is RUNNING or DONE and the flags do not allow to continue
    """
    project_id = project_obj.project_id
    sample_id = sample_obj.name
    charon_session = CharonSession()
    sample_libpreps = charon_session.sample_get_libpreps(
        projectid=project_id, sampleid=sample_id
    )
    for libprep in sample_libpreps["libpreps"]:
        libprep_id = libprep["libprepid"]
        for seqrun in charon_session.libprep_get_seqruns(
            projectid=project_id, sampleid=sample_id, libprepid=libprep_id
        )["seqruns"]:
            seqrun_id = seqrun["seqrunid"]
            aln_status = charon_session.seqrun_get(
                projectid=project_id,
                sampleid=sample_id,
                libprepid=libprep_id,
                seqrunid=seqrun_id,
            ).get(status_field)
            if (
                aln_status == "RUNNING"
                or aln_status == "UNDER_ANALYSIS"
                and not restart_running_jobs
            ) or (aln_status == "DONE" and not restart_finished_jobs):
                raise RuntimeError(
                    'Project/Sample "{}/{}" has a preexisting '
                    'seqrun "{}" with status "{}"'.format(
                        project_obj, sample_obj, seqrun_id, aln_status
                    )
                )


SBATCH_HEADER = """#!/bin/bash -l

#SBATCH -A {slurm_project_id}
#SBATCH -p {slurm_queue}
#SBATCH -n {num_cores}
#SBATCH -t {slurm_time}
#SBATCH -J {job_name}
#SBATCH -o {slurm_out_log}
#SBATCH -e {slurm_err_log}
"""


def create_sbatch_header(
    slurm_project_id,
    slurm_queue,
    num_cores,
    slurm_time,
    job_name,
    slurm_out_log,
    slurm_err_log,
):
    """
    :param str slurm_project_id: The project ID to use for accounting (e.g. "b2013064")
    :param str slurm_queue: "node" or "core"
    :param int num_cores: How many cores to use (max 16 at the moment)
    :param str slurm_time: The time for to schedule (e.g. "0-12:34:56")
    :param str job_name: The name to use for the job (e.g. "Your Mom")
    :param str slurm_out_log: The path to use for the slurm stdout log
    :param str slurm_err_log: The path to use for the slurm stderr log
    """
    ## TODO check how many cores are available for a given slurm queue
    if num_cores > 16:
        num_cores = 16
    return SBATCH_HEADER.format(
        slurm_project_id=slurm_project_id,
        slurm_queue=slurm_queue,
        num_cores=num_cores,
        slurm_time=slurm_time,
        job_name=job_name,
        slurm_out_log=slurm_out_log,
        slurm_err_log=slurm_err_log,
    )


def add_exit_code_recording(cl, exit_code_path):
    """Takes a command line and returns it with increased pizzaz"""
    record_exit_code = "; echo $? > {}".format(exit_code_path)
    if type(cl) is list:
        # This should work, right? Right
        cl = " ".join(cl)
    return cl + record_exit_code


def create_log_file_path(
    workflow_subtask,
    project_base_path,
    project_name,
    project_id=None,
    sample_id=None,
    libprep_id=None,
    seqrun_id=None,
):
    file_base_pathname = _create_generic_output_file_path(
        workflow_subtask,
        project_base_path,
        project_name,
        project_id,
        sample_id,
        libprep_id,
        seqrun_id,
    )
    return file_base_pathname + ".log"


def create_exit_code_file_path(
    workflow_subtask,
    project_base_path,
    project_name,
    project_id,
    sample_id=None,
    libprep_id=None,
    seqrun_id=None,
):
    file_base_pathname = _create_generic_output_file_path(
        workflow_subtask,
        project_base_path,
        project_name,
        project_id,
        sample_id,
        libprep_id,
        seqrun_id,
    )
    return file_base_pathname + ".exit"


def _create_generic_output_file_path(
    workflow_subtask,
    project_base_path,
    project_name,
    project_id,
    sample_id=None,
    libprep_id=None,
    seqrun_id=None,
):
    base_path = os.path.join(
        project_base_path, "ANALYSIS", project_id, "piper_ngi", "logs"
    )
    file_name = project_id
    if sample_id:
        file_name += "-{}".format(sample_id)
        if libprep_id:
            file_name += "-{}".format(libprep_id)
            if seqrun_id:
                file_name += "-{}".format(seqrun_id)
    file_name += "-{}".format(workflow_subtask)
    return os.path.join(base_path, file_name)
