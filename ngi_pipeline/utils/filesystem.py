from __future__ import print_function

import contextlib
import datetime
import fnmatch
import functools
import glob
import os
import re
import shlex
import shutil
import stat
import subprocess
import tempfile

from ngi_pipeline.conductor.classes import NGIProject
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.classes import with_ngi_config

from requests.exceptions import Timeout
from six.moves import map
from six.moves import filter


LOG = minimal_logger(__name__)


@with_ngi_config
def load_modules(modules_list, config=None, config_file_path=None):
    """
    Takes a list of environment modules to load (in order) and
    loads them using modulecmd python load

    :param list modules_list: The list of modules to load

    :raises RuntimeError: If there is a problem loading the modules
    """
    # Module loading is normally controlled by a bash function
    # As well as the modulecmd bash which is used in .bashrc, there's also
    # a modulecmd python which allows us to use modules from within python
    # UPPMAX support staff didn't seem to know this existed, so use with caution
    error_msgs = []
    for module in modules_list:
        # Yuck
        lmod_location = os.environ.get("LMOD_CMD", "/usr/lib/lmod/lmod/libexec/lmod")
        cl = "{lmod} python load {module}".format(lmod=lmod_location, module=module)
        p = subprocess.Popen(
            shlex.split(cl), stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        stdout, stderr = p.communicate()
        try:
            assert stdout, stderr
            exec(stdout)
        except Exception as e:
            error_msg = "Error loading module {}: {}".format(module, e)
            error_msgs.append(error_msg)
    if error_msgs:
        raise RuntimeError("".join(error_msgs))


@with_ngi_config
def locate_flowcell(flowcell, config=None, config_file_path=None):
    """Given a flowcell, returns the full path to the flowcell if possible,
    searching the config file's specified environment.flowcell_inbox
    if needed. If the flowcell passed in is already a valid path, returns that.

    :param str flowcell: The name of (or path to) the flowcell
    :returns: The path to the flowcell
    :rtype: str
    :raises ValueError: If a valid path cannot be found
    """
    if os.path.exists(flowcell):
        return os.path.abspath(flowcell)
    else:
        try:
            flowcell_inbox_dirs = config["environment"]["flowcell_inbox"]
        except (KeyError, TypeError) as e:
            raise ValueError(
                "Path to incoming flowcell directory not available in "
                "config file (environment.flowcell_inbox) and flowcell "
                "is not an absolute path ({}).".format(flowcell)
            )
        else:
            for flowcell_inbox_dir in flowcell_inbox_dirs:
                flowcell_dir = os.path.join(flowcell_inbox_dir, flowcell)
                if os.path.exists(flowcell_dir):
                    return flowcell_dir

            raise ValueError(
                "Flowcell directory passed as flowcell name (not full "
                "path) and does not exist under incoming flowcell dir "
                "as specified in configuration file (at {}).".format(flowcell_dir)
            )


@with_ngi_config
def locate_project(
    project, subdir="DATA", resolve_symlinks=True, config=None, config_file_path=None
):
    """Given a project, returns the full path to the project if possible,
    searching the config file's specified analysis.top_dir if needed.
    If the project passed in is already a valid path, returns that.

    :param str project: The name of (or path to) the project
    :param str subdir: The subdirectory to use ("DATA" or "ANALYSIS")
    :param bool resolve_symlinks: Resolve symlinks when found (default True)
    :returns: The path to the project
    :rtype: str
    :raises ValueError: If a valid path cannot be found
    """
    if os.path.exists(project):
        return os.path.abspath(project)
    else:
        try:
            project_data_dir = os.path.join(
                config["analysis"]["base_root"],
                config["analysis"]["sthlm_root"],
                config["analysis"]["top_dir"],
                subdir,
            )
            if not os.path.exists(project_data_dir):
                project_data_dir = os.path.join(
                    config["analysis"]["base_root"],
                    config["analysis"]["upps_root"],
                    config["analysis"]["top_dir"],
                    subdir,
                )
        except (KeyError, TypeError) as e:
            raise ValueError(
                "Path to project data directory not available in "
                "config file (analysis.top_dir) and project "
                "is not an absolute path ({}).".format(project)
            )
        else:
            project_dir = os.path.join(project_data_dir, project)
        if not os.path.exists(project_dir):
            raise ValueError(
                "project directory passed as project name (not "
                "full path) and does not exist under project "
                "data directory as specified in configuration "
                "file (at {}).".format(project_dir)
            )
        else:
            if os.path.islink(project_dir):
                try:
                    return os.path.realpath(project_dir)
                except OSError:
                    pass
            return project_dir


def execute_command_line(cl, shell=False, stdout=None, stderr=None, cwd=None):
    """Execute a command line and return the subprocess.Popen object.

    :param cl: Can be either a list or a string; if string, gets shlex.splitted
    :param bool shell: value of shell to pass to subprocess
    :param file stdout: The filehandle destination for STDOUT (can be None)
    :param file stderr: The filehandle destination for STDERR (can be None)
    :param str cwd: The directory to be used as CWD for the process launched

    :returns: The subprocess.Popen object
    :rtype: subprocess.Popen

    :raises RuntimeError: If the OS command-line execution failed.
    """
    if cwd and not os.path.isdir(cwd):
        LOG.warning(
            'CWD specified, "{}", is not a valid directory for '
            'command "{}". Setting to None.'.format(cwd, cl)
        )
        ## FIXME Better to just raise an exception
        cwd = None
    if type(cl) is str and shell == False:
        LOG.info("Executing command line: {}".format(cl))
        cl = shlex.split(cl)
    if type(cl) is list and shell == True:
        cl = " ".join(cl)
        LOG.info("Executing command line: {}".format(cl))
    try:
        p_handle = subprocess.Popen(
            cl,
            stdout=stdout,
            stderr=stderr,
            cwd=cwd,
            shell=shell,
            universal_newlines=True,
        )
        error_msg = None
    except OSError:
        error_msg = (
            "Cannot execute command; missing executable on the path? "
            '(Command "{}")'.format(cl)
        )
    except ValueError:
        error_msg = (
            "Cannot execute command; command malformed. " '(Command "{}")'.format(cl)
        )
    except subprocess.CalledProcessError as e:
        error_msg = 'Error when executing command: "{}" ' '(Command "{}")'.format(e, cl)
    if error_msg:
        raise RuntimeError(error_msg)
    return p_handle


def do_symlink(src_files, dst_dir):
    do_link(src_files, dst_dir, "soft")


def do_hardlink(src_files, dst_dir):
    do_link(src_files, dst_dir, "hard")


def do_link(src_files, dst_dir, link_type="soft"):
    if link_type == "hard":
        link_f = os.link
    else:
        link_f = os.symlink
    for src_file in src_files:
        base_file = os.path.basename(src_file)
        dst_file = os.path.join(dst_dir, base_file)
        if not os.path.isfile(dst_file):
            link_f(os.path.realpath(src_file), dst_file)


def safe_makedir(dname, mode=0o2770):
    """Make a directory (tree) if it doesn't exist, handling concurrent race
    conditions.
    """
    if not os.path.exists(dname):
        # we could get an error here if multiple processes are creating
        # the directory at the same time. Grr, concurrency.
        try:
            os.makedirs(dname, mode=mode)
        except OSError:
            if not os.path.isdir(dname):
                raise
    return dname


def rotate_file(file_path, new_subdirectory="rotated_files"):
    if os.path.exists(file_path) and os.path.isfile(file_path):
        file_dirpath, extension = os.path.splitext(file_path)
        file_name = os.path.basename(file_dirpath)
        current_datetime = datetime.datetime.now().strftime("%Y-%m-%d_%H:%M:%S:%f")
        if new_subdirectory:
            rotated_file_basepath = os.path.join(
                os.path.dirname(file_path), new_subdirectory
            )
        else:
            rotated_file_basepath = os.path.dirname(file_path)
        safe_makedir(rotated_file_basepath)

        rotate_file_path = os.path.join(
            rotated_file_basepath,
            "{}-{}.rotated{}".format(file_name, current_datetime, extension),
        )
        ## TODO what exceptions can we get here? OSError, else?
        try:
            LOG.info(
                'Attempting to rotate file "{}" to ' '"{}"...'.format(
                    file_path, rotate_file_path
                )
            )
            ## FIXME check if the log file is currently open!!?? How?!!
            shutil.move(file_path, rotate_file_path)
        except OSError as e:
            raise OSError(
                'Could not rotate log file "{}" to "{}": ' "{}".format(
                    file_path, rotate_file_path, e
                )
            )


@contextlib.contextmanager
def chdir(new_dir):
    """Context manager to temporarily change to a new directory."""
    cur_dir = os.getcwd()
    # This is weird behavior. I'm removing and and we'll see if anything breaks.
    # safe_makedir(new_dir)
    os.chdir(new_dir)
    try:
        yield
    finally:
        os.chdir(cur_dir)


@with_ngi_config
def recreate_project_from_filesystem(
    project_dir,
    restrict_to_samples=None,
    restrict_to_libpreps=None,
    restrict_to_seqruns=None,
    config=None,
    config_file_path=None,
):
    """Recreates the full project/sample/libprep/seqrun set of
    NGIObjects using the directory tree structure."""

    from ngi_pipeline.database.classes import CharonError
    from ngi_pipeline.database.communicate import get_project_id_from_name

    if not restrict_to_samples:
        restrict_to_samples = []
    if not restrict_to_libpreps:
        restrict_to_libpreps = []
    if not restrict_to_seqruns:
        restrict_to_seqruns = []

    project_dir = locate_project(project_dir)

    if os.path.islink(os.path.abspath(project_dir)):
        real_project_dir = os.path.realpath(project_dir)
        syml_project_dir = os.path.abspath(project_dir)
    else:
        real_project_dir = os.path.abspath(project_dir)
        search_dir = os.path.join(os.path.dirname(project_dir), "*")
        sym_files = list(filter(os.path.islink, glob.glob(search_dir)))
        for sym_file in sym_files:
            if os.path.realpath(sym_file) == os.path.realpath(real_project_dir):
                syml_project_dir = os.path.abspath(sym_file)
                break
        else:
            syml_project_dir = None
    project_base_path, project_id = os.path.split(real_project_dir)
    if syml_project_dir:
        project_base_path, project_name = os.path.split(syml_project_dir)
    else:  # project name is the same as project id (Uppsala perhaps)
        project_name = project_id
    if os.path.split(project_base_path)[1] == "DATA":
        project_base_path = os.path.split(project_base_path)[0]
    LOG.info('Setting up project "{}"'.format(project_id))
    project_obj = NGIProject(
        name=project_name,
        dirname=project_id,
        project_id=project_id,
        base_path=project_base_path,
    )
    samples_pattern = os.path.join(real_project_dir, "*")
    samples = list(filter(os.path.isdir, glob.glob(samples_pattern)))
    if not samples:
        LOG.warning('No samples found for project "{}"'.format(project_obj))
    for sample_dir in samples:
        sample_name = os.path.basename(sample_dir)
        if restrict_to_samples and sample_name not in restrict_to_samples:
            LOG.debug(
                'Skipping sample "{}": not in specified samples ' '"{}"'.format(
                    sample_name, ", ".join(restrict_to_samples)
                )
            )
            continue
        LOG.info('Setting up sample "{}"'.format(sample_name))
        sample_obj = project_obj.add_sample(name=sample_name, dirname=sample_name)

        libpreps_pattern = os.path.join(sample_dir, "*")
        libpreps = list(filter(os.path.isdir, glob.glob(libpreps_pattern)))
        if not libpreps:
            LOG.warning('No libpreps found for sample "{}"'.format(sample_obj))
        for libprep_dir in libpreps:
            libprep_name = os.path.basename(libprep_dir)
            if restrict_to_libpreps and libprep_name not in restrict_to_libpreps:
                LOG.debug(
                    'Skipping libprep "{}": not in specified libpreps ' '"{}"'.format(
                        libprep_name, ", ".join(restrict_to_libpreps)
                    )
                )
                continue
            LOG.info('Setting up libprep "{}"'.format(libprep_name))
            libprep_obj = sample_obj.add_libprep(
                name=libprep_name, dirname=libprep_name
            )

            seqruns_pattern = os.path.join(libprep_dir, "*_*_*")
            seqruns = list(filter(os.path.isdir, glob.glob(seqruns_pattern)))
            if not seqruns:
                LOG.warning('No seqruns found for libprep "{}"'.format(libprep_obj))
            for seqrun_dir in seqruns:
                seqrun_name = os.path.basename(seqrun_dir)
                if restrict_to_seqruns and seqrun_name not in restrict_to_seqruns:
                    LOG.debug(
                        'Skipping seqrun "{}": not in specified seqruns ' '"{}"'.format(
                            seqrun_name, ", ".join(restrict_to_seqruns)
                        )
                    )
                    continue
                LOG.info('Setting up seqrun "{}"'.format(seqrun_name))
                seqrun_obj = libprep_obj.add_seqrun(
                    name=seqrun_name, dirname=seqrun_name
                )
                for fq_file in fastq_files_under_dir(seqrun_dir, realpath=False):
                    fq_name = os.path.basename(fq_file)
                    LOG.info(
                        'Adding fastq file "{}" to seqrun "{}"'.format(
                            fq_name, seqrun_obj
                        )
                    )
                    seqrun_obj.add_fastq_files([fq_name])
    return project_obj


def is_index_file(fastq_file, index_file_pattern=r"_L00\d_I\d_"):
    """
    Returns True if the fastq file appears to be an index file, based on the file name pattern

    :param fastq_file: the file name of the fastq file
    :param index_file_pattern: a regexp pattern that discriminates index files from non-index files.
    Will use '_L00\d_I\d_' if not specified
    :return: True if file name matches the index file pattern, False otherwise
    """
    return re.search(index_file_pattern, os.path.basename(fastq_file)) is not None


def fastq_files_under_dir(dirname, realpath=True):
    return match_files_under_dir(
        dirname,
        pattern=".*\.(fastq|fq)(\.gz|\.gzip|\.bz2)?$",
        pt_style="regex",
        realpath=realpath,
    )


def match_files_under_dir(dirname, pattern, pt_style="regex", realpath=True):
    """Find all the files under a directory that match pattern.

    :parm str dirname: The directory under which to search
    :param str pattern: The pattern against which to match
    :param str pt_style: pattern style, "regex" or "shell"
    :param bool realpath: If true, dereferences symbolic links

    :returns: A list of full paths to the fastq files, using dereferenced paths if realpath=True
    :rtype: list
    """
    if pt_style not in ("regex", "shell"):
        LOG.warning(
            'Chosen pattern style "{}" invalid (must be "regex" or "shell"); '
            'falling back to "regex".'
        )
        pt_style = "regex"
    if pt_style == "regex":
        pt_comp = re.compile(pattern)
    matches = []
    for root, dirnames, filenames in os.walk(dirname):
        if pt_style == "shell":
            for filename in fnmatch.filter(filenames, pattern):
                match = os.path.abspath(os.path.join(root, filename))
                file_path = os.path.join(root, filename)
                if realpath:
                    matches.append(os.path.realpath(file_path))
                else:
                    matches.append(os.path.abspath(file_path))
        else:  # regex-style
            file_matches = list(filter(pt_comp.search, filenames))
            file_paths = [os.path.join(root, filename) for filename in file_matches]
            if file_paths:
                if realpath:
                    matches.extend(list(map(os.path.realpath, file_paths)))
                else:
                    matches.extend(list(map(os.path.abspath, file_paths)))
    return matches
