"""The Piper automated launcher script."""
from __future__ import print_function

import collections
import os
import re
import shlex
import shutil
import subprocess
import time

from ngi_pipeline.engines.piper_ngi import workflows
from ngi_pipeline.engines.piper_ngi.local_process_tracking import is_seqrun_analysis_running_local, \
                                                                  is_sample_analysis_running_local, \
                                                                  record_process_seqrun, \
                                                                  record_process_sample
from ngi_pipeline.engines.piper_ngi.utils import create_exit_code_file_path, \
                                                 create_log_file_path, \
                                                 create_sbatch_header
from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.log.loggers import log_process_non_blocking, minimal_logger
from ngi_pipeline.utils.filesystem import load_modules, execute_command_line, rotate_file, safe_makedir
from ngi_pipeline.utils.classes import with_ngi_config
from ngi_pipeline.utils.parsers import parse_lane_from_filename, find_fastq_read_pairs_from_dir, \
                                       get_flowcell_id_from_dirtree

LOG = minimal_logger(__name__)


def get_subtasks_for_level(level):
    """For a given level (e.g. "seqrun" or "sample"), get all the associated
    subtasks that should be run (e.g. "qc", "dna_alignonly")

    :param str level: The level ("seqrun", "sample")
    :returns: The names (strings) of the workflows that should be run at that level
    :rtype: tuple
    """
    if level == "seqrun":
        return ["dna_alignonly"] #"qc",
    elif level == "sample":
        return ["merge_process_variantcall"]
    else:
        raise NotImplementedError('The level "{}" has no associated subtasks.')


@with_ngi_config
def analyze_seqrun(project, sample, libprep, seqrun,
                   config=None, config_file_path=None):
    """Analyze data at the sequencing run (individual fastq) level.

    :param NGIProject project: the project to analyze
    :param NGISample sample: the sample to analyzed
    :param NGILibraryPrep libprep: The library prep to analyzed
    :seqrun NGISeqrun seqrun: The sequencing run to analyzed
    :param dict config: The parsed configuration file (optional)
    :param str config_file_path: The path to the configuration file (optional)
    """
    modules_to_load = config.get("piper", {}).get("load_modules")
    if modules_to_load: load_modules(modules_to_load)
    try:
        ## FIXME This isn't ideal -- we're copying data to a separate node for each workflow subtask.
        ##       These should most likely be combined into a single sbatch file and run sequentially?
        for workflow_subtask in get_subtasks_for_level(level="seqrun"):
            if not is_seqrun_analysis_running_local(workflow_subtask=workflow_subtask,
                                                    project_id=project.project_id,
                                                    sample_id=sample.name,
                                                    libprep_id=libprep.name,
                                                    seqrun_id=seqrun.name):
                ## Temporarily logging to a file until we get ELK set up
                log_file_path = create_log_file_path(workflow_subtask=workflow_subtask,
                                                     project_base_path=project.base_path,
                                                     project_name=project.name,
                                                     sample_id=sample.name,
                                                     libprep_id=libprep.name,
                                                     seqrun_id=seqrun.name)
                rotate_file(log_file_path)

                # Store the exit code of detached processes
                ## Exit code does not go to scratch -- keep on /apus
                exit_code_path = create_exit_code_file_path(workflow_subtask=workflow_subtask,
                                                            project_base_path=project.base_path,
                                                            project_name=project.name,
                                                            sample_id=sample.name,
                                                            libprep_id=libprep.name,
                                                            seqrun_id=seqrun.name)

                build_setup_xml(project, config, sample, libprep.name, seqrun.name)

                command_line = (build_piper_cl(project, workflow_subtask, exit_code_path, config))
                slurm_job_id = sbatch_piper_job(command_line, workflow_subtask,
                                                project, sample, libprep, seqrun)
                try:
                    record_process_seqrun(project=project,
                                          sample=sample,
                                          libprep=libprep,
                                          seqrun=seqrun,
                                          analysis_module_name="piper_ngi",
                                          analysis_dir=project.analysis_dir,
                                          slurm_job_id=slurm_job_id,
                                          workflow_subtask=workflow_subtask)
                except RuntimeError as e:
                    ## This is a problem. If the job isn't recorded, we won't
                    ## ever know that it has been run.
                    LOG.error('Could not record process for project/sample/libprep/seqrun'
                              '{}/{}/{}/{}, workflow {}'.format(project, sample,
                                                                libprep, seqrun,
                                                                workflow_subtask))
                    continue
    except (NotImplementedError, RuntimeError) as e:
        error_msg = ('Processing project "{}" / sample "{}" / libprep "{}" / '
                     'seqrun "{}" failed: {}'.format(project, sample, libprep, seqrun,
                                                   e.__repr__()))
        LOG.error(error_msg)

@with_ngi_config
def analyze_sample(project, sample, config=None, config_file_path=None):
    """Analyze data at the sample level.

    :param NGIProject project: the project to analyze
    :param NGISample sample: the sample to analyzed
    :param dict config: The parsed configuration file (optional)
    :param str config_file_path: The path to the configuration file (optional)
    """
    modules_to_load = ["java/sun_jdk1.7.0_25", "R/2.15.0"]
    load_modules(modules_to_load)
    charon_session = CharonSession()
    sample_total_autosomal_coverage = charon_session.sample_get(project.project_id,
                                      sample.name).get('total_autosomal_coverage')
    try:
        required_total_autosomal_coverage = int(config.get("piper", {}).get("sample", {}).get("required_autosomal_coverage"))
    except (TypeError, ValueError) as e:
        LOG.error('Unable to parse required total autosomal coverage value from '
                  'config file (value was "{}"); using 30 instead.'.format(required_total_autosomal_coverage))
        required_total_autosomal_coverage = 30
    if sample_total_autosomal_coverage >= required_total_autosomal_coverage:
        LOG.info('Sample "{}" in project "{}" is ready for processing.'.format(sample, project))
        for workflow_subtask in get_subtasks_for_level(level="sample"):
            if not is_sample_analysis_running_local(workflow_subtask=workflow_subtask,
                                                    project_id=project.project_id,
                                                    sample_id=sample.name):
                try:
                    ## Temporarily logging to a file until we get ELK set up
                    log_file_path = create_log_file_path(workflow_subtask=workflow_subtask,
                                                         project_base_path=project.base_path,
                                                         project_name=project.name,
                                                         sample_id=sample.name)
                    rotate_file(log_file_path)
                    # Store the exit code of detached processes
                    exit_code_path = create_exit_code_file_path(workflow_subtask=workflow_subtask,
                                                                project_base_path=project.base_path,
                                                                project_name=project.name,
                                                                sample_id=sample.name)

                    ## TODO Does this work? Are the paths relative?
                    ##      If not (and maybe in any event) we should move this step
                    ##      to the sbatch file after the files are copied over to local scratch
                    build_setup_xml(project, config, sample)
                    command_line = build_piper_cl(project, workflow_subtask, exit_code_path, config)
                    slurm_job_id = sbatch_piper_job(command_line, workflow_subtask,
                                                    project, sample, libprep, seqrun)
                    try:
                        record_process_sample(project=project,
                                              sample=sample,
                                              analysis_module_name="piper_ngi",
                                              analysis_dir=project.analysis_dir,
                                              slurm_job_id=slurm_job_id,
                                              workflow_subtask=workflow_subtask)
                    except RuntimeError as e:
                        LOG.error('Could not record process for project/sample '
                                  '{}/{}, workflow {}'.format(project, sample,
                                                              workflow_subtask))
                        continue
                except (NotImplementedError, RuntimeError) as e:
                    error_msg = ('Processing project "{}" / sample "{}" failed: '
                                 '{}'.format(project, sample, e.__repr__()))
                    LOG.error(error_msg)
    else:
        LOG.info('Sample "{}" in project "{}" is not yet ready for '
                 'processing.'.format(sample, project))


@with_ngi_config
def sbatch_piper_job(command_line, workflow_name, project, sample, libprep=None,
                     seqrun=None, config=None, config_file_path=None):
    """sbatch a piper workflow. This is a little messy at the moment but if
    the workflow name is "dna_alignonly" we also append a sample-level
    analysis. It seems kludgy but I guess it's kind of a kludgy workflow.

    :param str command_line: The command line to execute
    :param str workflow_name: The name of the workflow to execute
    :param NGIProject project: The NGIProject
    :param NGISample sample: The NGISample
    :param NGILibraryPrep libprep: The NGILibraryPrep
    :param NGISeqrun seqrun: The NGISeqrun
    :param dict config: The parsed configuration file (optional)
    :param str config_file_path: The path to the configuration file (optional)
    """
    job_identifier = "{}-{}-".format(project, sample)
    if libprep and seqrun:
        job_identifier += "{}-{}-".format(libprep, seqrun)
    job_identifier += workflow_name

    # Paths to the various data directories
    project_dirname = project.dirname
    sample_dirname = sample.dirname
    libprep_dirname = libprep.__str__() or ""
    seqrun_dirname = seqrun.__str__() or ""
    # Final "" adds a trailing slash (for rsync)
    ## TODO CHECK THIS LATER IT'S 16:55 FRIDAY
    data_specific_path = os.path.join(project_dirname, sample_dirname, libprep_dirname, seqrun_dirname)
    scratch_data_dir = os.path.join("$SNIC_TMP/DATA", data_specific_path, "")
    scratch_analysis_dir = os.path.join("$SNIC_TMP/ANALYSIS/", project_dirname, "")
    perm_data_dir = os.path.join(project.base_path, "DATA", data_specific_path, "")
    perm_data_topdir = os.path.join(project.base_path, "DATA", project_dirname, "")
    perm_analysis_dir = os.path.join(project.base_path, "ANALYSIS", project_dirname, "")

    # Slurm-specific data
    try:
        slurm_project_id = config["environment"]["project_id"]
    except KeyError:
        raise RuntimeError('No SLURM project id specified in configuration file '
                           'for job "{}"'.format(job_identifier))
    slurm_queue = config.get("slurm", {}).get("queue") or "node"
    num_cores = config.get("slurm", {}).get("cores") or 16
    ## This depends on the workflow and cluster but I guess I can just hardcode for now
    #slurm_time = config.get("piper", {}).get("job_walltime")
    slurm_time = "4-00:00:00" # 4 days
    slurm_out_log = os.path.join(perm_analysis_dir, "logs", "{}_sbatch.out".format(job_identifier))
    slurm_err_log = os.path.join(perm_analysis_dir, "logs", "{}_sbatch.err".format(job_identifier))
    sbatch_text = create_sbatch_header(slurm_project_id=slurm_project_id,
                                       slurm_queue=slurm_queue,
                                       num_cores=num_cores,
                                       slurm_time=slurm_time,
                                       job_name="piper_{}".format(job_identifier),
                                       slurm_out_log=slurm_out_log,
                                       slurm_err_log=slurm_err_log)
    sbatch_text_list = sbatch_text.split("\n")
    sbatch_extra_params = config.get("slurm", {}).get("extra_params", {})
    for param, value in sbatch_extra_params.iteritems():
        sbatch_text_list.append("#SBATCH {} {}".format(param, value))
    # Pull these from the config file
    for module_name in config.get("piper", {}).get("load_modules", []):
        sbatch_text_list.append("module load {}".format(module_name))

    # Move the input files to scratch
    sbatch_text_list.append("mkdir -p {}".format(scratch_data_dir))
    # These trailing slashes are of course important when using rsync
    sbatch_text_list.append("rsync -a {}/ {}/".format(perm_data_dir, scratch_data_dir))

    # Piper command line
    sbatch_text_list.append(command_line)

    if workflow_name == "dna_alignonly":
        # In this case we need to launch sample-level analysis as well
        try:
            conda_environment = config.get("environment", {}).get("conda_env") or \
                                os.environ["CONDA_DEFAULT_ENV"]
        except KeyError:
            LOG.error("Could not determine conda environment to activate for sample-level "
                      "analysis within the sbatch file; skipping automatic sample-level "
                      "analysis (checked config file and $CONDA_DEFAULT_ENV) for "
                      "analysis {}".format(job_identifier))
        else:
            # Need the path to the ngi_pipeline scripts to launch sample-level analysis
            # from within the sbatch file
            ngi_pipeline_scripts_dir = config.get("environment", {}).get("ngi_scripts_dir") or \
                                       os.environ["NGI_PIPELINE_SCRIPTS"]
            try:
                required_total_autosomal_coverage = int(config.get("piper", {}).get("sample", {}).get("required_autosomal_coverage"))
            except (TypeError, ValueError) as e:
                LOG.error('Unable to parse required total autosomal coverage value from '
                          'config file, using 30 instead.')
            required_total_autosomal_coverage = 30
            bash_conditional = \
            ('source activate {conda_environment}\n'
             'python {scripts_dir}/update_charon_with_local_jobs_status.py -e piper\n'
             'if [[ $(python {scripts_dir}/check_charon_coverage.py -p {project_id}, -s {sample_id} -c {req_coverage} && echo $?) ]]; then\n'
             '   python {scripts_dir}/start_pipeline_from_project \\ \n'
             '          --sample-only \\ \n'
             '          --sample {sample_id} \\ \n'
             '          {scratch_analysis_dir}\n'
             'fi'.format(project_id=project.project_id,
                         sample_id=sample.name,
                         scripts_dir=ngi_pipeline_scripts_dir,
                         scratch_analysis_dir=scratch_analysis_dir,
                         conda_environment=conda_environment,
                         req_coverage=required_total_autosomal_coverage))
            sbatch_text_list.extend(bash_conditional.split("\n"))

    sbatch_text_list.append("rsync -a {}/ {}/\n".format(scratch_analysis_dir, perm_analysis_dir))

    sbatch_dir = os.path.join(perm_analysis_dir, "sbatch")
    safe_makedir(sbatch_dir)
    sbatch_outfile = os.path.join(sbatch_dir, "{}.sbatch".format(job_identifier))
    if os.path.exists(sbatch_outfile):
        rotate_file(sbatch_outfile)
    with open(sbatch_outfile, 'w') as f:
        f.write("\n".join(sbatch_text_list))
    LOG.info("Queueing sbatch file {} for job {}".format(sbatch_outfile, job_identifier))
    p_handle = execute_command_line("sbatch {}".format(sbatch_outfile),
                                    stdout=subprocess.PIPE,
                                    stderr=subprocess.PIPE)
    p_out, p_err = p_handle.communicate()
    try:
        ## Parse the thing to get the slurm job id
        slurm_job_id = re.match(r'Submitted batch job (\d+)', p_out).groups()[0]
    except AttributeError:
        raise RuntimeError('Could not submit sbatch job for workflow "{}": '
                           '{}'.format(job_identifier, p_err))
    return int(slurm_job_id)


def launch_piper_job(command_line, project, log_file_path=None):
    """Launch the Piper command line.

    :param str command_line: The command line to execute
    :param Project project: The Project object (needed to set the CWD)

    :returns: The subprocess.Popen object for the process
    :rtype: subprocess.Popen
    """
    # Jobs launched via the command line do not utilize scratch space --
    # use the standard project base path
    working_dir = os.path.join(project.base_path, "ANALYSIS", project.dirname)
    # This section of code is all logging
    file_handle=None
    if log_file_path:
        try:
            file_handle = open(log_file_path, 'w')
        except Exception as e:
            LOG.error('Could not open log file "{}"; reverting to standard '
                      'logger (error: {})'.format(log_file_path, e))
            log_file_path = None
    # Execute
    popen_object = execute_command_line(command_line, cwd=working_dir, shell=True,
                                        stdout=(file_handle or subprocess.PIPE),
                                        stderr=(file_handle or subprocess.PIPE))
    if not log_file_path:
        log_process_non_blocking(popen_object.stdout, LOG.info)
        log_process_non_blocking(popen_object.stderr, LOG.warn)
    return popen_object


def build_piper_cl(project, workflow_name, exit_code_path, config):
    """Determine which workflow to run for a project and build the appropriate command line.
    :param NGIProject project: The project object to analyze.
    :param str workflow_name: The name of the workflow to execute (e.g. "dna_alignonly")
    :param str exit_code_path: The path to the file to which the exit code for this cl will be written
    :param dict config: The (parsed) configuration file for this machine/environment.

    :returns: A list of Project objects with command lines to execute attached.
    :rtype: list
    :raises ValueError: If a required configuration value is missing.
    """
    # Find Piper global configuration:
    #   Check environmental variable PIPER_GLOB_CONF_XML
    #   then the config file
    #   then the file globalConfig.xml in the piper root dir

    piper_rootdir = config.get("piper", {}).get("path_to_piper_rootdir")
    piper_global_config_path = (os.environ.get("PIPER_GLOB_CONF_XML") or
                                config.get("piper", {}).get("path_to_piper_globalconfig") or
                                (os.path.join(piper_rootdir, "globalConfig.xml") if
                                piper_rootdir else None))
    if not piper_global_config_path:
        error_msg = ("Could not find Piper global configuration file in config file, "
                     "as environmental variable (\"PIPER_GLOB_CONF_XML\"), "
                     "or in Piper root directory.")
        raise ValueError(error_msg)

    # Find Piper QScripts dir:
    #   Check environmental variable PIPER_QSCRIPTS_DIR
    #   then the config file
    piper_qscripts_dir = (os.environ.get("PIPER_QSCRIPTS_DIR") or
                          config['piper']['path_to_piper_qscripts'])
    if not piper_qscripts_dir:
        error_msg = ("Could not find Piper QScripts directory in config file or "
                    "as environmental variable (\"PIPER_QSCRIPTS_DIR\").")
        raise ValueError(error_msg)

    LOG.info('Building workflow command line(s) for '
             'project "{}" / workflow "{}"'.format(project, workflow_name))
    ## NOTE This key will probably exist on the project level, and may have multiple values.
    ##      Workflows may imply a number of substeps (e.g. basic = qc, alignment, etc.) ?
    try:
        setup_xml_path = project.setup_xml_path
    except AttributeError:
        error_msg = ('Project "{}" has no setup.xml file. Skipping project '
                     'command-line generation.'.format(project))
        raise ValueError(error_msg)

    cl = workflows.return_cl_for_workflow(workflow_name=workflow_name,
                                          qscripts_dir_path=piper_qscripts_dir,
                                          setup_xml_path=setup_xml_path,
                                          global_config_path=piper_global_config_path,
                                          output_dir=project.analysis_dir)
    # Blank out the file if it already exists
    open(exit_code_path, 'w').close()
    return add_exit_code_recording(cl, exit_code_path)


def add_exit_code_recording(cl, exit_code_path):
    """Takes a command line and returns it with increased pizzaz"""
    record_exit_code = "; echo $? > {}".format(exit_code_path)
    if type(cl) is list:
        # This should work, right? Right
        cl = " ".join(cl)
    return cl + record_exit_code


def build_setup_xml(project, config, sample=None, libprep_id=None, seqrun_id=None):
    """Build the setup.xml file for each project using the CLI-interface of
    Piper's SetupFileCreator.

    :param NGIProject project: The project to be converted.
    :param dict config: The (parsed) configuration file for this machine/environment.
    :param NGISample sample: the sample object
    :param str library_id: id of the library
    :param str seqrun_id: flowcell identifier

    :returns: A list of Project objects with setup.xml paths as attributes.
    :rtype: list
    """

    if not seqrun_id:
        LOG.info('Building Piper setup.xml file for project "{}" '
                 'sample "{}"'.format(project, sample.name))
    else:
        LOG.info('Building Piper setup.xml file for project "{}" '
                 'sample "{}", libprep "{}", seqrun "{}"'.format(project, sample,
                                                                 libprep_id, seqrun_id))

    project_top_level_dir = os.path.join(project.base_path, "DATA", project.dirname)
    analysis_dir = os.path.join(project.base_path, "ANALYSIS", project.dirname)
    safe_makedir(analysis_dir, 0770)
    safe_makedir(os.path.join(analysis_dir, "logs"))
    cl_args = {'project': project.name}
    # Load needed data from database
    try:
        # Information we need from the database:
        # - species / reference genome that should be used (hg19, mm9)
        # - analysis workflows to run (QC, DNA alignment, RNA alignment, variant calling, etc.)
        # - adapters to be trimmed (?)
        ## <open connection to project database>
        #reference_genome = proj_db.get('species')
        reference_genome = 'GRCh37'
        # sequencing_center = proj_db.get('Sequencing Center')
        cl_args["sequencing_center"] = "NGI"
    except:
        ## Handle database connection failures here once we actually try to connect to it
        pass

    # Load needed data from configuration file
    try:
        cl_args["reference_path"] = config['supported_genomes'][reference_genome]
        cl_args["uppmax_proj"] = config['environment']['project_id']
    except KeyError as e:
        error_msg = ("Could not load required information from "
                     "configuration file and cannot continue with project {}: "
                     "value \"{}\" missing".format(project, e.message))
        raise ValueError(error_msg)

    try:
        cl_args["sfc_binary"] = config['piper']['path_to_setupfilecreator']
    except KeyError:
        # Assume setupFileCreator is on path
        cl_args["sfc_binary"] = "setupFileCreator"


    if not seqrun_id:
        output_xml_filepath = os.path.join(analysis_dir,
                                        "{}-{}-setup.xml".format(project, sample.name))
    else:
        output_xml_filepath = os.path.join(analysis_dir,
                                        "{}-{}-{}_setup.xml".format(project, sample.name, seqrun_id))

    cl_args["output_xml_filepath"]  = output_xml_filepath
    cl_args["sequencing_tech"]      = "Illumina"
    cl_args["qos"] = "seqver"
    setupfilecreator_cl = ("{sfc_binary} "
                           "--output {output_xml_filepath} "
                           "--project_name {project} "
                           "--sequencing_platform {sequencing_tech} "
                           "--sequencing_center {sequencing_center} "
                           "--uppnex_project_id {uppmax_proj} "
                           "--reference {reference_path} ").format(**cl_args)
                           #"--qos {qos}").format(**cl_args)
    #NOTE: here I am assuming the different dir structure, it would be wiser to change the object type and have an uppsala project
    if not seqrun_id:
        #if seqrun_id is none it means I want to create a sample level setup xml
        for libprep in sample:
            for seqrun in libprep:
                sample_run_directory = os.path.join(project_top_level_dir, sample.dirname, libprep.name, seqrun.name )
                for fastq_file_name in os.listdir(sample_run_directory):
                    #MARIO: I am not a big fun of this, IGN object need to be created from file system in order to avoid this things
                    fastq_file = os.path.join(sample_run_directory, fastq_file_name)
                    setupfilecreator_cl += " --input_fastq {}".format(fastq_file)
    else:
        #I need to create an xml file for this sample_run
        sample_run_directory = os.path.join(project_top_level_dir, sample.dirname, libprep_id, seqrun_id )
        for fastq_file_name in sample.libpreps[libprep_id].seqruns[seqrun_id].fastq_files:
            fastq_file = os.path.join(sample_run_directory, fastq_file_name)
            setupfilecreator_cl += " --input_fastq {}".format(fastq_file)

    try:
        LOG.info("Executing command line: {}".format(setupfilecreator_cl))
        subprocess.check_call(shlex.split(setupfilecreator_cl))
        project.setup_xml_path = output_xml_filepath
        project.analysis_dir   = analysis_dir
    except (subprocess.CalledProcessError, OSError, ValueError) as e:
        error_msg = ("Unable to produce setup XML file for project {}; "
                     "skipping project analysis. "
                     "Error is: \"{}\". .".format(project, e))
        raise RuntimeError(error_msg)
