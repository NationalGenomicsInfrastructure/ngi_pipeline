import os

from ngi_pipeline.database.classes import CharonSession, CharonError
from ngi_pipeline.engines.piper_ngi import workflows
from ngi_pipeline.engines.piper_ngi.utils import add_exit_code_recording
from ngi_pipeline.log.loggers import minimal_logger
from ngi_pipeline.utils.filesystem import safe_makedir

LOG = minimal_logger(__name__)


## TODO change this to use local_scratch_mode boolean instead of exec_mode
def build_piper_cl(project, workflow_name, setup_xml_path, exit_code_path,
                   config, genotype_file=None, exec_mode="local", generate_bqsr_bam=False):
    """Determine which workflow to run for a project and build the appropriate command line.
    :param NGIProject project: The project object to analyze.
    :param str workflow_name: The name of the workflow to execute (e.g. "dna_alignonly")
    :param str exit_code_path: The path to the file to which the exit code for this cl will be written
    :param dict config: The (parsed) configuration file for this machine/environment.
    :param str genotype_file: The path to the genotype file (only relevant for genotype workflow)
    :param str exec_mode: "local" or "sbatch"

    :returns: A list of Project objects with command lines to execute attached.
    :rtype: list
    :raises ValueError: If a required configuration value is missing.
    """
    if exec_mode == "sbatch":
        output_dir = os.path.join("$SNIC_TMP/ANALYSIS/", project.dirname, 'piper_ngi')
        # Can't create these directories ahead of time of course
    elif exec_mode == "local":
        output_dir = os.path.join(project.base_path, "ANALYSIS", project.dirname, 'piper_ngi')
        safe_makedir(output_dir)
    else:
        raise ValueError('"exec_mode" must be one of "local", "sbatch" (value '
                         'was "{}"'.format(exec_mode))

    # Global Piper configuration
    piper_rootdir = config.get("piper", {}).get("path_to_piper_rootdir")

    # QScripts directory
    try:
        piper_qscripts_dir = (os.environ.get("PIPER_QSCRIPTS_DIR") or
                              os.environ.get("PIPER_QSCRIPTS") or
                              config['piper']['path_to_piper_qscripts'])
    except KeyError:
        raise ValueError('Could not find Piper QScripts directory in config file or '
                         'as environmental variable ("PIPER_QSCRIPTS_DIR").')

    # Build Piper cl
    LOG.info('Building workflow command line(s) for project "{}" / workflow '
             '"{}"'.format(project, workflow_name))
    cl = workflows.return_cl_for_workflow(workflow_name=workflow_name,
                                          qscripts_dir_path=piper_qscripts_dir,
                                          setup_xml_path=setup_xml_path,
                                          genotype_file=genotype_file,
                                          output_dir=output_dir,
                                          exec_mode=exec_mode,
                                          generate_bqsr_bam=generate_bqsr_bam)
    # Blank out the file if it already exists
    safe_makedir(os.path.dirname(exit_code_path))
    open(exit_code_path, 'w').close()
    return cl 


def build_setup_xml(project, sample, workflow, local_scratch_mode, config):
    """Build the setup.xml file for each project using the CLI-interface of
    Piper's SetupFileCreator.

    :param NGIProject project: The project to be converted.
    :param NGISample sample: the sample object
    :param str workflow: The name of the workflow to be executed
    :param bool local_scratch_mode: Whether the job will be run in scratch or permanent storage
    :param dict config: The (parsed) configuration file for this machine/environment.

    :raises ValueError: If a required configuration file value is missing
    :raises RuntimeError: If the setupFileCreator returns non-zero
    """
    LOG.info('Building Piper setup.xml file for project "{}" '
             'sample "{}"'.format(project, sample.name))

    if local_scratch_mode:
        project_top_level_dir = os.path.join("$SNIC_TMP/DATA/", project.dirname)
        analysis_dir = os.path.join("$SNIC_TMP/ANALYSIS/", project.dirname, "piper_ngi")
        # Can't create these directories ahead of time of course
    else:
        project_top_level_dir = os.path.join(project.base_path, "DATA", project.dirname)
        analysis_dir = os.path.join(project.base_path, "ANALYSIS", project.dirname, "piper_ngi")
        safe_makedir(analysis_dir)

    cl_args = {'project': project.dirname}
    try:
        charon_session = CharonSession()
        charon_project = charon_session.project_get(project.project_id)
        cl_args["sequencing_center"] = charon_project["sequencing_facility"]
    except (KeyError, CharonError) as e:
        LOG.warn('Could not determine sequencing center from Charon ({}); setting to "Unknown".'.format(e))
        cl_args["sequencing_center"] = "Unknown"
    cl_args["sequencing_tech"] = "Illumina"
    slurm_qos = config.get("slurm", {}).get("extra_params", {}).get("--qos")
    if slurm_qos:
        cl_args["qos"] = slurm_qos

    # TODO Eventually this will be loaded from e.g. Charon
    reference_genome = 'GRCh37'
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
        cl_args["sfc_binary"] = "setupFileCreator" # Assume setupFileCreator is on path

    # setup XML file is always stored in permanent analysis directory
    output_xml_filepath = os.path.join(project.base_path, "ANALYSIS",
                                       project.dirname, "piper_ngi", "setup_xml_files",
                                       "{}-{}-{}-setup.xml".format(project, sample, workflow))
    safe_makedir(os.path.dirname(output_xml_filepath))
    cl_args["output_xml_filepath"] = output_xml_filepath
    setupfilecreator_cl = ("{sfc_binary} "
                           "--output {output_xml_filepath} "
                           "--project_name {project} "
                           "--sequencing_platform {sequencing_tech} "
                           "--sequencing_center {sequencing_center} "
                           "--uppnex_project_id {uppmax_proj} "
                           "--reference {reference_path}").format(**cl_args)
    if "qos" in cl_args:
        setupfilecreator_cl += " --qos {qos}".format(**cl_args)
    for samp in project:
        for libprep in samp:
            for seqrun in libprep:
                sample_run_directory = os.path.join(project_top_level_dir, sample.dirname,
                                                    libprep.dirname, seqrun.dirname)
                for fastq_file_name in seqrun.fastq_files:
                    fastq_file = os.path.join(sample_run_directory, fastq_file_name)
                    setupfilecreator_cl += " --input_fastq {}".format(fastq_file)
    return (setupfilecreator_cl, output_xml_filepath)
