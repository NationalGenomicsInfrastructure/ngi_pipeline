#!/bin/env python
"""Piper workflow-specific code."""

import os
import sys

from ngi_pipeline.log import minimal_logger

LOG = minimal_logger(__name__)

def return_cl_for_workflow(workflow_name, qscripts_dir_path, setup_xml_path, global_config_path, output_dir= None):
    """Return an executable-ready Piper command line.

    :param str workflow_name: The name of the Piper workflow to be run.
    :param str qscripts_dir_path: The path to the directory containing the qscripts
    :param str setup_xml_path: The path to the project-level setup XML file
    :param dict global_config_path: The parsed Piper-specific globalConfig file.

    :returns: The Piper command line to be executed.
    :rtype: str
    :raises NotImplementedError: If the workflow requested has not been implemented.
    """
    workflow_fn_name = "workflow_{}".format(workflow_name)
    # Get the local function if it exists
    try:
        workflow_function = getattr(sys.modules[__name__], workflow_fn_name)
    except AttributeError as e:
        error_msg = "Workflow \"{}\" has no associated function.".format(workflow_fn_name)
        LOG.error(error_msg)
        raise NotImplementedError(error_msg)
   ## TODO need tmp, logging directory
    LOG.info("Building command line for workflow {}".format(workflow_name))
    return workflow_function(qscripts_dir_path, setup_xml_path, global_config_path, output_dir)


def workflow_dna_alignonly(*args, **kwargs):
    """Return the command line for basic DNA Alignment.

    :param strs qscripts_dir_path: The path to the Piper qscripts directory.
    :param str setup_xml_path: The path to the setup.xml file.
    :param dict global_config_path: The path to the Piper-specific globalConfig file.

    :returns: The Piper command to be executed.
    :rtype: str
    """
    # Same command line but with one additional option
    return workflow_dna_variantcalling(*args, **kwargs) + " --alignment_and_qc"


def workflow_merge_process_variantCall(*args, **kwargs):
    """Return the command line for best practice analysis: merging, procesing and variant calling.

    :param strs qscripts_dir_path: The path to the Piper qscripts directory.
    :param str setup_xml_path: The path to the setup.xml file.
    :param dict global_config_path: The path to the Piper-specific globalConfig file.

    :returns: The Piper command to be executed.
    :rtype: str
    """
    # Same command line but with one additional option
    return workflow_dna_variantcalling(*args, **kwargs) +  " --merge_alignments --data_processing --variant_calling --analyze_separatly "


def workflow_dna_variantcalling(qscripts_dir_path, setup_xml_path, global_config_path, output_dir=None):
    """Return the command line for DNA Variant Calling.

    :param strs qscripts_dir_path: The path to the Piper qscripts directory.
    :param str setup_xml_path: The path to the setup.xml file.
    :param dict global_config_path: The path to the Piper-specific globalConfig file.

    :returns: The Piper command to be executed.
    :rtype: str
    """
    workflow_qscript_path = os.path.join(qscripts_dir_path, "DNABestPracticeVariantCalling.scala")

    ## Should be able to figure this out dynamically I suppose
    #job_walltime = str(calculate_job_time(workflow, ...))
    job_walltime = "345600"
    # Pull from config file
    num_threads = 16

            #piper -S ${SCRIPTS_DIR}/DNABestPracticeVariantCalling.scala \
            #--xml_input ${PIPELINE_SETUP} \
            #--global_config uppmax_global_config.xml \
            #--number_of_threads 8 \
            #--scatter_Gather 23 \
            #-jobRunner ${JOB_RUNNER} \
            #-jobNative "${JOB_NATIVE_ARGS}" \
            #--job_walltime 345600 \
            #${RUN} ${ONLY_ALIGMENTS} ${DEBUG} 2>&1 | tee -a ${LOGS}/wholeGenome.log
    if output_dir == None:
        return  "piper -S {workflow_qscript_path} " \
            "--xml_input {setup_xml_path} " \
            "--global_config {global_config_path} " \
            "--number_of_threads {num_threads} " \
            "--scatter_gather 23 " \
            "-jobRunner Drmaa " \
            "--job_walltime {job_walltime} " \
            "-run".format(**locals())
    else:
        return  "piper -S {workflow_qscript_path} " \
            "--xml_input {setup_xml_path} " \
            "--global_config {global_config_path} " \
            "--number_of_threads {num_threads} " \
            "--scatter_gather 23 " \
            "-jobRunner Drmaa " \
            "--job_walltime {job_walltime} " \
            "--output_directory {output_dir} " \
            "-run".format(**locals())



