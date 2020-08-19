![NGI Pipeline mascot, Mr. Splash](artwork/gh-splash.png)

[![Build Status](https://travis-ci.org/NationalGenomicsInfrastructure/ngi_pipeline.svg?branch=master)](https://travis-ci.org/NationalGenomicsInfrastructure/ngi_pipeline)
[![Maintainability](https://api.codeclimate.com/v1/badges/6988dea77881c498ad78/maintainability)](https://codeclimate.com/github/NationalGenomicsInfrastructure/ngi_pipeline/maintainability)
[![codecov](https://codecov.io/gh/NationalGenomicsInfrastructure/ngi_pipeline/branch/master/graph/badge.svg)](https://codecov.io/gh/NationalGenomicsInfrastructure/ngi_pipeline)

Software designed to take demultiplexed Illumina flowcells and shove 'em through an analysis pipeline (e.g. Piper/GATK).

Nota bene
=========
This document is to be regareded as a internal instruction for developers at NGI at this stage - the instructions are not guaranteed to be complete nor comprehensive. Some time then things are more stable we'll write up a real README here. You have been warned!

Installation
============

NGI_pipeline is designed to be run on UPPMAX-cluster infrastructure. The main purpose of it is to allow  NGI-Uppsala and NGI-Stockholm to automatically process all the Whole Human Genomes samples that will be generated in the next years. However, ngi_pipeline is not limited to Piper as engine and can be easily expanded with other engines.

In the following we will provide a guide to deploy NGI_pipeline and Piper on UPPMAX-clusters.

NGI_pipeline Deployment on Milou/Nestor
-----------

Project `a2013205` is the designed production project for IGN and future Xten samples. It will be used to process all Human samples produced in the close future and, reasonably, also other projects once we will employ ngi_pipeline as main bioinformatic pipeline.

These are the steps to deploy the pipeline under project `a2014205`:
     
     ssh funk_001@milou-b
     cd /proj/a2014205/software/
     git clone git@github.com:NationalGenomicsInfrastructure/ngi_pipeline.git
     conda create -n NGI pip sqlalchemy # create virtual environment if it does not exists
     source activate NGI
     python setup.py develop

Set up config file for ngi_pipeline (an uppmax predefined is available in the main ngi_pipeline folder):

     mkdir /proj/a2014205/ngi_resources
     cd /proj/a2014205/ngi_resources
     ln -s /proj/a2014205/software/ngi_pipeline/uppmax_ngi_config.yaml .


The `$HOME/.bashrc` should look something like that (to get your API token from Charon on the charon website click users and then click your username) :


    #####CHARON#####
    export NGI_CONFIG=/proj/a2014205/ngi_resources/uppmax_ngi_config.yaml 
    export CHARON_API_TOKEN= YOUR_TOKEN_NOT_FRANCESCO_ONE
    export CHARON_BASE_URL=http://charon.scilifelab.se/
    #### MAVEN piper dependency #####
    export PATH=$PATH:/proj/a2014205/software/apache-maven-3.2.3/bin


To install Piper, make sure that you've installed all dependencies (see https://github.com/NationalGenomicsInfrastructure/piper for more information) then use the following:

    git clone git@github.com:NationalGenomicsInfrastructure/piper.git
    cd piper
    sbt/bin/sbt clean
    ./setup.sh /proj/a2014205/software/piper_bin/

Follow the instructions and add the following to you `.bashrc`

    #### PIPER ####
    module load java/sun_jdk1.7.0_25
    PATH=$PATH:/proj/a2014205/software/piper_bin/bin
    PATH=$PATH:/proj/a2014205/software/piper_bin/workflows
    export LD_LIBRARY_PATH=/sw/apps/build/slurm-drmaa/default/lib/:$LD_LIBRARY_PATH
    export PIPER_GLOB_CONF=/proj/a2014205/software/piper_bin/workflows/globalConfig.sh
    export PIPER_GLOB_CONF_XML=/proj/a2014205/software/piper_bin/workflows/uppmax_global_config.xml

N.B. by default the `uppmax_glocal_config.xml` points to files in project a2009002, execute the following command to change this:

    sed -i ’s/a2009002/a2014205/' /proj/a2014205/software/piper_bin/workflows/uppmax_global_config.xml


Resources
-----------
Project a2014205 contains all resources necessary to execute ngi_pipeline and Piper, in particular:


    |-- ngi_resources
    |-- piper_references
    |-- piper_resources


`ngi_resources` contains the configuration file needed to ngi_pipeline to know where process data and where find other necessary resources. Moreover it contains the local sql-lite database used to keep track of the jobs currently running on the cluster.

`piper_references` contains files used by piper to align and to call variants

`piper_resources` contains tools and other resources used by piper


Running the pipeline
============

Running ngi_pipeline on the Pilot data. 

Pilot project data is stored in INBOX of project a2014205 and it is processed in $WORK_FOLDER=/proj/a2014205/nobackup/NGI/analysis_ready/
$WORK_FOLDER looks like

    |— DATA 
    |— ANALYSIS

`DATA` contains the data stored into `INBOX` but sorted for `project/sample/library_prep/run`. Currently we soft-link the data here both for testing purposes (the pipeline can be tried from beginning to end without the need to copy huge files) and  to alleviate the load on nestor file system. This reshuffling of the data is managed by the ngi_pipeline using the information stored into Charon. For Uppsala project not yet present in the db there is currently a fix that I hope will soon disappear.

`ANALYSIS` contains the analysis for each project. For each project the following folders are present:

    ANALYSIS/
    ├── 01_raw_alignments
    ├── 02_preliminary_alignment_qc
    ├── 03_genotype_concordance
    ├── 04_merged_aligments
    ├── 05_processed_alignments
    ├── 06_final_alignment_qc
    ├── 07_variant_calls
    ├── 08_misc
    └── logs

which contains all the steps of the piper.

In the folder: `/proj/a2014205/software/ngi_pipeline/scripts/` there are a couple of utility scripts to run the pipeline at various stages. This will probably became the scripts called by new pm.

Let us see how to run analysis for M.Kaller_14_06:

Analysis
-----------

In the current workflow we want to start alignments every time data is generated. In this context we want to automatically start the pipeline every time data is produced. 
For now we need to simulate this, and this can be done with the script

    ngi_pipeline_start.py analyze flowcell </path/to/flowcell>

This script starts the alignment of all human WGS project (i.e., projects in charon or produced from uppsala) that are present in the flowcell.

The command first checks the local_db to check what processes are running. This is done in order to avoid starting analysis of flowcells that are already under analysis.

The local_db is checked and charon are updated; once this is done, ngi_pipeline attmepts to launch the analysis for the current flowcell (if any analysis is needed).

In general, analyses are triggered automatically through a REST API. An example of how to launch analyses manually follows:

    #### M.KALLER_14_05 and M.Kaller_14_08
    FCARRAY=()
    FCARRAY+=("/proj/a2014205/INBOX/140815_SN1025_0222_AC4HA6ACXX")
    FCARRAY+=("/proj/a2014205/INBOX/140815_SN1025_0223_BC4HAPACXX")
    FCARRAY+=("/proj/a2014205/INBOX/140919_SN1018_0203_BHA3THADXX")
    #### M.Kaller_14_06
    FCARRAY+=("/proj/a2014205/INBOX/140702_D00415_0052_AC41A2ANXX")
    FCARRAY+=("/proj/a2014205/INBOX/140905_D00415_0057_BC45KVANXX")
    ##### Uppsala projects
    FCARRAY+=("/proj/a2014205/INBOX/140821_D00458_0029_AC45JGANXX")
    FCARRAY+=("/proj/a2014205/INBOX/140917_D00458_0034_AC4FF3ANXX")
    for flowcell in ${FCARRAY[@]}; do
        ngi_pipeline_start.py analyze flowcell $flowcell
    done

    (Very soon this script will take as input mulitple flowcells, but now you know how bash arrays work. You're welcome!)



Rerunning failed jobs
---------------------
In the case a sample failed you can force the re-run:

    ngi_pipeline_start.py analyze project —-sample P1170_105 --restart-failed /proj/a2014205/nobackup/NGI/analysis_ready/DATA/M.Kaller_14_05/


Config file for scripts/gt_concordance.py
-------------------------
By default the script uses default ngi_pipeline config file. However, it requires some additional paramenters. Or, alternatively, can take a separate config file as `--config/-c` argument.

Any of those options require section 'gt_concordance' containing the following parameters:

```
gt_concordance:
    XL_FILES_PATH: /lupus/proj/ngi2016003/genotype_data/incoming
    XL_FILES_ARCHIVED:  /lupus/proj/ngi2016003/genotype_data/archived

    GATK_PATH: /sw/apps/bioinfo/GATK/3.5.0/GenomeAnalysisTK.jar
    GATK_REF_FILE: /sw/data/uppnex/reference/biodata/GATK/ftp.broadinstitute.org/bundle/2.8/b37/human_g1k_v37.fasta
    GATK_VAR_FILE: /sw/data/uppnex/reference/biodata/GATK/ftp.broadinstitute.org/bundle/2.8/b37/dbsnp_138.b37.vcf
    INTERVAL_FILE: /lupus/proj/ngi2016003/genotype_data/static/snps.interval_list
    SNPS_FILE: /lupus/proj/ngi2016003/genotype_data/static/maf_snps.txt

    # this one is only required for a custom config file (not ngi_config)
    ANALYSIS_PATH: /proj/ngi2016003/nobackup/NGI/ANALYSIS
```

Docker/Singularity image
========================
A recipe for building Docker and Singularity containers is provided in the `docker` subdirectory. This image is mainly 
useful for development as it enables testing the code in an isolated environment. 

To build the Docker and Singularity images, run the `docker/build_docker_and_singularity.sh` script.

To invoke the `ngi_pipeline` in a Singularity image on Irma, use (and modify as needed) the 
`docker/singularity_ngi_pipeline.sh` wrapper.

Note that when running in containers, the pipeline will not be able to access e.g. the SLURM scheduler, so you won't be 
able to actually launch analyses that are managed by Slurm.
