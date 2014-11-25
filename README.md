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


Pre-processing data
============
Before any analysis, data needs to be pre-processed. This process consists on the
conversion from BCL files to FASTQ files, as well as demultiplexing of the data.

The script `run_tracker.py` serves this purpose. The script will track finished runs
and start BCL conversion and demultiplexing on them. Once the process is dome, data
will be transfered to the analysis server/HPC. 

The script requires these options in the configuration file:

```yaml
preprocessing:
    hiseq_data_dir: /path/to/hiseq/data
    miseq_dat_dir: /path/to/miseq/data
    # MFS server to put metadata in
    mfs: /path/to/mfs/partition
    # File with info about already transfered runs
    transfer_file: /path/to/transfer/file
    bcl2fastq: 
        path: /path/to/bcl2fastq
        - all command line options of bcl2fastq , i.e runfolder, input-dir, etc.
    remote:
        user: remote_user_analysis_server
        host: analysis_server
        data_archive: /path/where/to/transfer/data
```

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

Run level analysis
-----------

In the current workflow we want to start alignments every time data is generated. In this context we want to automatically start the pipeline every time data is produced. 
For now we need to simulate this, and this can be done with the script

    start_flowcell_analysis.py

This script starts the alignment of all IGN project (i.e., projects in charon or produced from uppsala) that are present in the flowcell.

The command first checks for the local_db to check what processes are running. This is done in order to avoid to start analysis on flowcells that are already under analysis.

The local_db is checked and charon is updated, once this is done ngi_pipeline tries to start the analysis for the current flowcell (if any analysis is needed). 

N.B. after the analysis are started the local_db is updated but not charon. Therefore, if we run the script on a flowcelll it will happen that charon status will not change. By rerunning the same command you will see that the pipeline will refuse to start analysis as they are already running. When running the command for the second time ngi_pipeline will first (like before) check the current status of local_db and update charon. So only at this point it will be possible to see on charon that the status of the flowcell is RUNNING.

Once the pipeline will be totally in production this will no be longer a problem as there will be a cronjob monitoring the local db and updating charon status (or something similar to this).

    #### M.KALLER_14_05 and M.Kaller_14_08
    python /proj/a2014205/software/ngi_pipeline/scripts/start_flowcell_analysis.py /proj/a2014205/INBOX/140815_SN1025_0222_AC4HA6ACXX
    python  /proj/a2014205/software/ngi_pipeline/scripts/start_flowcell_analysis.py /proj/a2014205/INBOX/140815_SN1025_0223_BC4HAPACXX
    python  /proj/a2014205/software/ngi_pipeline/scripts/start_flowcell_analysis.py  /proj/a2014205/INBOX/140919_SN1018_0203_BHA3THADXX
    #### M.Kaller_14_06
    python  /proj/a2014205/software/ngi_pipeline/scripts/start_flowcell_analysis.py /proj/a2014205/INBOX/140702_D00415_0052_AC41A2ANXX
    python  /proj/a2014205/software/ngi_pipeline/scripts/start_flowcell_analysis.py  /proj/a2014205/INBOX/140905_D00415_0057_BC45KVANXX
    ##### Uppsala
    python  /proj/a2014205/software/ngi_pipeline/scripts/start_flowcell_analysis.py /proj/a2014205/INBOX/140821_D00458_0029_AC45JGANXX
    python  /proj/a2014205/software/ngi_pipeline/scripts/start_flowcell_analysis.py  /proj/a2014205/INBOX/140917_D00458_0034_AC4FF3ANXX


Sample level analysis
----------------------
Again, this is likely to change but this commands or similar will be run in order to force re-runs or to process problematic samples: the idea is to start here sample level analysis 
or the so called variant calling step. 

    python /proj/a2014205/software/ngi_pipeline/scripts/start_pipeline_from_project.py --sample_only /proj/a2014205/nobackup/NGI/analysis_ready/DATA/M.Kaller_14_05/
    python /proj/a2014205/software/ngi_pipeline/scripts/start_pipeline_from_project.py --sample_only /proj/a2014205/nobackup/NGI/analysis_ready/DATA/M.Kaller_14_06/
    python /proj/a2014205/software/ngi_pipeline/scripts/start_pipeline_from_project.py --sample_only /proj/a2014205/nobackup/NGI/analysis_ready/DATA/M.Kaller_14_08/
    python /proj/a2014205/software/ngi_pipeline/scripts/start_pipeline_from_project.py --sample_only /proj/a2014205/nobackup/NGI/analysis_ready/DATA/ND-0522/

Rerunning failed jobs
---------------------
In the case a sample failed you can force the re-run:

    python /proj/a2014205/software/ngi_pipeline/scripts/start_pipeline_from_project.py —sample P1170_105 --restart-failed  --sample_only /proj/a2014205/nobackup/NGI/analysis_ready/DATA/M.Kaller_14_05/
    

