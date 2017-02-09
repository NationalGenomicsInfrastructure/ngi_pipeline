#! /bin/bash -l

################################################################################
##
## This is a bash script for applying pre-computed recalibration statistics
## on a BAM file and generate a Base Quality Score Recalibrated (BQSR) BAM file.
## This is done with the PrintReads program of GATK.
##
## To use the script, pass the BAM file as the first argument to the script and
## the recalibration statistics file (output from GATK's BaseRecalibrator) as
## the second argument. The output BAM file name is created by stripping the .bam
## file extension and replacing it with .recal.bam.
##
## Example:
##   bash applyRecalibration.sh /path/to/bamfile.bam /path/to/recalibration.table
##
## To submit the script as a job to a SLURM cluster, just submit with the
## UPPNEX id of your project, e.g.:
##   sbatch -A b2015999 applyRecalibration.sh /path/to/bamfile.bam \
##     /path/to/recalibration.table
##
################################################################################

#SBATCH -p core
#SBATCH -n 2
#SBATCH -N 1
#SBATCH -J applyRecalibration
#SBATCH -t 48:00:00
#SBATCH -o applyRecalibration.%j.out
#SBATCH -e applyRecalibration.%j.err

# exit on error
set -e
set -o pipefail

# load required modules
module load bioinfo-tools
module load GATK/3.3.0

# inputs
BAMIN="${1}"
RECALTABLE="${2}"

# if running on a compute node, use the scratch area for output, otherwise write
# directly to the output directory
if [ -z "$SLURM_JOB_ID" ]
then
  SNIC_TMP="`dirname ${BAMIN}`"
fi

BAMOUT="$SNIC_TMP/`basename ${BAMIN/.bam/.recal.bam}`"
FASTAREF="/sw/data/uppnex/reference/biodata/GATK/ftp.broadinstitute.org/bundle/2.8/b37/human_g1k_v37.fasta"

# apply the recalibration
java -Xmx14G -jar ${GATK_HOME}/GenomeAnalysisTK.jar \
-T PrintReads \
-I ${BAMIN} \
-R ${FASTAREF} \
-baq CALCULATE_AS_NECESSARY \
--disable_indel_quals \
--emit_original_quals \
-BQSR ${RECALTABLE} \
-nct 2 \
-o ${BAMOUT}

# copy results back from compute node
if [ ! -z "$SLURM_JOB_ID" ]
then
  cp "${BAMOUT}" "`dirname ${BAMIN}`"
fi
