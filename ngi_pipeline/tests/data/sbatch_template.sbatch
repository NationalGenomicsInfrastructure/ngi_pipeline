#!/bin/bash -l

#SBATCH -A slurm_proj_id
#SBATCH -p core
#SBATCH -n 16
#SBATCH -t 1-00:00:00
#SBATCH -J qc_P123-P123_1001
#SBATCH -o {}
#SBATCH -e {}

#SBATCH Param Value


echo -ne '\n\nExecuting command lines at '
date
# Run the actual commands
echo
Hello!
echo -ne '\n\nFinished execution at '
date