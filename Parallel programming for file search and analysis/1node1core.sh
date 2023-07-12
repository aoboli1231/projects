#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --output=11_bigout.txt
#SBATCH --error=11_bigerror.txt
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=12G
#SBATCH --time=01:00:00

# The modules to load:
module load python/3.7.4
module load mpi4py/3.0.2-timed-pingpong
module load foss/2019b
source ~/virtualenv/python3.7.4/bin/activate

python ass1.py bigTwitter.json

my-job-stats -a -n -s
