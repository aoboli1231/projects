#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=8
#SBATCH --output=18out.txt
#SBATCH --error=18error.txt
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-core=4G
#SBATCH --time=0:15:00

# The modules to load:
module load python/3.7.4
module load mpi4py/3.0.2-timed-pingpong
module load foss/2019b
source ~/virtualenv/python3.7.4/bin/activate

mpirun -np 8 python ass1.py bigTwitter.json

my-job-stats -a -n -s
