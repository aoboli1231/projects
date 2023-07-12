#!/bin/bash
#SBATCH --nodes=2
#SBATCH --ntasks-per-node=4
#SBATCH --output=24_bigout.txt
#SBATCH --error=24_bigerror.txt
#SBATCH --cpus-per-task=1
#SBATCH --mem-per-cpu=4G
#SBATCH --time=0:15:00

# The modules to load:
module load python/3.7.4
module load mpi4py/3.0.2-timed-pingpong
module load foss/2019b
source ~/virtualenv/python3.7.4/bin/activate

mpirun -np 8 python ass1.py bigTwitter.json

my-job-stats -a -n -s
