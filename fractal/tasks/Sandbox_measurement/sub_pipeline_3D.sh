#!/bin/bash
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=16
#SBATCH --partition=main
#SBATCH --time=10:00:00

date
echo

poetry run python pipeline_3D.py

echo
date
