#! /bin/bash
#SBATCH --job-name=assignment1_trapizoid
#SBATCH --comment='Prog5 assignment1: trapezoid integrate cos(x); sweep n -> results.csv'

#SBATCH --account=ZahraTaheri
#SBATCH --partition=workstations

#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

#SBATCH --mail-user=z.taheri.hanjani@st.hanze.nl
#SBATCH --mail-type=ALL

#SBATCH --time=00:3:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=512M
