#!/usr/bin/env bash
#SBATCH --job-name=assignment1_trapezoid
#SBATCH --comment='Prog5 assignment1: trapezoid integrate cos(x); sweep n -> results.csv'

#SBATCH --partition=workstations
#SBATCH --output=%x-%j.out
#SBATCH --error=%x-%j.err

#SBATCH --mail-user=z.taheri.hanjani@st.hanze.nl
#SBATCH --mail-type=ALL

#SBATCH --time=00:03:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=512M

A=0                              # lower 
B=1.5707963267948966             # pi/2 (upper)
RESULTS="Results.csv"            # for saving the error after trapezoid Integral calculations

echo "n,error" > "$RESULTS"


for p in {1..12}; do
  n_steps=$((2**p))
  echo "Running n_steps=$n_steps"
  srun -n 1 /usr/bin/python3 -u assignment1.py -a "$A" -b "$B" -n "$n_steps" >> "$RESULTS" 
done


