#!/usr/bin/env bash

#SBATCH --job-name=assignment2
#SBATCH --output=%x-%j.out
#SBATCH --error=%x-%j.err
#SBATCH --time=00:03:00
#SBATCH --nodes=1
#SBATCH --ntasks=33   # 32 workers + 1 supervisor
#SBATCH --cpus-per-task=1
#SBATCH --mem=512M

# Print the name of the machine (node) where this job runs
hostname

# Print the current date and time when job starts
date

echo "Starting integration jobs..."

A=0.0
B=3.1415926535
N_STEPS=1000000
PY=python3
SCRIPT=assignment2.py
RESULTS=results.csv

# write CSV header
echo "workers,real_seconds,user_cpu_seconds,sys_cpu_seconds" > "$RESULTS"

# loop over workers from 1 to 32
for WORKERS in $(seq 1 32); do
  TASKS=$((WORKERS + 1))   # total ranks = workers + 1 supervisor

  # run program and measure time
  /usr/bin/time -f "${WORKERS},%e,%U,%S" -a -o "$RESULTS" \
    srun -n "$TASKS" $PY $SCRIPT -a $A -b $B -n $N_STEPS

  echo "Completed run with workers=$WORKERS"
done

echo "Trapezoid integration jobs finished successfully."
