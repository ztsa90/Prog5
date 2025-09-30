#!/bin/bash
#SBATCH --job-name=assignment3
#SBATCH --partition=workstations
#SBATCH --time=00:30:00
#SBATCH --mem=1G
#SBATCH --cpus-per-task=1
#SBATCH --ntasks-per-node=1
#SBATCH --output=%x_%j.out
#SBATCH --error=%x_%j.err

STEPS=10000000
PY_SCRIPT=./assignment3.py
RESULTS=results_nodes.csv

if [[ ! -f "$RESULTS" ]]; then
  echo "nodes,ntasks,steps,elapsed_s,user_s,sys_s,max_rss_kb,jobid,nodelist" > "$RESULTS"
fi

NTASKS=$SLURM_JOB_NUM_NODES

/usr/bin/time -f "${SLURM_JOB_NUM_NODES},${NTASKS},${STEPS},%e,%U,%S,%M,${SLURM_JOB_ID},${SLURM_JOB_NODELIST}" \
  -a -o "$RESULTS" \
  srun -n $NTASKS /usr/bin/python3 "$PY_SCRIPT" -n "$STEPS"