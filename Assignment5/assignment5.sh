#!/bin/bash
#SBATCH --job-name=assignment5_yourname
#SBATCH --ntasks=16
#SBATCH --mem=128G
#SBATCH --time=01:00:00
#SBATCH --output=assignment5_%j.out
#SBATCH --error=assignment5_%j.err

# Print job information
echo "=========================================="
echo "Job started at: $(date)"
echo "Job ID: $SLURM_JOB_ID"
echo "Running on node: $(hostname)"
echo "Number of tasks: $SLURM_NTASKS"
echo "Memory allocated: 128G"
echo "=========================================="
echo

# Run the PySpark analysis script on archaea.2.genomic.gbff
python3 assignment5.py /data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.2.genomic.gbff

# Print completion information
echo
echo "=========================================="
echo "Job finished at: $(date)"
echo "=========================================="
