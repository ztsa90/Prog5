#!/usr/bin/env bash
#SBATCH --job-name=assignment4_refseq
#SBATCH --comment="Prog5 assignment4: parse Archaea GenBank and store in MariaDB"

#SBATCH --partition=workstations
#SBATCH --output=%x-%j.out
#SBATCH --error=%x-%j.err

#SBATCH --mail-user=z.taheri.hanjani@st.hanze.nl
#SBATCH --mail-type=ALL

#SBATCH --time=00:10:00
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G

# --- Variables ---
GBFF_FILE="/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.1.genomic.gbff"

echo "Running assignment4 on file: $GBFF_FILE"
date

# Run the Python script (hardcoded input)
srun -n 1 python3 -u assignment4.py "$GBFF_FILE"

date
echo "Job finished."
