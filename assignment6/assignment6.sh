#!/bin/bash
#SBATCH --job-name=assignment6_ztaherihanjani
#SBATCH --partition=assemblix
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --time=02:00:00
#SBATCH --output=assignment6_%j.out
#SBATCH --error=assignment6_%j.err

# ------- Job info -------
echo "=========================================="
echo "Job started at: $(date)"
echo "Job ID: $SLURM_JOB_ID"
echo "Node: $(hostname)"
echo "CPUs: ${SLURM_CPUS_PER_TASK:-1}"
echo "Mem:  ${SLURM_MEM_PER_NODE:-128G}"
echo "=========================================="
echo

# ------- (Optional) local scratch for Spark -------
export SPARK_LOCAL_DIRS="${TMPDIR:-/tmp}"

# ------- Python/Spark env (adjust if needed) -------
export PYSPARK_PYTHON=python3

# ------- Paths & knobs you may tweak quickly -------
DATA="/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz"
JDBC_JAR="/homes/ztaherihanjani/jars/mariadb-java-client-3.5.0.jar"
MYCNF="/homes/ztaherihanjani/.my.cnf"

# Run mode toggles:
TEST_ROWS=2000              # set -1 for full dataset
LOCAL_CORES="${SLURM_CPUS_PER_TASK:-8}"

# Choose ONE of these blocks:

# --- A) DRY RUN (no DB write) ---
python3 assignment6.py \
  --data "$DATA" \
  --jdbc-jar "$JDBC_JAR" \
  --mycnf "$MYCNF" \
  --test-rows "$TEST_ROWS" \
  --local-cores "$LOCAL_CORES" \
  --dry-run

# # --- B) FULL WRITE to MariaDB (uncomment to use) ---
# DB_NAME="Ztaherihanjani"
# python3 assignment6.py \
#   --data "$DATA" \
#   --jdbc-jar "$JDBC_JAR" \
#   --mycnf "$MYCNF" \
#   --no-dry-run \
#   --db-name "$DB_NAME" \
#   --local-cores "$LOCAL_CORES" \
#   --test-rows -1 \
#   --overwrite

echo
echo "=========================================="
echo "Job finished at: $(date)"
echo "=========================================="
