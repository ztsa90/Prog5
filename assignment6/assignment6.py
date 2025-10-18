#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 6 — Stage 1
Author: Z. Taherihanjani

Goal of this stage
------------------
1️⃣ Load a manageable sample of the dbNSFP 4.9a dataset (gzipped TSV) into PySpark.
2️⃣ Explore structure (row/column count, a few rows).
3️⃣ Automatically discover classifier columns by suffix patterns:
      *_score, *_pred, *_rankscore, *_phred
4️⃣ Count how many non-missing predictions each classifier contributes,
    rank them by total predictions, and keep only the top-5 (plus ID columns).
5️⃣ Merge chr and pos into a unique genomic identifier (chr_pos).
"""

# =============================================================================
# 0) Setup and imports
# =============================================================================
import sys
from collections import defaultdict
from pyspark.sql import SparkSession, functions as F

# Ensure Spark libraries are available (BIN environment)
sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")

# =============================================================================
# 1) Spark Session setup
# =============================================================================
spark = (
    SparkSession.builder.appName("assignment6_stage1_Ztaherihanjani")
    .config("spark.ui.enabled", "false")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .master("local[8]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# =============================================================================
# 2) Load a small sample from the compressed TSV
# =============================================================================
DATA_PATH = "/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz"

df = (
    spark.read.csv(
        DATA_PATH,
        sep="\t",
        header=True,       # Header row contains all column names (~458)
        inferSchema=False  # Read all as string (faster & safer)
    )
    .limit(2000)          # Sample 2,000 rows for quick iteration
)

# =============================================================================
# 3) Discover classifier columns
# =============================================================================
PRED_SUFFIXES = ("_score", "_pred", "_rankscore", "_phred")

# Collect all classifier-related columns
classifier_cols = [c for c in df.columns if any(suffix in c for suffix in PRED_SUFFIXES)]
print(f"\n🧩 Classifier-related columns: {len(classifier_cols)}")

# Extract base tool names (before first underscore)
bases = [c.split("_", 1)[0] for c in classifier_cols]
classifiers = sorted(set(bases))
print(f"🧪 Distinct classifiers detected: {len(classifiers)}")

# Map: classifier → list of its columns
clf_to_cols = defaultdict(list)
for c in classifier_cols:
    base = c.split("_", 1)[0]
    clf_to_cols[base].append(c)

# Simple preview
print(f"Found {len(clf_to_cols)} classifier groups.")
for i, (k, v) in enumerate(clf_to_cols.items()):
    if i == 5:
        break
    print(f"  {k}: {v}")

# =============================================================================
# 4) Count non-missing predictions and keep Top-5 classifiers
# =============================================================================
MISSING = {".", "", "NA", "nan", "NaN", "null", "NULL"}

# Count non-missing values per column (one pass)
per_col_counts_row = df.agg(
    *[F.sum(F.when(~F.col(c).isin(MISSING), 1).otherwise(0)).alias(c)
      for c in classifier_cols]
).collect()[0]
per_col_counts = per_col_counts_row.asDict()

# Sum per classifier (total predictions per tool)
clf_counts = {
    clf: int(sum(per_col_counts.get(c, 0) for c in cols))
    for clf, cols in clf_to_cols.items()
}

# Build DataFrame for sorting and display
clf_count_df = (
    spark.createDataFrame(
        [(k, v) for k, v in clf_counts.items()],
        ["classifier", "non_missing_count"]
    )
    .orderBy(F.desc("non_missing_count"))
)

print("\n🔹 Top classifiers by total predictions:")
clf_count_df.show(10, truncate=False)

# Extract Top-5 tool names
top5 = [r["classifier"] for r in clf_count_df.limit(5).collect()]
print("✅ Top-5 classifiers:", top5)

# =============================================================================
# 5) Keep only Top-5 columns + ID columns
# =============================================================================
ID_COLS = ["chr", "pos", "Ensembl_proteinid"]  # Always keep ID columns

keep_cols = list(ID_COLS)
for clf in top5:
    for c in clf_to_cols[clf]:
        if c in df.columns:  # safety check
            keep_cols.append(c)

# Select only those columns
df_top5 = df.select(*keep_cols)
print(f"✅ Kept {len(df_top5.columns)} columns (Top-5 + IDs)")
df_top5.show(5, truncate=False)

# =============================================================================
# 6) Merge chr and pos into a unique identifier
# =============================================================================
df_top5 = df_top5.withColumn("chr_pos", F.concat_ws(":", F.col("chr"), F.col("pos")))

print("\n✅ Added 'chr_pos' as a unique genomic identifier:")
df_top5.select("chr", "pos", "chr_pos").show(5, truncate=False)

# =============================================================================
# 
# =============================================================================
spark.stop()
print("\n🎯 Stage 1 completed successfully.")
