#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 6 — Stage 1
Author: Z. Taherihanjani

Goal of this stage
------------------
1) Load a manageable sample of the dbNSFP 4.9a dataset (gzipped TSV) into PySpark.
2) Explore structure (row/column count, a few rows).
3) Automatically discover classifier columns by suffix patterns:
      *_score, *_pred, *_rankscore, *_phred
4) Count how many non-missing predictions each classifier contributes,
   rank them by total predictions, and keep only the top-5 (plus ID columns).
5) Merge chr and pos into a unique genomic identifier (chr_pos).
6) Save a brief README-style markdown summary of results for git.
"""

# =============================================================================
# 0) Setup and imports
# =============================================================================
import sys                             # For adjusting sys.path so Spark python libs are resolvable
import time                            # Simple timing for visibility
from collections import defaultdict    # For mapping classifier -> its columns
from functools import reduce           # Safe reduction over Spark Column expressions
from operator import add               # Used with reduce(add, ...)

# Ensure Spark libraries are available (BIN environment)
sys.path.append("/opt/spark/python")                                   # Spark Python path (environment-specific)
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")         # Add Py4J zip
from pyspark.sql import SparkSession, functions as F                   # Core Spark SQL APIs

# =============================================================================
# 1) Spark Session setup
# =============================================================================
spark = (
    SparkSession.builder.appName("assignment6_stage1_Ztaherihanjani")  # App name for UI/logs
    .config("spark.ui.enabled", "false")                                # Disable Spark UI (optional on clusters)
    .config("spark.executor.memory", "8g")                              # Memory per executor (tune as needed)
    .config("spark.driver.memory", "8g")                                # Memory for the driver process
    .master("local[8]")                                                 # Run locally with 8 cores; switch to cluster for SLURM
    .getOrCreate()                                                      # Create or reuse the SparkSession
)
spark.sparkContext.setLogLevel("ERROR")  # Quieter logs; only errors shown

# =============================================================================
# 2) Load a small sample from the compressed TSV
# =============================================================================
DATA_PATH = "/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz"  # Path to gzipped dbNSFP

t0 = time.time()
df = (
    spark.read.csv(
        DATA_PATH,
        sep="\t",          # dbNSFP is tab-delimited
        header=True,       # First row is the header with ~hundreds of columns
        inferSchema=False  # Read all as string for speed and safety
    )
    .limit(2000)          # Take a 2k-row sample for fast iteration during development
)
total_rows = df.count()
print("\n" + "=" * 80)
print(f"📂 Loaded sample: {total_rows:,} rows, {len(df.columns)} columns in {time.time() - t0:.2f}s")
print("=" * 80)
df.show(3, truncate=False)

# =============================================================================
# 3) Discover classifier columns
# =============================================================================
PRED_SUFFIXES = ("_score", "_pred", "_rankscore", "_phred")  # Common suffixes for prediction fields

# Collect all classifier-related columns by suffix presence
classifier_cols = [c for c in df.columns if any(suffix in c for suffix in PRED_SUFFIXES)]
print(f"\n🧩 Classifier-related columns: {len(classifier_cols)}")  # Quick sanity count

# Extract base tool names (portion before first underscore), e.g., "CADD" from "CADD_raw"
clf_to_cols = defaultdict(list)
for col in classifier_cols:
    base = col.split("_", 1)[0]  # Tool name is the prefix before first underscore
    clf_to_cols[base].append(col)

classifiers = sorted(clf_to_cols.keys())
print(f"🧪 Distinct classifiers detected: {len(classifiers)}")
print("🧪 Example groups (tool -> first few columns):")
for i, (tool, cols) in enumerate(clf_to_cols.items()):
    if i == 5:
        break
    print(f"  - {tool}: {cols[:5]}{' ...' if len(cols) > 5 else ''}")

# =============================================================================
# 4) Count non-missing predictions and keep Top-5 classifiers
# =============================================================================
MISSING = {".", "", "NA", "nan", "NaN", "null", "NULL"}  # Canonical missing tokens in dbNSFP

t1 = time.time()
# Count non-missing values per column in one pass (vectorized aggregation)
per_col_counts_row = df.agg(
    *[F.sum(F.when(~F.col(c).isin(MISSING), 1).otherwise(0)).alias(c)
      for c in classifier_cols]
).collect()[0]  # Single Row result containing counts per column
per_col_counts = per_col_counts_row.asDict()  # Convert Row to a dict: column -> count

# Sum counts per classifier (tool) across its columns
clf_counts = {
    clf: int(sum(per_col_counts.get(c, 0) for c in cols))
    for clf, cols in clf_to_cols.items()
}

# Build a small DataFrame for sorting and display of classifier totals
clf_count_df = (
    spark.createDataFrame(
        [(k, v) for k, v in clf_counts.items()],
        ["classifier", "non_missing_count"]
    )
    .orderBy(F.desc("non_missing_count"))  # Highest coverage tools first
)

print(f"\n⏱️ Counted non-missing predictions in {time.time() - t1:.2f}s")
print("🔹 Top classifiers by total predictions:")
clf_count_df.show(10, truncate=False)  # Show top 10 tools for visibility

# Extract Top-5 tool names for downstream column filtering
top5 = [r["classifier"] for r in clf_count_df.limit(5).collect()]
print("✅ Top-5 classifiers:", top5)

# =============================================================================
# 5) Keep only Top-5 columns + ID columns
# =============================================================================
# IMPORTANT: dbNSFP uses '#chr' and 'pos(1-based)' as coordinate columns (not 'chr'/'pos')
ID_COLS = ["#chr", "pos(1-based)", "Ensembl_proteinid"]  # Always retain core identifiers

# Build keep list: IDs + all columns for the Top-5 tools (filter only existing columns)
keep_cols = [c for c in ID_COLS if c in df.columns]
for clf in top5:
    for col in clf_to_cols[clf]:
        if col in df.columns:
            keep_cols.append(col)

# Select only ID + Top-5 classifier columns (narrow the table)
df_top5 = df.select(*keep_cols)
print(f"✅ Kept {len(df_top5.columns)} columns (Top-5 + IDs)")
df_top5.show(5, truncate=False)  # Peek at the narrowed dataset

# =============================================================================
# 6) Merge chr and pos into a unique identifier
# =============================================================================
# Create "chr:pos" key using dbNSFP headers '#chr' and 'pos(1-based)'
df_top5 = df_top5.withColumn("chr_pos", F.concat_ws(":", F.col("#chr"), F.col("pos(1-based)")))
print("\n✅ Added 'chr_pos' as a unique genomic identifier:")
df_top5.select("#chr", "pos(1-based)", "chr_pos").show(5, truncate=False)

# =============================================================================
# 7) Position with the most predictions (group by chr_pos)
# =============================================================================
# Prediction columns only (exclude ID and derived key columns)
predict_cols = [c for c in df_top5.columns if c not in ["#chr", "pos(1-based)", "Ensembl_proteinid", "chr_pos"]]

# Build per-row 0/1 flags: 1 if prediction present (not in MISSING), else 0
present_exprs = [F.when(~F.col(c).isin(MISSING), F.lit(1)).otherwise(F.lit(0)) for c in predict_cols]

# Row-wise sum across all 0/1 flags → total non-missing predictions per row
# IMPORTANT: use reduce(add, ...) — Python sum() on Column objects is unsafe
non_missing_expr = reduce(add, present_exprs) if present_exprs else F.lit(0)

# Attach the per-row count as a new column (do not mutate original if you prefer)
df_with_counts = df_top5.withColumn("non_missing_count", non_missing_expr)

# Aggregate by genomic position: sum per position across all rows for that chr_pos
pos_summary = (
    df_with_counts.groupBy("chr_pos")
    .agg(F.sum("non_missing_count").alias("total_predictions"))
    .orderBy(F.desc("total_predictions"))  # Most predicted positions first
)
print("\n🔹 Top genomic positions by total predictions:")
pos_summary.show(5, truncate=False)  # Show the top positions

# Fetch the single best (max) position and print a friendly message
best_pos = pos_summary.limit(1).collect()[0]
print(f"🏆 Most predicted position: {best_pos['chr_pos']} ({best_pos['total_predictions']} predictions)")

# =============================================================================
# 8) Protein with the most predictions (group by Ensembl_proteinid)
# =============================================================================
# Aggregate by protein ID: total predictions per Ensembl protein across rows
prot_summary = (
    df_with_counts.groupBy("Ensembl_proteinid")
    .agg(F.sum("non_missing_count").alias("total_predictions"))
    .orderBy(F.desc("total_predictions"))
)
print("\n🔹 Top proteins by total predictions:")
prot_summary.show(5, truncate=False)

# Fetch the single best (max) protein and print a friendly message
best_prot = prot_summary.limit(1).collect()[0]
print(f"🏆 Most predicted protein: {best_prot['Ensembl_proteinid']} ({best_prot['total_predictions']} predictions)")

# =============================================================================
# 9) Save a short README-style markdown (for git)
# =============================================================================
report = f"""# Assignment 6 — Stage 1 (Sample Run)

**Author:** Z. Taherihanjani  
**Mode:** STAGE 1 (2,000 rows sample)

- Total rows: {total_rows:,}
- Total columns: {len(df.columns)}
- Classifiers detected: {len(classifiers)}
- Top-5: {', '.join(top5)}
- Most predicted position: {best_pos['chr_pos']} ({best_pos['total_predictions']})
- Most predicted protein: {best_prot['Ensembl_proteinid']} ({best_prot['total_predictions']})

*Stage 1 stores no data in SQL; JDBC/3NF will be added in Stage 2.*
"""
with open("assignment6_stage1.md", "w", encoding="utf-8") as fh:
    fh.write(report)
print("📝 Saved summary to assignment6_stage1.md")

# =============================================================================
# 10) Clean shutdown of the Spark session
# =============================================================================
spark.stop()
print("\n🎯 Stage 1 completed successfully.\n")
