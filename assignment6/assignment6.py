#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 6 — Stage 1
Load the dbNSFP 4.9a dataset into a PySpark DataFrame (sampled),
inspect basic structure, and auto-detect “classifier” columns
by column-name suffix patterns.
"""

import sys
# --- Add Spark libraries before importing SparkSession (BIN/assemblix paths) ---
sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")

from pyspark.sql import SparkSession

# ---------------------------------------------------------------------
# 1) Create a local Spark session (no UI; modest memory fits a sample)
# ---------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("assignment6_stage1_Ztaherihanjani")
    .config("spark.ui.enabled", "false")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .master("local[8]")   # use 8 local cores
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------------------
# 2) Load a manageable sample from the compressed TSV
#    (Spark can read .gz transparently; inferSchema=False keeps all as strings)
# ---------------------------------------------------------------------
DATA_PATH = "/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz"

df = (
    spark.read.csv(
        DATA_PATH,
        sep="\t",
        header=True,
        inferSchema=False
    )
    .limit(2000)  # sample only; avoids scanning 37GB in stage 1
)

# ---------------------------------------------------------------------
# 3) Quick exploration
# ---------------------------------------------------------------------
row_count = df.count()  # action (safe here; limited to 2000)
col_count = len(df.columns)

print(f"✅ Rows: {row_count:,}")
print(f"✅ Columns: {col_count}")
print("✅ First 30 column names:", df.columns[:30])

print("\n🔹 Sample rows:")
df.show(5, truncate=False)

# ---------------------------------------------------------------------
# 4) Identify columns that belong to classifier outputs
#    Rationale: in dbNSFP, prediction tools expose *_score, *_pred, *_rankscore, *_phred
#    (e.g., SIFT_score, Polyphen2_HDIV_pred, REVEL_rankscore, CADD_phred, ...)
# ---------------------------------------------------------------------
from collections import Counter

classifier_cols = [
    c for c in df.columns
    if any(suffix in c for suffix in ("_score", "_pred", "_rankscore", "_phred"))
]

print("\n🧩 Number of classifier-related columns:", len(classifier_cols))
print("🧩 Example classifier columns:", classifier_cols[:20])

# Exact base (before first underscore)
bases = [c.split("_", 1)[0] for c in classifier_cols]
classifiers = sorted(set(bases))
print(f"\n🧪 Distinct classifiers detected: {len(classifiers)}")
print("🧪 First 30 classifier bases:", classifiers[:30])

# Count columns per classifier (exact-base match)
clf_counts = Counter(bases)
print("\n🔹 Columns per classifier (top 20):")
for clf, n in sorted(clf_counts.items(), key=lambda x: -x[1])[:20]:
    print(f"{clf:<20} {n} columns")
print(f"\n✅ Total unique classifiers: {len(clf_counts)}")

# or 
# WHITELIST_CLASSIFIERS = {
#     "SIFT", "SIFT4G", "Polyphen2", "LRT", "MutationTaster",
#     "MutationAssessor", "FATHMM", "PROVEAN", "VEST4", "MetaSVM",
#     "MetaLR", "MetaRNN", "M-CAP", "REVEL", "MutPred", "MVP", "gMVP",
#     "MPC", "PrimateAI", "DEOGEN2", "BayesDel", "ClinPred", "LIST-S2",
#     "VARITY_R", "VARITY_ER", "ESM1b", "EVE", "AlphaMissense",
#     "PHACTboost", "MutFormer", "MutScore", "Aloft", "CADD", "DANN",
#     "fathmm-MKL", "fathmm-XF", "Eigen", "GenoCanyon", "LINSIGHT"
# }

# classifiers = [c for c in classifiers if c in WHITELIST_CLASSIFIERS]


