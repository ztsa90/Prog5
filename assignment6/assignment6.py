#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 6 — Stage 1

Goal of this stage
------------------
1) Load a manageable sample of the **dbNSFP 4.9a** dataset (a huge, gzipped TSV) into a PySpark DataFrame.
2) Inspect basic structure (row/column counts, a few rows).
3) Automatically discover columns that belong to **prediction tools (“classifiers”)** by name patterns:
     *_score, *_pred, *_rankscore, *_phred
   These suffixes are the standard way dbNSFP exposes prediction outputs for each tool.
4) Count how many **present (non-missing)** predictions each tool contributes (on the sample),
   rank tools by total present predictions, and **keep only the top-5 tools** (plus a few core ID columns).
5) Print out what we kept so we can verify before moving on to later stages.

Notes
-----
- We deliberately read only a **sample** (`limit(2000)`) to be fast and interactive. For final results,
  remove the `.limit(2000)` and run with SLURM.
- We do **not** infer schema here; everything is read as strings. That’s safer for the very wide table,
  and is enough for presence/absence counting.
- dbNSFP uses special sentinels for missing values (e.g., ".", "", "NA"). We treat those as “not present”.
"""

import sys

# -- Make sure Spark libraries are on sys.path for the BIN/assemblix environment
sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from collections import Counter, defaultdict

# -----------------------------------------------------------------------------
# 1) Create a local Spark session
#    - Disable Spark UI (saves a port and noise on shared machines)
#    - Give modest memory (enough for a 2k-row sample with 400+ columns)
#    - Use 8 local cores to parse quickly
# -----------------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("assignment6_stage1_Ztaherihanjani")
    .config("spark.ui.enabled", "false")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .master("local[8]")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# -----------------------------------------------------------------------------
# 2) Load a small sample from the compressed TSV
#    Spark can read .gz transparently. We keep everything as string (inferSchema=False).
# -----------------------------------------------------------------------------
DATA_PATH = "/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz"

df = (
    spark.read.csv(
        DATA_PATH,
        sep="\t",
        header=True,       # first line is the header row with 458 column names
        inferSchema=False  # keep strings; avoids expensive inference across 37GB
    )
    .limit(2000)          # SAMPLE ONLY for fast iteration; remove for full run
)

# -----------------------------------------------------------------------------
# 3) Quick exploration so we know the shape and content look reasonable
# -----------------------------------------------------------------------------
row_count = df.count()            # Action (OK on 2k-row sample)
col_count = len(df.columns)

print(f"✅ Rows: {row_count:,}")
print(f"✅ Columns: {col_count}")
print("✅ First 30 column names:", df.columns[:30])

print("\n🔹 Sample rows (first 5):")
df.show(5, truncate=False)

# -----------------------------------------------------------------------------
# 4) Discover "classifier" columns by suffix pattern
#
# Rationale: dbNSFP exposes per-tool outputs through consistently named columns:
#   *_score, *_pred, *_rankscore, *_phred
# Example:
#   SIFT_score, SIFT4G_score, Polyphen2_HDIV_pred, REVEL_rankscore, CADD_phred, ...
#
# We first collect ALL columns whose names contain any of these suffixes, then
# derive the "base" tool name as everything before the first underscore.
#   "Polyphen2_HDIV_score" -> base "Polyphen2"
#   "fathmm-MKL_coding_score" -> base "fathmm-MKL"
#
# This makes sub-flavors (e.g., Polyphen2_HDIV vs Polyphen2_HVAR) map back to one tool.
# -----------------------------------------------------------------------------
PRED_SUFFIXES = ("_score", "_pred", "_rankscore", "_phred")

classifier_cols = [
    c for c in df.columns
    if any(suffix in c for suffix in PRED_SUFFIXES)
]

print("\n🧩 Number of classifier-related columns:", len(classifier_cols))
print("🧩 Example classifier columns:", classifier_cols[:20])

# Base/tool name = substring before the first underscore
bases = [c.split("_", 1)[0] for c in classifier_cols]
classifiers = sorted(set(bases))

print(f"\n🧪 Distinct classifiers detected (by base name): {len(classifiers)}")
print("🧪 First 30 classifier bases:", classifiers[:30])

# Also helpful: how many columns per tool (just an overview of schema width per tool)
clf_col_counts = Counter(bases)
print("\n🔹 Columns per classifier (top 20 by column count):")
for clf, n in sorted(clf_col_counts.items(), key=lambda x: -x[1])[:20]:
    print(f"{clf:<20} {n} columns")
print(f"\n✅ Total unique classifiers: {len(clf_col_counts)}")

# -----------------------------------------------------------------------------
# 5) Count "present" predictions per tool on the SAMPLE
#
# We define "present" as: value is not NULL and not a dbNSFP missing sentinel (".", "", "NA").
# We count per COLUMN first, then sum counts per TOOL (base name).
#
# Why sum across columns? Each tool typically publishes multiple outputs (score/pred/rankscore/phred).
# Summing gives a quick sense of how complete/usable the tool’s outputs are in practice on our sample.
# -----------------------------------------------------------------------------

def present(colname):
    """Return a boolean expression that is True when the cell contains a usable value."""
    return (F.col(colname).isNotNull()) & (~F.col(colname).isin(".", "", "NA"))

# Map tool -> list of its columns
clf_to_cols = defaultdict(list)
for c in df.columns:
    if any(suf in c for suf in PRED_SUFFIXES):
        base = c.split("_", 1)[0]
        clf_to_cols[base].append(c)

# Compute one big projection with the count of "present" values per column (in one pass)
col_count_row = df.select(
    *[F.sum(present(c).cast("int")).alias(c)
      for cols in clf_to_cols.values() for c in cols]
).collect()[0]

# Convert to dict: column -> count_of_present_values (on the sample)
col_present_counts = {c: col_count_row[c] for cols in clf_to_cols.values() for c in cols}

# Aggregate to tool level: tool -> sum of present counts over its columns
clf_present_counts = {
    clf: sum(col_present_counts[c] for c in cols)
    for clf, cols in clf_to_cols.items()
}

print("\n📊 Present-value counts per classifier (SAMPLE, not full file):")
for clf, total in sorted(clf_present_counts.items(), key=lambda x: -x[1]):
    print(f"{clf:<18} {total:>10,}  (cols={len(clf_to_cols[clf])})")

# -----------------------------------------------------------------------------
# 6) Keep only the TOP-5 tools (by total present predictions) + a few core identifiers
#
# This implements:
#   “Make a top five of classifiers based on the total prediction it makes,
#    and drop all the columns associated with the others.”
#
# Core columns kept so later stages can:
#   - build a unique genomic position ID,
#   - find the position with most predictions,
#   - find the protein with most predictions.
# -----------------------------------------------------------------------------
top5 = [clf for clf, _ in sorted(clf_present_counts.items(), key=lambda x: -x[1])[:5]]
print(f"\n🏆 Top-5 classifiers by total present predictions (SAMPLE): {top5}")

CORE_COLS = [
    "#chr", "pos(1-based)", "ref", "alt",
    "Ensembl_proteinid", "genename", "HGVSp_VEP", "HGVSc_VEP"
]

# Build the keep-list: core identifiers + all columns for the top-5 tools
keep_cols = []
for clf in top5:
    keep_cols.extend(clf_to_cols[clf])

# De-duplicate and filter to columns that actually exist in df (defensive)
keep_cols = CORE_COLS + [c for c in keep_cols if c in df.columns]
keep_cols = [c for c in keep_cols if c in df.columns]

df_top5 = df.select(*keep_cols)

print(f"\n✅ Kept {len(keep_cols)} columns total: "
      f"{len([c for c in CORE_COLS if c in df.columns])} core + classifier columns for top-5")
print("✅ Resulting schema:")
df_top5.printSchema()

print("\n🔹 Preview after dropping others (top-5 kept):")
df_top5.show(5, truncate=False)


spark.stop()
