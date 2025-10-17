#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 6 — Stage 1: Load dbNSFP dataset into a PySpark DataFrame
"""

import sys
# --- add Spark library paths BEFORE importing SparkSession ---
sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")

# --- now import Spark libraries ---
from pyspark.sql import SparkSession


# Add Spark paths (specific to BIN/assemblix environment)
sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")

# ---------------------------------------------------------------------
# 1. Create Spark session
# ---------------------------------------------------------------------
spark = (
    SparkSession.builder.appName("assignment6_Ztaherihanjani")
    .config("spark.ui.enabled", "false")
    .config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .master("local[8]")  # Run locally using 8 cores
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

# ---------------------------------------------------------------------
# 2. Load the dataset (compressed TSV)
# ---------------------------------------------------------------------
DATA_PATH = "/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz"

# # Read as tab-delimited text file with header
# df = spark.read.csv(
#     DATA_PATH,
#     sep="\t",
#     header=True,
#     inferSchema=True
# )



df = spark.read.csv(
    DATA_PATH,
    sep="\t",
    header=True,
    inferSchema=False
).limit(100)


# ---------------------------------------------------------------------
# 3. Explore the dataset
# ---------------------------------------------------------------------
print(f"✅ Rows: {df.count():,}")
print(f"✅ Columns: {len(df.columns)}")
print("✅ Column names:")
print(df.columns[:30])  # show first 30 columns for brevity

print("\n🔹 Sample data:")
df.show(5, truncate=False)

spark.stop()
