#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 6 — Full Script (Stage 1 + Stage 2)
Author: Z. Taherihanjani

High-level flow
---------------
1. Load dbNSFP (gzipped TSV) with Spark.
2. Automatically discover classifier columns by suffix.
3. Count per-classifier non-missing predictions, keep Top-5.
4. Merge chr+pos → unique ID (chr_pos), compute per-row prediction counts.
5. Find the most-predicted position and protein.
6. Normalize to 3NF tables (positions / proteins / predictions).
7. Optionally write to MariaDB (via JDBC).
8. Always save a Markdown report (assignment6.md) answering all questions.
"""

from __future__ import annotations

import sys
import time
import argparse
import configparser
from collections import defaultdict
from functools import reduce
from operator import add
from typing import Dict, List

# =============================================================================
# 0) Spark imports and environment setup
# =============================================================================
sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")
from pyspark.sql import SparkSession, DataFrame, functions as F

# =============================================================================
# 1) Global constants
# =============================================================================
PRED_SUFFIXES = ("_score", "_pred", "_rankscore", "_phred")  # Patterns identifying classifiers
MISSING = {".", "", "NA", "nan", "NaN", "null", "NULL"}      # Canonical missing tokens


# =============================================================================
# 2) Utility functions
# =============================================================================
def safe_sum(columns: List) -> F.Column:
    """Safely sum a list of Spark Columns, return 0 if list empty."""
    return reduce(add, columns) if columns else F.lit(0)


def read_mycnf(path: str) -> Dict[str, str]:
    """Parse ~/.my.cnf to extract user/password credentials."""
    cfg = configparser.ConfigParser()
    read = cfg.read(path)
    if not read or "client" not in cfg:
        return {}
    creds = {}
    if cfg.has_option("client", "user"):
        creds["user"] = cfg.get("client", "user")
    if cfg.has_option("client", "password"):
        creds["password"] = cfg.get("client", "password")
    return creds


def discover_classifiers(df: DataFrame) -> Dict[str, List[str]]:
    """Return mapping of base tool name → list of its columns in df."""
    clf_to_cols: Dict[str, List[str]] = defaultdict(list)
    for c in df.columns:
        if any(sfx in c for sfx in PRED_SUFFIXES):
            base = c.split("_", 1)[0]
            clf_to_cols[base].append(c)
    return clf_to_cols


def count_non_missing(df: DataFrame, columns: List[str]) -> Dict[str, int]:
    """Count non-missing values per column and return as dict."""
    row = df.agg(
        *[
            F.sum(F.when(~F.col(c).isin(MISSING), F.lit(1)).otherwise(F.lit(0))).alias(c)
            for c in columns
        ]
    ).collect()[0]
    return row.asDict()


# =============================================================================
# 3) Main workflow
# =============================================================================
def main() -> None:
    """Execute Stage 1 + Stage 2 analysis and optional JDBC export."""
    # -------------------------------------------------------------------------
    # 3.1 Parse CLI arguments
    # -------------------------------------------------------------------------
    ap = argparse.ArgumentParser(description="Assignment 6: dbNSFP analysis (Stage 1 + 2)")
    ap.add_argument("--data", default="/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz")
    ap.add_argument("--test-rows", type=int, default=2000)
    ap.add_argument("--local-cores", type=int, default=8)
    ap.add_argument("--jdbc-jar", default="/homes/ztaherihanjani/jars/mariadb-java-client-3.5.0.jar")
    ap.add_argument("--db-host", default="mariadb.bin.bioinf.nl")
    ap.add_argument("--db-port", default="3306")
    ap.add_argument("--db-name", default="")
    ap.add_argument("--db-user", default="")
    ap.add_argument("--db-pass", default="")
    ap.add_argument("--mycnf", default="/homes/ztaherihanjani/.my.cnf")
    ap.add_argument("--dry-run", action="store_true", default=True)
    ap.add_argument("--no-dry-run", dest="dry_run", action="store_false")
    ap.add_argument("--overwrite", action="store_true")
    args = ap.parse_args()

    # -------------------------------------------------------------------------
    # 3.2 Start Spark session
    # -------------------------------------------------------------------------
    spark = (
        SparkSession.builder
        .appName("assignment6_full_Ztaherihanjani")
        .config("spark.ui.enabled", "false")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.memory", "8g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.jars", args.jdbc_jar)
        .master(f"local[{args.local_cores}]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # -------------------------------------------------------------------------
    # 3.3 Load gzipped dbNSFP file
    # -------------------------------------------------------------------------
    t0 = time.time()
    df = (
        spark.read.csv(args.data, sep="\t", header=True, inferSchema=False)
        .limit(args.test_rows if args.test_rows > 0 else None)
    )
    df.cache()
    total_rows = df.count()
    print(f"📂 Loaded {total_rows:,} rows, {len(df.columns)} columns in {time.time()-t0:.2f}s")
    df.show(3, truncate=False)

    # -------------------------------------------------------------------------
    # 3.4 Discover classifiers and count predictions
    # -------------------------------------------------------------------------
    clf_to_cols = discover_classifiers(df)
    classifier_cols = [c for cols in clf_to_cols.values() for c in cols]
    per_col = count_non_missing(df, classifier_cols)
    clf_counts = {tool: int(sum(per_col.get(c, 0) for c in cols)) for tool, cols in clf_to_cols.items()}
    clf_count_df = (
        spark.createDataFrame(clf_counts.items(), ["classifier", "non_missing_count"])
        .orderBy(F.desc("non_missing_count"))
    )
    clf_count_df.show(10, truncate=False)
    top5 = [r["classifier"] for r in clf_count_df.limit(5).collect()]
    print("✅ Top-5:", top5)

    # -------------------------------------------------------------------------
    # 3.5 Keep ID + Top-5 columns and add chr_pos
    # -------------------------------------------------------------------------
    id_cols = [c for c in ["#chr", "pos(1-based)", "Ensembl_proteinid"] if c in df.columns]
    keep_cols = id_cols + [col for clf in top5 for col in clf_to_cols[clf] if col in df.columns]
    df_top5 = df.select(*keep_cols)
    if {"#chr", "pos(1-based)"} <= set(df_top5.columns):
        df_top5 = df_top5.withColumn("chr_pos", F.concat_ws(":", F.col("#chr"), F.col("pos(1-based)")))

    # -------------------------------------------------------------------------
    # 3.6 Count per-row prediction presence, find top position/protein
    # -------------------------------------------------------------------------
    predict_cols = [c for c in df_top5.columns if c not in id_cols + ["chr_pos"]]
    present_flags = [F.when(~F.col(c).isin(MISSING), 1).otherwise(0) for c in predict_cols]
    df_with_counts = df_top5.withColumn("non_missing_count", safe_sum(present_flags))

    pos_summary = (
        df_with_counts.groupBy("chr_pos")
        .agg(F.sum("non_missing_count").alias("total_predictions"))
        .orderBy(F.desc("total_predictions"))
    )
    prot_summary = (
        df_with_counts.groupBy("Ensembl_proteinid")
        .agg(F.sum("non_missing_count").alias("total_predictions"))
        .orderBy(F.desc("total_predictions"))
    )
    best_pos = pos_summary.limit(1).collect()[0]
    best_prot = prot_summary.limit(1).collect()[0]

    # -------------------------------------------------------------------------
    # 3.7 Normalize to 3NF tables
    # -------------------------------------------------------------------------
    positions_df = (
        df_top5.select("#chr", "pos(1-based)", "chr_pos").distinct()
        .withColumn("position_id", F.monotonically_increasing_id())
    )
    proteins_df = (
        df_top5.select("Ensembl_proteinid").distinct()
        .withColumn("protein_id", F.monotonically_increasing_id())
    )
    predictions_df = (
        df_top5
        .join(positions_df, on=["#chr", "pos(1-based)", "chr_pos"], how="inner")
        .join(proteins_df, on="Ensembl_proteinid", how="inner")
        .select("position_id", "protein_id", *predict_cols)
    )

    # -------------------------------------------------------------------------
    # 3.8 Optional JDBC export
    # -------------------------------------------------------------------------
    wrote = False
    if not args.dry_run:
        if not args.db_name:
            raise SystemExit("--db-name required when writing to DB.")
        creds = {"user": args.db_user, "password": args.db_pass}
        if not creds["user"] or not creds["password"]:
            creds.update(read_mycnf(args.mycnf))
        url = f"jdbc:mariadb://{args.db_host}:{args.db_port}/{args.db_name}"
        mode = "overwrite" if args.overwrite else "append"
        props = {"driver": "org.mariadb.jdbc.Driver", **creds}
        positions_df.write.jdbc(url=url, table="a6_positions", mode=mode, properties=props)
        proteins_df.write.jdbc(url=url, table="a6_proteins", mode=mode, properties=props)
        predictions_df.write.jdbc(url=url, table="a6_predictions", mode=mode, properties=props)
        wrote = True

    # -------------------------------------------------------------------------
    # 3.9 Markdown report answering the 5 questions
    # -------------------------------------------------------------------------
    def md_table(headers, rows):
        """Render simple Markdown table."""
        h = "|" + "|".join(headers) + "|\n|" + "|".join(["---"] * len(headers)) + "|\n"
        b = "".join("|" + "|".join(str(x) for x in row) + "|\n" for row in rows)
        return h + b

    q1_top = clf_count_df.limit(10).collect()
    q1_rows = [(i + 1, r["classifier"], int(r["non_missing_count"])) for i, r in enumerate(q1_top)]

    report = f"""# Assignment 6 — Results

**Author:** Z. Taherihanjani  
**Mode:** {'DRY-RUN' if args.dry_run else 'WRITE'} ({total_rows:,} rows)

## Q1 – How many predictions per classifier?
{md_table(['Rank','Classifier','Non-missing Predictions'], q1_rows)}

## Q2 – Top-5 classifiers kept
{', '.join(top5)}

## Q3 – Merged genomic identifier
Created column `chr_pos` = `#chr:pos(1-based)`.

## Q4 – Position with most predictions
- Position: {best_pos['chr_pos']}
- Total predictions: {best_pos['total_predictions']:,}

## Q5 – Protein with most predictions
- Ensembl_proteinid: {best_prot['Ensembl_proteinid']}
- Total predictions: {best_prot['total_predictions']:,}

## Normalization / SQL Write
- Positions: {positions_df.count():,}
- Proteins: {proteins_df.count():,}
- JDBC: {'skipped (dry-run)' if not wrote else f'written to {args.db_host}/{args.db_name} (a6_*)'}

*All tables normalized to 3NF (a6_positions, a6_proteins, a6_predictions).*
"""
    with open("assignment6.md", "w", encoding="utf-8") as fh:
        fh.write(report)
    print("📝 Saved assignment6.md")

    # -------------------------------------------------------------------------
    # 3.10 Clean shutdown
    # -------------------------------------------------------------------------
    spark.stop()
    print("🎯 Assignment 6 completed successfully.")


# =============================================================================
# Entry point
# =============================================================================
if __name__ == "__main__":
    main()
