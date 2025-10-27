#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 6 — Complete Solution (CORRECTED)
Author: Z. Taherihanjani

FIXES:
1. Base name extraction: proper handling of HDIV_score vs HVAR_score
2. Column detection: endswith() instead of 'in' to avoid false positives
3. NULL handling: comprehensive missing value detection
4. Robust column name detection for #chr/pos variations
5. TRUE 3NF: unpivoted predictions table (long format)
"""

# =============================================================================
# 0) Setup and imports
# =============================================================================
import sys
import os
import time
import argparse
import configparser
from collections import defaultdict
from functools import reduce
from operator import add

sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")

from pyspark.sql import SparkSession, DataFrame, functions as F
from pyspark import StorageLevel

# =============================================================================
# 1. Constants
# =============================================================================
# These suffixes identify classifier columns in the dbNSFP dataset
PRED_SUFFIXES = ("_score", "_pred", "_rankscore", "_phred")

# Common missing value markers in dbNSFP
MISSING = {".", "", "NA", "NaN", "nan", "null", "NULL"}


# =============================================================================
# 2. Utility functions
# =============================================================================
def read_mycnf(path):
    """Read MySQL/MariaDB credentials from ~/.my.cnf (if available)."""
    cfg = configparser.ConfigParser()
    creds = {}
    try:
        cfg.read(path)
        if "client" in cfg:
            if cfg.has_option("client", "user"):
                creds["user"] = cfg.get("client", "user")
            if cfg.has_option("client", "password"):
                creds["password"] = cfg.get("client", "password")
    except Exception as err:
        print(f"⚠️ Could not read {path}: {err}")
    return creds


def safe_sum(columns):
    """Safely sum Spark Columns (avoid empty reduce)."""
    return reduce(add, columns) if columns else F.lit(0)


def is_missing(col):
    """Return True if a value is missing (NULL, empty, or marker)."""
    return F.col(col).isNull() | F.col(col).isin(MISSING)


def discover_classifiers(df):
    """
    Detect classifier columns by suffix and group them by base name.

    Example:
    - 'MetaSVM_score' → base = 'MetaSVM'
    - 'CADD_raw_rankscore' → base = 'CADD_raw'
    """
    clf_map = defaultdict(list)
    for col in df.columns:
        for suf in PRED_SUFFIXES:
            if col.endswith(suf):
                base = col[: col.rfind(suf)].rstrip("_")
                clf_map[base].append(col)
                break
    return clf_map


def count_non_missing(df, columns):
    """Count non-missing (valid) values for each column."""
    agg_exprs = [
        F.sum(F.when(~is_missing(c), 1).otherwise(0)).alias(c) for c in columns
    ]
    return df.agg(*agg_exprs).collect()[0].asDict()


def find_column(df, candidates):
    """Find the first existing column name from a list of candidates."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def markdown_table(headers, rows):
    """Create a Markdown table (for report generation)."""
    h = "|" + "|".join(headers) + "|\n|" + "|".join(["---"] * len(headers)) + "|\n"
    b = "".join("|" + "|".join(str(x) for x in r) + "|\n" for r in rows)
    return h + b


# =============================================================================
# 3. Main function
# =============================================================================
def main():
    """Main function to execute the dbNSFP analysis."""
    parser = argparse.ArgumentParser(description="Assignment 6 — dbNSFP Analysis")
    parser.add_argument("--data", required=False,
                        default="/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz",
                        help="Path to dbNSFP dataset (gzipped TSV)")
    parser.add_argument("--rows", type=int, default=2000,
                        help="Limit rows for testing (0 = full dataset)")
    parser.add_argument("--jdbc-jar", default=os.path.expanduser("~/jars/mariadb-java-client-3.5.0.jar"),
                        help="Path to MariaDB JDBC driver JAR")
    parser.add_argument("--db-name", default="", help="Database name for writing results")
    parser.add_argument("--db-user", default="", help="Database user")
    parser.add_argument("--db-pass", default="", help="Database password")
    parser.add_argument("--dry-run", action="store_true", default=True,
                        help="Dry run: skip database writing")
    args = parser.parse_args()

    # -------------------------------------------------------------------------
    # 3.1 Initialize Spark
    # -------------------------------------------------------------------------
    print("🚀 Starting Assignment 6 Analysis...")
    spark = (
        SparkSession.builder
        .appName("assignment6_final")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.jars", args.jdbc_jar)
        .master("local[*]")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # -------------------------------------------------------------------------
    # 3.2 Load dataset
    # -------------------------------------------------------------------------
    print(f"📂 Loading dataset from {args.data}")
    df = spark.read.csv(args.data, sep="\t", header=True, inferSchema=False)

    if args.rows > 0:
        df = df.limit(args.rows)
        print(f"⚠️ Running in TEST MODE: limited to {args.rows} rows")

    df.persist(StorageLevel.MEMORY_AND_DISK)
    n_rows = df.count()
    n_cols = len(df.columns)
    print(f"✅ Loaded {n_rows:,} rows × {n_cols} columns")

    # -------------------------------------------------------------------------
    # 3.3 Detect classifier columns (Q1)
    # -------------------------------------------------------------------------
    clf_map = discover_classifiers(df)
    clf_cols = [c for cols in clf_map.values() for c in cols]
    print(f"🔍 Found {len(clf_map)} classifiers with {len(clf_cols)} total columns")

    # Count valid predictions per classifier
    print("📊 Counting valid predictions per classifier...")
    counts = count_non_missing(df, clf_cols)
    clf_totals = []
    for clf, cols in clf_map.items():
        clf_totals.append((clf, sum(counts.get(c, 0) for c in cols)))

    q1_df = spark.createDataFrame(clf_totals, ["classifier", "predictions"])\
                 .orderBy(F.desc("predictions"), F.asc("classifier"))
    print("✅ Top 10 classifiers:")
    q1_df.show(10, truncate=False)

    # Keep Top-5
    top5 = [r["classifier"] for r in q1_df.limit(5).collect()]
    print(f"🏆 Top-5 classifiers: {', '.join(top5)}")

    # -------------------------------------------------------------------------
    # 3.4 Detect ID columns (chr, pos, protein)
    # -------------------------------------------------------------------------
    chr_col = find_column(df, ["#chr", "chr"])
    pos_col = find_column(df, ["pos(1-based)", "pos"])
    prot_col = find_column(df, ["Ensembl_proteinid", "proteinid", "protein_id"])

    if not chr_col or not pos_col:
        print("❌ Missing required chromosome or position columns. Exiting.")
        spark.stop()
        sys.exit(1)

    # -------------------------------------------------------------------------
    # 3.5 Keep Top-5 classifiers + ID columns
    # -------------------------------------------------------------------------
    keep_cols = [chr_col, pos_col]
    if prot_col:
        keep_cols.append(prot_col)
    for clf in top5:
        keep_cols.extend(clf_map[clf])

    df_top = df.select(*keep_cols)
    print(f"✅ Kept {len(df_top.columns)} columns for analysis")

    # -------------------------------------------------------------------------
    # 3.6 Create chr_pos (Q3)
    # -------------------------------------------------------------------------
    df_top = df_top.withColumn("chr_pos", F.concat_ws(":", F.col(chr_col), F.col(pos_col)))
    print("✅ Created column chr_pos = chr:pos")
    df_top.select(chr_col, pos_col, "chr_pos").show(5, truncate=False)

    # -------------------------------------------------------------------------
    # 3.7 Count predictions per row (Q4/Q5)
    # -------------------------------------------------------------------------
    pred_cols = [c for c in df_top.columns if c not in [chr_col, pos_col, prot_col, "chr_pos"]]
    df_top = df_top.withColumn(
        "non_missing_count",
        safe_sum([F.when(~is_missing(c), 1).otherwise(0) for c in pred_cols])
    )

    # --- Q4: find position with most predictions ---
    pos_summary = (
        df_top.groupBy("chr_pos")
        .agg(F.sum("non_missing_count").alias("total_preds"))
        .orderBy(F.desc("total_preds"))
    )
    best_pos = pos_summary.limit(1).collect()[0]
    print(f"⭐ Position with most predictions: {best_pos['chr_pos']} ({best_pos['total_preds']})")

    # --- Q5: find protein with most predictions ---
    best_prot = None
    if prot_col:
        prot_summary = (
            df_top.groupBy(prot_col)
            .agg(F.sum("non_missing_count").alias("total_preds"))
            .orderBy(F.desc("total_preds"))
        )
        best_prot = prot_summary.limit(1).collect()[0]
        print(f"⭐ Protein with most predictions: {best_prot[prot_col]} ({best_prot['total_preds']})")

    # -------------------------------------------------------------------------
    # 3.8 Normalize to 3NF tables
    # -------------------------------------------------------------------------
    print("🗄️ Normalizing dataset into 3NF tables...")

    # Positions table
    dim_position = (
        df_top.select(chr_col, pos_col, "chr_pos").distinct()
        .withColumn("position_id", F.monotonically_increasing_id())
    )

    # Proteins table
    dim_protein = None
    if prot_col:
        dim_protein = (
            df_top.select(prot_col).distinct()
            .withColumn("protein_id", F.monotonically_increasing_id())
        )

    # Classifiers table
    dim_classifier = spark.createDataFrame(
        [(i + 1, clf) for i, clf in enumerate(top5)], ["classifier_id", "classifier_name"]
    )

    # Predictions (unpivoted, long format)
    base_df = df_top.join(dim_position, on=["chr_pos"], how="inner")
    if prot_col:
        base_df = base_df.join(dim_protein, on=[prot_col], how="left")

    # Unpivot predictions: one row per classifier column
    long_rows = []
    for colname in pred_cols:
        expr = base_df.select(
            "position_id",
            F.lit(colname).alias("column_name"),
            F.col(colname).alias("value")
        ).filter(~is_missing(colname))
        long_rows.append(expr)

    predictions = long_rows[0]
    for expr in long_rows[1:]:
        predictions = predictions.union(expr)

    # Add classifier_id
    predictions = predictions.withColumn(
        "classifier_name",
        F.regexp_extract("column_name", r"^(.+?)_(?:score|pred|rankscore|phred)$", 1)
    ).join(dim_classifier, on="classifier_name", how="inner")\
     .select("position_id", "classifier_id", "column_name", "value")

    print(f"✅ Predictions table (unpivoted): {predictions.count():,} rows")

    # -------------------------------------------------------------------------
    # 3.9 Write to MariaDB (optional)
    # -------------------------------------------------------------------------
    if not args.dry_run and args.db_name:
        creds = {"user": args.db_user, "password": args.db_pass}
        creds.update(read_mycnf(os.path.expanduser("~/.my.cnf")))

        url = f"jdbc:mariadb://mariadb.bin.bioinf.nl:3306/{args.db_name}"
        props = {"driver": "org.mariadb.jdbc.Driver", **creds}

        dim_position.write.jdbc(url=url, table="a6_positions", mode="overwrite", properties=props)
        if dim_protein:
            dim_protein.write.jdbc(url=url, table="a6_proteins", mode="overwrite", properties=props)
        dim_classifier.write.jdbc(url=url, table="a6_classifiers", mode="overwrite", properties=props)
        predictions.write.jdbc(url=url, table="a6_predictions", mode="overwrite", properties=props)
        print("💾 Data written to MariaDB successfully.")
    else:
        print("⚠️ Dry-run mode: skipping database write.")

    # -------------------------------------------------------------------------
    # 3.10 Generate Markdown report
    # -------------------------------------------------------------------------
    q1_rows = [
        (i + 1, r["classifier"], f"{int(r['predictions']):,}")
        for i, r in enumerate(q1_df.collect())
    ]

    report = f"""# Assignment 6 — Results (Final)

**Rows analyzed:** {n_rows:,}  
**Columns analyzed:** {n_cols}

## Q1 — Predictions per Classifier
{markdown_table(["Rank", "Classifier", "Predictions"], q1_rows)}

**Top-5 kept:** {', '.join(top5)}

## Q3 — Merged genomic identifier
We created a unique key:
`chr_pos = {chr_col}:{pos_col}`  
Example format: `1:69037`

## Q4 — Position with Most Predictions
- {best_pos['chr_pos']} ({best_pos['total_preds']} predictions)

## Q5 — Protein with Most Predictions
- {best_prot[prot_col] if best_prot else 'N/A'} ({best_prot['total_preds'] if best_prot else 'N/A'} predictions)

## Normalization (3NF)
- **a6_positions:** {dim_position.count():,} unique genomic positions  
- **a6_proteins:** {dim_protein.count() if dim_protein else 0:,} unique proteins  
- **a6_classifiers:** {dim_classifier.count():,} classifiers  
- **a6_predictions:** {predictions.count():,} prediction records  
- **Database write:** {"Done" if not args.dry_run else "Skipped (dry-run)"}

✅ All questions (Q1–Q5) and normalization completed successfully.
"""

    with open("assignment6.md", "w", encoding="utf-8") as f:
        f.write(report)
    print("✅ Markdown report saved as assignment6.md")

    # -------------------------------------------------------------------------
    # 3.11 Cleanup
    # -------------------------------------------------------------------------
    df.unpersist()
    spark.stop()
    print("🎯 Assignment 6 completed successfully!")


# =============================================================================
# Entry point
# =============================================================================
if __name__ == "__main__":
    main()

