#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 6 — dbNSFP Persistence (no DDL export file)

This script:
- Reads the dbNSFP 4.9a dataset (gzipped TSV) using Spark (no manual unzip).
- Detects all prediction/classifier columns by suffix pattern.
- Counts predictions per classifier and keeps Top-5 classifiers (plus ID columns).
- Creates a merged genomic key (chr:pos).
- Finds the position and the protein with the highest number of predictions.
- Normalizes the data into 3NF-style tables:
    * a6_positions (variant dimension for SNVs only),
    * a6_proteins  (protein dimension),
    * a6_predictions (long format predictions).
- Optionally writes to MariaDB when --write is provided.
- Always writes a Markdown report "assignment6.md" that answers the assignment.

Notes:
- The DDL strings are included only for reference; they are not executed by this script.
- The JDBC protocol is "mariadb", not "mysql" (as per assignment hint).
"""

from __future__ import annotations  # Allow future annotations behavior for typing

import argparse  # CLI parsing
import configparser  # Reading ~/.my.cnf for DB credentials
import os  # OS utilities (paths, env)
import sys  # Python path manipulation
from functools import reduce  # For safe sum over Spark Columns
from operator import add  # For Column addition with reduce()
from typing import Dict, Iterable, List, Optional, Sequence, Tuple  # Type hints

# Spark imports (paths and API). In the BIN environment, Spark is installed system-wide.
# We extend sys.path to make sure pyspark and py4j can be imported on the cluster nodes.
# pylint: disable=import-error  # Spark libs may not be visible to pylint locally.
sys.path.append("/opt/spark/python")  # Where pyspark usually lives on BIN
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")  # Py4J bridge
from pyspark.sql import DataFrame, SparkSession  # type: ignore  # Spark types
from pyspark.sql import functions as F  # type: ignore  # Spark SQL functions
from pyspark import StorageLevel  # type: ignore  # For persist() levels

# ----------------------------- Constants -------------------------------------

# Column suffixes used by many dbNSFP classifier outputs; we treat any column that
# ends with one of these as a "prediction column" and infer the classifier base name.
PRED_SUFFIXES: Tuple[str, ...] = ("_score", "_pred", "_rankscore", "_phred")

# The dataset encodes missing values with multiple markers. We handle all these
# uniformly when counting "non-missing" values.
MISSING_MARKERS: Tuple[str, ...] = (".", "", "NA", "NaN", "nan", "null", "NULL")

# Default dataset path on the BIN environment (gz file; Spark can read gz TSV directly).
DEFAULT_DATA = "/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz"

# Default path to the MariaDB JDBC driver JAR (adjust if needed per your environment).
DEFAULT_JDBC = os.path.expanduser("~/jars/mariadb-java-client-3.5.0.jar")

# Spark application name (appears in Spark UI/logs).
DEFAULT_APPNAME = "assignment6_dbnsfp_final"

# Report filename that summarizes the assignment answers.
REPORT_FILE = "assignment6.md"

# ----------------------------- SQL DDL (reference only) ----------------------

# The following DDL strings are kept ONLY for documentation/reference.
# This script does NOT execute them. Tables should exist in MariaDB before writing.

DDL_A6_POSITIONS = """
CREATE TABLE IF NOT EXISTS a6_positions (
  position_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  chr         VARCHAR(10)  NOT NULL,
  pos_1based  INT UNSIGNED NOT NULL,
  ref         CHAR(1)      NOT NULL,
  alt         CHAR(1)      NOT NULL,
  rs_dbSNP    VARCHAR(20)  NULL,
  PRIMARY KEY (position_id),
  UNIQUE KEY uq_chr_pos_ref_alt (chr, pos_1based, ref, alt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_A6_PROTEINS = """
CREATE TABLE IF NOT EXISTS a6_proteins (
  protein_id        BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  ensembl_proteinid VARCHAR(64) NOT NULL,
  ensembl_geneid    VARCHAR(64) NULL,
  genename          VARCHAR(64) NULL,
  uniprot_acc       VARCHAR(32) NULL,
  uniprot_entry     VARCHAR(64) NULL,
  PRIMARY KEY (protein_id),
  UNIQUE KEY uq_ensembl_protein (ensembl_proteinid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_A6_PREDICTIONS = """
CREATE TABLE IF NOT EXISTS a6_predictions (
  prediction_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
  position_id       BIGINT UNSIGNED NOT NULL,
  ensembl_proteinid VARCHAR(64) NULL,
  classifier_name   VARCHAR(64) NOT NULL,
  column_name       VARCHAR(64) NOT NULL,
  value_raw         VARCHAR(128) NOT NULL,
  PRIMARY KEY (prediction_id),
  CONSTRAINT fk_pred_pos
    FOREIGN KEY (position_id)
    REFERENCES a6_positions(position_id)
    ON DELETE CASCADE ON UPDATE CASCADE,
  CONSTRAINT fk_pred_prot
    FOREIGN KEY (ensembl_proteinid)
    REFERENCES a6_proteins(ensembl_proteinid)
    ON DELETE SET NULL ON UPDATE CASCADE,
  KEY idx_pred_pos (position_id),
  KEY idx_pred_cls (classifier_name),
  KEY idx_pred_cls_col (classifier_name, column_name),
  KEY idx_pred_prot (ensembl_proteinid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

# ----------------------------- Helper functions ------------------------------

def read_mycnf(path: str) -> Dict[str, str]:
    """
    Read MariaDB credentials from a MySQL-style option file (e.g., ~/.my.cnf).

    The function looks for a [client] section and reads "user" and "password"
    keys if present. Missing file or missing keys are handled gracefully.

    Args:
        path: Filesystem path to the .my.cnf file.

    Returns:
        A dictionary with optional keys "user" and "password".
    """
    creds: Dict[str, str] = {}
    cfg = configparser.ConfigParser()
    try:
        # Parse the INI-format file using UTF-8 encoding.
        cfg.read(path, encoding="utf-8")
        if "client" in cfg:
            # Populate creds only if options exist; do not KeyError on missing.
            if cfg.has_option("client", "user"):
                creds["user"] = cfg.get("client", "user")
            if cfg.has_option("client", "password"):
                creds["password"] = cfg.get("client", "password")
    except Exception as exc:  # pylint: disable=broad-except
        # We do not hard-fail on credential file issues; we just warn to stdout.
        print(f"Warning: Could not read {path}: {exc}")
    return creds


def is_missing(colname: str) -> F.Column:
    """
    Construct a boolean Spark Column that is True when the given column is "missing".

    We consider a value missing if:
    - It is NULL, or
    - It equals any of the markers in MISSING_MARKERS (string equality).

    Args:
        colname: Name of the Spark column to check.

    Returns:
        A Spark Column of type boolean that is True when value is missing.
    """
    # Start with NULL check.
    col = F.col(colname)
    condition = col.isNull()
    # Combine with OR across all missing markers.
    for marker in MISSING_MARKERS:
        condition = condition | (col == marker)
    return condition


def safe_sum(cols: Sequence) -> F.Column:
    """
    Sum a sequence of Spark Columns safely.

    reduce(add, cols) fails when cols is empty. This helper returns lit(0) if
    cols is empty, otherwise returns a Column that is the sum.

    Args:
        cols: Sequence of numeric Columns to sum.

    Returns:
        A Spark Column representing the sum (or 0 if empty).
    """
    return reduce(add, cols) if cols else F.lit(0)


def discover_classifiers(df: DataFrame) -> Dict[str, List[str]]:
    """
    Map classifier "base names" to their corresponding prediction columns.

    A classifier base name is inferred by removing one of the known suffixes:
    _score, _pred, _rankscore, _phred. For example, "SIFT_pred" -> base "SIFT".

    Args:
        df: Input Spark DataFrame with dbNSFP columns.

    Returns:
        Dict mapping base classifier name -> list of column names for that base.
    """
    clf_map: Dict[str, List[str]] = {}
    # Iterate over all columns and record those ending with a known suffix.
    for col in df.columns:
        for suf in PRED_SUFFIXES:
            if col.endswith(suf):
                # Strip the suffix; also strip trailing underscore if present.
                base = col[: -len(suf)].rstrip("_")
                clf_map.setdefault(base, []).append(col)
                break  # Stop at first matching suffix for this column.
    return clf_map


def count_non_missing(df: DataFrame, columns: Iterable[str]) -> Dict[str, int]:
    """
    Count the number of non-missing values for each column in 'columns'.

    We define "non-missing" as NOT NULL and not equal to any MISSING_MARKERS.
    This avoids using .isin([...]) on large literal arrays which can be brittle.

    Args:
        df: Input DataFrame.
        columns: Iterable of column names to evaluate.

    Returns:
        Dict column_name -> integer count of non-missing entries.
    """
    aggs = []
    # Build conditional sums per column using when(...,1).otherwise(0).
    for c_name in columns:
        cond = ~F.col(c_name).isNull()
        for marker in MISSING_MARKERS:
            cond = cond & (F.col(c_name) != marker)
        aggs.append(F.sum(F.when(cond, 1).otherwise(0)).alias(c_name))
    # Aggregate into a single Row and convert to a dict.
    row = df.agg(*aggs).collect()[0].asDict()
    # Cast values to int for a clean Python dictionary.
    return {k: int(v) for k, v in row.items()}


def find_column(df: DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """
    Return the first column name found in df.columns among candidates.

    This function helps handle minor header name variations across dbNSFP versions.

    Args:
        df: Input DataFrame.
        candidates: Ordered candidate column names.

    Returns:
        The first matching column name, or None if none are present.
    """
    for name in candidates:
        if name in df.columns:
            return name
    return None


def select_top_classifiers(
    df: DataFrame,
    clf_count_df: DataFrame,
    clf_to_cols: Dict[str, List[str]],
    id_cols: Sequence[str],
    top_n: int = 5,
) -> Tuple[DataFrame, List[str]]:
    """
    Select Top-N classifier groups and keep only their columns plus ID columns.

    Args:
        df: Original DataFrame.
        clf_count_df: DataFrame with schema (classifier, predictions), sorted desc.
        clf_to_cols: Mapping from classifier base -> its column list.
        id_cols: Columns to always keep (chr, pos, ref, alt, proteinid if present).
        top_n: Number of top classifiers to keep.

    Returns:
        (df_top, top_classifiers)
        df_top: DataFrame with ID columns + columns of Top-N classifiers.
        top_classifiers: List of classifier base names kept.
    """
    # Pull the top 'top_n' classifier names from the aggregated count DataFrame.
    top_classifiers = [r["classifier"] for r in clf_count_df.limit(top_n).collect()]
    # Start keep list with the ID columns (filter out None just in case).
    keep_cols: List[str] = [c for c in id_cols if c]
    # Add all columns belonging to each of the chosen top classifiers.
    for clf_name in top_classifiers:
        for col in clf_to_cols.get(clf_name, []):
            if col in df.columns:  # Guard in case of unexpected column absence
                keep_cols.append(col)
    # Select only the desired subset of columns.
    df_top = df.select(*keep_cols)
    return df_top, top_classifiers


# ----------------------------- CLI / Main ------------------------------------

def _build_argparser() -> argparse.ArgumentParser:
    """
    Build and return the CLI argument parser for this script.

    Returns:
        An argparse.ArgumentParser instance with all supported arguments.
    """
    parser = argparse.ArgumentParser(description="Assignment 6 — dbNSFP Persistence")
    parser.add_argument(
        "--data",
        default=DEFAULT_DATA,
        help="Path to dbNSFP gzipped TSV (Spark can read .gz directly).",
    )
    parser.add_argument(
        "--rows",
        type=int,
        default=2000,
        help="Row limit for testing; use 0 to read the full dataset.",
    )
    parser.add_argument(
        "--jdbc-jar",
        default=DEFAULT_JDBC,
        help="Path to MariaDB JDBC JAR file (added to spark.jars).",
    )
    parser.add_argument(
        "--db-name",
        default="",
        help="Target MariaDB database name (required when --write is used).",
    )
    parser.add_argument(
        "--db-user",
        default="",
        help="DB user; overrides value in ~/.my.cnf if provided.",
    )
    parser.add_argument(
        "--db-pass",
        default="",
        help="DB password; overrides value in ~/.my.cnf if provided.",
    )
    parser.add_argument(
        "--write",
        dest="dry_run",
        action="store_false",
        help="Actually write results to MariaDB (append mode).",
    )
    # By default we do a dry run (no DB writes).
    parser.set_defaults(dry_run=True)
    return parser


def main() -> int:
    """
    Entry point for the Assignment 6 pipeline.

    Returns:
        Process exit status code (0 on success, non-zero on failure).
    """
    # Parse CLI args.
    parser = _build_argparser()
    args = parser.parse_args()

    # Show mode to the user (dry run vs write).
    print(f"Starting Assignment 6 (dry_run={args.dry_run}) ...")

    # ------------------------ 1) SPARK SESSION SETUP -------------------------
    # Build a SparkSession and register the MariaDB driver JAR so Spark JDBC can use it.
    spark = (
        SparkSession.builder
        .appName(DEFAULT_APPNAME)
        .config("spark.ui.enabled", "false")  # Optional: reduce overhead on clusters
        .config("spark.driver.memory", "8g")  # Tweak per cluster limits
        .config("spark.executor.memory", "8g")  # Tweak per cluster limits
        .config("spark.jars", args.jdbc_jar)  # MariaDB JDBC driver
        .getOrCreate()
    )
    # Reduce Spark logging verbosity for cleaner output.
    spark.sparkContext.setLogLevel("ERROR")

    # ------------------------ 2) LOAD DATASET --------------------------------
    print(f"Loading dataset: {args.data}")
    # Read gzipped TSV with header; inferSchema=False to keep strings unless cast explicitly.
    df = spark.read.csv(args.data, sep="\t", header=True, inferSchema=False)

    # Optionally limit rows for testing to speed up local/early runs.
    if args.rows and args.rows > 0:
        df = df.limit(args.rows)
        print(f"TEST MODE: limited to {args.rows} rows")

    # Persist to memory and disk to avoid recomputation across multiple actions.
    df.persist(StorageLevel.MEMORY_AND_DISK)

    # Count rows and columns to report in the Markdown.
    n_rows = df.count()
    n_cols = len(df.columns)
    print(f"Loaded {n_rows:,} rows x {n_cols} cols")

    # ------------------------ Resolve key ID columns --------------------------
    # dbNSFP headers vary slightly between versions. We try a list of candidates.
    chr_col = find_column(df, ["#chr", "chr"])
    pos_col = find_column(df, ["pos(1-based)", "pos"])
    ref_col = find_column(df, ["ref"])
    alt_col = find_column(df, ["alt"])
    prot_col = find_column(df, ["Ensembl_proteinid", "proteinid", "protein_id"])
    gene_col = find_column(df, ["Ensembl_geneid", "geneid"])
    genename_col = find_column(df, ["genename"])
    uniprot_acc_col = find_column(df, ["Uniprot_acc"])
    uniprot_entry_col = find_column(df, ["Uniprot_entry"])
    rs_dbsnp_col = find_column(df, ["rs_dbSNP"])

    # If required genomic coordinates are missing, we cannot proceed.
    if not chr_col or not pos_col:
        print("ERROR: Missing required chr/pos columns; aborting.")
        df.unpersist()
        spark.stop()
        return 1

    # ------------------------ 3) Q1: CLASSIFIER COUNTS -----------------------
    # Build a map of classifier base -> list of its columns (by suffix detection).
    clf_map = discover_classifiers(df)
    # Flatten the list of lists to a single list of all prediction columns.
    clf_cols = [c for cols in clf_map.values() for c in cols]
    print(f"Detected {len(clf_map)} classifiers over {len(clf_cols)} columns")

    # Count non-missing entries per prediction column, then sum per classifier base.
    counts = count_non_missing(df, clf_cols)
    q1_rows_py = [(base, sum(counts.get(c, 0) for c in cols))
                  for base, cols in clf_map.items()]
    # Create a small DataFrame for reporting and sorting by total predictions.
    q1_df = (
        spark.createDataFrame(q1_rows_py, ["classifier", "predictions"])
        .orderBy(F.desc("predictions"), F.asc("classifier"))
    )
    print("Top-10 classifiers by predictions:")
    q1_df.show(10, truncate=False)

    # ------------------------ 4) KEEP TOP-N CLASSIFIERS ----------------------
    # ID columns to always keep in the downstream subset.
    id_keep: List[str] = [chr_col, pos_col]
    if ref_col:
        id_keep.append(ref_col)
    if alt_col:
        id_keep.append(alt_col)
    if prot_col:
        id_keep.append(prot_col)

    # Select only the columns belonging to Top-5 classifiers plus ID columns.
    df_top, topN = select_top_classifiers(
        df=df,
        clf_count_df=q1_df,
        clf_to_cols=clf_map,
        id_cols=tuple(id_keep),
        top_n=5,
    )
    print("Top-5 classifiers:", ", ".join(topN))

    # Build the merged genomic key (as specified by the assignment).
    df_top = df_top.withColumn("chr_pos", F.concat_ws(":", F.col(chr_col), F.col(pos_col)))
    print("Created chr_pos key.")

    # ------------------------ 5) PER-ROW NON-MISSING COUNT -------------------
    # Identify the kept prediction columns (exclude ID and helper columns).
    exclude_cols = {chr_col, pos_col, ref_col, alt_col, "chr_pos", prot_col}
    pred_cols = [c for c in df_top.columns if c not in exclude_cols]

    # Count how many kept prediction columns are non-missing in each row.
    df_top = df_top.withColumn(
        "non_missing_count",
        safe_sum([F.when(~is_missing(c), 1).otherwise(0) for c in pred_cols]),
    )

    # ------------------------ 6) Q4: BEST POSITION ---------------------------
    # Aggregate the total predictions per genomic position (chr:pos).
    pos_summary = (
        df_top.groupBy("chr_pos")
        .agg(F.sum("non_missing_count").alias("total_preds"))
        .orderBy(F.desc("total_preds"))
    )
    # Take the top row as the "position with most predictions".
    best_pos = pos_summary.first()
    print(f"Max position: {best_pos['chr_pos']} ({int(best_pos['total_preds'])})")

    # ------------------------ 7) Q5: BEST PROTEIN ----------------------------
    best_prot = None
    if prot_col:
        # Aggregate total predictions per protein.
        prot_summary = (
            df_top.groupBy(prot_col)
            .agg(F.sum("non_missing_count").alias("total_preds"))
            .orderBy(F.desc("total_preds"))
        )
        best_prot = prot_summary.first()
        print(f"Max protein: {best_prot[prot_col]} ({int(best_prot['total_preds'])})")
    else:
        print("Protein column not present; skipping Q5.")

    # ------------------------ 8) 3NF: DIMENSION TABLES -----------------------
    # Build the a6_positions dimension (SNVs only).
    df_positions = df.select(
        F.col(chr_col).alias("chr"),
        F.col(pos_col).cast("int").alias("pos_1based"),
        F.col(ref_col).alias("ref") if ref_col else F.lit(None).alias("ref"),
        F.col(alt_col).alias("alt") if alt_col else F.lit(None).alias("alt"),
        F.col(rs_dbsnp_col).alias("rs_dbSNP") if rs_dbsnp_col else F.lit(None).alias("rs_dbSNP"),
    )

    # Filter for valid chromosome, non-null position, and enforce SNVs (single base ref/alt).
    filter_condition = F.col("chr").isNotNull() & (F.col("chr") != "")
    for marker in MISSING_MARKERS:
        filter_condition = filter_condition & (F.col("chr") != marker)

    df_positions = (
        df_positions
        .withColumn("ref", F.upper(F.trim(F.col("ref"))))
        .withColumn("alt", F.upper(F.trim(F.col("alt"))))
        .filter(F.length("ref") == 1)
        .filter(F.length("alt") == 1)
        .filter(filter_condition)
        .filter(F.col("pos_1based").isNotNull())
        .dropDuplicates(["chr", "pos_1based", "ref", "alt"])
    )

    # Count distinct SNV positions for reporting.
    dim_position_count = df_positions.count()
    print(f"a6_positions: {dim_position_count:,} unique positions")

    # Build the a6_proteins dimension if the column exists in the dataset.
    dim_protein = None
    dim_protein_count = 0
    if prot_col:
        df_proteins = df.select(
            F.col(prot_col).alias("ensembl_proteinid"),
            F.col(gene_col).alias("ensembl_geneid") if gene_col else F.lit(None).alias("ensembl_geneid"),
            F.col(genename_col).alias("genename") if genename_col else F.lit(None).alias("genename"),
            F.col(uniprot_acc_col).alias("uniprot_acc") if uniprot_acc_col else F.lit(None).alias("uniprot_acc"),
            F.col(uniprot_entry_col).alias("uniprot_entry") if uniprot_entry_col else F.lit(None).alias("uniprot_entry"),
        )

        # Keep only rows with a non-missing Ensembl protein ID, trim strings, and deduplicate.
        prot_filter = F.col("ensembl_proteinid").isNotNull()
        for marker in MISSING_MARKERS:
            prot_filter = prot_filter & (F.col("ensembl_proteinid") != marker)

        df_proteins = (
            df_proteins
            .withColumn("ensembl_proteinid", F.trim(F.col("ensembl_proteinid")))
            .withColumn("ensembl_geneid", F.trim(F.col("ensembl_geneid")))
            .withColumn("genename", F.trim(F.col("genename")))
            .withColumn("uniprot_acc", F.trim(F.col("uniprot_acc")))
            .withColumn("uniprot_entry", F.trim(F.col("uniprot_entry")))
            .filter(prot_filter)
            .dropDuplicates(["ensembl_proteinid"])
        )

        dim_protein = df_proteins
        dim_protein_count = dim_protein.count()
        print(f"a6_proteins: {dim_protein_count:,} unique proteins")
    else:
        print("Protein column not found; skipping a6_proteins table.")

    # For completeness we also show a classifier dimension (not persisted).
    dim_classifier = spark.createDataFrame(
        [(name,) for name in clf_map.keys()], ["classifier_name"]
    )
    dim_classifier_count = dim_classifier.count()
    print(f"a6_classifiers: {dim_classifier_count} classifiers")

    # ------------------------ 9) LONG-FORM PREDICTIONS -----------------------
    # Collect all prediction columns based on the known suffixes.
    all_pred_cols = [
        c for c in df.columns
        if c.endswith(("_score", "_pred", "_rankscore", "_phred"))
    ]

    # Build parts of the long table by selecting the same keys for each column,
    # adding "column_name" and casting value to string for uniform storage.
    long_parts = []
    for colname in all_pred_cols:
        part = (
            df.select(
                F.col(chr_col).alias("chr"),
                F.col(pos_col).cast("int").alias("pos_1based"),
                F.col(ref_col).alias("ref") if ref_col else F.lit(None).alias("ref"),
                F.col(alt_col).alias("alt") if alt_col else F.lit(None).alias("alt"),
                F.col(prot_col).alias("ensembl_proteinid") if prot_col else F.lit(None).alias("ensembl_proteinid"),
                F.lit(colname).alias("column_name"),
                F.col(colname).cast("string").alias("value_raw"),
            )
            .filter(~is_missing(colname))  # Keep only non-missing predictions
        )
        long_parts.append(part)

    # Union all parts into a single long-format DataFrame with a uniform schema.
    if not long_parts:
        # Create an empty DataFrame with the intended schema if no columns match.
        predictions_long = spark.createDataFrame(
            [],
            schema=(
                "chr string, pos_1based int, ref string, alt string, "
                "ensembl_proteinid string, column_name string, value_raw string"
            ),
        )
    else:
        predictions_long = long_parts[0]
        for p in long_parts[1:]:
            predictions_long = predictions_long.unionByName(p)

    # Extract classifier_name from column_name by removing the known suffixes.
    predictions_long = predictions_long.withColumn(
        "classifier_name",
        F.regexp_extract(
            F.col("column_name"), r"^(.+?)_(?:score|pred|rankscore|phred)$", 1
        ),
    )

    # Enforce that we keep only SNVs in the long table too (single-character ref/alt).
    predictions_long = (
        predictions_long
        .withColumn("ref", F.upper(F.trim(F.col("ref"))))
        .withColumn("alt", F.upper(F.trim(F.col("alt"))))
        .filter(F.length("ref") == 1)
        .filter(F.length("alt") == 1)
    )

    # Count rows for reporting (before joining to position IDs in DB).
    pred_count = predictions_long.count()
    print(f"a6_predictions (before FK join): {pred_count:,} rows")

    # ------------------------ 10) WRITE TO MARIADB (OPTIONAL) ----------------
    # By default we do not write; we only write when --write is provided.
    db_status = "DRY RUN — not written"

    if not args.dry_run:
        # When writing, a database name is mandatory.
        if not args.db_name:
            print("ERROR: --db-name is required for writing to database")
            df.unpersist()
            spark.stop()
            return 1

        # Build credentials from CLI overrides first.
        creds = {"user": args.db_user, "password": args.db_pass}
        # Then merge in credentials from ~/.my.cnf if not already provided.
        file_creds = read_mycnf(os.path.expanduser("~/.my.cnf"))
        for key, val in file_creds.items():
            creds.setdefault(key, val)

        # JDBC URL for MariaDB on BIN (note protocol "mariadb").
        url = f"jdbc:mariadb://mariadb.bin.bioinf.nl:3306/{args.db_name}"

        # Write the positions dimension. Mode "append" expects table to exist.
        print("Writing a6_positions ...")
        (
            df_positions.write.format("jdbc")
            .option("url", url)
            .option("driver", "org.mariadb.jdbc.Driver")
            .option("dbtable", "a6_positions")
            .option("user", creds["user"])
            .option("password", creds["password"])
            .mode("append")
            .save()
        )
        print("OK: a6_positions written")

        # Write the proteins dimension if it exists in the dataset.
        if dim_protein is not None:
            print("Writing a6_proteins ...")
            (
                dim_protein.write.format("jdbc")
                .option("url", url)
                .option("driver", "org.mariadb.jdbc.Driver")
                .option("dbtable", "a6_proteins")
                .option("user", creds["user"])
                .option("password", creds["password"])
                .mode("append")
                .save()
            )
            print("OK: a6_proteins written")

        # Read back positions with their generated position_id to build FKs.
        print("Reading back a6_positions for FK join...")
        pos_db = (
            spark.read.format("jdbc")
            .option("url", url)
            .option("driver", "org.mariadb.jdbc.Driver")
            .option("dbtable", "a6_positions")
            .option("user", creds["user"])
            .option("password", creds["password"])
            .load()
            .select("position_id", "chr", "pos_1based", "ref", "alt")
        )

        # Join predictions to positions to attach the numeric position_id FK.
        predictions_with_fk = (
            predictions_long.alias("p")
            .join(
                pos_db.alias("d"),
                on=[
                    F.col("p.chr") == F.col("d.chr"),
                    F.col("p.pos_1based") == F.col("d.pos_1based"),
                    F.col("p.ref") == F.col("d.ref"),
                    F.col("p.alt") == F.col("d.alt"),
                ],
                how="inner",
            )
            .select(
                F.col("d.position_id").alias("position_id"),
                F.col("p.ensembl_proteinid").alias("ensembl_proteinid"),
                F.col("p.classifier_name").alias("classifier_name"),
                F.col("p.column_name").alias("column_name"),
                F.col("p.value_raw").alias("value_raw"),
            )
            .dropDuplicates()
        )

        # Count rows for reporting after FK assignment.
        pred_final_count = predictions_with_fk.count()
        print(f"Predictions with FK: {pred_final_count:,} rows")

        # Write the long predictions table to MariaDB.
        print("Writing a6_predictions ...")
        (
            predictions_with_fk.write.format("jdbc")
            .option("url", url)
            .option("driver", "org.mariadb.jdbc.Driver")
            .option("dbtable", "a6_predictions")
            .option("user", creds["user"])
            .option("password", creds["password"])
            .mode("append")
            .save()
        )
        print("OK: a6_predictions written")

        # Update DB status string for the report.
        db_status = f"Written to {args.db_name}"

    # ------------------------ 11) GENERATE MARKDOWN REPORT -------------------
    # Materialize q1_df to Python rows for listing into the report (optional).
    q1_rows = [
        (i + 1, r["classifier"], int(r["predictions"]))
        for i, r in enumerate(q1_df.collect())
    ]

    # Build the report text with key answers and stats required by the assignment.
    report = (
        f"# Assignment 6 — Results\n\n"
        f"**Rows analyzed:** {n_rows:,}  \n"
        f"**Columns analyzed:** {n_cols}\n\n"
        f"## Q1 — Predictions per Classifier\n"
        f"**Top-5 kept:** {', '.join(topN)}\n\n"
        f"## Q3 — Merged genomic identifier\n"
        f"`chr_pos = {chr_col}:{pos_col}`  (e.g., `1:69037`)\n\n"
        f"## Q4 — Position with Most Predictions\n"
        f"- {best_pos['chr_pos']} ({int(best_pos['total_preds'])} predictions)\n\n"
        f"## Q5 — Protein with Most Predictions\n"
        f"- {(best_prot[prot_col] if best_prot else 'N/A')} "
        f"({(int(best_prot['total_preds']) if best_prot else 'N/A')} predictions)\n\n"
        f"## Normalization (3NF)\n"
        f"- **a6_positions:** {dim_position_count:,} unique positions  \n"
        f"- **a6_proteins:** {dim_protein_count:,} unique proteins  \n"
        f"- **a6_classifiers:** {dim_classifier_count} classifiers  \n"
        f"- **a6_predictions:** {pred_count:,} rows (before FK join)  \n"
        f"- **Database write:** {db_status}\n"
    )

    # Write the Markdown file to the current working directory.
    with open(REPORT_FILE, "w", encoding="utf-8") as f_out:
        f_out.write(report)
    print(f"Saved report: {REPORT_FILE}")

    # ------------------------ 12) CLEANUP ------------------------------------
    # Unpersist the main DataFrame to release cluster memory and storage.
    df.unpersist()
    # Stop SparkSession cleanly.
    spark.stop()
    print("Done.")
    return 0


if __name__ == "__main__":
    # Use SystemExit to return the main() exit code as the process' status.
    raise SystemExit(main())
