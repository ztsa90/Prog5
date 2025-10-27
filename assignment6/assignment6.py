#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 6 — dbNSFP Persistence (Final, simplified & well-commented)

Pipeline:
1) Read gzipped dbNSFP TSV
2) Detect classifier columns by suffixes
3) Q1: count predictions per classifier (non-missing)
4) Keep Top-N classifiers (default 5) + ID columns via select_top_classifiers()
5) Create chr_pos key
6) Q4/Q5: most-predicted position/protein
7) Normalize to 3NF (positions, proteins, classifiers, predictions-long)
8) Optional: write to MariaDB via JDBC
9) Save assignment6.md report
"""

from __future__ import annotations

import argparse
import configparser
import os
import sys
from functools import reduce
from operator import add
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

# Spark imports (BIN). In some environments pylint can't see pyspark stubs.
# pylint: disable=import-error
sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")
from pyspark.sql import DataFrame, SparkSession  # type: ignore
from pyspark.sql import functions as F            # type: ignore
from pyspark import StorageLevel                  # type: ignore

# ----------------------------- Constants -------------------------------------

# Suffixes that indicate prediction classifier columns
PRED_SUFFIXES: Tuple[str, ...] = ("_score", "_pred", "_rankscore", "_phred")

# Text markers used in dbNSFP to denote missing values (besides real NULL)
MISSING_MARKERS: Tuple[str, ...] = (".", "", "NA", "NaN", "nan", "null", "NULL")

DEFAULT_DATA = "/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz"
DEFAULT_JDBC = os.path.expanduser("~/jars/mariadb-java-client-3.5.0.jar")
DEFAULT_APPNAME = "assignment6_dbnsfp_final"
REPORT_FILE = "assignment6.md"

# ----------------------------- Helpers ---------------------------------------

def read_mycnf(path: str) -> Dict[str, str]:
    """
    Read MariaDB credentials from ~/.my.cnf if present.

    Returns a dict with possible keys 'user' and 'password'.
    """
    creds: Dict[str, str] = {}
    cfg = configparser.ConfigParser()
    try:
        cfg.read(path, encoding="utf-8")
        if "client" in cfg:
            if cfg.has_option("client", "user"):
                creds["user"] = cfg.get("client", "user")
            if cfg.has_option("client", "password"):
                creds["password"] = cfg.get("client", "password")
    except Exception as exc:  # pylint: disable=broad-except
        print(f"Warning: Could not read {path}: {exc}")
    return creds


def is_missing(colname: str):
    """
    Build a Spark Column expressing "value in column colname is missing".

    Missing if:
      - value is NULL (Spark null), OR
      - value equals one of MISSING_MARKERS.
    """
    return F.col(colname).isNull() | F.col(colname).isin(list(MISSING_MARKERS))


def safe_sum(cols: Sequence) -> F.Column:
    """
    Sum a list of Spark Columns safely (works even if the list is empty).
    """
    return reduce(add, cols) if cols else F.lit(0)


def discover_classifiers(df: DataFrame) -> Dict[str, List[str]]:
    """
    Map: classifier base → list of its columns (detected by known suffixes).

    Examples
    --------
    - 'MetaSVM_score'        → base 'MetaSVM'
    - 'CADD_raw_rankscore'   → base 'CADD_raw'
    """
    clf_map: Dict[str, List[str]] = {}
    for col in df.columns:
        for suf in PRED_SUFFIXES:
            if col.endswith(suf):
                # Remove suffix safely, then trim a possible trailing underscore.
                base = col[: -len(suf)].rstrip("_")
                if base not in clf_map:
                    clf_map[base] = []
                clf_map[base].append(col)
                break  # this column already matched a suffix
    return clf_map


def count_non_missing(df: DataFrame, columns: Iterable[str]) -> Dict[str, int]:
    """
    Count non-missing (non-null and not in MISSING_MARKERS) values per column.

    Parameters
    ----------
    df : DataFrame
        Spark DataFrame to analyze.
    columns : Iterable[str]
        Column names to count on.

    Returns
    -------
    Dict[str, int]
        Mapping col_name → non-missing count for that column.

    Notes
    -----
    Runs all counts in a single pass over the DataFrame using .agg().
    """
    missing = set(MISSING_MARKERS)  # local set for Spark .isin()
    aggs = [
        F.sum(
            F.when(~(F.col(c).isin(missing) | F.col(c).isNull()), 1).otherwise(0)
        ).alias(c)
        for c in columns
    ]
    row = df.agg(*aggs).collect()[0].asDict()
    return {k: int(v) for k, v in row.items()}


def md_table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """
    Build a simple Markdown table string given headers and rows.
    """
    head = "|" + "|".join(headers) + "|\n|" + "|".join(["---"] * len(headers)) + "|\n"
    body = "".join("|" + "|".join(str(x) for x in r) + "|\n" for r in rows)
    return head + body


def find_column(df: DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """
    Return the first column name from 'candidates' that exists in df.columns, else None.

    Example
    -------
    >>> find_column(df, ["#chr", "chr"])
    'chr'
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
    Select Top-N classifier columns plus ID columns from the main DataFrame.

    Parameters
    ----------
    df : DataFrame
        Full dbNSFP Spark DataFrame.
    clf_count_df : DataFrame
        DataFrame with columns ['classifier', <score_col>] sorted desc by <score_col>.
        (Only 'classifier' and its order are used here.)
    clf_to_cols : Dict[str, List[str]]
        Mapping of classifier base names → their prediction columns in df.
    id_cols : Sequence[str]
        Columns that should always be kept (e.g., chr, pos, Ensembl_proteinid).
        Pass the *actual* column names present in df (resolved via find_column()).
    top_n : int, default 5
        Number of top classifiers to keep.

    Returns
    -------
    Tuple[DataFrame, List[str]]
        (filtered DataFrame, list of top classifier names)
    """
    # Extract ordered top-N classifier names
    top_classifiers = [r["classifier"] for r in clf_count_df.limit(top_n).collect()]

    # Build the list of columns to keep: IDs + columns of each top classifier
    keep_cols: List[str] = [c for c in id_cols if c]  # drop possible None
    for clf_name in top_classifiers:
        for col in clf_to_cols.get(clf_name, []):
            if col in df.columns:  # safety
                keep_cols.append(col)

    # Select only required columns
    df_top = df.select(*keep_cols)
    return df_top, top_classifiers

# ----------------------------- CLI / Main ------------------------------------

def _build_argparser() -> argparse.ArgumentParser:
    """Create CLI parser."""
    parser = argparse.ArgumentParser(description="Assignment 6 — dbNSFP Persistence")
    parser.add_argument("--data", default=DEFAULT_DATA, help="Path to dbNSFP gzipped TSV")
    parser.add_argument("--rows", type=int, default=2000, help="Row limit for testing (0 = full dataset)")
    parser.add_argument("--jdbc-jar", default=DEFAULT_JDBC,
                        help="Path to MariaDB JDBC JAR (e.g., ~/jars/mariadb-java-client-3.5.0.jar)")
    parser.add_argument("--db-name", default="", help="Target MariaDB database name")
    parser.add_argument("--db-user", default="", help="DB user (overrides .my.cnf)")
    parser.add_argument("--db-pass", default="", help="DB password (overrides .my.cnf)")
    parser.add_argument("--write", dest="dry_run", action="store_false", help="Write results to DB")
    parser.set_defaults(dry_run=True)
    return parser


def main() -> int:
    """Entry point for Assignment 6 pipeline (simplified, no logging)."""
    parser = _build_argparser()
    args = parser.parse_args()

    print(f"Starting Assignment 6 (dry_run={args.dry_run}) ...")

    # Spark session
    spark = (
        SparkSession.builder
        .appName(DEFAULT_APPNAME)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.jars", args.jdbc_jar)  # JDBC jar on classpath
        # NOTE: do NOT set .master() here; pass via spark-submit --master
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ---------------- Load dataset ----------------
    print(f"Loading dataset: {args.data}")
    df = spark.read.csv(args.data, sep="\t", header=True, inferSchema=False)
    if args.rows and args.rows > 0:
        df = df.limit(args.rows)
        print(f"TEST MODE: limited to {args.rows} rows")

    df.persist(StorageLevel.MEMORY_AND_DISK)
    n_rows = df.count()
    n_cols = len(df.columns)
    print(f"Loaded {n_rows:,} rows x {n_cols} cols")

    # Resolve ID columns actually present in df
    chr_col = find_column(df, ["#chr", "chr"])
    pos_col = find_column(df, ["pos(1-based)", "pos"])
    prot_col = find_column(df, ["Ensembl_proteinid", "proteinid", "protein_id"])
    if not chr_col or not pos_col:
        print("ERROR: Missing required chr/pos columns; aborting.")
        df.unpersist()
        spark.stop()
        return 1

    # ---------------- Q1: classifier counts ----------------
    # 1) Detect all classifier columns grouped by base tool name
    clf_map = discover_classifiers(df)
    clf_cols = [c for cols in clf_map.values() for c in cols]
    print(f"Detected {len(clf_map)} classifiers over {len(clf_cols)} columns")

    # 2) Count non-missing per column, then sum per classifier
    counts = count_non_missing(df, clf_cols)  # dict: col -> count

    q1_rows_py = [
        (base, sum(counts.get(c, 0) for c in cols))
        for base, cols in clf_map.items()
    ]
    q1_df = (
        spark.createDataFrame(q1_rows_py, ["classifier", "predictions"])
        .orderBy(F.desc("predictions"), F.asc("classifier"))
    )
    print("Top-10 classifiers by predictions:")
    q1_df.show(10, truncate=False)

    # ---------------- Keep Top-N via helper ----------------
    id_keep: List[str] = [chr_col, pos_col] + ([prot_col] if prot_col else [])
    df_top, topN = select_top_classifiers(
        df=df,
        clf_count_df=q1_df,        # only 'classifier' & ordering are used
        clf_to_cols=clf_map,
        id_cols=tuple(id_keep),
        top_n=5,
    )
    print("Top-5 classifiers:", ", ".join(topN))

    # Add merged genomic key (chr:pos)
    df_top = df_top.withColumn("chr_pos", F.concat_ws(":", F.col(chr_col), F.col(pos_col)))
    print("Created chr_pos key.")

    # ---------------- Per-row non-missing across kept predictions -------------
    # Identify the kept prediction columns (all non-ID, non-chr_pos)
    pred_cols = [c for c in df_top.columns if c not in {chr_col, pos_col, "chr_pos", prot_col}]
    df_top = df_top.withColumn(
        "non_missing_count",
        safe_sum([F.when(~is_missing(c), 1).otherwise(0) for c in pred_cols]),
    )

    # ---------------- Q4: position with most predictions ----------------------
    pos_summary = (
        df_top.groupBy("chr_pos")
        .agg(F.sum("non_missing_count").alias("total_preds"))
        .orderBy(F.desc("total_preds"))
    )
    best_pos = pos_summary.first()
    print(f"Max position: {best_pos['chr_pos']} ({int(best_pos['total_preds'])})")

    # ---------------- Q5: protein with most predictions -----------------------
    best_prot = None
    if prot_col:
        prot_summary = (
            df_top.groupBy(prot_col)
            .agg(F.sum("non_missing_count").alias("total_preds"))
            .orderBy(F.desc("total_preds"))
        )
        best_prot = prot_summary.first()
        print(f"Max protein: {best_prot[prot_col]} ({int(best_prot['total_preds'])})")
    else:
        print("Protein column not present; skipping Q5.")

    # ---------------- Normalize to 3NF ---------------------------------------
    print("Normalizing to 3NF ...")
    dim_position = (
        df_top.select(chr_col, pos_col, "chr_pos").distinct()
        .withColumn("position_id", F.monotonically_increasing_id())
    )

    dim_protein = None
    if prot_col:
        dim_protein = (
            df_top.select(prot_col).distinct()
            .withColumn("protein_id", F.monotonically_increasing_id())
        )

    dim_classifier = SparkSession.getActiveSession().createDataFrame(  # type: ignore
        [(i + 1, name) for i, name in enumerate(topN)],
        ["classifier_id", "classifier_name"],
    )

    # Base table with FK to positions (and proteins if present)
    base_df = df_top.join(dim_position, on="chr_pos", how="inner")
    if prot_col:
        base_df = base_df.join(dim_protein, on=prot_col, how="left")

    # Long-form predictions (one row per position_id x classifier column)
    long_parts: List[DataFrame] = []
    for colname in pred_cols:
        part = (
            base_df.select(
                "position_id",
                F.lit(colname).alias("column_name"),
                F.col(colname).alias("value"),
            )
            .filter(~is_missing(colname))
        )
        long_parts.append(part)

    if long_parts:
        predictions = long_parts[0]
        for part in long_parts[1:]:
            predictions = predictions.union(part)
        predictions = (
            predictions.withColumn(
                # Greedy group: capture full base (e.g., CADD_raw from CADD_raw_rankscore)
                "classifier_name",
                F.regexp_extract("column_name", r"^(.+)_(?:score|pred|rankscore|phred)$", 1),
            )
            .join(dim_classifier, on="classifier_name", how="inner")
            .select("position_id", "classifier_id", "column_name", "value")
        )
        pred_count = predictions.count()
    else:
        predictions = spark.createDataFrame(
            [], schema="position_id long, classifier_id int, column_name string, value string"
        )
        pred_count = 0

    print(f"Predictions-long rows: {pred_count:,}")

    # ---------------- Optional DB write --------------------------------------
    db_status = "Skipped (dry-run)"
    if not args.dry_run and args.db_name:
        # CLI creds override file creds if provided; else read ~/.my.cnf
        creds = {"user": args.db_user, "password": args.db_pass}
        file_creds = read_mycnf(os.path.expanduser("~/.my.cnf"))
        for key, val in file_creds.items():
            creds.setdefault(key, val)

        url = f"jdbc:mariadb://mariadb.bin.bioinf.nl:3306/{args.db_name}"
        props = {"driver": "org.mariadb.jdbc.Driver", **creds}

        print("Writing tables to MariaDB ...")
        dim_position.write.jdbc(url=url, table="a6_positions", mode="overwrite", properties=props)
        if dim_protein is not None:
            dim_protein.write.jdbc(url=url, table="a6_proteins", mode="overwrite", properties=props)
        dim_classifier.write.jdbc(url=url, table="a6_classifiers", mode="overwrite", properties=props)
        predictions.write.jdbc(url=url, table="a6_predictions", mode="overwrite", properties=props)
        db_status = "Done"
        print("Write complete.")

    # ---------------- Report --------------------------------------------------
    q1_rows = [(i + 1, r["classifier"], int(r["predictions"])) for i, r in enumerate(q1_df.collect())]
    report = (
        f"# Assignment 6 — Results\n\n"
        f"**Rows analyzed:** {n_rows:,}  \n"
        f"**Columns analyzed:** {n_cols}\n\n"
        f"## Q1 — Predictions per Classifier\n"
        f"{md_table(['Rank', 'Classifier', 'Predictions'], q1_rows)}\n\n"
        f"**Top-5 kept:** {', '.join(topN)}\n\n"
        f"## Q3 — Merged genomic identifier\n"
        f"`chr_pos = {chr_col}:{pos_col}`  (e.g., `1:69037`)\n\n"
        f"## Q4 — Position with Most Predictions\n"
        f"- {best_pos['chr_pos']} ({int(best_pos['total_preds'])} predictions)\n\n"
        f"## Q5 — Protein with Most Predictions\n"
        f"- {(best_prot[prot_col] if best_prot else 'N/A')} "
        f"({(int(best_prot['total_preds']) if best_prot else 'N/A')} predictions)\n\n"
        f"## Normalization (3NF)\n"
        f"- **a6_positions:** {dim_position.count():,} unique positions  \n"
        f"- **a6_proteins:** {dim_protein.count() if dim_protein is not None else 0:,} unique proteins  \n"
        f"- **a6_classifiers:** {dim_classifier.count():,} classifiers  \n"
        f"- **a6_predictions:** {pred_count:,} rows  \n"
        f"- **Database write:** {db_status}\n"
    )

    with open(REPORT_FILE, "w", encoding="utf-8") as f_out:
        f_out.write(report)
    print(f"Saved report: {REPORT_FILE}")

    # ---------------- Cleanup -------------------------------------------------
    df.unpersist()
    spark.stop()
    print("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
