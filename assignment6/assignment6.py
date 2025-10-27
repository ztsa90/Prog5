#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 6 — dbNSFP Persistence (Final)

Pipeline:
1) Read gzipped dbNSFP TSV
2) Detect classifier columns by suffixes
3) Q1: count predictions per classifier (non-missing)
4) Keep Top-5 classifiers; drop the rest
5) Create chr_pos key
6) Q4/Q5: most-predicted position/protein
7) Normalize to 3NF (positions, proteins, classifiers, predictions-long)
8) Optional: write to MariaDB via JDBC
9) Save assignment6.md report

Run (test, dry-run):
spark-submit --master spark://spark.bin.bioinf.nl:7077 \
  --jars ~/jars/mariadb-java-client-3.5.0.jar \
  assignment6.py --rows 2000 --jdbc-jar ~/jars/mariadb-java-client-3.5.0.jar

Run (full + write):
spark-submit --master spark://spark.bin.bioinf.nl:7077 \
  --jars ~/jars/mariadb-java-client-3.5.0.jar \
  assignment6.py --write --db-name YOUR_DB \
  --jdbc-jar ~/jars/mariadb-java-client-3.5.0.jar
"""

# pylint: disable=too-many-locals,too-many-statements,too-many-branches

from __future__ import annotations

import argparse
import configparser
import logging
import os
import sys
from collections import defaultdict
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

PRED_SUFFIXES: Tuple[str, ...] = ("_score", "_pred", "_rankscore", "_phred")
MISSING_MARKERS: Tuple[str, ...] = (".", "", "NA", "NaN", "nan", "null", "NULL")

DEFAULT_DATA = "/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz"
DEFAULT_JDBC = os.path.expanduser("~/jars/mariadb-java-client-3.5.0.jar")
DEFAULT_APPNAME = "assignment6_dbnsfp_final"
REPORT_FILE = "assignment6.md"


# ----------------------------- Logging ---------------------------------------

def _setup_logging() -> None:
    """Configure root logger."""
    fmt = "%(asctime)s | %(levelname)s | %(message)s"
    logging.basicConfig(level=logging.INFO, format=fmt)


# ----------------------------- Helpers ---------------------------------------

def read_mycnf(path: str) -> Dict[str, str]:
    """Read MariaDB credentials from ~/.my.cnf if present."""
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
        logging.warning("Could not read %s: %s", path, exc)
    return creds


def is_missing(colname: str):
    """Spark Column expression for missing values."""
    return F.col(colname).isNull() | F.col(colname).isin(list(MISSING_MARKERS))


def safe_sum(cols: Sequence):  # Spark Columns
    """Sum a list of Spark Columns safely (works for empty)."""
    return reduce(add, cols) if cols else F.lit(0)


def discover_classifiers(df: DataFrame) -> Dict[str, List[str]]:
    """
    Group columns by classifier base using known suffixes.

    Examples
    --------
    MetaSVM_score         -> base 'MetaSVM'
    CADD_raw_rankscore    -> base 'CADD_raw'
    """
    mapping: Dict[str, List[str]] = defaultdict(list)
    for col in df.columns:
        for suf in PRED_SUFFIXES:
            if col.endswith(suf):
                base = col[: col.rfind(suf)].rstrip("_")
                mapping[base].append(col)
                break
    return mapping


def count_non_missing(df: DataFrame, columns: Iterable[str]) -> Dict[str, int]:
    """Count non-missing values per column, return dict[col] -> count."""
    aggs = [F.sum(F.when(~is_missing(c), 1).otherwise(0)).alias(c) for c in columns]
    row = df.agg(*aggs).collect()[0].asDict()
    # cast Any to int safely
    return {k: int(v) for k, v in row.items()}


def find_column(df: DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """Return first matching column from candidates, else None."""
    for name in candidates:
        if name in df.columns:
            return name
    return None


def md_table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """Build a simple Markdown table."""
    head = "|" + "|".join(headers) + "|\n|" + "|".join(["---"] * len(headers)) + "|\n"
    body = "".join("|" + "|".join(str(x) for x in r) + "|\n" for r in rows)
    return head + body


# ----------------------------- CLI / Main ------------------------------------

def _build_argparser() -> argparse.ArgumentParser:
    """Create CLI parser."""
    parser = argparse.ArgumentParser(description="Assignment 6 — dbNSFP Persistence")
    parser.add_argument(
        "--data", default=DEFAULT_DATA, help="Path to dbNSFP gzipped TSV"
    )
    parser.add_argument(
        "--rows", type=int, default=2000, help="Row limit for testing (0 = full dataset)"
    )
    parser.add_argument(
        "--jdbc-jar",
        default=DEFAULT_JDBC,
        help="Path to MariaDB JDBC JAR (e.g., ~/jars/mariadb-java-client-3.5.0.jar)",
    )
    parser.add_argument("--db-name", default="", help="Target MariaDB database name")
    parser.add_argument("--db-user", default="", help="DB user (overrides .my.cnf)")
    parser.add_argument("--db-pass", default="", help="DB password (overrides .my.cnf)")
    # dry-run default True; use --write to actually write
    parser.add_argument(
        "--write", dest="dry_run", action="store_false", help="Write results to DB"
    )
    parser.set_defaults(dry_run=True)
    return parser


def main() -> int:
    """Entry point for Assignment 6 pipeline."""
    _setup_logging()
    parser = _build_argparser()
    args = parser.parse_args()

    logging.info("Starting Assignment 6 (dry_run=%s) ...", args.dry_run)

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
    logging.info("Loading dataset: %s", args.data)
    df = spark.read.csv(args.data, sep="\t", header=True, inferSchema=False)
    if args.rows and args.rows > 0:
        df = df.limit(args.rows)
        logging.info("TEST MODE: limited to %d rows", args.rows)

    df.persist(StorageLevel.MEMORY_AND_DISK)
    n_rows = df.count()
    n_cols = len(df.columns)
    logging.info("Loaded %s rows x %d cols", f"{n_rows:,}", n_cols)

    # ---------------- Q1: classifiers ----------------
    clf_map = discover_classifiers(df)
    clf_cols = [c for cols in clf_map.values() for c in cols]
    logging.info("Detected %d classifiers over %d columns", len(clf_map), len(clf_cols))

    counts = count_non_missing(df, clf_cols)
    q1_rows_py = [(base, sum(counts.get(c, 0) for c in cols))
                  for base, cols in clf_map.items()]
    q1_df = (
        spark.createDataFrame(q1_rows_py, ["classifier", "predictions"])
        .orderBy(F.desc("predictions"), F.asc("classifier"))
    )
    logging.info("Top-10 classifiers by predictions:")
    q1_df.show(10, truncate=False)

    top5 = [r["classifier"] for r in q1_df.limit(5).collect()]
    logging.info("Top-5: %s", ", ".join(top5))

    # ---------------- required columns ----------------
    chr_col = find_column(df, ["#chr", "chr"])
    pos_col = find_column(df, ["pos(1-based)", "pos"])
    prot_col = find_column(df, ["Ensembl_proteinid", "proteinid", "protein_id"])
    if not chr_col or not pos_col:
        logging.error("Missing required chr/pos columns; aborting.")
        df.unpersist()
        spark.stop()
        return 1

    keep_cols: List[str] = [chr_col, pos_col] + ([prot_col] if prot_col else [])
    for base in top5:
        keep_cols.extend(clf_map[base])

    df_top = df.select(*keep_cols).withColumn(
        "chr_pos", F.concat_ws(":", F.col(chr_col), F.col(pos_col))
    )
    logging.info("Created chr_pos key.")

    # ---------------- per-row non-missing across predictions ----------------
    pred_cols = [c for c in df_top.columns if c not in {chr_col, pos_col, "chr_pos", prot_col}]
    df_top = df_top.withColumn(
        "non_missing_count",
        safe_sum([F.when(~is_missing(c), 1).otherwise(0) for c in pred_cols]),
    )

    # ---------------- Q4: position with most predictions ----------------
    pos_summary = (
        df_top.groupBy("chr_pos")
        .agg(F.sum("non_missing_count").alias("total_preds"))
        .orderBy(F.desc("total_preds"))
    )
    best_pos = pos_summary.first()
    logging.info("Max position: %s (%d)", best_pos["chr_pos"], int(best_pos["total_preds"]))

    # ---------------- Q5: protein with most predictions ----------------
    best_prot = None
    if prot_col:
        prot_summary = (
            df_top.groupBy(prot_col)
            .agg(F.sum("non_missing_count").alias("total_preds"))
            .orderBy(F.desc("total_preds"))
        )
        best_prot = prot_summary.first()
        logging.info(
            "Max protein: %s (%d)",
            best_prot[prot_col],
            int(best_prot["total_preds"]),
        )
    else:
        logging.info("Protein column not present; skipping Q5.")

    # ---------------- Normalize to 3NF ----------------
    logging.info("Normalizing to 3NF ...")
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

    dim_classifier = spark.createDataFrame(
        [(i + 1, name) for i, name in enumerate(top5)],
        ["classifier_id", "classifier_name"],
    )

    base_df = df_top.join(dim_position, on="chr_pos", how="inner")
    if prot_col:
        base_df = base_df.join(dim_protein, on=prot_col, how="left")

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
                "classifier_name",
                F.regexp_extract("column_name", r"^(.+?)_(?:score|pred|rankscore|phred)$", 1),
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

    logging.info("Predictions-long rows: %s", f"{pred_count:,}")

    # ---------------- Optional DB write ----------------
    db_status = "Skipped (dry-run)"
    if not args.dry_run and args.db_name:
        # CLI creds override file creds if provided
        creds = {"user": args.db_user, "password": args.db_pass}
        file_creds = read_mycnf(os.path.expanduser("~/.my.cnf"))
        for key, val in file_creds.items():
            creds.setdefault(key, val)

        url = f"jdbc:mariadb://mariadb.bin.bioinf.nl:3306/{args.db_name}"
        props = {"driver": "org.mariadb.jdbc.Driver", **creds}

        logging.info("Writing tables to MariaDB ...")
        dim_position.write.jdbc(url=url, table="a6_positions", mode="overwrite", properties=props)
        if dim_protein is not None:
            dim_protein.write.jdbc(url=url, table="a6_proteins", mode="overwrite", properties=props)
        dim_classifier.write.jdbc(url=url, table="a6_classifiers", mode="overwrite", properties=props)
        predictions.write.jdbc(url=url, table="a6_predictions", mode="overwrite", properties=props)
        db_status = "Done"
        logging.info("Write complete.")

    # ---------------- Report ----------------
    q1_rows = [(i + 1, r["classifier"], int(r["predictions"])) for i, r in enumerate(q1_df.collect())]
    report = (
        f"# Assignment 6 — Results\n\n"
        f"**Rows analyzed:** {n_rows:,}  \n"
        f"**Columns analyzed:** {n_cols}\n\n"
        f"## Q1 — Predictions per Classifier\n"
        f"{md_table(['Rank', 'Classifier', 'Predictions'], q1_rows)}\n\n"
        f"**Top-5 kept:** {', '.join(top5)}\n\n"
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
    logging.info("Saved report: %s", REPORT_FILE)

    # ---------------- Cleanup ----------------
    df.unpersist()
    spark.stop()
    logging.info("Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
