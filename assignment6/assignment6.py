#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Assignment 6 — dbNSFP Persistence (Fixed for literal compatibility)
"""

from __future__ import annotations

import argparse
import configparser
import os
import sys
from functools import reduce
from operator import add
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

# Spark imports
sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark import StorageLevel

# ----------------------------- Constants -------------------------------------

PRED_SUFFIXES: Tuple[str, ...] = ("_score", "_pred", "_rankscore", "_phred")
MISSING_MARKERS: Tuple[str, ...] = (".", "", "NA", "NaN", "nan", "null", "NULL")

DEFAULT_DATA = "/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz"
DEFAULT_JDBC = os.path.expanduser("~/jars/mariadb-java-client-3.5.0.jar")
DEFAULT_APPNAME = "assignment6_dbnsfp_final"
REPORT_FILE = "assignment6.md"

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
    except Exception as exc:
        print(f"Warning: Could not read {path}: {exc}")
    return creds


def is_missing(colname: str):
    """
    Build a Spark Column expressing 'value in column colname is missing'.
    Fixed to avoid literal array issues.
    """
    col = F.col(colname)
    # Build OR chain instead of using isin() with a list
    condition = col.isNull()
    for marker in MISSING_MARKERS:
        condition = condition | (col == marker)
    return condition


def safe_sum(cols: Sequence) -> F.Column:
    """Sum a list of Spark Columns safely (works even if the list is empty)."""
    return reduce(add, cols) if cols else F.lit(0)


def discover_classifiers(df: DataFrame) -> Dict[str, List[str]]:
    """Map: classifier base → list of its columns (detected by known suffixes)."""
    clf_map: Dict[str, List[str]] = {}
    for col in df.columns:
        for suf in PRED_SUFFIXES:
            if col.endswith(suf):
                base = col[: -len(suf)].rstrip("_")
                if base not in clf_map:
                    clf_map[base] = []
                clf_map[base].append(col)
                break
    return clf_map


def count_non_missing(df: DataFrame, columns: Iterable[str]) -> Dict[str, int]:
    """
    Count non-missing values per column.
    Fixed to avoid literal array issues.
    """
    aggs = []
    for c in columns:
        # Build condition without isin()
        condition = ~F.col(c).isNull()
        for marker in MISSING_MARKERS:
            condition = condition & (F.col(c) != marker)
        
        aggs.append(
            F.sum(F.when(condition, 1).otherwise(0)).alias(c)
        )
    
    row = df.agg(*aggs).collect()[0].asDict()
    return {k: int(v) for k, v in row.items()}


def md_table(headers: Sequence[str], rows: Sequence[Sequence[object]]) -> str:
    """Build a simple Markdown table string given headers and rows."""
    head = "|" + "|".join(headers) + "|\n|" + "|".join(["---"] * len(headers)) + "|\n"
    body = "".join("|" + "|".join(str(x) for x in r) + "|\n" for r in rows)
    return head + body


def find_column(df: DataFrame, candidates: Sequence[str]) -> Optional[str]:
    """Return the first column name from 'candidates' that exists in df.columns."""
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
    """Select Top-N classifier columns plus ID columns from the main DataFrame."""
    top_classifiers = [r["classifier"] for r in clf_count_df.limit(top_n).collect()]
    keep_cols: List[str] = [c for c in id_cols if c]
    for clf_name in top_classifiers:
        for col in clf_to_cols.get(clf_name, []):
            if col in df.columns:
                keep_cols.append(col)
    df_top = df.select(*keep_cols)
    return df_top, top_classifiers

# ----------------------------- CLI / Main ------------------------------------

def _build_argparser() -> argparse.ArgumentParser:
    """Create CLI parser."""
    parser = argparse.ArgumentParser(description="Assignment 6 — dbNSFP Persistence")
    parser.add_argument("--data", default=DEFAULT_DATA, help="Path to dbNSFP gzipped TSV")
    parser.add_argument("--rows", type=int, default=2000, help="Row limit for testing (0 = full dataset)")
    parser.add_argument("--jdbc-jar", default=DEFAULT_JDBC, help="Path to MariaDB JDBC JAR")
    parser.add_argument("--db-name", default="", help="Target MariaDB database name")
    parser.add_argument("--db-user", default="", help="DB user (overrides .my.cnf)")
    parser.add_argument("--db-pass", default="", help="DB password (overrides .my.cnf)")
    parser.add_argument("--write", dest="dry_run", action="store_false", help="Write results to DB")
    parser.set_defaults(dry_run=True)
    return parser


def main() -> int:
    """Entry point for Assignment 6 pipeline."""
    parser = _build_argparser()
    args = parser.parse_args()

    print(f"Starting Assignment 6 (dry_run={args.dry_run}) ...")

    # ============================================================================
    # 1) SPARK SESSION SETUP
    # ============================================================================
    spark = (
        SparkSession.builder
        .appName(DEFAULT_APPNAME)
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.jars", args.jdbc_jar)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # ============================================================================
    # 2) LOAD DATASET
    # ============================================================================
    print(f"Loading dataset: {args.data}")
    df = spark.read.csv(args.data, sep="\t", header=True, inferSchema=False)
    if args.rows and args.rows > 0:
        df = df.limit(args.rows)
        print(f"TEST MODE: limited to {args.rows} rows")

    df.persist(StorageLevel.MEMORY_AND_DISK)
    n_rows = df.count()
    n_cols = len(df.columns)
    print(f"Loaded {n_rows:,} rows x {n_cols} cols")

    # Resolve ID columns
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

    if not chr_col or not pos_col:
        print("ERROR: Missing required chr/pos columns; aborting.")
        df.unpersist()
        spark.stop()
        return 1

    # ============================================================================
    # 3) Q1: CLASSIFIER COUNTS
    # ============================================================================
    clf_map = discover_classifiers(df)
    clf_cols = [c for cols in clf_map.values() for c in cols]
    print(f"Detected {len(clf_map)} classifiers over {len(clf_cols)} columns")

    counts = count_non_missing(df, clf_cols)
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

    # ============================================================================
    # 4) KEEP TOP-N CLASSIFIERS
    # ============================================================================
    id_keep: List[str] = [chr_col, pos_col]
    if ref_col:
        id_keep.append(ref_col)
    if alt_col:
        id_keep.append(alt_col)
    if prot_col:
        id_keep.append(prot_col)
    
    df_top, topN = select_top_classifiers(
        df=df,
        clf_count_df=q1_df,
        clf_to_cols=clf_map,
        id_cols=tuple(id_keep),
        top_n=5,
    )
    print("Top-5 classifiers:", ", ".join(topN))

    # Add merged genomic key (chr:pos)
    df_top = df_top.withColumn("chr_pos", F.concat_ws(":", F.col(chr_col), F.col(pos_col)))
    print("Created chr_pos key.")

    # ============================================================================
    # 5) PER-ROW NON-MISSING COUNT
    # ============================================================================
    pred_cols = [c for c in df_top.columns if c not in {chr_col, pos_col, ref_col, alt_col, "chr_pos", prot_col}]
    df_top = df_top.withColumn(
        "non_missing_count",
        safe_sum([F.when(~is_missing(c), 1).otherwise(0) for c in pred_cols]),
    )

    # ============================================================================
    # 6) Q4: POSITION WITH MOST PREDICTIONS
    # ============================================================================
    pos_summary = (
        df_top.groupBy("chr_pos")
        .agg(F.sum("non_missing_count").alias("total_preds"))
        .orderBy(F.desc("total_preds"))
    )
    best_pos = pos_summary.first()
    print(f"Max position: {best_pos['chr_pos']} ({int(best_pos['total_preds'])})")

    # ============================================================================
    # 7) Q5: PROTEIN WITH MOST PREDICTIONS
    # ============================================================================
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

    # ============================================================================
    # 8) NORMALIZE TO 3NF: BUILD DIMENSION TABLES
    # ============================================================================
    
    # -------- a6_positions --------
    df_positions = df.select(
        F.col(chr_col).alias("chr"),
        F.col(pos_col).cast("int").alias("pos_1based"),
        F.col(ref_col).alias("ref") if ref_col else F.lit(None).alias("ref"),
        F.col(alt_col).alias("alt") if alt_col else F.lit(None).alias("alt"),
        F.col(rs_dbsnp_col).alias("rs_dbSNP") if rs_dbsnp_col else F.lit(None).alias("rs_dbSNP")
    )
    
    # Clean and filter to SNVs only
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
    
    dim_position_count = df_positions.count()
    print(f"a6_positions: {dim_position_count:,} unique positions")

    # -------- a6_proteins --------
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
        
        # Build filter without isin()
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

    # -------- a6_classifiers --------
    dim_classifier = spark.createDataFrame(
        [(name,) for name in clf_map.keys()],
        ["classifier_name"]
    )
    dim_classifier_count = dim_classifier.count()
    print(f"a6_classifiers: {dim_classifier_count} classifiers")

    # ============================================================================
    # 9) BUILD PREDICTIONS (LONG FORMAT)
    # ============================================================================
    all_pred_cols = [c for c in df.columns if c.endswith(("_score", "_pred", "_rankscore", "_phred"))]
    
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
                F.col(colname).cast("string").alias("value_raw")
            )
            .filter(~is_missing(colname))
        )
        long_parts.append(part)

    if not long_parts:
        predictions_long = spark.createDataFrame(
            [], schema="chr string, pos_1based int, ref string, alt string, ensembl_proteinid string, column_name string, value_raw string"
        )
    else:
        predictions_long = long_parts[0]
        for p in long_parts[1:]:
            predictions_long = predictions_long.unionByName(p)

    # Extract classifier_name
    predictions_long = predictions_long.withColumn(
        "classifier_name",
        F.regexp_extract(F.col("column_name"), r"^(.+?)_(?:score|pred|rankscore|phred)$", 1)
    )

    # Clean to SNVs
    predictions_long = (
        predictions_long
        .withColumn("ref", F.upper(F.trim(F.col("ref"))))
        .withColumn("alt", F.upper(F.trim(F.col("alt"))))
        .filter(F.length("ref") == 1)
        .filter(F.length("alt") == 1)
    )

    pred_count = predictions_long.count()
    print(f"a6_predictions (before FK join): {pred_count:,} rows")

    # ============================================================================
    # 10) WRITE TO MARIADB (IF NOT DRY RUN)
    # ============================================================================
    db_status = "DRY RUN — not written"
    
    if not args.dry_run:
        if not args.db_name:
            print("ERROR: --db-name is required for writing to database")
            df.unpersist()
            spark.stop()
            return 1

        # Get credentials
        creds = {"user": args.db_user, "password": args.db_pass}
        file_creds = read_mycnf(os.path.expanduser("~/.my.cnf"))
        for key, val in file_creds.items():
            creds.setdefault(key, val)

        url = f"jdbc:mariadb://mariadb.bin.bioinf.nl:3306/{args.db_name}"
        
        print("\n" + "="*60)
        print("WRITING TO MARIADB")
        print("="*60)

        # Write a6_positions
        print("Writing a6_positions ...")
        (
            df_positions
            .write
            .format("jdbc")
            .option("url", url)
            .option("driver", "org.mariadb.jdbc.Driver")
            .option("dbtable", "a6_positions")
            .option("user", creds["user"])
            .option("password", creds["password"])
            .mode("append")
            .save()
        )
        print("✅ a6_positions written")

        # Write a6_proteins
        if dim_protein is not None:
            print("Writing a6_proteins ...")
            (
                dim_protein
                .write
                .format("jdbc")
                .option("url", url)
                .option("driver", "org.mariadb.jdbc.Driver")
                .option("dbtable", "a6_proteins")
                .option("user", creds["user"])
                .option("password", creds["password"])
                .mode("append")
                .save()
            )
            print("✅ a6_proteins written")

        # Read back a6_positions with position_id for FK join
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

        # Join predictions with position_id
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
                how="inner"
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

        pred_final_count = predictions_with_fk.count()
        print(f"Predictions with FK: {pred_final_count:,} rows")

        # Write a6_predictions
        print("Writing a6_predictions ...")
        (
            predictions_with_fk
            .write
            .format("jdbc")
            .option("url", url)
            .option("driver", "org.mariadb.jdbc.Driver")
            .option("dbtable", "a6_predictions")
            .option("user", creds["user"])
            .option("password", creds["password"])
            .mode("append")
            .save()
        )
        print("✅ a6_predictions written")

        db_status = f"Written to {args.db_name}"

    # ============================================================================
    # 11) GENERATE REPORT
    # ============================================================================
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
        f"- **a6_positions:** {dim_position_count:,} unique positions  \n"
        f"- **a6_proteins:** {dim_protein_count:,} unique proteins  \n"
        f"- **a6_classifiers:** {dim_classifier_count} classifiers  \n"
        f"- **a6_predictions:** {pred_count:,} rows (before FK join)  \n"
        f"- **Database write:** {db_status}\n"
    )

    with open(REPORT_FILE, "w", encoding="utf-8") as f_out:
        f_out.write(report)
    print(f"\n✅ Saved report: {REPORT_FILE}")

    # ============================================================================
    # 12) CLEANUP
    # ============================================================================
    df.unpersist()
    spark.stop()
    print("\n✅ Done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())