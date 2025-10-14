#!/usr/bin/env python3
# pylint: disable=invalid-name, line-too-long, too-many-locals, too-many-branches, too-many-statements
"""
Assignment 5: PySpark DataFrame Analysis of GenBank Archaea Data

This script parses a GenBank GBFF file and analyzes genomic features using PySpark.
It answers 6 questions about feature counts, ratios, and lengths.

Usage: python assignment5.py /path/to/archaea.genomic.gbff
"""

from __future__ import annotations

import os
import sys
from typing import Dict, List, Optional

# Set up Spark paths (required for BIN cluster environment)
sys.path.append("/opt/spark/python")
sys.path.append("/opt/spark/python/lib/py4j-0.10.9.7-src.zip")

# Import Spark libraries
from pyspark.sql import SparkSession  # type: ignore
from pyspark.sql import functions as F  # type: ignore
from pyspark.sql import types as T  # type: ignore

# Import Biopython for parsing GenBank files
from Bio import SeqIO  # type: ignore
from Bio.SeqFeature import (  # type: ignore
    CompoundLocation,
    BeforePosition,
    AfterPosition,
    UnknownPosition,
)

# ============================================================================
# SPARK SESSION SETUP
# ============================================================================


def create_spark_session() -> SparkSession:
    """
    Create and return a Spark session in local mode.

    Uses 16 cores and 128GB memory. Make sure your SLURM script
    requests matching resources!
    """
    spark = (
        SparkSession.builder.appName("assignment5_yourname")
        .master("local[16]")  # Use 16 CPU cores
        .config("spark.executor.memory", "128g")
        .config("spark.driver.memory", "128g")
        .getOrCreate()
    )
    return spark


# ============================================================================
# FEATURE TYPE DEFINITIONS
# ============================================================================

# Coding features: CDS (genes), mat_peptide, propeptide
CODING_FEATURES = {"CDS", "mat_peptide", "propeptide"}

# RNA features: ribosomal RNA and non-coding RNA
RNA_FEATURES = {"rRNA", "ncRNA"}

# Gene features (used to find cryptic genes)
GENE_FEATURE = "gene"


# ============================================================================
# LOCATION PARSING HELPERS
# ============================================================================


def has_ambiguous_location(location) -> bool:
    """
    Check if a feature location is ambiguous (has < or > or ? symbols).

    Returns:
        True if location is fuzzy/ambiguous, False otherwise.
    """
    parts = location.parts if isinstance(location, CompoundLocation) else [location]

    for part in parts:
        if isinstance(part.start, (BeforePosition, AfterPosition, UnknownPosition)):
            return True
        if isinstance(part.end, (BeforePosition, AfterPosition, UnknownPosition)):
            return True
    return False


def calculate_feature_length(location) -> Optional[int]:
    """
    Calculate the total length of a feature in base pairs.
    Returns None if the location is ambiguous.

    For joined locations, sums the lengths of all parts.
    NOTE: This keeps your original logic (Biopython end-exclusive),
    i.e., sum(int(end) - int(start)) with no +1.
    """
    if has_ambiguous_location(location):
        return None

    parts = location.parts if isinstance(location, CompoundLocation) else [location]

    total_length = 0
    for part in parts:
        total_length += int(part.end) - int(part.start)

    return total_length


# ============================================================================
# GENBANK FILE PARSING
# ============================================================================


def parse_genbank_record(record) -> List[Dict[str, object]]:
    """
    Parse one GenBank record and extract relevant features.

    Returns a list of dictionaries, one per feature.
    Each dictionary contains: accession, species, feature info, location, etc.
    """
    rows: List[Dict[str, object]] = []
    accession: str = record.id
    species = record.annotations.get("organism")

    # STEP 1: Find all locus_tags that belong to coding features
    # This helps us identify "cryptic genes" (genes without coding features)
    coding_locus_tags = set()
    for feature in record.features:
        if feature.type in CODING_FEATURES:
            locus_tag = feature.qualifiers.get("locus_tag", [None])[0]
            if locus_tag:
                coding_locus_tags.add(locus_tag)

    # Helper function to add a feature row
    def add_feature_row(feature, is_coding: bool, is_rna: bool) -> None:
        """Add one feature to our rows list."""
        length = calculate_feature_length(feature.location)
        if length is None:
            return  # Skip ambiguous features

        rows.append(
            {
                "accession": accession,
                "species": species,
                "feature_type": feature.type,
                "locus_tag": feature.qualifiers.get("locus_tag", [None])[0],
                "gene": feature.qualifiers.get("gene", [None])[0],
                "product": feature.qualifiers.get("product", [None])[0],
                "start": int(feature.location.start),
                "end": int(feature.location.end),
                "strand": feature.location.strand,  # 1 = forward, -1 = reverse
                "length": length,
                "is_coding": is_coding,
                "is_rna": is_rna,
            }
        )

    # STEP 2: Process coding features (CDS, peptides)
    for feature in record.features:
        if feature.type in CODING_FEATURES:
            add_feature_row(feature, is_coding=True, is_rna=False)

    # STEP 3: Process RNA features (rRNA, ncRNA)
    for feature in record.features:
        if feature.type in RNA_FEATURES:
            add_feature_row(feature, is_coding=False, is_rna=True)

    # STEP 4: Process cryptic genes
    # These are "gene" features whose locus_tag is NOT in any coding feature
    for feature in record.features:
        if feature.type == GENE_FEATURE:
            locus_tag = feature.qualifiers.get("locus_tag", [None])[0]
            if locus_tag and locus_tag not in coding_locus_tags:
                add_feature_row(feature, is_coding=False, is_rna=False)

    return rows


def parse_genbank_file(gbff_path: str) -> List[Dict[str, object]]:
    """
    Parse entire GenBank file and return all feature rows.

    Args:
        gbff_path: Path to the GBFF file.

    Returns:
        List of dictionaries, one per feature.
    """
    all_rows: List[Dict[str, object]] = []

    for record in SeqIO.parse(gbff_path, "genbank"):
        rows = parse_genbank_record(record)
        all_rows.extend(rows)

    return all_rows


# ============================================================================
# DATAFRAME SCHEMA
# ============================================================================

FEATURE_SCHEMA = T.StructType(
    [
        T.StructField("accession", T.StringType(), False),  # Genome accession ID
        T.StructField("species", T.StringType(), True),  # Organism name
        T.StructField("feature_type", T.StringType(), False),  # CDS, rRNA, etc.
        T.StructField("locus_tag", T.StringType(), True),  # Gene identifier
        T.StructField("gene", T.StringType(), True),  # Gene name
        T.StructField("product", T.StringType(), True),  # Protein/RNA product
        T.StructField("start", T.IntegerType(), True),  # Start position
        T.StructField("end", T.IntegerType(), True),  # End position
        T.StructField("strand", T.IntegerType(), True),  # DNA strand (1 or -1)
        T.StructField("length", T.IntegerType(), True),  # Feature length in bp
        T.StructField("is_coding", T.BooleanType(), False),  # Is it a coding feature?
        T.StructField("is_rna", T.BooleanType(), False),  # Is it an RNA feature?
    ]
)


# ============================================================================
# ANALYSIS FUNCTIONS (Q1-Q6)
# ============================================================================


def answer_question_1(df) -> None:
    """
    Q1: How many features does an pylial genome have on average?
    (Kept exactly as in your original: groups by 'accession'.)
    """
    features_per_genome = df.groupBy("accession").count()
    avg_features = features_per_genome.select(F.avg("count")).first()[0]
    print(f"Q1) Average features per genome: {avg_features:.2f}")


def answer_question_2(df) -> None:
    """
    Q2: What is the ratio between coding and non-coding features?
    """
    coding_count = df.filter(F.col("is_coding") == True).count()  # pylint: disable=singleton-comparison
    noncoding_count = df.filter(F.col("is_coding") == False).count()  # pylint: disable=singleton-comparison

    ratio = (coding_count / noncoding_count) if noncoding_count > 0 else float("inf")
    print(
        f"Q2) Coding vs Non-coding ratio: {coding_count}:{noncoding_count} "
        f"(ratio ~{ratio:.2f})"
    )


def answer_question_3(df) -> None:
    """
    Q3: What is the minimal and maximal number of proteins in a genome?
    (Proteins = CDS features only)
    """
    cds_per_genome = (
        df.filter(F.col("feature_type") == "CDS")
        .groupBy("accession")
        .agg(F.count("*").alias("protein_count"))
    )

    min_row = cds_per_genome.orderBy(F.col("protein_count").asc()).first()
    min_count = min_row["protein_count"] if min_row else 0
    min_accession = min_row["accession"] if min_row else None

    max_row = cds_per_genome.orderBy(F.col("protein_count").desc()).first()
    max_count = max_row["protein_count"] if max_row else 0
    max_accession = max_row["accession"] if max_row else None

    print(
        "Q3) Proteins per genome — min: "
        f"{min_count} ({min_accession}), max: {max_count} ({max_accession})"
    )


def answer_question_4(df) -> None:
    """
    Q4: What is the average length of a feature?
    """
    avg_length = df.select(F.avg("length")).first()[0]
    if avg_length is not None:
        print(f"Q4) Average feature length: {avg_length:.2f} bp")
    else:
        print("Q4) Average feature length: N/A")


def answer_questions_5_and_6(df) -> None:
    """
    Q5: Remove all non-coding RNA features and save cleaned DataFrame.
    Q6: What is the average length in the cleaned version?
    """
    cleaned_df = df.filter(F.col("is_rna") == False)  # pylint: disable=singleton-comparison
    output_path = "features_cleaned.parquet"
    cleaned_df.write.mode("overwrite").parquet(output_path)
    print(f"Q5) Cleaned DataFrame saved to: {output_path}")

    avg_length_cleaned = cleaned_df.select(F.avg("length")).first()[0]
    if avg_length_cleaned is not None:
        print(f"Q6) Average feature length after cleaning: {avg_length_cleaned:.2f} bp")
    else:
        print("Q6) Average feature length after cleaning: N/A")


# ============================================================================
# MAIN PROGRAM
# ============================================================================


def main() -> None:
    """Main program: parse GBFF file and answer all questions."""
    if len(sys.argv) != 2:
        print("Usage: python assignment5.py /path/to/archaea.genomic.gbff")
        sys.exit(1)

    gbff_path = sys.argv[1]

    if not os.path.exists(gbff_path):
        print(f"ERROR: File not found: {gbff_path}")
        sys.exit(1)

    print("Starting Spark session...")
    spark = create_spark_session()
    print("✅ Spark started")

    print(f"Parsing GenBank file: {gbff_path}")
    feature_rows = parse_genbank_file(gbff_path)

    df = spark.createDataFrame(feature_rows, schema=FEATURE_SCHEMA)
    df.cache()  # Cache for faster repeated queries

    total_rows = df.count()
    print(f"✅ Loaded {total_rows} features")

    print("\nSample data:")
    df.show(10, truncate=False)

    print("\n" + "=" * 60)
    print("ANALYSIS RESULTS")
    print("=" * 60)
    answer_question_1(df)
    answer_question_2(df)
    answer_question_3(df)
    answer_question_4(df)
    answer_questions_5_and_6(df)
    print("=" * 60)

    spark.stop()
    print("\n🎉 Analysis complete!")


if __name__ == "__main__":
    main()

