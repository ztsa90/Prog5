#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# pylint: disable=import-error,line-too-long,too-many-statements,too-many-branches,too-many-locals,consider-using-f-string,bare-except,invalid-name

"""
Assignment 6 - dbNSFP Persistence with Spark and JDBC

This script analyzes the dbNSFP 4.9a dataset using Apache Spark and saves
the results to a MariaDB database in Third Normal Form (3NF).

Author: Z. Taherihanjani
Date: October 29, 2025
Course: Programming 5 - Data Science & Large-Scale Systems
"""

# =============================================================================
# IMPORTS
# =============================================================================

import sys
import os
import configparser
from datetime import datetime

# Add Spark Python libraries to path (BIN cluster specific)
sys.path.append('/opt/spark/python')
sys.path.append('/opt/spark/python/lib/py4j-0.10.9.7-src.zip')

# Import Spark libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Import MySQL client library
import pymysql

# =============================================================================
# CONFIGURATION
# =============================================================================

# Path to the compressed dbNSFP dataset on BIN cluster
DATA_FILE = "/data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz"

# Database connection parameters
DB_HOST = "mariadb.bin.bioinf.nl"  # MariaDB server hostname
DB_PORT = 3306                      # Default MySQL/MariaDB port
DB_NAME = "Ztaherihanjani"          # Target database name

# Path to JDBC driver JAR file
JDBC_JAR = os.path.expanduser("~/jars/mariadb-java-client-3.5.0.jar")

# Row limit for testing (set to 0 for full dataset)
ROW_LIMIT = 1000

# Missing value markers in dbNSFP dataset
MISSING = (".", "", "NA", "NaN", "nan", "null", "NULL")

# Classifier column suffixes (to identify prediction columns)
SUFFIXES = ("_score", "_pred", "_rankscore", "_phred")

# =============================================================================
# READ DATABASE CREDENTIALS
# =============================================================================

# Read MySQL credentials from ~/.my.cnf file
cfg = configparser.ConfigParser()
cfg.read(os.path.expanduser("~/.my.cnf"))
USER = cfg.get("client", "user")      # Database username
PASS = cfg.get("client", "password")  # Database password

# =============================================================================
# MAIN SCRIPT START
# =============================================================================

print("=" * 70)
print("ASSIGNMENT 6 - dbNSFP Analysis with Spark and JDBC")
print("=" * 70)

# =============================================================================
# STEP 1: CREATE DATABASE SCHEMA
# =============================================================================

print("\n[1/7] Creating database tables...")

# Connect to MariaDB database
# Note: Database must already exist (CREATE DATABASE is skipped to avoid timeout)
conn = pymysql.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=USER,
    password=PASS,
    database=DB_NAME,
    connect_timeout=60,  # Connection timeout in seconds
    read_timeout=60,     # Read operation timeout
    write_timeout=60     # Write operation timeout
)

# Create or truncate tables (avoid slow DROP)
with conn.cursor() as cur:
    
    # Try to truncate existing tables (faster than DROP)
    # If tables don't exist, we'll create them
    print("    Preparing tables...")
    
    try:
        # Disable foreign key checks temporarily for truncation
        cur.execute("SET FOREIGN_KEY_CHECKS = 0")
        
        # Try to truncate (if tables exist)
        try:  # pylint: disable=too-many-try-statements
            cur.execute("TRUNCATE TABLE a6_predictions")
            print("      Truncated a6_predictions")
        except:  # pylint: disable=bare-except
            print("      a6_predictions will be created")
            
        try:
            cur.execute("TRUNCATE TABLE a6_proteins")
            print("      Truncated a6_proteins")
        except:  # pylint: disable=bare-except
            print("      a6_proteins will be created")
            
        try:
            cur.execute("TRUNCATE TABLE a6_positions")
            print("      Truncated a6_positions")
        except:  # pylint: disable=bare-except
            print("      a6_positions will be created")
            
        # Re-enable foreign key checks
        cur.execute("SET FOREIGN_KEY_CHECKS = 1")
    except Exception as e:
        print(f"      Note: {e}")
    
    # Create a6_positions table (IF NOT EXISTS)
    # Stores unique genomic positions (SNVs only)
    print("    Creating a6_positions...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS a6_positions (
          position_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
          chr VARCHAR(10) NOT NULL,           -- Chromosome (e.g., '1', 'X', 'MT')
          pos_1based INT UNSIGNED NOT NULL,   -- Position (1-based coordinate)
          ref CHAR(1) NOT NULL,               -- Reference allele (single nucleotide)
          alt CHAR(1) NOT NULL,               -- Alternate allele (single nucleotide)
          rs_dbSNP VARCHAR(50),               -- dbSNP rs identifier
          UNIQUE KEY (chr, pos_1based, ref, alt)  -- Natural key (prevents duplicates)
        ) ENGINE=InnoDB CHARSET=utf8mb4
    """)
    
    # Create a6_proteins table (IF NOT EXISTS)
    # Stores unique protein identifiers and metadata
    print("    Creating a6_proteins...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS a6_proteins (
          protein_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
          ensembl_proteinid VARCHAR(500) NOT NULL,  -- Ensembl protein ID (may be long)
          ensembl_geneid VARCHAR(500),              -- Ensembl gene ID
          genename VARCHAR(500),                    -- Gene name (e.g., 'BRCA1')
          uniprot_acc VARCHAR(500),                 -- UniProt accession
          uniprot_entry VARCHAR(500),               -- UniProt entry name
          UNIQUE KEY (ensembl_proteinid)            -- No duplicate proteins
        ) ENGINE=InnoDB CHARSET=utf8mb4
    """)
    
    # Create a6_predictions table (IF NOT EXISTS)
    # Stores classifier predictions with foreign keys to dimension tables
    print("    Creating a6_predictions...")
    cur.execute("""
        CREATE TABLE IF NOT EXISTS a6_predictions (
          prediction_id BIGINT UNSIGNED AUTO_INCREMENT PRIMARY KEY,
          position_id BIGINT UNSIGNED NOT NULL,     -- FK to a6_positions
          protein_id BIGINT UNSIGNED,               -- FK to a6_proteins (nullable)
          classifier_name VARCHAR(100) NOT NULL,    -- Name of classifier (e.g., 'SIFT')
          column_name VARCHAR(100) NOT NULL,        -- Original column name
          value_raw VARCHAR(500) NOT NULL,          -- Prediction value (as string)
          FOREIGN KEY (position_id) 
            REFERENCES a6_positions(position_id)    -- FK constraint with CASCADE
            ON DELETE CASCADE,
          FOREIGN KEY (protein_id) 
            REFERENCES a6_proteins(protein_id)      -- FK constraint with SET NULL
            ON DELETE SET NULL,
          KEY (classifier_name)                      -- Index for faster queries
        ) ENGINE=InnoDB CHARSET=utf8mb4
    """)

# Commit schema changes
conn.commit()
conn.close()
print("    ✓ Schema created successfully")

# =============================================================================
# STEP 2: START SPARK SESSION
# =============================================================================

print("\n[2/7] Starting Spark session...")

# Initialize Spark session with cluster configuration
spark = (
    SparkSession.builder
    .appName("assignment6")                                      # Application name
    .master("spark://spark.bin.bioinf.nl:7077")                # Spark master URL
    .config("spark.jars", JDBC_JAR)                            # JDBC driver JAR
    .config("spark.executor.memory", "2g")                     # Executor memory
    .config("spark.driver.memory", "2g")                       # Driver memory
    .config("spark.driver.host",                               # Driver hostname
            os.popen("hostname -f").read().strip())            # Auto-detect
    .getOrCreate()
)

# Set log level to reduce output noise
spark.sparkContext.setLogLevel("ERROR")
print("    ✓ Spark connected to cluster")

# =============================================================================
# STEP 3: LOAD DATA FROM FILE
# =============================================================================

print("\n[3/7] Loading data...")

# Read compressed TSV file (Spark can read .gz directly)
# =============================================================================
# STEP 3: LOAD DATA FROM FILE
# =============================================================================

print("\n[3/7] Loading data...")

# Read compressed TSV file (Spark can read .gz directly)
df = spark.read.csv(
    DATA_FILE,
    sep="\t",           # Tab-separated values
    header=True,        # First row contains column names
    inferSchema=False   # Keep all columns as strings for safety
)

# Apply row limit for faster testing
if ROW_LIMIT > 0:
    df = df.limit(ROW_LIMIT)

# Fix column name if it starts with '#' (common in TSV files)
if df.columns[0].startswith('#'):
    df = df.withColumnRenamed(df.columns[0], df.columns[0].lstrip('#'))

# ★★★ ADD THIS LINE - Cache the dataframe ★★★
df = df.cache()

# Count rows and columns - this triggers the cache
n_rows = df.count()
n_cols = len(df.columns)
print(f"    ✓ Loaded {n_rows:,} rows × {n_cols} columns")

# =============================================================================
# STEP 4: Q1 - COUNT PREDICTIONS PER CLASSIFIER
# =============================================================================

print("\n[4/7] Q1: Counting predictions per classifier...")

# Build a dictionary mapping classifier names to their column lists
# Example: {"SIFT": ["SIFT_score", "SIFT_pred"], ...}
clf_map = {}
for col in df.columns:
    for suf in SUFFIXES:
        if col.endswith(suf):
            # Extract base name by removing suffix
            base = col[:-len(suf)].rstrip("_")
            clf_map.setdefault(base, []).append(col)
            break  # Only match one suffix per column

# Get all prediction columns (flattened list)
all_pred_cols = [c for cols in clf_map.values() for c in cols]

# Define function to check if a value is missing
def is_missing(col):  # pylint: disable=missing-function-docstring
    """
    Create a boolean Spark column that is True if value is missing.
    A value is missing if it's NULL or matches any MISSING marker.
    """
    cond = F.col(col).isNull()
    for m in MISSING:
        cond = cond | (F.col(col) == m)
    return cond

# Count non-missing values per column using aggregation
aggs = [
    F.sum(F.when(~is_missing(c), 1).otherwise(0)).alias(c)
    for c in all_pred_cols
]
totals = df.agg(*aggs).collect()[0].asDict()

# Sum counts per classifier (across all its columns)
clf_counts = []
for name, cols in clf_map.items():
    total = sum(int(totals.get(c, 0)) for c in cols)
    clf_counts.append((name, total))

# Create DataFrame and sort by count (descending)
clf_df = spark.createDataFrame(
    clf_counts,
    ["classifier", "predictions"]
).orderBy(F.desc("predictions"))

# Get top 5 classifier names
top5 = [r["classifier"] for r in clf_df.limit(5).collect()]

print(f"    ✓ Found {len(clf_map)} classifiers")
print(f"    ✓ Top-5: {', '.join(top5)}")

# Save full classifier counts to CSV file
clf_df.toPandas().to_csv("assignment6_Q1_counts.csv", index=False)

# =============================================================================
# STEP 5: Q2-Q5 - ANALYSIS
# =============================================================================

print("\n[5/7] Q2-Q5: Running analysis...")

# Get all columns from top 5 classifiers
top5_cols = [c for name in top5 for c in clf_map[name]]

# Select only ID columns and top-5 classifier columns
keep_cols = [
    "chr",
    "pos(1-based)",
    "ref",
    "alt",
    "Ensembl_proteinid"
] + top5_cols

# Create analysis dataframe
df_analysis = df.select(*keep_cols)

# Create merged genomic identifier (Q3)
# Format: "chr:pos" (e.g., "1:925942")
df_analysis = df_analysis.withColumn(
    "chr_pos",
    F.concat_ws(":", F.col("chr"), F.col("pos(1-based)"))
)

# Count non-missing predictions per row
# Build SQL CASE expression to count non-missing values
expr_parts = []
for c in top5_cols:
    # For each column, add 1 if not missing, 0 otherwise
    case_expr = (
        f"CASE WHEN (`{c}` IS NOT NULL "
        f"AND `{c}` NOT IN ('" + "','".join(MISSING) + "')) "
        f"THEN 1 ELSE 0 END"
    )
    expr_parts.append(case_expr)

# Sum all the 1s and 0s
expr = " + ".join(expr_parts)
df_analysis = df_analysis.withColumn("pred_count", F.expr(expr))

# Q4: Find position with most predictions
# Group by chr_pos, sum prediction counts, sort descending
pos_agg = (
    df_analysis
    .groupBy("chr_pos")
    .agg(F.sum("pred_count").alias("total"))
    .orderBy(F.desc("total"))
)
best_pos = pos_agg.first()
print(f"    ✓ Q4: {best_pos['chr_pos']} ({int(best_pos['total'])} predictions)")

# Q5: Find protein with most predictions
# Group by Ensembl_proteinid, sum prediction counts, sort descending
prot_agg = (
    df_analysis
    .where(F.col("Ensembl_proteinid").isNotNull())
    .groupBy("Ensembl_proteinid")
    .agg(F.sum("pred_count").alias("total"))
    .orderBy(F.desc("total"))
)
best_prot = prot_agg.first()
print(f"    ✓ Q5: {best_prot['Ensembl_proteinid']} ({int(best_prot['total'])} predictions)")

# =============================================================================
# STEP 6: PREPARE NORMALIZED DATA (3NF)
# =============================================================================

print("\n[6/7] Preparing normalized tables...")

# --- Table 1: a6_positions ---
# Unique genomic positions (deduplicated)
df_pos = (
    df.select(
        F.col("chr"),                              # Chromosome
        F.col("pos(1-based)")                      # Position
            .cast("int")
            .alias("pos_1based"),
        F.upper(F.trim(F.col("ref")))              # Reference allele (uppercase)
            .alias("ref"),
        F.upper(F.trim(F.col("alt")))              # Alternate allele (uppercase)
            .alias("alt"),
        F.col("rs_dbSNP")                          # dbSNP ID
    )
    .filter(F.col("chr").isNotNull())              # Remove NULL chromosomes
    .filter(F.length("ref") == 1)                  # Keep only SNVs (single nucleotide)
    .filter(F.length("alt") == 1)                  # Keep only SNVs
    .dropDuplicates(["chr", "pos_1based", "ref", "alt"])  # Remove duplicates
)

pos_count = df_pos.count()
print(f"    ✓ Positions: {pos_count:,}")

# --- Table 2: a6_proteins ---
# Unique proteins with metadata
df_prot = (
    df.select(
        # Truncate to 500 chars to prevent overflow
        F.substring(F.trim(F.col("Ensembl_proteinid")), 1, 500)
            .alias("ensembl_proteinid"),
        F.substring(F.trim(F.col("Ensembl_geneid")), 1, 500)
            .alias("ensembl_geneid"),
        F.substring(F.trim(F.col("genename")), 1, 500)
            .alias("genename"),
        F.substring(F.trim(F.col("Uniprot_acc")), 1, 500)
            .alias("uniprot_acc"),
        F.substring(F.trim(F.col("Uniprot_entry")), 1, 500)
            .alias("uniprot_entry")
    )
    .where(F.col("ensembl_proteinid").isNotNull())     # Remove NULL proteins
    .dropDuplicates(["ensembl_proteinid"])             # Remove duplicates
)

prot_count = df_prot.count()
print(f"    ✓ Proteins: {prot_count:,}")

# --- Table 3: a6_predictions (long format) ---
# Convert wide format to long format (one row per prediction)
parts = []
for col in top5_cols:
    # Extract classifier name from column name
    for suf in SUFFIXES:
        if col.endswith(suf):
            clf_name = col[:-len(suf)].rstrip("_")
            break
    
    # Create one part of the union for this column
    part = (
        df.select(
            F.col("chr"),
            F.col("pos(1-based)")
                .cast("int")
                .alias("pos_1based"),
            F.upper(F.trim(F.col("ref")))
                .alias("ref"),
            F.upper(F.trim(F.col("alt")))
                .alias("alt"),
            F.substring(F.trim(F.col("Ensembl_proteinid")), 1, 500)
                .alias("ensembl_proteinid"),
            F.lit(clf_name)                          # Classifier name (literal)
                .alias("classifier_name"),
            F.lit(col)                               # Column name (literal)
                .alias("column_name"),
            F.substring(F.col(col).cast("string"), 1, 500)  # Value (truncated)
                .alias("value_raw")
        )
        .where(~is_missing(col))                     # Keep only non-missing values
    )
    parts.append(part)

# Union all parts into one long DataFrame
df_pred = parts[0]
for p in parts[1:]:
    df_pred = df_pred.unionByName(p)

# Filter to keep only SNVs
df_pred = (
    df_pred
    .filter(F.length("ref") == 1)
    .filter(F.length("alt") == 1)
    .dropDuplicates()                                # Remove any duplicates
)

pred_count = df_pred.count()
print(f"    ✓ Predictions: {pred_count:,}")

# =============================================================================
# STEP 7: WRITE TO DATABASE VIA JDBC
# =============================================================================

print("\n[7/7] Writing to database via JDBC...")

# JDBC connection URL and properties
jdbc_url = f"jdbc:mariadb://{DB_HOST}:{DB_PORT}/{DB_NAME}"
jdbc_props = {
    "driver": "org.mariadb.jdbc.Driver",
    "user": USER,
    "password": PASS,
    "batchsize": "500",                    # Smaller batch size to avoid timeout
    "connectTimeout": "60000",             # 60 seconds
    "socketTimeout": "60000",              # 60 seconds
}

# Write a6_positions table
print("    Writing a6_positions...")
df_pos.write.jdbc(
    url=jdbc_url,
    table="a6_positions",
    mode="append",              # Use append since table is already empty (truncated)
    properties=jdbc_props
)

# Write a6_proteins table
print("    Writing a6_proteins...")
df_prot.write.jdbc(
    url=jdbc_url,
    table="a6_proteins",
    mode="append",              # Use append since table is already empty (truncated)
    properties=jdbc_props
)

# Write predictions to temporary table (without foreign keys)
temp_table = "temp_predictions"
print(f"    Writing {temp_table}...")
df_pred.write.jdbc(
    url=jdbc_url,
    table=temp_table,
    mode="overwrite",           # Overwrite temp table if exists
    properties=jdbc_props
)

# Insert predictions with foreign keys using SQL
print("    Inserting with foreign keys...")
conn = pymysql.connect(
    host=DB_HOST,
    port=DB_PORT,
    user=USER,
    password=PASS,
    database=DB_NAME,
    connect_timeout=60,
    read_timeout=60,
    write_timeout=60
)

with conn.cursor() as cur:
    # Join temp table with dimension tables to get foreign keys
    cur.execute(f"""
        INSERT INTO a6_predictions 
            (position_id, protein_id, classifier_name, column_name, value_raw)
        SELECT 
            pos.position_id,                    -- FK from positions table
            prot.protein_id,                    -- FK from proteins table
            t.classifier_name,
            t.column_name,
            t.value_raw
        FROM {temp_table} t
        INNER JOIN a6_positions pos             -- Must match a position
            ON pos.chr = t.chr 
            AND pos.pos_1based = t.pos_1based
            AND pos.ref = t.ref 
            AND pos.alt = t.alt
        LEFT JOIN a6_proteins prot              -- May not have a protein
            ON prot.ensembl_proteinid = t.ensembl_proteinid
    """)
    final_count = cur.rowcount
    
    # Drop temporary table
    cur.execute(f"DROP TABLE {temp_table}")

conn.commit()
conn.close()

print(f"    ✓ Final predictions: {final_count:,}")

# =============================================================================
# STEP 8: GENERATE MARKDOWN REPORT
# =============================================================================

print("\nGenerating report...")

# Build report content
report = f"""# Assignment 6 — Results

**Date:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}  
**Rows analyzed:** {n_rows:,}  
**Columns:** {n_cols}  
**Data file:** {DATA_FILE}

## Q1 — How many predictions each classifier makes

Found {len(clf_map)} classifiers in total.

**Top 5 classifiers (kept in database):**
1. {top5[0]}
2. {top5[1]}
3. {top5[2]}
4. {top5[3]}
5. {top5[4]}

Full classifier counts saved to: `assignment6_Q1_counts.csv`

## Q2 — Top five classifiers

The following classifiers were kept: **{", ".join(top5)}**

All other classifiers were dropped to save space.

## Q3 — Merge chr and pos columns

Created merged identifier: **chr_pos = chr:pos**

Example: `1:925942` represents chromosome 1, position 925942

The 3NF natural key is: `(chr, pos_1based, ref, alt)` in table `a6_positions`

## Q4 — Position with most predictions

**Position:** {best_pos['chr_pos']}  
**Total predictions:** {int(best_pos['total']):,}

## Q5 — Protein with most predictions

**Protein (Ensembl ID):** {best_prot['Ensembl_proteinid']}  
**Total predictions:** {int(best_prot['total']):,}

## Database Normalization (3NF)

The data was normalized to Third Normal Form with three tables:

### Tables
- **a6_positions:** {pos_count:,} unique genomic positions
- **a6_proteins:** {prot_count:,} unique proteins
- **a6_predictions:** {final_count:,} prediction records

### Connection Details
- **Host:** {DB_HOST}:{DB_PORT}
- **Database:** {DB_NAME}
- **Driver:** org.mariadb.jdbc.Driver (JDBC)
- **Normalization:** 3NF with foreign key constraints

```python
### Tables Structure
````sql
a6_positions: (position_id, chr, pos_1based, ref, alt, rs_dbSNP)
a6_proteins: (protein_id, ensembl_proteinid, ensembl_geneid, genename, uniprot_acc, uniprot_entry)
a6_predictions: (prediction_id, position_id, protein_id, classifier_name, column_name, value_raw)
"""
