# Assignment 5 — Spark DataFrame Analysis of GenBank (Archaea)

This assignment demonstrates how to analyze **NCBI RefSeq (Archaea)** GenBank (.gbff) data using **PySpark DataFrames**.  
It answers six bioinformatics questions about genomic features by applying filtering and aggregation operations in Spark.

---

## 🧠 Goal

- Learn to use **Spark DataFrames** for large-scale data manipulation (feature engineering).  
- Re-use the GenBank parsing logic from Assignment 4 (SQL version).  
- Compare the Spark approach to database-style data analysis.  
- Perform a simple analysis on a single `.gbff` file from the Archaea dataset.

Input files are available in:

```
/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/
```

---

## ⚙️ Setup and Execution

### 1. SLURM Script

Submit the provided batch job:

```bash
sbatch assignment5.sh
```

This runs the PySpark analysis on one `.gbff` file, for example:

```
/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.2.genomic.gbff
```

---

### 2. Spark Configuration

The script starts Spark in **local mode** using:

```python
.master("local[16]")
.config("spark.executor.memory", "128g")
.config("spark.driver.memory", "128g")
```

Make sure your SLURM request matches:
```bash
#SBATCH --ntasks=16
#SBATCH --mem=128G
```

If the BIN Spark cluster is available, you may switch to cluster mode by changing:
```python
.master("spark://spark.bin.bioinf.nl:7077")
```

---

## 📜 Files

| File | Description |
|------|--------------|
| `assignment5.py` | Main PySpark analysis script |
| `assignment5.sh` | SLURM batch script for execution |
| `features_cleaned.parquet/` | Output folder (created automatically) |
| `README.md` | Documentation file |

---

## 🧩 How It Works

### 1. Parse GenBank File

The script uses **Biopython (SeqIO)** to read `.gbff` files and extract only the relevant features.

Included feature types:

| Category | GenBank Tags | Description |
|-----------|---------------|-------------|
| Coding | `CDS`, `mat_peptide`, `propeptide` | Protein-coding features |
| Non-coding RNA | `rRNA`, `ncRNA` | RNA genes |
| Cryptic genes | `gene` without CDS | Genes lacking a CDS |
| Skipped | Features with `<` or `>` in their locations | Ambiguous |

Each feature becomes one **row** with columns:
`accession`, `species`, `feature_type`, `locus_tag`, `gene`, `product`,
`start`, `end`, `strand`, `length`, `is_coding`, `is_rna`.

---

### 2. Spark DataFrame

A DataFrame is created using an explicit schema (`FEATURE_SCHEMA`) with the correct data types for all columns.  
Spark is then used to answer the six questions via DataFrame transformations.

---

## 🧮 Questions Answered

| Question | Description | Spark Operation |
|-----------|--------------|-----------------|
| **Q1** | How many features does an Archaeal genome have on average? | `groupBy("accession").count()` → `avg("count")` |
| **Q2** | What is the ratio between coding and non-coding features? | Filter by `is_coding` True/False and count |
| **Q3** | What is the minimal and maximal number of proteins per genome? | Filter `feature_type == "CDS"` → `groupBy("accession").count()` |
| **Q4** | What is the average length of a feature? | `avg("length")` |
| **Q5** | Remove all non-coding RNA features and save cleaned DataFrame | `filter(is_rna == False)` → `.write.parquet()` |
| **Q6** | What is the average feature length after cleaning? | `avg("length")` on cleaned DataFrame |

---

## ✅ Actual Results (from my run)

```
==========================================
ANALYSIS RESULTS
==========================================
Q1) Average features per genome: 58.30
Q2) Coding vs Non-coding ratio: 13765926:27222 (ratio ~50.57)
Q3) Proteins per genome — min: 1 (NZ_JABGBQ010000362.1), max: 4038 (NZ_JABUQZ010000001.1)
Q4) Average feature length: 846.96 bp
Q5) Cleaned DataFrame saved to: features_cleaned.parquet
Q6) Average feature length after cleaning: 846.86 bp
==========================================
🎉 Analysis complete!
```

---

## 📂 Output Files

After completion, you will have:

```
features_cleaned.parquet/     # Cleaned dataset (RNA features removed)
assignment5_<jobid>.out       # Output log with results
assignment5_<jobid>.err       # Error log (if any)
```

You can later reload the Parquet file in Spark using:

```python
df_cleaned = spark.read.parquet("features_cleaned.parquet")
df_cleaned.show()
```

---

## 🧰 Dependencies

All dependencies are available system-wide on the BIN cluster:

- **Python ≥ 3.8**
- **PySpark** (preinstalled under `/opt/spark/`)
- **Biopython** (`Bio.SeqIO`)

No external installations or `module load` commands are needed.

---

## 🧪 Testing Locally

If you want to test on a small `.gbff` file:

```bash
python3 assignment5.py /homes/yourname/test.gbff
```

---



© 2025 — Assignment 5 (Spark + Python) – Bioinformatics Programming
