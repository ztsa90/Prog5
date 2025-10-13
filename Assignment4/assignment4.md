# Assignment 4 — RefSeq (Archaea) to MariaDB

This project parses an NCBI **RefSeq Archaea GenBank (.gbff)** file and stores its biological data in a **MariaDB** database using Python and Biopython.  

Data are structured into three related tables: **Species**, **Genome**, and **Protein**.

---

## 🧩 Files

| File | Description |
|------|--------------|
| `assignment4.py` | Python script that parses the `.gbff` file and loads the data into MariaDB |
| `assignment4.sh` | SLURM batch script to run the Python script automatically on the cluster |

---

## ⚙️ Requirements

- **Python 3**
- Install the following packages:
  ```bash
  pip install biopython sqlalchemy mysqlclient
  ```
- MariaDB access via `~/.my.cnf`:
  ```ini
  [client]
  user=YOUR_USERNAME
  password=YOUR_PASSWORD
  host=mariadb.bin.bioinf.nl
  database=YOUR_DATABASE
  ```

---

## 🧬 Database Structure

### Table: **Species**
| Column | Type | Description |
|---------|------|-------------|
| `taxdb_id` | VARCHAR(50) PRIMARY KEY | NCBI Taxonomy ID |
| `name` | VARCHAR(255) | Scientific species name |

### Table: **Genome**
| Column | Type | Description |
|---------|------|-------------|
| `accession` | VARCHAR(50) PRIMARY KEY | Genome accession (e.g. NZ_XXXX.1) |
| `genome_size` | INT | Genome length in base pairs |
| `num_genes` | INT | Total number of genes |
| `num_proteins` | INT | Total number of CDS features |
| `species_id` | VARCHAR(50) FOREIGN KEY | → Species.taxdb_id |

### Table: **Protein**
| Column | Type | Description |
|---------|------|-------------|
| `id` | BIGINT AUTO_INCREMENT PRIMARY KEY | Unique ID for each row |
| `protein_id` | VARCHAR(50) | Protein accession (e.g. WP_/YP_/NP_) |
| `genome_accession` | VARCHAR(50) FOREIGN KEY | → Genome.accession |
| `product_name` | TEXT | Protein product name |
| `locus_tag` | VARCHAR(50) | Gene locus tag |
| `gene_ref` | VARCHAR(50) | Gene name (if any) |
| `ec_number` | VARCHAR(50) | Enzyme Commission number |
| `go_annotations` | TEXT | GO term annotations |
| `location` | VARCHAR(100) | Genomic location string |

---

## ▶️ How to Run

### Option 1 — Run directly
```bash
python3 assignment4.py
```

### Option 2 — Run via SLURM
```bash
sbatch assignment4.sh
```
SLURM will save logs to:
```
assignment4_refseq-<jobid>.out
assignment4_refseq-<jobid>.err
```

---

## 🧾 Example Output

```
🔌 Connecting to database...
✅ Connected!

📖 Step 1: Parsing species...
✅ Step 1 complete! Inserted 620 species

📖 Step 2: Parsing genomes...
✅ Step 2 complete! Inserted 36,353 genomes

📖 Step 3: Parsing proteins (CDS)...
   → inserted 50,000 rows...
   → inserted 1,374,787 rows total
✅ Step 3 complete!
```

---

## 🔍 Example SQL Queries

### 1️⃣ List genomes for a specific species
```sql
SELECT s.name, g.accession, g.genome_size
FROM Species s
JOIN Genome g ON s.taxdb_id = g.species_id
WHERE s.name = 'Halorubrum amylolyticum';
```

### 2️⃣ List all proteins from one genome
```sql
SELECT p.protein_id, p.product_name, p.locus_tag
FROM Protein p
WHERE p.genome_accession = 'NZ_SDJP01000015.1';
```

### 3️⃣ Find all genomes containing a given protein
```sql
SELECT p.genome_accession, p.product_name
FROM Protein p
WHERE p.protein_id = 'WP_004045866.1';
```

### 4️⃣ Count genomes per species
```sql
SELECT s.name, COUNT(*) AS genome_count
FROM Species s
JOIN Genome g ON s.taxdb_id = g.species_id
GROUP BY s.name
ORDER BY genome_count DESC
LIMIT 10;
```

---

## 💡 Extended SQL Examples

### 5️⃣ Summary of total genome size, genes, and proteins for one species
```sql
SELECT 
  s.name,
  COUNT(*)                 AS contigs,
  SUM(g.genome_size)       AS total_bp,
  SUM(g.num_genes)         AS total_genes,
  SUM(g.num_proteins)      AS total_proteins
FROM Species s
JOIN Genome g ON g.species_id = s.taxdb_id
WHERE s.name = 'Halorubrum amylolyticum';
```

### 6️⃣ Top 10 genomes with the most proteins
```sql
SELECT accession, num_proteins
FROM Genome
ORDER BY num_proteins DESC
LIMIT 10;
```

### 7️⃣ Verify protein counts between Genome and Protein tables
```sql
WITH p AS (
  SELECT genome_accession, COUNT(*) AS protein_rows
  FROM Protein
  WHERE genome_accession IN (
    SELECT g.accession
    FROM Species s
    JOIN Genome g ON g.species_id = s.taxdb_id
    WHERE s.name = 'Halorubrum amylolyticum'
  )
  GROUP BY genome_accession
)
SELECT 
  g.accession,
  g.num_proteins          AS num_proteins_reported,
  COALESCE(p.protein_rows, 0) AS proteins_counted
FROM Genome g
LEFT JOIN p ON p.genome_accession = g.accession
WHERE g.species_id = (
  SELECT taxdb_id FROM Species WHERE name = 'Halorubrum amylolyticum' LIMIT 1
)
ORDER BY g.genome_size DESC
LIMIT 20;
```

### 8️⃣ Genomes with no CDS (no proteins)
```sql
SELECT accession, genome_size, num_genes, num_proteins
FROM Genome
WHERE num_proteins = 0
LIMIT 20;
```

### 9️⃣ Proteins that have EC numbers
```sql
SELECT protein_id, genome_accession, ec_number, product_name
FROM Protein
WHERE ec_number IS NOT NULL AND ec_number <> ''
LIMIT 50;
```

### 🔟 Search proteins by GO term (example: GO:0005524)
```sql
SELECT protein_id, genome_accession, go_annotations
FROM Protein
WHERE go_annotations LIKE '%GO:0005524%'
LIMIT 50;
```

### 11️⃣ Count unique protein IDs in a genome
```sql
SELECT COUNT(DISTINCT protein_id) AS unique_wp
FROM Protein
WHERE genome_accession = 'NZ_SDJP01000015.1';
```

### 12️⃣ Random sample of proteins (quick browse)
```sql
SELECT id, protein_id, product_name, genome_accession
FROM Protein
ORDER BY RAND()
LIMIT 20;
```

---

## 🧠 Notes

- All tables use `ENGINE=InnoDB` and `CHARSET=utf8mb4` for compatibility and speed.  
- Batch insertion is used for efficiency:
  - Species → 1000 rows per batch  
  - Genomes → 2000 rows per batch  
  - Proteins → 500 rows per batch  
- Primary key for `Protein` is `id` (auto-increment) to allow duplicates of `protein_id` across genomes.  
- Foreign keys enforce integrity between Species → Genome → Protein.

---

## ✅ Summary

| Step | Description | Example Count | Example Time |
|------|--------------|----------------|---------------|
| 1 | Species parsed and inserted | ~620 | ~100s |
| 2 | Genomes parsed and inserted | ~36,000 | ~140s |
| 3 | Proteins parsed and inserted | ~1.37M | ~190s |

---

## 🛠️ Common Issues

| Error | Cause | Fix |
|-------|--------|-----|
| **1205: Lock wait timeout** | Table locked by another session | Wait or restart transaction |
| **1005 / errno 150** | Bad foreign key definition | Ensure all tables use `InnoDB` |
| **Duplicate entry** | Same protein ID in multiple genomes | Handled via auto PK on `id` |
| **Connection refused** | Wrong credentials | Check `.my.cnf` file |

---

## 📬 Author
**Name:** Zahra Taheri Hanjani  
**Course:** Programming 5 — Bioinformatics  
**Assignment:** 4 — RefSeq Data Integration  
**Institution:** Hanze University of Applied Sciences  
