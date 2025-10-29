# Assignment 6 — Results

**Date:** 2025-10-29 16:31:46  
**Rows analyzed:** 1,000  
**Columns:** 458  
**Data file:** /data/datasets/dbNSFP/snpEff/data/dbNSFP4.9a.txt.gz

## Q1 — How many predictions each classifier makes

Found 76 classifiers in total.

**Top 5 classifiers (kept in database):**
1. fathmm-MKL_coding
2. MetaRNN
3. LIST-S2
4. AlphaMissense
5. MetaSVM

Full classifier counts saved to: `assignment6_Q1_counts.csv`

## Q2 — Top five classifiers

The following classifiers were kept: **fathmm-MKL_coding, MetaRNN, LIST-S2, AlphaMissense, MetaSVM**

All other classifiers were dropped to save space.

## Q3 — Merge chr and pos columns

Created merged identifier: **chr_pos = chr:pos**

Example: `1:925942` represents chromosome 1, position 925942

The 3NF natural key is: `(chr, pos_1based, ref, alt)` in table `a6_positions`

## Q4 — Position with most predictions

**Position:** 1:69037  
**Total predictions:** 45

## Q5 — Protein with most predictions

**Protein (Ensembl ID):** ENSP00000493376;ENSP00000334393  
**Total predictions:** 12,387

## Database Normalization (3NF)

The data was normalized to Third Normal Form with three tables:

### Tables
- **a6_positions:** 1,000 unique genomic positions
- **a6_proteins:** 2 unique proteins
- **a6_predictions:** 14,484 prediction records

### Connection Details
- **Host:** mariadb.bin.bioinf.nl:3306
- **Database:** Ztaherihanjani
- **Driver:** org.mariadb.jdbc.Driver (JDBC)
- **Normalization:** 3NF with foreign key constraints

### Tables Structure
```sql
a6_positions: (position_id, chr, pos_1based, ref, alt, rs_dbSNP)
a6_proteins: (protein_id, ensembl_proteinid, ensembl_geneid, genename, uniprot_acc, uniprot_entry)
a6_predictions: (prediction_id, position_id, protein_id, classifier_name, column_name, value_raw)
```
