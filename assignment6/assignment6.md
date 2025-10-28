# Assignment 6 — Results

**Rows analyzed:** 2,000  
**Columns analyzed:** 458

## Q1 — Predictions per Classifier
**Top-5 kept:** fathmm-MKL_coding, LIST-S2, MetaRNN, AlphaMissense, MetaLR

## Q3 — Merged genomic identifier
`chr_pos = #chr:pos(1-based)`  (e.g., `1:69037`)

## Q4 — Position with Most Predictions
- 1:69037 (45 predictions)

## Q5 — Protein with Most Predictions
- ENSP00000493376;ENSP00000334393 (27111 predictions)

## Normalization (3NF)
- **a6_positions:** 2,000 unique positions  
- **a6_proteins:** 2 unique proteins  
- **a6_classifiers:** 76 classifiers  
- **a6_predictions:** 244,377 rows (before FK join)  
- **Database write:** DRY RUN — not written
