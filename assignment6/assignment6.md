# Assignment 6 — Results

**Author:** Z. Taherihanjani  
**Mode:** DRY-RUN (2,000 rows)

## Q1 – How many predictions per classifier?
|Rank|Classifier|Non-missing Predictions|
|---|---|---|
|1|EVE|22044|
|2|VARITY|14400|
|3|BayesDel|11022|
|4|Polyphen2|10874|
|5|CADD|8000|
|6|fathmm-MKL|6000|
|7|MetaRNN|5863|
|8|LIST-S2|5863|
|9|AlphaMissense|5836|
|10|MetaSVM|5646|


## Q2 – Top-5 classifiers kept
EVE, VARITY, BayesDel, Polyphen2, CADD

## Q3 – Merged genomic identifier
Created column `chr_pos` = `#chr:pos(1-based)`.

## Q4 – Position with most predictions
- Position: 1:69091
- Total predictions: 108

## Q5 – Protein with most predictions
- Ensembl_proteinid: ENSP00000493376;ENSP00000334393
- Total predictions: 65,688

## Normalization / SQL Write
- Positions: 729
- Proteins: 2
- JDBC: skipped (dry-run)

*All tables normalized to 3NF (a6_positions, a6_proteins, a6_predictions).*
