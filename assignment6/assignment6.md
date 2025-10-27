# Assignment 6 — Results (Final)

**Rows analyzed:** 2,000  
**Columns analyzed:** 458

## Q1 — Predictions per Classifier
|Rank|Classifier|Predictions|
|---|---|---|
|1|fathmm-MKL_coding|6,000|
|2|LIST-S2|5,863|
|3|MetaRNN|5,863|
|4|AlphaMissense|5,836|
|5|MetaLR|5,646|
|6|MetaSVM|5,646|
|7|BayesDel_addAF|5,511|
|8|BayesDel_noAF|5,511|
|9|fathmm-XF_coding|5,511|
|10|DEOGEN2|5,437|
|11|ESM1b|5,437|
|12|Polyphen2_HDIV|5,437|
|13|Polyphen2_HVAR|5,437|
|14|MutationAssessor|5,428|
|15|ClinPred|5,289|
|16|M-CAP|5,271|
|17|PrimateAI|5,262|
|18|DANN|4,000|
|19|GM12878_fitCons|4,000|
|20|GenoCanyon|4,000|
|21|H1-hESC_fitCons|4,000|
|22|HUVEC_fitCons|4,000|
|23|integrated_fitCons|4,000|
|24|MutPred|3,755|
|25|FATHMM|3,674|
|26|LRT|3,674|
|27|PROVEAN|3,674|
|28|SIFT|3,674|
|29|SIFT4G|3,674|
|30|VEST4|3,674|
|31|MutationTaster|3,668|
|32|REVEL|3,600|
|33|VARITY_ER|3,600|
|34|VARITY_ER_LOO|3,600|
|35|VARITY_R|3,600|
|36|VARITY_R_LOO|3,600|
|37|gMVP|3,600|
|38|MVP|3,595|
|39|MPC|3,591|
|40|MutScore|3,526|
|41|MutFormer|3,508|
|42|Aloft|2,000|
|43|CADD|2,000|
|44|CADD_raw|2,000|
|45|GERP++_RS|2,000|
|46|SiPhy_29way_logOdds|2,000|
|47|bStatistic_converted|2,000|
|48|phastCons100way_vertebrate|2,000|
|49|phastCons17way_primate|2,000|
|50|phastCons470way_mammalian|2,000|
|51|phyloP100way_vertebrate|2,000|
|52|phyloP17way_primate|2,000|
|53|Eigen-PC-raw_coding|1,969|
|54|Eigen-raw_coding|1,969|
|55|EVE|1,837|
|56|EVE_Class10|1,837|
|57|EVE_Class20|1,837|
|58|EVE_Class25|1,837|
|59|EVE_Class30|1,837|
|60|EVE_Class40|1,837|
|61|EVE_Class50|1,837|
|62|EVE_Class60|1,837|
|63|EVE_Class70|1,837|
|64|EVE_Class75|1,837|
|65|EVE_Class80|1,837|
|66|EVE_Class90|1,837|
|67|LRT_converted|1,837|
|68|PHACTboost|1,837|
|69|MutationTaster_converted|1,834|
|70|FATHMM_converted|1,763|
|71|PROVEAN_converted|1,763|
|72|SIFT4G_converted|1,763|
|73|SIFT_converted|1,763|
|74|LINSIGHT|163|
|75|GERP_91_mammals|0|
|76|phyloP470way_mammalian|0|


**Top-5 kept:** fathmm-MKL_coding, LIST-S2, MetaRNN, AlphaMissense, MetaLR

## Q3 — Merged genomic identifier
We created a unique key:
`chr_pos = #chr:pos(1-based)`  
Example format: `1:69037`

## Q4 — Position with Most Predictions
- 1:69037 (45 predictions)

## Q5 — Protein with Most Predictions
- ENSP00000493376;ENSP00000334393 (27111 predictions)

## Normalization (3NF)
- **a6_positions:** 729 unique genomic positions  
- **a6_proteins:** 2 unique proteins  
- **a6_classifiers:** 5 classifiers  
- **a6_predictions:** 29,208 prediction records  
- **Database write:** Skipped (dry-run)

✅ All questions (Q1–Q5) and normalization completed successfully.
