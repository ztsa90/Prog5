# Assignment 6 — Results

**Rows analyzed:** 2,000  
**Columns analyzed:** 458

## Q1 — Predictions per Classifier
|Rank|Classifier|Predictions|
|---|---|---|
|1|fathmm-MKL_coding|6000|
|2|LIST-S2|5863|
|3|MetaRNN|5863|
|4|AlphaMissense|5836|
|5|MetaLR|5646|
|6|MetaSVM|5646|
|7|BayesDel_addAF|5511|
|8|BayesDel_noAF|5511|
|9|fathmm-XF_coding|5511|
|10|DEOGEN2|5437|
|11|ESM1b|5437|
|12|Polyphen2_HDIV|5437|
|13|Polyphen2_HVAR|5437|
|14|MutationAssessor|5428|
|15|ClinPred|5289|
|16|M-CAP|5271|
|17|PrimateAI|5262|
|18|DANN|4000|
|19|GM12878_fitCons|4000|
|20|GenoCanyon|4000|
|21|H1-hESC_fitCons|4000|
|22|HUVEC_fitCons|4000|
|23|integrated_fitCons|4000|
|24|MutPred|3755|
|25|FATHMM|3674|
|26|LRT|3674|
|27|PROVEAN|3674|
|28|SIFT|3674|
|29|SIFT4G|3674|
|30|VEST4|3674|
|31|MutationTaster|3668|
|32|REVEL|3600|
|33|VARITY_ER|3600|
|34|VARITY_ER_LOO|3600|
|35|VARITY_R|3600|
|36|VARITY_R_LOO|3600|
|37|gMVP|3600|
|38|MVP|3595|
|39|MPC|3591|
|40|MutScore|3526|
|41|MutFormer|3508|
|42|Aloft|2000|
|43|CADD|2000|
|44|CADD_raw|2000|
|45|GERP++_RS|2000|
|46|SiPhy_29way_logOdds|2000|
|47|bStatistic_converted|2000|
|48|phastCons100way_vertebrate|2000|
|49|phastCons17way_primate|2000|
|50|phastCons470way_mammalian|2000|
|51|phyloP100way_vertebrate|2000|
|52|phyloP17way_primate|2000|
|53|Eigen-PC-raw_coding|1969|
|54|Eigen-raw_coding|1969|
|55|EVE|1837|
|56|EVE_Class10|1837|
|57|EVE_Class20|1837|
|58|EVE_Class25|1837|
|59|EVE_Class30|1837|
|60|EVE_Class40|1837|
|61|EVE_Class50|1837|
|62|EVE_Class60|1837|
|63|EVE_Class70|1837|
|64|EVE_Class75|1837|
|65|EVE_Class80|1837|
|66|EVE_Class90|1837|
|67|LRT_converted|1837|
|68|PHACTboost|1837|
|69|MutationTaster_converted|1834|
|70|FATHMM_converted|1763|
|71|PROVEAN_converted|1763|
|72|SIFT4G_converted|1763|
|73|SIFT_converted|1763|
|74|LINSIGHT|163|
|75|GERP_91_mammals|0|
|76|phyloP470way_mammalian|0|


**Top-5 kept:** fathmm-MKL_coding, LIST-S2, MetaRNN, AlphaMissense, MetaLR

## Q3 — Merged genomic identifier
`chr_pos = #chr:pos(1-based)`  (e.g., `1:69037`)

## Q4 — Position with Most Predictions
- 1:69037 (45 predictions)

## Q5 — Protein with Most Predictions
- ENSP00000493376;ENSP00000334393 (27111 predictions)

## Normalization (3NF)
- **a6_positions:** 729 unique positions  
- **a6_proteins:** 2 unique proteins  
- **a6_classifiers:** 5 classifiers  
- **a6_predictions:** 29,208 rows  
- **Database write:** Skipped (dry-run)
