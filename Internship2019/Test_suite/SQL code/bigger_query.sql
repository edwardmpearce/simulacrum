WITH 
at_tumour AS
(SELECT TUMOURID, LSOA11_CODE, GRADE, AGE, SEX, CREG_CODE, SCREENINGSTATUSFULL_CODE, ER_STATUS, ER_SCORE, PR_STATUS, PR_SCORE, HER2_STATUS, GLEASON_PRIMARY, GLEASON_SECONDARY, GLEASON_TERTIARY, GLEASON_COMBINED, LATERALITY, DIAGNOSISDATEBEST, SITE_ICD10_O2, SITE_ICD10_O2_3CHAR, MORPH_ICD10_O2, BEHAVIOUR_ICD10_O2, T_BEST, N_BEST, M_BEST, STAGE_BEST, STAGE_BEST_SYSTEM 
FROM AV2017.AT_TUMOUR_ENGLAND 
WHERE (diagnosisdatebest BETWEEN '01-JAN-2013' AND '31-DEC-2017')  AND STATUSOFREGISTRATION = 'F' AND CTRY_CODE = 'E' AND DEDUP_FLAG = 1),

at_tumour_exp AS
(SELECT TUMOURID, CANCERCAREPLANINTENT, PERFORMANCESTATUS, CNS, ACE27, DATE_FIRST_SURGERY 
FROM AV2017.AT_TUMOUR_EXPERIMENTAL_ENGLAND),

population_ref AS 
(SELECT multi_depr_index.QUINTILE_2015, at_tumour.GRADE, at_tumour.AGE, at_tumour.SEX, at_tumour.CREG_CODE, at_tumour.SCREENINGSTATUSFULL_CODE, at_tumour.ER_STATUS, at_tumour.ER_SCORE, at_tumour.PR_STATUS, at_tumour.PR_SCORE, at_tumour.HER2_STATUS, at_tumour.GLEASON_PRIMARY, at_tumour.GLEASON_SECONDARY, at_tumour.GLEASON_TERTIARY, at_tumour.GLEASON_COMBINED, at_tumour.LATERALITY, at_tumour.DIAGNOSISDATEBEST, at_tumour.SITE_ICD10_O2, at_tumour.SITE_ICD10_O2_3CHAR, at_tumour.MORPH_ICD10_O2, at_tumour.BEHAVIOUR_ICD10_O2, at_tumour.T_BEST, at_tumour.N_BEST, at_tumour.M_BEST, at_tumour.STAGE_BEST, at_tumour.STAGE_BEST_SYSTEM, at_tumour_exp.CANCERCAREPLANINTENT, at_tumour_exp.PERFORMANCESTATUS, at_tumour_exp.CNS, at_tumour_exp.ACE27, at_tumour_exp.DATE_FIRST_SURGERY 
FROM at_tumour
LEFT JOIN at_tumour_exp 
ON at_tumour.tumourid = at_tumour_exp.tumourid 
LEFT JOIN IMD.ID2015 multi_depr_index 
ON at_tumour.LSOA11_CODE = multi_depr_index.LSOA11_CODE),

population_sim_final AS (SELECT * FROM analysispaulclarke.sim_av_tumour_final), 
population_sim_mid AS (SELECT * FROM analysispaulclarke.SIM_AV_TUMOUR), 
population_sim_newest AS (SELECT * FROM analysispaulclarke.SIM_AV_TUMOUR_SIMII),

totals_ref AS 
(SELECT 'GRADE' AS column_name, TO_CHAR(GRADE) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY GRADE UNION ALL SELECT 'AGE' AS column_name, TO_CHAR(AGE) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY AGE UNION ALL SELECT 'SEX' AS column_name, TO_CHAR(SEX) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY SEX UNION ALL SELECT 'CREG_CODE' AS column_name, TO_CHAR(CREG_CODE) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY CREG_CODE UNION ALL SELECT 'SCREENINGSTATUSFULL_CODE' AS column_name, TO_CHAR(SCREENINGSTATUSFULL_CODE) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY SCREENINGSTATUSFULL_CODE UNION ALL SELECT 'ER_STATUS' AS column_name, TO_CHAR(ER_STATUS) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY ER_STATUS UNION ALL SELECT 'ER_SCORE' AS column_name, TO_CHAR(ER_SCORE) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY ER_SCORE UNION ALL SELECT 'PR_STATUS' AS column_name, TO_CHAR(PR_STATUS) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY PR_STATUS UNION ALL SELECT 'PR_SCORE' AS column_name, TO_CHAR(PR_SCORE) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY PR_SCORE UNION ALL SELECT 'HER2_STATUS' AS column_name, TO_CHAR(HER2_STATUS) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY HER2_STATUS UNION ALL SELECT 'CANCERCAREPLANINTENT' AS column_name, TO_CHAR(CANCERCAREPLANINTENT) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY CANCERCAREPLANINTENT UNION ALL SELECT 'PERFORMANCESTATUS' AS column_name, TO_CHAR(PERFORMANCESTATUS) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY PERFORMANCESTATUS UNION ALL SELECT 'CNS' AS column_name, TO_CHAR(CNS) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY CNS UNION ALL SELECT 'ACE27' AS column_name, TO_CHAR(ACE27) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY ACE27 UNION ALL SELECT 'GLEASON_PRIMARY' AS column_name, TO_CHAR(GLEASON_PRIMARY) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY GLEASON_PRIMARY UNION ALL SELECT 'GLEASON_SECONDARY' AS column_name, TO_CHAR(GLEASON_SECONDARY) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY GLEASON_SECONDARY UNION ALL SELECT 'GLEASON_TERTIARY' AS column_name, TO_CHAR(GLEASON_TERTIARY) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY GLEASON_TERTIARY UNION ALL SELECT 'GLEASON_COMBINED' AS column_name, TO_CHAR(GLEASON_COMBINED) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY GLEASON_COMBINED UNION ALL SELECT 'DATE_FIRST_SURGERY' AS column_name, TO_CHAR(DATE_FIRST_SURGERY) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY DATE_FIRST_SURGERY UNION ALL SELECT 'LATERALITY' AS column_name, TO_CHAR(LATERALITY) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY LATERALITY UNION ALL SELECT 'QUINTILE_2015' AS column_name, TO_CHAR(QUINTILE_2015) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY QUINTILE_2015 UNION ALL SELECT 'DIAGNOSISDATEBEST' AS column_name, TO_CHAR(DIAGNOSISDATEBEST) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY DIAGNOSISDATEBEST UNION ALL SELECT 'SITE_ICD10_O2' AS column_name, TO_CHAR(SITE_ICD10_O2) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY SITE_ICD10_O2 UNION ALL SELECT 'SITE_ICD10_O2_3CHAR' AS column_name, TO_CHAR(SITE_ICD10_O2_3CHAR) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY SITE_ICD10_O2_3CHAR UNION ALL SELECT 'MORPH_ICD10_O2' AS column_name, TO_CHAR(MORPH_ICD10_O2) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY MORPH_ICD10_O2 UNION ALL SELECT 'BEHAVIOUR_ICD10_O2' AS column_name, TO_CHAR(BEHAVIOUR_ICD10_O2) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY BEHAVIOUR_ICD10_O2 UNION ALL SELECT 'T_BEST' AS column_name, TO_CHAR(T_BEST) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY T_BEST UNION ALL SELECT 'N_BEST' AS column_name, TO_CHAR(N_BEST) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY N_BEST UNION ALL SELECT 'M_BEST' AS column_name, TO_CHAR(M_BEST) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY M_BEST UNION ALL SELECT 'STAGE_BEST' AS column_name, TO_CHAR(STAGE_BEST) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY STAGE_BEST UNION ALL SELECT 'STAGE_BEST_SYSTEM' AS column_name, TO_CHAR(STAGE_BEST_SYSTEM) AS val, COUNT(*) AS counts_ref FROM population_ref GROUP BY STAGE_BEST_SYSTEM), 
totals_sim_final AS 
(SELECT 'GRADE' AS column_name, TO_CHAR(GRADE) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY GRADE UNION ALL SELECT 'AGE' AS column_name, TO_CHAR(AGE) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY AGE UNION ALL SELECT 'SEX' AS column_name, TO_CHAR(SEX) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY SEX UNION ALL SELECT 'CREG_CODE' AS column_name, TO_CHAR(CREG_CODE) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY CREG_CODE UNION ALL SELECT 'SCREENINGSTATUSFULL_CODE' AS column_name, TO_CHAR(SCREENINGSTATUSFULL_CODE) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY SCREENINGSTATUSFULL_CODE UNION ALL SELECT 'ER_STATUS' AS column_name, TO_CHAR(ER_STATUS) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY ER_STATUS UNION ALL SELECT 'ER_SCORE' AS column_name, TO_CHAR(ER_SCORE) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY ER_SCORE UNION ALL SELECT 'PR_STATUS' AS column_name, TO_CHAR(PR_STATUS) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY PR_STATUS UNION ALL SELECT 'PR_SCORE' AS column_name, TO_CHAR(PR_SCORE) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY PR_SCORE UNION ALL SELECT 'HER2_STATUS' AS column_name, TO_CHAR(HER2_STATUS) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY HER2_STATUS UNION ALL SELECT 'CANCERCAREPLANINTENT' AS column_name, TO_CHAR(CANCERCAREPLANINTENT) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY CANCERCAREPLANINTENT UNION ALL SELECT 'PERFORMANCESTATUS' AS column_name, TO_CHAR(PERFORMANCESTATUS) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY PERFORMANCESTATUS UNION ALL SELECT 'CNS' AS column_name, TO_CHAR(CNS) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY CNS UNION ALL SELECT 'ACE27' AS column_name, TO_CHAR(ACE27) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY ACE27 UNION ALL SELECT 'GLEASON_PRIMARY' AS column_name, TO_CHAR(GLEASON_PRIMARY) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY GLEASON_PRIMARY UNION ALL SELECT 'GLEASON_SECONDARY' AS column_name, TO_CHAR(GLEASON_SECONDARY) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY GLEASON_SECONDARY UNION ALL SELECT 'GLEASON_TERTIARY' AS column_name, TO_CHAR(GLEASON_TERTIARY) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY GLEASON_TERTIARY UNION ALL SELECT 'GLEASON_COMBINED' AS column_name, TO_CHAR(GLEASON_COMBINED) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY GLEASON_COMBINED UNION ALL SELECT 'DATE_FIRST_SURGERY' AS column_name, TO_CHAR(DATE_FIRST_SURGERY) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY DATE_FIRST_SURGERY UNION ALL SELECT 'LATERALITY' AS column_name, TO_CHAR(LATERALITY) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY LATERALITY UNION ALL SELECT 'QUINTILE_2015' AS column_name, TO_CHAR(QUINTILE_2015) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY QUINTILE_2015 UNION ALL SELECT 'DIAGNOSISDATEBEST' AS column_name, TO_CHAR(DIAGNOSISDATEBEST) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY DIAGNOSISDATEBEST UNION ALL SELECT 'SITE_ICD10_O2' AS column_name, TO_CHAR(SITE_ICD10_O2) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY SITE_ICD10_O2 UNION ALL SELECT 'SITE_ICD10_O2_3CHAR' AS column_name, TO_CHAR(SITE_ICD10_O2_3CHAR) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY SITE_ICD10_O2_3CHAR UNION ALL SELECT 'MORPH_ICD10_O2' AS column_name, TO_CHAR(MORPH_ICD10_O2) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY MORPH_ICD10_O2 UNION ALL SELECT 'BEHAVIOUR_ICD10_O2' AS column_name, TO_CHAR(BEHAVIOUR_ICD10_O2) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY BEHAVIOUR_ICD10_O2 UNION ALL SELECT 'T_BEST' AS column_name, TO_CHAR(T_BEST) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY T_BEST UNION ALL SELECT 'N_BEST' AS column_name, TO_CHAR(N_BEST) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY N_BEST UNION ALL SELECT 'M_BEST' AS column_name, TO_CHAR(M_BEST) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY M_BEST UNION ALL SELECT 'STAGE_BEST' AS column_name, TO_CHAR(STAGE_BEST) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY STAGE_BEST UNION ALL SELECT 'STAGE_BEST_SYSTEM' AS column_name, TO_CHAR(STAGE_BEST_SYSTEM) AS val, COUNT(*) AS counts_sim_final FROM population_sim_final GROUP BY STAGE_BEST_SYSTEM), 
totals_sim_mid AS 
(SELECT 'GRADE' AS column_name, TO_CHAR(GRADE) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY GRADE UNION ALL SELECT 'AGE' AS column_name, TO_CHAR(AGE) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY AGE UNION ALL SELECT 'SEX' AS column_name, TO_CHAR(SEX) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY SEX UNION ALL SELECT 'CREG_CODE' AS column_name, TO_CHAR(CREG_CODE) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY CREG_CODE UNION ALL SELECT 'SCREENINGSTATUSFULL_CODE' AS column_name, TO_CHAR(SCREENINGSTATUSFULL_CODE) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY SCREENINGSTATUSFULL_CODE UNION ALL SELECT 'ER_STATUS' AS column_name, TO_CHAR(ER_STATUS) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY ER_STATUS UNION ALL SELECT 'ER_SCORE' AS column_name, TO_CHAR(ER_SCORE) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY ER_SCORE UNION ALL SELECT 'PR_STATUS' AS column_name, TO_CHAR(PR_STATUS) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY PR_STATUS UNION ALL SELECT 'PR_SCORE' AS column_name, TO_CHAR(PR_SCORE) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY PR_SCORE UNION ALL SELECT 'HER2_STATUS' AS column_name, TO_CHAR(HER2_STATUS) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY HER2_STATUS UNION ALL SELECT 'CANCERCAREPLANINTENT' AS column_name, TO_CHAR(CANCERCAREPLANINTENT) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY CANCERCAREPLANINTENT UNION ALL SELECT 'PERFORMANCESTATUS' AS column_name, TO_CHAR(PERFORMANCESTATUS) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY PERFORMANCESTATUS UNION ALL SELECT 'CNS' AS column_name, TO_CHAR(CNS) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY CNS UNION ALL SELECT 'ACE27' AS column_name, TO_CHAR(ACE27) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY ACE27 UNION ALL SELECT 'GLEASON_PRIMARY' AS column_name, TO_CHAR(GLEASON_PRIMARY) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY GLEASON_PRIMARY UNION ALL SELECT 'GLEASON_SECONDARY' AS column_name, TO_CHAR(GLEASON_SECONDARY) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY GLEASON_SECONDARY UNION ALL SELECT 'GLEASON_TERTIARY' AS column_name, TO_CHAR(GLEASON_TERTIARY) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY GLEASON_TERTIARY UNION ALL SELECT 'GLEASON_COMBINED' AS column_name, TO_CHAR(GLEASON_COMBINED) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY GLEASON_COMBINED UNION ALL SELECT 'DATE_FIRST_SURGERY' AS column_name, TO_CHAR(DATE_FIRST_SURGERY) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY DATE_FIRST_SURGERY UNION ALL SELECT 'LATERALITY' AS column_name, TO_CHAR(LATERALITY) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY LATERALITY UNION ALL SELECT 'QUINTILE_2015' AS column_name, TO_CHAR(QUINTILE_2015) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY QUINTILE_2015 UNION ALL SELECT 'DIAGNOSISDATEBEST' AS column_name, TO_CHAR(DIAGNOSISDATEBEST) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY DIAGNOSISDATEBEST UNION ALL SELECT 'SITE_ICD10_O2' AS column_name, TO_CHAR(SITE_ICD10_O2) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY SITE_ICD10_O2 UNION ALL SELECT 'SITE_ICD10_O2_3CHAR' AS column_name, TO_CHAR(SITE_ICD10_O2_3CHAR) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY SITE_ICD10_O2_3CHAR UNION ALL SELECT 'MORPH_ICD10_O2' AS column_name, TO_CHAR(MORPH_ICD10_O2) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY MORPH_ICD10_O2 UNION ALL SELECT 'BEHAVIOUR_ICD10_O2' AS column_name, TO_CHAR(BEHAVIOUR_ICD10_O2) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY BEHAVIOUR_ICD10_O2 UNION ALL SELECT 'T_BEST' AS column_name, TO_CHAR(T_BEST) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY T_BEST UNION ALL SELECT 'N_BEST' AS column_name, TO_CHAR(N_BEST) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY N_BEST UNION ALL SELECT 'M_BEST' AS column_name, TO_CHAR(M_BEST) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY M_BEST UNION ALL SELECT 'STAGE_BEST' AS column_name, TO_CHAR(STAGE_BEST) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY STAGE_BEST UNION ALL SELECT 'STAGE_BEST_SYSTEM' AS column_name, TO_CHAR(STAGE_BEST_SYSTEM) AS val, COUNT(*) AS counts_sim_mid FROM population_sim_mid GROUP BY STAGE_BEST_SYSTEM), 
totals_sim_newest AS 
(SELECT 'GRADE' AS column_name, TO_CHAR(GRADE) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY GRADE UNION ALL SELECT 'AGE' AS column_name, TO_CHAR(AGE) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY AGE UNION ALL SELECT 'SEX' AS column_name, TO_CHAR(SEX) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY SEX UNION ALL SELECT 'CREG_CODE' AS column_name, TO_CHAR(CREG_CODE) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY CREG_CODE UNION ALL SELECT 'SCREENINGSTATUSFULL_CODE' AS column_name, TO_CHAR(SCREENINGSTATUSFULL_CODE) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY SCREENINGSTATUSFULL_CODE UNION ALL SELECT 'ER_STATUS' AS column_name, TO_CHAR(ER_STATUS) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY ER_STATUS UNION ALL SELECT 'ER_SCORE' AS column_name, TO_CHAR(ER_SCORE) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY ER_SCORE UNION ALL SELECT 'PR_STATUS' AS column_name, TO_CHAR(PR_STATUS) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY PR_STATUS UNION ALL SELECT 'PR_SCORE' AS column_name, TO_CHAR(PR_SCORE) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY PR_SCORE UNION ALL SELECT 'HER2_STATUS' AS column_name, TO_CHAR(HER2_STATUS) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY HER2_STATUS UNION ALL SELECT 'CANCERCAREPLANINTENT' AS column_name, TO_CHAR(CANCERCAREPLANINTENT) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY CANCERCAREPLANINTENT UNION ALL SELECT 'PERFORMANCESTATUS' AS column_name, TO_CHAR(PERFORMANCESTATUS) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY PERFORMANCESTATUS UNION ALL SELECT 'CNS' AS column_name, TO_CHAR(CNS) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY CNS UNION ALL SELECT 'ACE27' AS column_name, TO_CHAR(ACE27) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY ACE27 UNION ALL SELECT 'GLEASON_PRIMARY' AS column_name, TO_CHAR(GLEASON_PRIMARY) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY GLEASON_PRIMARY UNION ALL SELECT 'GLEASON_SECONDARY' AS column_name, TO_CHAR(GLEASON_SECONDARY) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY GLEASON_SECONDARY UNION ALL SELECT 'GLEASON_TERTIARY' AS column_name, TO_CHAR(GLEASON_TERTIARY) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY GLEASON_TERTIARY UNION ALL SELECT 'GLEASON_COMBINED' AS column_name, TO_CHAR(GLEASON_COMBINED) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY GLEASON_COMBINED UNION ALL SELECT 'DATE_FIRST_SURGERY' AS column_name, TO_CHAR(DATE_FIRST_SURGERY) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY DATE_FIRST_SURGERY UNION ALL SELECT 'LATERALITY' AS column_name, TO_CHAR(LATERALITY) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY LATERALITY UNION ALL SELECT 'QUINTILE_2015' AS column_name, TO_CHAR(QUINTILE_2015) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY QUINTILE_2015 UNION ALL SELECT 'DIAGNOSISDATEBEST' AS column_name, TO_CHAR(DIAGNOSISDATEBEST) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY DIAGNOSISDATEBEST UNION ALL SELECT 'SITE_ICD10_O2' AS column_name, TO_CHAR(SITE_ICD10_O2) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY SITE_ICD10_O2 UNION ALL SELECT 'SITE_ICD10_O2_3CHAR' AS column_name, TO_CHAR(SITE_ICD10_O2_3CHAR) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY SITE_ICD10_O2_3CHAR UNION ALL SELECT 'MORPH_ICD10_O2' AS column_name, TO_CHAR(MORPH_ICD10_O2) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY MORPH_ICD10_O2 UNION ALL SELECT 'BEHAVIOUR_ICD10_O2' AS column_name, TO_CHAR(BEHAVIOUR_ICD10_O2) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY BEHAVIOUR_ICD10_O2 UNION ALL SELECT 'T_BEST' AS column_name, TO_CHAR(T_BEST) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY T_BEST UNION ALL SELECT 'N_BEST' AS column_name, TO_CHAR(N_BEST) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY N_BEST UNION ALL SELECT 'M_BEST' AS column_name, TO_CHAR(M_BEST) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY M_BEST UNION ALL SELECT 'STAGE_BEST' AS column_name, TO_CHAR(STAGE_BEST) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY STAGE_BEST UNION ALL SELECT 'STAGE_BEST_SYSTEM' AS column_name, TO_CHAR(STAGE_BEST_SYSTEM) AS val, COUNT(*) AS counts_sim_newest FROM population_sim_newest GROUP BY STAGE_BEST_SYSTEM),

join_table1 AS 
(SELECT
NVL(totals_ref.column_name, totals_sim_final.column_name) AS column_name, 
NVL(totals_ref.val, NVL(totals_sim_final.val, 'None')) AS val, 
NVL(counts_ref, 0) AS counts_ref, 
NVL(counts_sim_final, 0) AS counts_sim_final
FROM totals_ref 
FULL OUTER JOIN totals_sim_final 
ON ((totals_ref.column_name = totals_sim_final.column_name AND (totals_ref.val = totals_sim_final.val OR (totals_ref.val IS NULL AND totals_sim_final.val IS NULL))) 
OR (totals_ref.column_name = 'CREG_CODE' AND totals_sim_final.column_name = 'CREG_CODE' AND SUBSTR(totals_ref.val, 2) = SUBSTR(totals_sim_final.val, 2)) 
OR (totals_ref.column_name = 'QUINTILE_2015' AND totals_sim_final.column_name = 'QUINTILE_2015' AND SUBSTR(totals_ref.val, 1, 1) = SUBSTR(totals_sim_final.val, 1, 1)))),

join_table2 AS 
(SELECT
NVL(join_table1.column_name, totals_sim_mid.column_name) AS column_name, 
NVL(join_table1.val, NVL(totals_sim_mid.val, 'None')) AS val,
NVL(counts_ref, 0) AS counts_ref, 
NVL(counts_sim_final, 0) AS counts_sim_final,
NVL(counts_sim_mid, 0) AS counts_sim_mid
FROM join_table1
FULL OUTER JOIN totals_sim_mid 
ON ((join_table1.column_name = totals_sim_mid.column_name AND (join_table1.val = totals_sim_mid.val OR (join_table1.val IS NULL AND totals_sim_mid.val IS NULL)))
OR (join_table1.column_name = 'CREG_CODE' AND totals_sim_mid.column_name = 'CREG_CODE' AND SUBSTR(join_table1.val, 2) = SUBSTR(totals_sim_mid.val, 2)) 
OR (join_table1.column_name = 'QUINTILE_2015' AND totals_sim_mid.column_name = 'QUINTILE_2015' AND SUBSTR(join_table1.val, 1, 1) = SUBSTR(totals_sim_mid.val, 1, 1)))),

join_table3 AS 
(SELECT
NVL(join_table2.column_name, totals_sim_newest.column_name) AS column_name, 
NVL(join_table2.val, NVL(totals_sim_newest.val, 'None')) AS val,
NVL(counts_ref, 0) AS counts_ref, 
NVL(counts_sim_final, 0) AS counts_sim_final,
NVL(counts_sim_mid, 0) AS counts_sim_mid,
NVL(counts_sim_newest, 0) AS counts_sim_newest 
FROM join_table2
FULL OUTER JOIN totals_sim_newest 
ON ((join_table2.column_name = totals_sim_newest.column_name AND (join_table2.val = totals_sim_newest.val OR (join_table2.val IS NULL AND totals_sim_newest.val IS NULL))) 
OR (join_table2.column_name = 'CREG_CODE' AND totals_sim_newest.column_name = 'CREG_CODE' AND SUBSTR(join_table2.val, 2) = SUBSTR(totals_sim_newest.val, 2)) 
OR (join_table2.column_name = 'QUINTILE_2015' AND totals_sim_newest.column_name = 'QUINTILE_2015' AND SUBSTR(join_table2.val, 1, 1) = SUBSTR(totals_sim_newest.val, 1, 1))))

SELECT * FROM join_table3
