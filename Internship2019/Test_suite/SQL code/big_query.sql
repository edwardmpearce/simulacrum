WITH
population_real AS
(SELECT multi_depr_index.QUINTILE_2015, at_tumour.GRADE, at_tumour.AGE, at_tumour.SEX, at_tumour.CREG_CODE, at_tumour.SCREENINGSTATUSFULL_CODE, at_tumour.ER_STATUS, at_tumour.ER_SCORE, at_tumour.PR_STATUS, at_tumour.PR_SCORE, at_tumour.HER2_STATUS, at_tumour.GLEASON_PRIMARY, at_tumour.GLEASON_SECONDARY, at_tumour.GLEASON_TERTIARY, at_tumour.GLEASON_COMBINED, at_tumour.LATERALITY, at_tumour.DIAGNOSISDATEBEST, at_tumour.SITE_ICD10_O2, at_tumour.SITE_ICD10_O2_3CHAR, at_tumour.MORPH_ICD10_O2, at_tumour.BEHAVIOUR_ICD10_O2, at_tumour.T_BEST, at_tumour.N_BEST, at_tumour.M_BEST, at_tumour.STAGE_BEST, at_tumour.STAGE_BEST_SYSTEM, at_tumour_exp.CANCERCAREPLANINTENT, at_tumour_exp.PERFORMANCESTATUS, at_tumour_exp.CNS, at_tumour_exp.ACE27, at_tumour_exp.DATE_FIRST_SURGERY 
FROM
    (SELECT TUMOURID, LSOA11_CODE, GRADE, AGE, SEX, CREG_CODE, SCREENINGSTATUSFULL_CODE, ER_STATUS, ER_SCORE, PR_STATUS, PR_SCORE, HER2_STATUS, GLEASON_PRIMARY, GLEASON_SECONDARY, GLEASON_TERTIARY, GLEASON_COMBINED, LATERALITY, DIAGNOSISDATEBEST, SITE_ICD10_O2, SITE_ICD10_O2_3CHAR, MORPH_ICD10_O2, BEHAVIOUR_ICD10_O2, T_BEST, N_BEST, M_BEST, STAGE_BEST, STAGE_BEST_SYSTEM
	FROM AV2017.AT_TUMOUR_ENGLAND WHERE (diagnosisdatebest BETWEEN '01-JAN-2013' AND '31-DEC-2015')  AND STATUSOFREGISTRATION = 'F' AND CTRY_CODE = 'E' AND DEDUP_FLAG = 1) at_tumour
    LEFT JOIN (SELECT TUMOURID, CANCERCAREPLANINTENT, PERFORMANCESTATUS, CNS, ACE27, DATE_FIRST_SURGERY FROM AV2017.AT_TUMOUR_EXPERIMENTAL_ENGLAND) at_tumour_exp
    ON at_tumour.tumourid = at_tumour_exp.tumourid
    LEFT JOIN IMD.ID2015 multi_depr_index
    ON at_tumour.LSOA11_CODE = multi_depr_index.LSOA11_CODE ),

population_sim AS 
(SELECT * FROM analysispaulclarke.sim_av_tumour_final),
-- (SELECT * FROM analysispaulclarke.sim_av_tumour),

r AS
(SELECT 'GRADE' AS column_name, TO_CHAR(GRADE) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY GRADE UNION ALL SELECT 'AGE' AS column_name, TO_CHAR(AGE) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY AGE UNION ALL SELECT 'SEX' AS column_name, TO_CHAR(SEX) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY SEX UNION ALL SELECT 'CREG_CODE' AS column_name, TO_CHAR(CREG_CODE) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY CREG_CODE UNION ALL SELECT 'SCREENINGSTATUSFULL_CODE' AS column_name, TO_CHAR(SCREENINGSTATUSFULL_CODE) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY SCREENINGSTATUSFULL_CODE UNION ALL SELECT 'ER_STATUS' AS column_name, TO_CHAR(ER_STATUS) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY ER_STATUS UNION ALL SELECT 'ER_SCORE' AS column_name, TO_CHAR(ER_SCORE) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY ER_SCORE UNION ALL SELECT 'PR_STATUS' AS column_name, TO_CHAR(PR_STATUS) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY PR_STATUS UNION ALL SELECT 'PR_SCORE' AS column_name, TO_CHAR(PR_SCORE) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY PR_SCORE UNION ALL SELECT 'HER2_STATUS' AS column_name, TO_CHAR(HER2_STATUS) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY HER2_STATUS UNION ALL SELECT 'CANCERCAREPLANINTENT' AS column_name, TO_CHAR(CANCERCAREPLANINTENT) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY CANCERCAREPLANINTENT UNION ALL SELECT 'PERFORMANCESTATUS' AS column_name, TO_CHAR(PERFORMANCESTATUS) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY PERFORMANCESTATUS UNION ALL SELECT 'CNS' AS column_name, TO_CHAR(CNS) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY CNS UNION ALL SELECT 'ACE27' AS column_name, TO_CHAR(ACE27) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY ACE27 UNION ALL SELECT 'GLEASON_PRIMARY' AS column_name, TO_CHAR(GLEASON_PRIMARY) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY GLEASON_PRIMARY UNION ALL SELECT 'GLEASON_SECONDARY' AS column_name, TO_CHAR(GLEASON_SECONDARY) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY GLEASON_SECONDARY UNION ALL SELECT 'GLEASON_TERTIARY' AS column_name, TO_CHAR(GLEASON_TERTIARY) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY GLEASON_TERTIARY UNION ALL SELECT 'GLEASON_COMBINED' AS column_name, TO_CHAR(GLEASON_COMBINED) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY GLEASON_COMBINED UNION ALL SELECT 'DATE_FIRST_SURGERY' AS column_name, TO_CHAR(DATE_FIRST_SURGERY) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY DATE_FIRST_SURGERY UNION ALL SELECT 'LATERALITY' AS column_name, TO_CHAR(LATERALITY) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY LATERALITY UNION ALL SELECT 'QUINTILE_2015' AS column_name, TO_CHAR(QUINTILE_2015) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY QUINTILE_2015 UNION ALL SELECT 'DIAGNOSISDATEBEST' AS column_name, TO_CHAR(DIAGNOSISDATEBEST) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY DIAGNOSISDATEBEST UNION ALL SELECT 'SITE_ICD10_O2' AS column_name, TO_CHAR(SITE_ICD10_O2) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY SITE_ICD10_O2 UNION ALL SELECT 'SITE_ICD10_O2_3CHAR' AS column_name, TO_CHAR(SITE_ICD10_O2_3CHAR) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY SITE_ICD10_O2_3CHAR UNION ALL SELECT 'MORPH_ICD10_O2' AS column_name, TO_CHAR(MORPH_ICD10_O2) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY MORPH_ICD10_O2 UNION ALL SELECT 'BEHAVIOUR_ICD10_O2' AS column_name, TO_CHAR(BEHAVIOUR_ICD10_O2) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY BEHAVIOUR_ICD10_O2 UNION ALL SELECT 'T_BEST' AS column_name, TO_CHAR(T_BEST) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY T_BEST UNION ALL SELECT 'N_BEST' AS column_name, TO_CHAR(N_BEST) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY N_BEST UNION ALL SELECT 'M_BEST' AS column_name, TO_CHAR(M_BEST) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY M_BEST UNION ALL SELECT 'STAGE_BEST' AS column_name, TO_CHAR(STAGE_BEST) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY STAGE_BEST UNION ALL SELECT 'STAGE_BEST_SYSTEM' AS column_name, TO_CHAR(STAGE_BEST_SYSTEM) AS val, COUNT(*) AS counts_real FROM population_real GROUP BY STAGE_BEST_SYSTEM),
s AS
(SELECT 'GRADE' AS column_name, TO_CHAR(GRADE) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY GRADE UNION ALL SELECT 'AGE' AS column_name, TO_CHAR(AGE) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY AGE UNION ALL SELECT 'SEX' AS column_name, TO_CHAR(SEX) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY SEX UNION ALL SELECT 'CREG_CODE' AS column_name, TO_CHAR(CREG_CODE) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY CREG_CODE UNION ALL SELECT 'SCREENINGSTATUSFULL_CODE' AS column_name, TO_CHAR(SCREENINGSTATUSFULL_CODE) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY SCREENINGSTATUSFULL_CODE UNION ALL SELECT 'ER_STATUS' AS column_name, TO_CHAR(ER_STATUS) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY ER_STATUS UNION ALL SELECT 'ER_SCORE' AS column_name, TO_CHAR(ER_SCORE) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY ER_SCORE UNION ALL SELECT 'PR_STATUS' AS column_name, TO_CHAR(PR_STATUS) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY PR_STATUS UNION ALL SELECT 'PR_SCORE' AS column_name, TO_CHAR(PR_SCORE) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY PR_SCORE UNION ALL SELECT 'HER2_STATUS' AS column_name, TO_CHAR(HER2_STATUS) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY HER2_STATUS UNION ALL SELECT 'CANCERCAREPLANINTENT' AS column_name, TO_CHAR(CANCERCAREPLANINTENT) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY CANCERCAREPLANINTENT UNION ALL SELECT 'PERFORMANCESTATUS' AS column_name, TO_CHAR(PERFORMANCESTATUS) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY PERFORMANCESTATUS UNION ALL SELECT 'CNS' AS column_name, TO_CHAR(CNS) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY CNS UNION ALL SELECT 'ACE27' AS column_name, TO_CHAR(ACE27) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY ACE27 UNION ALL SELECT 'GLEASON_PRIMARY' AS column_name, TO_CHAR(GLEASON_PRIMARY) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY GLEASON_PRIMARY UNION ALL SELECT 'GLEASON_SECONDARY' AS column_name, TO_CHAR(GLEASON_SECONDARY) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY GLEASON_SECONDARY UNION ALL SELECT 'GLEASON_TERTIARY' AS column_name, TO_CHAR(GLEASON_TERTIARY) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY GLEASON_TERTIARY UNION ALL SELECT 'GLEASON_COMBINED' AS column_name, TO_CHAR(GLEASON_COMBINED) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY GLEASON_COMBINED UNION ALL SELECT 'DATE_FIRST_SURGERY' AS column_name, TO_CHAR(DATE_FIRST_SURGERY) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY DATE_FIRST_SURGERY UNION ALL SELECT 'LATERALITY' AS column_name, TO_CHAR(LATERALITY) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY LATERALITY UNION ALL SELECT 'QUINTILE_2015' AS column_name, TO_CHAR(QUINTILE_2015) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY QUINTILE_2015 UNION ALL SELECT 'DIAGNOSISDATEBEST' AS column_name, TO_CHAR(DIAGNOSISDATEBEST) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY DIAGNOSISDATEBEST UNION ALL SELECT 'SITE_ICD10_O2' AS column_name, TO_CHAR(SITE_ICD10_O2) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY SITE_ICD10_O2 UNION ALL SELECT 'SITE_ICD10_O2_3CHAR' AS column_name, TO_CHAR(SITE_ICD10_O2_3CHAR) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY SITE_ICD10_O2_3CHAR UNION ALL SELECT 'MORPH_ICD10_O2' AS column_name, TO_CHAR(MORPH_ICD10_O2) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY MORPH_ICD10_O2 UNION ALL SELECT 'BEHAVIOUR_ICD10_O2' AS column_name, TO_CHAR(BEHAVIOUR_ICD10_O2) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY BEHAVIOUR_ICD10_O2 UNION ALL SELECT 'T_BEST' AS column_name, TO_CHAR(T_BEST) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY T_BEST UNION ALL SELECT 'N_BEST' AS column_name, TO_CHAR(N_BEST) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY N_BEST UNION ALL SELECT 'M_BEST' AS column_name, TO_CHAR(M_BEST) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY M_BEST UNION ALL SELECT 'STAGE_BEST' AS column_name, TO_CHAR(STAGE_BEST) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY STAGE_BEST UNION ALL SELECT 'STAGE_BEST_SYSTEM' AS column_name, TO_CHAR(STAGE_BEST_SYSTEM) AS val, COUNT(*) AS counts_sim FROM population_sim GROUP BY STAGE_BEST_SYSTEM),

all_counts AS
(SELECT
NVL(r.column_name, s.column_name) AS col_name,
NVL(r.val, s.val) AS val,
NVL(counts_real, 0) AS counts_r,
NVL(counts_sim, 0) AS counts_s
FROM
r FULL OUTER JOIN s
ON (r.column_name = s.column_name AND (r.val = s.val OR (r.val IS NULL AND s.val IS NULL)))
    OR (r.column_name = 'CREG_CODE' AND s.column_name = 'CREG_CODE' AND SUBSTR(r.val, 2) = SUBSTR(s.val, 2))
    OR (r.column_name = 'QUINTILE_2015' AND s.column_name = 'QUINTILE_2015' AND SUBSTR(r.val, 1, 1) = SUBSTR(s.val, 1, 1))),

pop_sizes AS (SELECT * FROM
(SELECT COUNT(*) AS total_real FROM population_real),
(SELECT COUNT(*) AS total_sim FROM population_sim)),

proportions AS
(SELECT col_name, val, counts_r, counts_s,
counts_r/total_real AS proportion_r,
counts_s/total_sim AS proportion_s,
counts_s/total_sim - counts_r/total_real AS abs_diff,
CASE WHEN counts_r = 0 THEN NULL
    ELSE (counts_s * total_real)/(counts_r * total_sim) - 1
    END AS rel_diff,
(counts_r + counts_s)/(total_real + total_sim) AS average
FROM all_counts, pop_sizes),

results AS
(SELECT col_name, val, counts_r, counts_s, proportion_r, proportion_s, abs_diff, rel_diff, average,
CASE WHEN proportion_r = 0 THEN NULL
    ELSE (counts_s - total_sim * proportion_r)/SQRT(total_sim * proportion_r * (1 - proportion_r))
    END AS binom_z_test_one_sample,
CASE WHEN proportion_r = 0 THEN NULL
    WHEN (total_sim > 9 * (1 - proportion_r)/proportion_r AND total_sim > 9 * proportion_r / (1 - proportion_r)) THEN 1
    ELSE 0
    END AS one_sample_z_approx_valid,
abs_diff/SQRT(average * (1 - average) * ((total_real + total_sim)/(total_real * total_sim))) 
AS z_test_two_sample_pooled,
CASE WHEN counts_r = 0 THEN NULL
    ELSE (counts_s - total_sim * proportion_r)*(counts_s - total_sim * proportion_r)/total_sim * proportion_r
    END AS Pearson_summand,
CASE WHEN counts_r = 0 THEN NULL
    WHEN counts_s = 0 THEN 0
    ELSE counts_s * LOG(EXP(1), proportion_s/proportion_r)
    END AS LR_summand
FROM proportions, pop_sizes)

SELECT * FROM results
-- WHERE (counts_r = 0) OR (counts_s = 0);
-- WHERE one_sample_z_approx_valid != 1;
-- SELECT * FROM pop_sizes