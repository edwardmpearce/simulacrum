-- Real patient data table
SELECT * FROM AV2015.av_patient;

-- Equivalent simulated patient data table
SELECT * FROM SIMULACRUM.sim_av_patient_final;

-- Simulacrum data is based on registration data which is _Finalised_, from England, and over the period 2013-2015. For fair comparison, we select the corresponding real data.
-- Could also use DIAGNOSISYEAR, and require CASCADE_INCI_FLAG = 1 (also removes duplicates, should check counts).
SELECT * FROM AV2015.av_tumour WHERE (diagnosisdatebest BETWEEN '01-JAN-2013' AND '31-DEC-2015') 
AND STATUSOFREGISTRATION = 'F' AND CTRY_CODE = 'E';

-- Equivalent simulated tumour data table
SELECT * FROM analysispaulclarke.sim_av_tumour_final;

-- Number of data rows (tumour diagnoses) over the selected cohort = 1,460,537
SELECT COUNT(*) FROM AV2015.av_tumour WHERE (diagnosisdatebest BETWEEN '01-JAN-2013' AND '31-DEC-2015') 
AND STATUSOFREGISTRATION = 'F' AND CTRY_CODE = 'E';

-- Number of simulated data rows (simulated tumour diagnoses) = 1,402,817
SELECT COUNT(*) FROM analysispaulclarke.sim_av_tumour_final;

-- Number of data rows (tumour diagnoses) over the selected cohort = 1,478,425
SELECT COUNT(*) FROM AV2017.AT_TUMOUR_ENGLAND WHERE (diagnosisdatebest BETWEEN '01-JAN-2013' AND '31-DEC-2015') 
AND STATUSOFREGISTRATION = 'F' AND CTRY_CODE = 'E' AND DEDUP_FLAG = 1;

-- Find the column names in a given table
SELECT * FROM all_tab_cols
WHERE owner = 'AV2017' AND table_name = 'AT_TUMOUR_ENGLAND';

SELECT * FROM all_tab_cols
WHERE owner = 'AV2017' AND table_name = 'AT_TUMOUR_EXPERIMENTAL_ENGLAND';

SELECT * FROM all_tab_cols
WHERE owner = 'ANALYSISPAULCLARKE' AND table_name = 'SIM_AV_TUMOUR_FINAL';

SELECT * FROM all_tab_cols
WHERE owner = 'IMD' AND table_name = 'ID2015';

-- Index of Multiple Depravity lookup table
SELECT * FROM IMD.ID2015;

-- Join experimental fields to the AV2017 AT_TUMOUR_ENGLAND table
SELECT * FROM
(SELECT * FROM AV2017.AT_TUMOUR_ENGLAND WHERE (diagnosisdatebest BETWEEN '01-JAN-2013' AND '31-DEC-2015') 
AND STATUSOFREGISTRATION = 'F' AND CTRY_CODE = 'E' AND DEDUP_FLAG = 1) at_tumour
INNER JOIN 
(SELECT TUMOURID, CANCERCAREPLANINTENT, PERFORMANCESTATUS, CNS, ACE27, DATE_FIRST_SURGERY 
FROM AV2017.AT_TUMOUR_EXPERIMENTAL_ENGLAND) at_tumour_exp
ON at_tumour.tumourid = at_tumour_exp.tumourid
INNER JOIN IMD.ID2015 multi_depr_index
ON at_tumour.LSOA11_CODE = multi_depr_index.LSOA11_CODE;

-- Miscellaneous Oracle SQL functionality
SELECT fiveyearageband, COUNT (*) 
FROM AV2015.AV_TUMOUR
WHERE site_icd10_o2_3char IN ('C33','C34')
GROUP BY fiveyearageband
ORDER BY fiveyearageband; 

SELECT MAX (age), SUM (age), COUNT (age), AVG (age)  
FROM AV2015.AV_TUMOUR;

SELECT tumour_count, BASISOFDIAGNOSIS, SHORTDESC 
FROM 
(SELECT COUNT(TUMOURID) AS TUMOUR_COUNT, BASISOFDIAGNOSIS 
FROM AV2015.AV_TUMOUR 
GROUP BY basisofdiagnosis
ORDER BY basisofdiagnosis) avt
LEFT OUTER JOIN SPRINGMVC3.zbasis@CAS1906 zba
ON avt.basisofdiagnosis = zba.zbasisid;

