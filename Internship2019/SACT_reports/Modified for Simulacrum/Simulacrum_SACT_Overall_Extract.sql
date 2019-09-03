/* Written by Edward Pearce - 15th August 2019 */
/* Based on work by Bukky Juwa - 17th Jan 2019, ADAM BROWN - 17TH JANUARY 2019, and Carrie - 25th June 2018 */

/* This creates the non-CTYA level SAS extracts (regimen/cycle, drug, and outcome levels) from Simulacrum datasets (simulated SACT and AV tables) */

/* User Instructions */
/* 1. Set the dates in the Extract_Dates table below - to be regularly updated when producing new extracts/snapshots of the datasets */
/*   a) Extract_Start specifies the start of the date-range for the data to be extracted (Default to extract data for April 2018 onwards) */
/*   b) Latest_Treatment_Check should be updated to be 3 months before the latest available SACT data. (Set at '31-12-2018' at time of writing) */
/*      Latest_Treatment_Check is used to check whether or not the patient has received any treatment within the last 3 months */
/*   c) Outcome_Extract_End specifies the end of the date-range for the Outcome-level extract ONLY. (Default to extract outcomes between April and November 2018) */
/* 2. Choose which extract you want to create by uncommenting the relevant line at the end of the document */

/* Code Explanation */
/* The code is split up into four sections: */
/* In Part One, the tables 'TreatmentDates' and 'Derived_Regimen_Fields' are introduced to define some intermediate variables */
/* In Part Two, the table 'SIM_SACT_AllLevels' is introduced to join the various Simulacrum data sources together and define several derived fields */
/* In Part Three, the tables 'SIM_SACT_RegimenLevel', 'SIM_SACT_DrugLevel', and 'SIM_SACT_OutcomeLevel' are defined using subsets of fields from 'SIM_SACT_AllLevels' and the user-input dates from the Extract_Dates table */
/* In Part Four, the user is able to select which table they want to view at the end of the file */

/* **************************************** Part One **************************************** */

WITH
/* Change these dates when creating a new monthly update */	
Extract_Dates AS
(SELECT 
	TO_DATE('01-04-2018','DD-MM-YYYY') AS Extract_Start,
	TO_DATE('31-12-2018','DD-MM-YYYY') AS Latest_Treatment_Check, -- Change date on each new extraction to 3 months before most recent SACT data
	TO_DATE('30-11-2018','DD-MM-YYYY') AS Outcome_Extract_End
FROM DUAL), 


TreatmentDates AS
(SELECT 
	SIM_SACT_C.Merged_Regimen_ID,
    MIN (SIM_SACT_C.Start_Date_of_Cycle) OVER (PARTITION BY SIM_SACT_C.Merged_Regimen_ID) AS Earliest_Cycle,
    MAX (SIM_SACT_C.Start_Date_of_Cycle) OVER (PARTITION BY SIM_SACT_C.Merged_Regimen_ID) AS Latest_Cycle,
	MIN (SIM_SACT_D.Administration_Date) OVER (PARTITION BY SIM_SACT_C.Merged_Regimen_ID) AS Earliest_Admin_Date,
	MAX (SIM_SACT_D.Administration_Date) OVER (PARTITION BY SIM_SACT_C.Merged_Regimen_ID) AS Latest_Admin_Date
FROM SIMULACRUM_1_2.SIM_SACT_DRUG_DETAIL_FINAL SIM_SACT_D
LEFT JOIN SIMULACRUM_1_2.SIM_SACT_CYCLE_FINAL SIM_SACT_C 
ON SIM_SACT_C.Merged_Cycle_ID = SIM_SACT_D.Merged_Cycle_ID),


Derived_Regimen_Fields AS
(SELECT
	SIM_SACT_R.Merged_Regimen_ID AS Merged_Regimen_ID,
/*  The Simulacrum currently does NOT contain the field 'Adult_Perf_Stat_Start_of_Reg' */
/*  An estimate is generated using the earliest Perf_Status_Start_of_Cycle in a regimen */
	NVL(SIM_SACT_C.Perf_Status_Start_of_Cycle, '') AS Adult_Perf_Stat_Start_of_Reg,
/*  Selects the earliest (minimum) date out of all Cycle start dates, Drug Administration dates and the Regimen start date within a regimen	*/
	CASE -- Can this be written more clearly as a MIN statement over the three quantities of interest?
    WHEN SIM_SACT_R.Start_Date_of_Regimen <= TDates.Earliest_Cycle AND SIM_SACT_R.Start_Date_of_Regimen <= TDates.Earliest_Admin_Date THEN SIM_SACT_R.Start_Date_of_Regimen	
	WHEN TDates.Earliest_Cycle <= TDates.Earliest_Admin_Date AND TDates.Earliest_Cycle <= SIM_SACT_R.Start_Date_of_Regimen THEN TDates.Earliest_Cycle
    WHEN TDates.Earliest_Admin_Date <= TDates.Earliest_Cycle AND TDates.Earliest_Admin_Date <= SIM_SACT_R.Start_Date_of_Regimen THEN TDates.Earliest_Admin_Date
    ELSE NULL END AS Earliest_Treatment_Date,
/*  Selects the latest (maximum) date out of all Cycle start dates, Drug Administration dates and the Regimen start date within a regimen */
	CASE -- Can this be written more clearly as a MAX statement over the three quantities of interest?
    WHEN TDates.Latest_Admin_Date >= TDates.Latest_Cycle AND TDates.Latest_Admin_Date >= SIM_SACT_R.Start_Date_of_Regimen THEN TDates.Latest_Admin_Date
	WHEN TDates.Latest_Cycle >= TDates.Latest_Admin_Date AND TDates.Latest_Cycle >= SIM_SACT_R.Start_Date_of_Regimen THEN TDates.Latest_Cycle
    WHEN SIM_SACT_R.Start_Date_of_Regimen >= TDates.Latest_Cycle AND SIM_SACT_R.Start_Date_of_Regimen >= TDates.Latest_Admin_Date THEN SIM_SACT_R.Start_Date_of_Regimen
	ELSE NULL END AS Latest_Treatment_Date	
/*  If the comparisons above cannot be streamlined into MIN/MAX statements, could we reduce the number of comparisons to 3 instead of 6 using binary search logic? */	
FROM SIMULACRUM_1_2.SIM_SACT_REGIMEN_FINAL SIM_SACT_R
LEFT JOIN TreatmentDates TDates
ON TDates.Merged_Regimen_ID = SIM_SACT_R.Merged_Regimen_ID
LEFT JOIN SIMULACRUM_1_2.SIM_SACT_CYCLE_FINAL SIM_SACT_C
ON SIM_SACT_C.Start_Date_of_Cycle = TDates.Earliest_Cycle),

/* **************************************** Part Two **************************************** */

SIM_SACT_AllLevels AS
(SELECT
/*  Patient-level fields */
	SIM_SACT_P.Merged_Patient_ID AS Merged_Patient_ID,
/*  Patient level data is extracted from the linked simulated AV tables, as absent from the simulated SACT tables */
	NVL(SIM_AV_P.Sex, '') AS Gender_Current,
	NVL(SIM_AV_P.Ethnicity, '') AS Ethnicity,
/*  The Simulacrum currently does NOT contain the field 'Date_of_Death' */
/*  This is instead derived from NewVitalStatus and VitalStatusDate in SIM_AV_PATIENT */
    CASE WHEN SIM_AV_P.NewVitalStatus = 'D' THEN TO_CHAR(SIM_AV_P.VitalStatusDate, 'MON/YYYY')
	ELSE NULL END AS Date_of_Death,

/*  Tumour-level fields */
	NVL(SIM_SACT_T.Merged_Tumour_ID, '') AS Merged_Tumour_ID, 
	NVL(SIM_SACT_T.Primary_Diagnosis, '') AS Primary_Diagnosis,
    NVL(SUBSTR(SIM_SACT_T.primary_diagnosis, 1, 3), '') as Primary_Diagnosis_3Char,	
/*  The field 'GroupDescription2' is derived from Primary Diagnosis using the Diagnosis Subgroup lookup */	
	NVL(DSG.Group_Description2, '') AS Group_Description2,
	NVL(SIM_SACT_T.Morphology_clean, '') AS Morphology_clean,
	NVL(SUBSTR(SIM_SACT_T.Morphology_clean, 1, 4), '') AS Morphology_code,
	NVL(SUBSTR(SIM_SACT_T.Morphology_clean, -1, 1), '') AS Behaviour,

/*  The Simulacrum currently does NOT contain the tumour-level field 'Organisation_Code_of_Provider' */
/*  We instead derive 'Provider' and 'Trust' fields from the drug-level field 'Org_Code_of_Drug_Provider' */
/*  This introduces the assumption that the Provider/Trust that initiated treatment coincides with that of the drug provider */
    NVL(SIM_SACT_D.Org_Code_of_Drug_Provider, '') AS Provider,
    NVL(SUBSTR(SIM_SACT_D.Org_Code_of_Drug_Provider, 1, 3), '') as Trust,
/*  The Simulacrum currently does NOT contain the tumour-level field 'Consultant_gmc_code' */
/*  Consultant_gmc_code is used to derive 'Consultant_Code_Name' and 'ref_no_with_c' */	
/*  Therefore the fields 'Consultant_Code_Name' and 'ref_no_with_c' cannot be extracted from the Simulacrum */

/*  Regimen-level fields */
    NVL(SIM_SACT_R.Merged_Regimen_ID, '') AS Merged_Regimen_ID,
	NVL(TO_CHAR(SIM_SACT_R.Start_Date_of_Regimen, 'MON/YYYY'), '') AS Start_Date_of_Regimen,
	NVL(SIM_SACT_R.Start_Date_of_Regimen, '') AS Start_Date_of_Regimen_Full,
/*  The AgeGroup field defined below is based on age at regimen start date, whilst the Age field in SIM_AV_TUMOUR denotes age at diagnosis */
/*  Therefore we add the difference in time between DiagnosisDateBest and Start_Date_of_Regimen to calculate age at regimen start date */
	NVL(TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)), '') AS Age_at_Regimen_Start,
	NVL(CASE
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 0 AND 15 THEN '0-15'
    WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 16 AND 18 THEN '16-18' 
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 19 AND 24 THEN '19-24'			
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 25 AND 29 THEN '25-29'			 
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 30 AND 34 THEN '30-34' 
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 35 AND 39 THEN '35-39' 
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 40 AND 44 THEN '40-44' 
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 45 AND 49 THEN '45-49' 
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 50 AND 54 THEN '50-54' 
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 55 AND 59 THEN '55-59' 
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 60 AND 64 THEN '60-64' 
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 65 AND 69 THEN '65-69' 
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 70 AND 74 THEN '70-74' 
	WHEN TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) BETWEEN 75 AND 79 THEN '75-79' 
	ELSE '80+' END,
	'') AS AgeGroup,		
	NVL(SIM_SACT_R.Intent_of_Treatment, '') AS Intent_of_Treatment,
    NVL(CAST(SIM_SACT_R.Weight_At_Start_of_Regimen AS VARCHAR(20)), '') AS Weight_at_Start_of_Regimen,
    NVL(CAST(SIM_SACT_R.Height_At_Start_of_Regimen AS VARCHAR(20)), '') AS Height_at_Start_of_Regimen,	
    CASE WHEN SIM_SACT_R.Weight_at_Start_of_Regimen > 0.0 THEN 'Y' ELSE '' END AS Weight_Regimen_Completeness,
    CASE WHEN SIM_SACT_R.Height_At_Start_of_Regimen > 0.0 THEN 'Y' ELSE '' END AS Height_Regimen_Completeness,
/*  The Simulacrum currently does NOT contain the field 'Adult_Perf_Stat_Start_of_Reg' */
/*  An estimate is generated using the first Perf_Status_Start_of_Cycle in a regimen */
    NVL(R1.Adult_Perf_Stat_Start_of_Reg, '') AS Adult_Perf_Stat_Start_of_Reg,
    NVL(SIM_SACT_R.Mapped_Regimen, '') AS Mapped_Regimen,
/*  The fields 'Benchmark' and 'Analysis' are derived from MappedRegimen using the Benchmark Analysis Lookup */	
    NVL(BAL.Benchmark, '') as Benchmark,
    NVL(BAL.Analysis, '') as Analysis,
	
/*  Cycle-level fields */	
    NVL(SIM_SACT_C.Merged_Cycle_ID, '') AS Merged_Cycle_ID,
    NVL(TO_CHAR(SIM_SACT_C.Start_Date_of_Cycle, 'MON/YYYY'), '') as Start_Date_of_Cycle,
    NVL(SIM_SACT_C.Start_Date_of_Cycle, '') as Start_Date_of_Cycle_Full,
/*  This field 'Perf_Status_Start_of_Cycle' has a different name to 'Perf_Stat_Start_of_Cycle_Adult' */
    NVL(SIM_SACT_C.Perf_Status_Start_of_Cycle, '') AS Perf_Status_Start_of_Cycle,
/*  The Simulacrum currently does NOT contain the field 'Weight_at_Start_of_Cycle' */
/*  Therefore the fields 'Weight_at_Start_of_Cycle' and 'Weight_Cycle_Completeness' cannot be extracted from the Simulacrum */

/*  Drug-level fields */	
    NVL(SIM_SACT_D.Merged_Drug_Detail_ID, '') AS Merged_Drug_Detail_ID,
    NVL(TO_CHAR(SIM_SACT_D.Administration_Date, 'MON/YYYY'), '') as Administration_Date,
    NVL(SIM_SACT_D.Administration_Date, '') as Administration_Date_Full,
	NVL(TO_CHAR(SIM_SACT_D.Administration_Date, 'DAY'), '') as Weekday,
    NVL(SIM_SACT_D.Administration_Route, '') AS Administration_Route,
/*  The Simulacrum currently does NOT contain the field 'Drug_Name', but does contain 'Drug_Group' */
    NVL(SIM_SACT_D.Drug_Group, '') AS Drug_Group,
    NVL(CAST(SIM_SACT_D.Actual_Dose_per_Administration AS VARCHAR(20)), '') AS Actual_Dose_per_Administration,
    NVL(SIM_SACT_D.Org_Code_of_Drug_Provider, '') AS Org_Code_of_Drug_Provider,
    NVL(SUBSTR(SIM_SACT_D.Org_Code_of_Drug_Provider, 1, 3), '') as Trust_of_Drug_Provider,
	
/*  Outcome-level fields */	
    NVL(SIM_SACT_O.Merged_Outcome_ID, '') AS Merged_Outcome_ID,
    NVL(SIM_SACT_O.Regimen_Outcome_Summary, '') AS Regimen_Outcome_Summary,

/*  Additional derived fields */	
/*  We create a field called Outcome_Expected which has codes 1,2,3 to denote: */
/*  3. Regimen Outcome Summary received */
/*  1. NO Regimen Outcome Summary received and Patient died */
/*  2. NO Regimen Outcome Summary received and Patient Inactive - Patient has not received any treatment within the last 3 months */
/*  Code number 2 depends on a hard coded date which should be updated with each new extraction */
	CASE
	WHEN SIM_SACT_O.Regimen_Outcome_Summary IS NOT NULL THEN '3'	
	WHEN SIM_AV_P.NewVitalStatus = 'D' THEN '1'
    WHEN R1.Latest_Treatment_Date < Extract_dates.Latest_Treatment_Check THEN '2' -- Change date on each new extraction to 3 months before most recent SACT data
	ELSE NULL END
	AS Outcome_Expected,
/*  Exclusions - A field primarily based on regimen-level field Mapped_Regimen, though the E5 sometimes also depends on tumour-level field Primary_Diagnosis */
/*  The CDF exclusions depend not only on the type of treatment, but also the tumour being treated and treatment dates */
/*  Note: An exclusions lookup table for all types of exclusions may be useful in the future */
    CASE
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) = 'NOT CHEMO' OR UPPER(BAL.Benchmark) = 'NOT CHEMO') THEN 'E1'
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) IN ('PAMIDRONATE','ZOLEDRONIC ACID') OR UPPER(BAL.Benchmark) IN ('PAMIDRONATE','ZOLEDRONIC ACID')) THEN 'E2'
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) = 'DENOSUMAB' OR UPPER(BAL.Benchmark) = 'DENOSUMAB') THEN 'E3'	
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) = 'HORMONES' OR UPPER(BAL.Benchmark) = 'HORMONES') THEN 'E4'
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) IN ('BCG INTRAVESICAL','MITOMYCIN INTRAVESICAL','EPIRUBICIN INTRAVESICAL'))
      OR (UPPER(SIM_SACT_R.Mapped_Regimen) IN ('MITOMYCIN', 'EPIRUBICIN') AND (SIM_SACT_T.Primary_Diagnosis LIKE 'C67%' OR SIM_SACT_T.Primary_Diagnosis LIKE 'D41%')) THEN 'E5'														
	WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) LIKE '%TRIAL%' OR UPPER(BAL.Benchmark) LIKE '%TRIAL%') THEN 'E6'
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) = 'NOT MATCHED' OR UPPER(BAL.Benchmark) = 'NOT MATCHED') THEN 'E7'
/*  CDF Exclusions (coded as 'E8' exclusions) are currently excluded from this extract since they will require extra work to be derived from Simulacrum data */
	ELSE SIM_SACT_R.Mapped_Regimen END
	AS Exclusion
--	NVL(, '') AS Birch_Classification,
--  NVL(, '') AS ICCC3_Paed_Grouping,
--  NVL(, '') AS ICCC3_Site_Group,
FROM SIMULACRUM_1_2.SIM_SACT_PATIENT_FINAL SIM_SACT_P
INNER JOIN SIMULACRUM_1_2.SIM_SACT_TUMOUR_FINAL SIM_SACT_T
ON SIM_SACT_T.Merged_Patient_ID = SIM_SACT_P.Merged_Patient_ID
INNER JOIN SIMULACRUM_1_2.SIM_SACT_REGIMEN_FINAL SIM_SACT_R
ON SIM_SACT_R.Merged_Tumour_ID = SIM_SACT_T.Merged_Tumour_ID
INNER JOIN SIMULACRUM_1_2.SIM_SACT_CYCLE_FINAL SIM_SACT_C
ON SIM_SACT_C.Merged_Regimen_ID = SIM_SACT_R.Merged_Regimen_ID
LEFT JOIN SIMULACRUM_1_2.SIM_SACT_DRUG_DETAIL_FINAL SIM_SACT_D
ON SIM_SACT_D.Merged_Cycle_ID = SIM_SACT_C.Merged_Cycle_ID
LEFT JOIN SIMULACRUM_1_2.SIM_SACT_OUTCOME_FINAL SIM_SACT_O 
ON SIM_SACT_O.Merged_Regimen_ID = SIM_SACT_R.Merged_Regimen_ID
/*  Used to derive patient-level fields 'Gender_Current', 'Ethnicity', and 'Date_of_Death */
LEFT JOIN SIMULACRUM_1_2.SIM_AV_PATIENT_FINAL SIM_AV_P
ON SIM_AV_P.LinkNumber = SIM_SACT_P.Link_Number
/*  Used to derive regimen-level field 'AgeGroup' (at regimen start date) */
LEFT JOIN SIMULACRUM_1_2.SIM_AV_TUMOUR_FINAL SIM_AV_T
ON SIM_AV_T.LinkNumber = SIM_SACT_P.Link_Number
/*  Used to derive tumour-level field 'GroupDescription2' */
LEFT JOIN ANALYSISBUKKYJUWA.DIAGNOSIS_SUBGROUP_SACT DSG
ON DSG.ICD_Code = SIM_SACT_T.Primary_Diagnosis
/*  Used to derive regimen-level fields 'Benchmark' and 'Analysis' */
LEFT JOIN ANALYSISBUKKYJUWA.BENCHMARK_ANALYSIS_LOOKUP_NEW BAL
ON BAL.Mapped_Regimen = SIM_SACT_R.Mapped_Regimen
/*  Used to derive regimen-level fields 'Adult_Perf_Stat_Start_of_Reg' and 'Analysis' */
LEFT JOIN Derived_Regimen_Fields R1
ON R1.Merged_Regimen_ID = SIM_SACT_R.Merged_Regimen_ID, 
Extract_dates),

/* **************************************** Part Three **************************************** */

SIM_SACT_RegimenLevel AS
(SELECT
    Merged_Patient_ID,
    Gender_Current,
    Ethnicity,
    Date_of_death,
    Merged_Tumour_ID,
    Primary_Diagnosis,
    Group_Description2,
/*  The Simulacrum currently does NOT contain the tumour-level field 'Organisation_Code_of_Provider' */
/*  We instead derive 'Provider' and 'Trust' fields from the drug-level field 'Org_Code_of_Drug_Provider' */
/*  This introduces the assumption that the Provider/Trust that initiated treatment coincides with that of the drug provider */
    Provider,
    Trust,
    Merged_Regimen_ID,
    Start_Date_of_Regimen,
    AgeGroup,
    Intent_of_Treatment,
    Mapped_Regimen,
    Benchmark,
    Analysis,
/*  The Simulacrum currently does NOT contain the field 'Adult_Perf_Stat_Start_of_Reg' */
/*  An estimate is generated using the first Perf_Status_Start_of_Cycle in a regimen */
    Adult_Perf_Stat_Start_of_Reg,
    Weight_at_Start_of_Regimen,
    Weight_Regimen_Completeness,
    Height_at_Start_of_Regimen,
    Height_Regimen_Completeness,
    Merged_Cycle_ID,
    Start_Date_of_Cycle,
    Perf_Status_Start_of_Cycle,
/*  The Simulacrum currently does NOT contain the field 'Weight_at_Start_of_Cycle' */
/*  Therefore the fields 'Weight_at_Start_of_Cycle' and 'Weight_Cycle_Completeness' cannot be extracted from the Simulacrum */
    Regimen_Outcome_Summary,
    Outcome_Expected,
    Exclusion
FROM SIM_SACT_AllLevels, Extract_dates
WHERE (Start_Date_of_Regimen_Full >= Extract_Start OR Start_Date_of_Cycle_Full >= Extract_Start) 
AND Administration_Date_Full >= Extract_Start),


SIM_SACT_DrugLevel AS
(SELECT
    Merged_Patient_ID,
    Gender_Current,
    Ethnicity,
    Merged_Tumour_ID,
    Primary_Diagnosis,
    Group_Description2,
/*  The Simulacrum currently does NOT contain the tumour-level field 'Organisation_Code_of_Provider' */
/*  We instead derive 'Provider' and 'Trust' fields from the drug-level field 'Org_Code_of_Drug_Provider' */
/*  This introduces the assumption that the Provider/Trust that initiated treatment coincides with that of the drug provider */
    Provider,
    Trust,
/*  The Simulacrum currently does NOT contain the tumour-level field 'Consultant_gmc_code' */
/*  Therefore the derived field 'Consultant_Code_Name' cannot be extracted from the Simulacrum */
    Merged_Regimen_ID,
    Start_Date_of_Regimen,
    AgeGroup,
    Intent_of_Treatment,
    Mapped_Regimen,
    Benchmark,
    Analysis,
/*  The Simulacrum currently does NOT contain the field 'Adult_Perf_Stat_Start_of_Reg' */
/*  An estimate is generated using the first Perf_Status_Start_of_Cycle in a regimen */
    Adult_Perf_Stat_Start_of_Reg,
    Weight_at_Start_of_Regimen,
    Weight_Regimen_Completeness,
    Height_at_Start_of_Regimen,
    Height_Regimen_Completeness,
    Merged_Cycle_ID,
    Start_Date_of_Cycle,
    Perf_Status_Start_of_Cycle,
/*  The Simulacrum currently does NOT contain the field 'Weight_at_Start_of_Cycle' */
/*  Therefore the fields 'Weight_at_Start_of_Cycle' and 'Weight_Cycle_Completeness' cannot be extracted from the Simulacrum */
    Merged_Drug_Detail_ID,
    Administration_Date,
	Weekday,
    Administration_Route,
    Drug_Group,
    Actual_Dose_per_Administration,
    Org_Code_of_Drug_Provider,
    Trust_of_Drug_Provider,
    Regimen_Outcome_Summary,
    Exclusion
FROM SIM_SACT_AllLevels, Extract_dates
WHERE (Start_Date_of_Regimen_Full >= Extract_Start OR Start_Date_of_Cycle_Full >= Extract_Start OR Administration_Date_Full >= Extract_Start)),


SIM_SACT_OutcomeLevel AS
(SELECT
    Merged_Patient_ID,
    Gender_Current,
    Ethnicity,
    Date_of_death,
    Merged_Tumour_ID,
    Primary_Diagnosis,
    Group_Description2,
/*  The Simulacrum currently does NOT contain the tumour-level field 'Organisation_Code_of_Provider' */
/*  We instead derive 'Provider' and 'Trust' fields from the drug-level field 'Org_Code_of_Drug_Provider' */
/*  This introduces the assumption that the Provider/Trust that initiated treatment coincides with that of the drug provider */
    Provider,
    Trust,
    Merged_Regimen_ID,
    Start_Date_of_Regimen,
    AgeGroup,
    Intent_of_Treatment,
    Mapped_Regimen,
/*  The Simulacrum currently does NOT contain the field 'Adult_Perf_Stat_Start_of_Reg' */
/*  An estimate is generated using the first Perf_Status_Start_of_Cycle in a regimen */
    Adult_Perf_Stat_Start_of_Reg,
    Weight_at_Start_of_Regimen,
    Weight_Regimen_Completeness,
    Height_at_Start_of_Regimen,
    Height_Regimen_Completeness,
    Merged_Cycle_ID,
    Start_Date_of_Cycle,
    Perf_Status_Start_of_Cycle,
/*  The Simulacrum currently does NOT contain the field 'Weight_at_Start_of_Cycle' */
/*  Therefore the fields 'Weight_at_Start_of_Cycle' and 'Weight_Cycle_Completeness' cannot be extracted from the Simulacrum */
    Administration_Date,
	Merged_Outcome_ID,
	Regimen_Outcome_Summary,
    Outcome_Expected,
    Exclusion
FROM SIM_SACT_AllLevels, Extract_dates
WHERE (Start_Date_of_Regimen_Full BETWEEN Extract_Start AND Outcome_Extract_End OR Start_Date_of_Cycle_Full BETWEEN Extract_Start AND Outcome_Extract_End)
AND Outcome_Expected IS NOT NULL)

/* **************************************** Part Four **************************************** */

/* Uncomment the relevant line of code to choose the extract you want to create/the table you want to view. */
SELECT *
--FROM SIM_SACT_RegimenLevel;
--FROM SIM_SACT_DrugLevel;
--FROM SIM_SACT_OutcomeLevel;
