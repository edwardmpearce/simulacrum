/* Written by Edward Pearce - 9th September 2019 */
/* Based on work by ADAM BROWN - 17TH JANUARY 2019, and Carrie - 25th June 2018 */

/* This creates the Outcome level SAS extract from Simulacrum datasets (simulated SACT and AV tables) */

/* User Instructions */
/* 1. Set the dates in the Extract_Dates table below - to be regularly updated when producing new extracts/snapshots of the datasets */
/*   a) Extract_Start specifies the start of the date-range for the data to be extracted (Default to extract data for April 2018 onwards) */
/*   b) Latest_Treatment_Check should be updated to be 3 months before the latest available SACT data. (Set at '31-12-2018' at time of writing) */
/*      Latest_Treatment_Check is used to check whether or not the patient has received any treatment within the last 3 months */
/*   c) Outcome_Extract_End specifies the end of the date-range for the Outcome-level extract ONLY. (Default to extract outcomes between April and November 2018) */
/* 2. Run the code as a procedure or copy and paste directly into SQL Developer (or your favourite SQL IDE) */

/* Code Explanation */
/* The code is split up into two sections: */
/* In Part One, the 'Derived_Regimen_Fields' table is introduced to define some intermediate variables */
/* In Part Two, the table 'SIM_SACT_OutcomeLevel' is defined, joining various Simulacrum data sources and constructing several derived fields */
/* The 'SIM_SACT_OutcomeLevel' table is constrained according to the user-input dates from the Extract_Dates table */

/* **************************************** Part One **************************************** */

WITH
/* Change this date at your leisure/when creating a new monthly update */
/* Note: Version 1 of the Simulacrum is based on legacy SACT tables and will only require updating with new versions */
/* We report the following extract sizes (number of rows) for given extract start/end dates when running the code on Simulacrum Version 1 (SIM_SACT_X_FINAL tables): */
/* Outcomes between 01-04-2018 and 30-11-2018 returns 580 rows; Outcomes between 01-04-2017 and 30-11-2017 returns 11,335 rows */
/* We report the following extract sizes (number of rows) for given extract start/end dates when running the code on Simulacrum Version 2 (SIM_SACT_X_SimII tables): */
/* Outcomes between 01-04-2017 and 30-11-2017 returns 341,018 rows */
Extract_Dates AS
(SELECT 
	TO_DATE('01-04-2017','DD-MM-YYYY') AS Extract_Start,
	TO_DATE('31-12-2017','DD-MM-YYYY') AS Latest_Treatment_Check, -- Change date on each new extraction to 3 months before most recent SACT data
	TO_DATE('30-11-2017','DD-MM-YYYY') AS Outcome_Extract_End
FROM DUAL), 


Derived_Fields AS
(SELECT
	SIM_SACT_C.Merged_Regimen_ID AS Merged_Regimen_ID,
/*  The Simulacrum currently does NOT contain the field 'Adult_Perf_Stat_Start_of_Reg' */
/*  An estimate is generated using the earliest Perf_Status_Start_of_Cycle in a regimen */
	MAX(SIM_SACT_C.Perf_Status_Start_of_Cycle) KEEP (DENSE_RANK FIRST ORDER BY SIM_SACT_C.Start_Date_of_Cycle, SIM_SACT_C.Merged_Cycle_ID) AS Perf_Status_Start_of_Reg,
/*  The Simulacrum currently does NOT contain the tumour-level field 'Organisation_Code_of_Provider' (initiating treatment) */
/*  We obtain estimates for the 'Provider' and 'Trust' (initiating treatment) fields using the drug-level field 'Org_Code_of_Drug_Provider' with the earliest 'Administration_Date' in the regimen */
/*  This means that the 'Provider/Trust that initiated treatment' may change between regimens for the same Merged_Tumour_ID where they would not in the real SACT data */
	MAX(SIM_SACT_D.Org_Code_of_Drug_Provider) KEEP (DENSE_RANK FIRST ORDER BY SIM_SACT_D.Administration_Date, SIM_SACT_D.Merged_Drug_Detail_ID) AS Provider,
	MAX(SUBSTR(SIM_SACT_D.Org_Code_of_Drug_Provider, 1, 3)) KEEP (DENSE_RANK FIRST ORDER BY SIM_SACT_D.Administration_Date, SIM_SACT_D.Merged_Drug_Detail_ID) AS Trust,
	MAX(SIM_SACT_C.Start_Date_of_Cycle) AS Latest_Cycle,
	MAX(SIM_SACT_D.Administration_Date) AS Latest_Admin_Date
FROM ANALYSISPAULCLARKE.SIM_SACT_CYCLE_SimII SIM_SACT_C
LEFT JOIN ANALYSISPAULCLARKE.SIM_SACT_DRUG_DETAIL_SimII SIM_SACT_D
ON SIM_SACT_D.Merged_Cycle_ID = SIM_SACT_C.Merged_Cycle_ID
GROUP BY SIM_SACT_C.Merged_Regimen_ID),


Derived_Regimen_Fields AS
(SELECT
	SIM_SACT_R.Merged_Regimen_ID AS Merged_Regimen_ID,
	DF.Perf_Status_Start_of_Reg AS Perf_Status_Start_of_Reg,
	DF.Provider AS Provider,
	DF.Trust AS Trust,
/*  Latest_Treatment_Date - Selects the latest (maximum) date out of all Cycle start dates, Drug Administration dates and the Regimen start date within a regimen, ignoring null values */
/*  The COALESCE function returns the first non-null expression in the expression list */
/*  and is used here to ensure that null values do not propagate through the GREATEST function to give an invalid results unless all three dates are null */
	GREATEST(
	    COALESCE(DF.Latest_Admin_Date, DF.Latest_Cycle, SIM_SACT_R.Start_Date_of_Regimen),
		COALESCE(DF.Latest_Cycle, SIM_SACT_R.Start_Date_of_Regimen, DF.Latest_Admin_Date), 
		COALESCE(SIM_SACT_R.Start_Date_of_Regimen, DF.Latest_Admin_Date, DF.Latest_Cycle)
	) AS Latest_Treatment_Date	
FROM ANALYSISPAULCLARKE.SIM_SACT_REGIMEN_SimII SIM_SACT_R
LEFT JOIN Derived_Fields DF
ON DF.Merged_Regimen_ID = SIM_SACT_R.Merged_Regimen_ID),

/* **************************************** Part Two **************************************** */

SIM_SACT_OutcomeLevel AS
(SELECT
/*  The Simulacrum currently does NOT contain the field 'Date_of_Death' */
/*  This is instead derived from NewVitalStatus and VitalStatusDate in SIM_AV_PATIENT */
    CASE WHEN SIM_AV_P.NewVitalStatus = 'D' THEN TO_CHAR(SIM_AV_P.VitalStatusDate, 'MON/YYYY')
	ELSE NULL END AS Month_of_Death,
/*  The Simulacrum currently does NOT contain the tumour-level field 'Organisation_Code_of_Provider' (initiating treatment) */
/*  We obtain estimates for the 'Provider' and 'Trust' (initiating treatment) fields using the drug-level field 'Org_Code_of_Drug_Provider' with the earliest 'Administration_Date' in the regimen */
/*  This means that the 'Provider/Trust that initiated treatment' may change between regimens for the same Merged_Tumour_ID where they would not in the real SACT data */
    R1.Provider AS Provider,
    R1.Trust as Trust,
    SIM_SACT_R.Merged_Regimen_ID AS Merged_Regimen_ID,
	TO_CHAR(SIM_SACT_R.Start_Date_of_Regimen, 'MON/YYYY') AS Start_Month_of_Regimen,
	SIM_SACT_R.Intent_of_Treatment AS Intent_of_Treatment,
    SIM_SACT_R.Mapped_Regimen AS Mapped_Regimen,
/*  The fields 'Benchmark' and 'Analysis' are derived from MappedRegimen using the Benchmark Analysis Lookup */	
    BAL.Benchmark as Benchmark,
    BAL.Analysis as Analysis,
/*  The Simulacrum currently does NOT contain the field 'Adult_Perf_Stat_Start_of_Reg' */
/*  An estimate is generated using the first Perf_Status_Start_of_Cycle in a regimen */
    R1.Perf_Status_Start_of_Reg AS Perf_Status_Start_of_Reg,
    CAST(SIM_SACT_R.Weight_At_Start_of_Regimen AS VARCHAR(20)) AS Weight_at_Start_of_Regimen,
    CAST(SIM_SACT_R.Height_At_Start_of_Regimen AS VARCHAR(20)) AS Height_at_Start_of_Regimen,	
    CASE WHEN SIM_SACT_R.Weight_at_Start_of_Regimen > 0.0 THEN 'Y' ELSE '' END AS Weight_Regimen_Completeness,
    CASE WHEN SIM_SACT_R.Height_At_Start_of_Regimen > 0.0 THEN 'Y' ELSE '' END AS Height_Regimen_Completeness,
    SIM_SACT_C.Merged_Cycle_ID AS Merged_Cycle_ID,
    TO_CHAR(SIM_SACT_C.Start_Date_of_Cycle, 'MON/YYYY') as Start_Month_of_Cycle,
    SIM_SACT_C.Perf_Status_Start_of_Cycle AS Perf_Status_Start_of_Cycle,
/*  The Simulacrum currently does NOT contain the field 'Weight_at_Start_of_Cycle' */
/*  Therefore the fields 'Weight_at_Start_of_Cycle' and 'Weight_Cycle_Completeness' cannot be extracted from the Simulacrum */
    SIM_SACT_O.Merged_Outcome_ID AS Merged_Outcome_ID,
    SIM_SACT_O.Regimen_Outcome_Summary AS Regimen_Outcome_Summary,
/*  We create a field called Outcome_Expected which has codes 1,2,3 to denote: */
/*  3. Regimen Outcome Summary received */
/*  1. NO Regimen Outcome Summary received and Patient died */
/*  2. NO Regimen Outcome Summary received and Patient Inactive - Patient has not received any treatment within the last 3 months */
/*  Code number 2 depends on a hard coded date which should be updated with each new extraction in the Extract_Dates table above */
	CASE
	WHEN SIM_SACT_O.Regimen_Outcome_Summary IS NOT NULL THEN '3'	
	WHEN SIM_AV_P.NewVitalStatus = 'D' THEN '1'
    WHEN R1.Latest_Treatment_Date < Extract_Dates.Latest_Treatment_Check THEN '2' -- Change date on each new extraction to 3 months before most recent SACT data
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
FROM ANALYSISPAULCLARKE.SIM_SACT_REGIMEN_SimII SIM_SACT_R
INNER JOIN ANALYSISPAULCLARKE.SIM_SACT_CYCLE_SimII SIM_SACT_C
ON SIM_SACT_C.Merged_Regimen_ID = SIM_SACT_R.Merged_Regimen_ID
LEFT JOIN ANALYSISPAULCLARKE.SIM_SACT_OUTCOME_SimII SIM_SACT_O 
ON SIM_SACT_O.Merged_Regimen_ID = SIM_SACT_R.Merged_Regimen_ID
/* SIM_SACT_T and SIM_SACT_P are used only to link to SIM_AV_P to derive 'Month_of_Death' and 'Outcome_Expected' */
INNER JOIN ANALYSISPAULCLARKE.SIM_SACT_TUMOUR_SimII SIM_SACT_T
ON SIM_SACT_R.Merged_Tumour_ID = SIM_SACT_T.Merged_Tumour_ID
INNER JOIN ANALYSISPAULCLARKE.SIM_SACT_PATIENT_SimII SIM_SACT_P
ON SIM_SACT_T.Merged_Patient_ID = SIM_SACT_P.Merged_Patient_ID
/*  Used to derive patient-level field 'Month_of_Death', and 'Outcome_Expected' */
LEFT JOIN ANALYSISPAULCLARKE.SIM_AV_PATIENT_SimII SIM_AV_P
ON SIM_AV_P.LinkNumber = SIM_SACT_P.Link_Number
/*  Used to obtain regimen-level field 'Benchmark', which is used when deriving the 'Exclusion' field */
LEFT JOIN ANALYSISBUKKYJUWA.BENCHMARK_ANALYSIS_LOOKUP_NEW BAL
ON BAL.Mapped_Regimen = SIM_SACT_R.Mapped_Regimen
/*  Used to derive 'tumour-level' fields 'Provider' and 'Trust' and regimen-level field 'Adult_Perf_Stat_Start_of_Reg' */
LEFT JOIN Derived_Regimen_Fields R1
ON R1.Merged_Regimen_ID = SIM_SACT_R.Merged_Regimen_ID, 
Extract_dates
WHERE (SIM_SACT_R.Start_Date_of_Regimen BETWEEN Extract_Start AND Outcome_Extract_End OR SIM_SACT_C.Start_Date_of_Cycle BETWEEN Extract_Start AND Outcome_Extract_End))

SELECT * FROM SIM_SACT_OutcomeLevel
WHERE Outcome_Expected IS NOT NULL;