/* Written by Bukky Juwa - 17th Jan 2019 */

/* This creates the regimen/cycle level SAS extract from April 2018 onward */

/* This is a two step process */
/* Part one extracts all the data from the SACT tables and stores it in a new table */
/* Part two adds an 'exclusion' field as it pulls the data from SQL server to a .rpt file */

/* **************************************** Part one **************************************** */

-- USING CAS1907
--CREATE TABLE SACT_SAS_Extract_RegimenLevel_April2018 AS

WITH SAS_SACT_REGIMENLEVEL_Apr2018 AS
(
select distinct 
    p.Encore_Patient_ID,
    NVL(p.Gender_Current, '') as Gender_Current,
    NVL(p.Ethnicity, '') as Ethnicity,
    NVL(
    (case when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 0 and 15 then '0-15'
       when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 16 and 18 then '16-18' 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 19 and 24 then '19-24'			
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 25 and 29 then '25-29'			 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 30 and 34 then '30-34' 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 35 and 39 then '35-39' 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 40 and 44 then '40-44' 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 45 and 49 then '45-49' 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 50 and 54 then '50-54' 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 55 and 59 then '55-59' 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 60 and 64 then '60-64' 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 65 and 69 then '65-69' 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 70 and 74 then '70-74' 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 75 and 79 then '75-79' 
	   else '80+' end),'') as AgeGroup,   
    NVL(t.SACT_Tumour_ID, '') as Tumour_ID,
    NVL(t.Primary_Diagnosis, '') as Primary_Diagnosis,
    NVL(dsg.Group_Description2, '') as Group_Description2,
    NVL(t.Organisation_Code_of_Provider, '') as Provider, 
    NVL(SUBSTR(t.Organisation_Code_of_Provider,1, 3), '') as Trust, 
    NVL(r.Merged_Regimen_ID, '') as Merged_Regimen_ID,
    NVL(TO_CHAR(r.Start_Date_of_Regimen,'MON/YYYY'), '') as Start_Date_of_Regimen,
    NVL(r.Intent_of_Treatment, '') as Intent_of_Treatment,
    NVL(r.Perf_Stat_Start_of_Reg_adult, '') as Adult_Perf_Stat_Start_of_Reg,
    NVL(Cast(r.Weight_At_Start_of_Regimen as varchar(20)),'') as Weight_at_Start_of_Regimen,
    (case when r.Weight_at_Start_of_Regimen > 0.0 then 'Y' else '' end) as Weight_Regimen_Completeness,
    NVL(Cast(r.Height_At_Start_of_Regimen as varchar(20)),'') as Height_at_Start_of_Regimen,
    (case when r.Height_At_Start_of_Regimen > 0.0 then 'Y' else '' end) as Height_Regimen_Completeness,
    NVL(r.Mapped_Regimen, '') as Mapped_Regimen,
    NVL(c.Merged_Cycle_ID, '') as Merged_Cycle_ID,
    NVL(TO_CHAR(c.Start_Date_of_Cycle,'MON/YYYY'), '') as Start_Date_of_Cycle,
    NVL(Cast(c.Weight_at_Start_of_Cycle as varchar(20)),'') as Weight_at_Start_of_Cycle,
    (case when c.Weight_at_Start_of_Cycle > 0.0 then 'Y' else '' end) as Weight_Cycle_Completeness,
    NVL(c.Perf_Stat_Start_of_Cycle_Adult, '') as Adult_Perf_Stat_Start_Cycle,
    NVL(o.Regimen_Outcome_Summary, '') as Regimen_Outcome_Summary,
    NVL(BAL.Benchmark, '') as Benchmark,
    NVL(BAL.Analysis, '') as Analysis,
    NVL(TO_CHAR(p.Date_of_Death,'MON/YYYY'), '') as Date_of_Death,	
    case
	when p.date_of_death is not null then '1'
     WHEN (CASE WHEN LDD.LATEST_CYCLE >= LDD.LATEST_ADMIN_DATE AND LDD.LATEST_CYCLE >= R.START_DATE_OF_REGIMEN THEN LDD.LATEST_CYCLE
                 WHEN LDD.LATEST_ADMIN_DATE >= LDD.LATEST_CYCLE AND LDD.LATEST_ADMIN_DATE >= R.START_DATE_OF_REGIMEN THEN LDD.LATEST_ADMIN_DATE
                 WHEN R.START_DATE_OF_REGIMEN >= LDD.LATEST_CYCLE AND R.START_DATE_OF_REGIMEN >= LDD.LATEST_ADMIN_DATE THEN R.START_DATE_OF_REGIMEN
                 ELSE NULL 
                 END) < TO_DATE('31-12-2018','DD-MM-YYYY') THEN '2' --change date on new extraction to 3 months before most recent sact data
		WHEN O.REGIMEN_OUTCOME_SUMMARY IS NOT NULL THEN '3'
		ELSE NULL
	END AS OUTCOME_EXPECTED	
    
from SACT.AT_PATIENT P
    inner join SACT.AT_TUMOUR T on T.Encore_Patient_ID = P.Encore_Patient_ID
    inner join SACT.AT_REGIMEN R on R.SACT_Tumour_ID = T.SACT_Tumour_ID
    inner join SACT.AT_CYCLE C on C.Merged_Regimen_ID = R.Merged_Regimen_ID
    left join SACT.AT_OUTCOME O on O.Merged_Regimen_ID = R.Merged_Regimen_ID
    left join DIAGNOSIS_SUBGROUP_SACT@CAS1907 DSG on DSG.ICD_Code = T.Primary_Diagnosis
    left join BENCHMARK_ANALYSIS_LOOKUP@CAS1907 BAL on BAL.Mapped_regimen = R.Mapped_regimen
    --left join CONSULTANT_LOOKUP@CAS1901 CO on T.Consultant_gmc_code = CO.Consultant_gmc_code
    left join (select C1.merged_regimen_id
                        ,Max (C1.Start_date_of_cycle) OVER (PARTITION BY C1.merged_regimen_ID) AS Latest_cycle
                        ,Max (D1.Administration_date) OVER (PARTITION BY C1.merged_regimen_ID) AS Latest_admin_date
                    from SACT.AT_DRUG_DETAIL D1
                        left join SACT.AT_CYCLE C1 ON C1.Merged_cycle_id = D1.Merged_Cycle_ID) LDD ON LDD.merged_regimen_ID = R.merged_regimen_ID

/* Change these dates when creating a new monthly update */

Where 
    (r.Start_date_of_regimen >= '01-APR-18'
    or c.Start_Date_of_Cycle >= '01-APR-18')
)
--select *
--from SAS_SACT_REGIMENLEVEL;


SELECT 
     Activity.Encore_Patient_ID as Merged_patient_id
    ,Activity.Gender_Current
    ,Activity.Ethnicity
    ,Activity.AgeGroup
    ,Activity.Tumour_ID as Merged_tumour_id
    ,Activity.Primary_Diagnosis
    ,Activity.Group_Description2
    ,Activity.Provider
    ,Activity.Trust
    ,Activity.Merged_Regimen_ID
    ,Activity.Start_Date_of_Regimen
    ,Activity.Intent_of_Treatment
    ,Activity.Benchmark
    ,Activity.Analysis
    ,Activity.Adult_Perf_Stat_Start_of_Reg
    ,Activity.Weight_at_Start_of_Regimen
    ,Activity.Weight_Regimen_Completeness
    ,Activity.Height_at_Start_of_Regimen
    ,Activity.Height_Regimen_Completeness
    ,Activity.Mapped_Regimen
    ,Activity.Merged_Cycle_ID
    ,Activity.Start_Date_of_Cycle
    ,Activity.Weight_at_Start_of_Cycle
    ,Activity.Weight_Cycle_Completeness
    ,Activity.Adult_Perf_Stat_Start_Cycle
    ,Activity.Regimen_Outcome_Summary
    ,Activity.Date_of_death
    ,Activity.Outcome_expected
    ,case 
        --Exclusions
        when (UPPER(Activity.mapped_regimen) = 'NOT CHEMO' or UPPER(Activity.benchmark) = 'NOT CHEMO') then 'E1'
        when (UPPER(Activity.mapped_regimen) in ('PAMIDRONATE','ZOLEDRONIC ACID') or UPPER(Activity.benchmark) in ('PAMIDRONATE','ZOLEDRONIC ACID')) then 'E2'
        when (UPPER(Activity.mapped_regimen) = 'DENOSUMAB' or UPPER(Activity.benchmark) = 'DENOSUMAB') then 'E3'	
        when (UPPER(Activity.mapped_regimen) = 'HORMONES' or UPPER(Activity.benchmark) = 'HORMONES') then 'E4'
        when (UPPER(Activity.mapped_regimen) in ('BCG INTRAVESICAL','MITOMYCIN INTRAVESICAL','EPIRUBICIN INTRAVESICAL'))
            or (UPPER(Activity.mapped_regimen) in ('MITOMYCIN', 'EPIRUBICIN') and (Activity.Primary_Diagnosis like 'C67%' or Activity.Primary_Diagnosis like 'D41%')) then 'E5'														
	    when (UPPER(Activity.mapped_regimen) like '%TRIAL%' or UPPER(Activity.benchmark) like '%TRIAL%') then 'E6'
        when (UPPER(Activity.mapped_regimen) in ('NOT MATCHED') or UPPER(Activity.benchmark) in ('NOT MATCHED')) then 'E7'
        -- CDF Exclusion
        when CDFEX.merged_regimen_id = activity.merged_regimen_id then 'E8'
		ELSE Activity.Mapped_Regimen
        end as Exclusion
from SAS_SACT_REGIMENLEVEL_Apr2018 ACTIVITY
left join CDF_EXCLUSIONS@CAS1907 CDFEX ON CDFEX.merged_regimen_id = activity.merged_regimen_id;















