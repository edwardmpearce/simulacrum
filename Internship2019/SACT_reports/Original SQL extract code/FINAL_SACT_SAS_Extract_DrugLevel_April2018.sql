/* Written by Bukky Juwa - 17th Jan 2019 */

/* This creates the Drug level SAS extract from April 2018 onward */

/* This is a two step process */
/* Part one extracts all the data from the SACT tables and stores it in a new table */
/* Part two adds an 'exclusion' field as it pulls the data from SQL server to a .rpt file */

/* **************************************** Part one **************************************** */

--CREATE TABLE SACT_SAS_DrugLevel_April2018 AS
WITH SACT_SAS_DrugLevel_April2018 AS
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
    NVL(TO_CHAR(d.Administration_Date, 'DAY'), '') as Weekday,
    NVL(r.Intent_of_Treatment, '') as Intent_of_Treatment,
    NVL(BAL.Benchmark, '') as Benchmark,
    NVL(BAL.Analysis, '') as Analysis,
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
    NVL(d.Merged_Drug_Detail_ID, '') as Merged_Drug_Detail_ID,
    NVL(TO_CHAR(d.Administration_Date,'MON/YYYY'), '') as Administration_Date,
    NVL(d.Administration_Route, '') as Administration_Route,
    NVL(dl.Drug_Group, '') as Drug_Group,
    NVL(d.Drug_Name, '') as Drug_Name,
    NVL(Cast(d.Actual_Dose_per_Administration as varchar(20)),'') as Actual_Dose_per_Administration,
    NVL(d.Org_Code_of_Drug_Provider, '') as Org_Code_of_Drug_Provider,  
    NVL(SUBSTR(d.Org_Code_of_Drug_Provider,1, 3), '') as Trust_of_Drug_Provider,
    NVL(o.Regimen_Outcome_Summary, '') as Regimen_Outcome_Summary,
    T.CONSULTANT_GMC_CODE || ' ' || CO.SURNAME || ' ' || CO.INITIALS as Consultant_Code_Name
    
from SACT.AT_PATIENT P
    inner join SACT.AT_TUMOUR T on T.Encore_Patient_ID = P.Encore_Patient_ID
    inner join SACT.AT_REGIMEN R on R.SACT_Tumour_ID = T.SACT_Tumour_ID
    inner join SACT.AT_CYCLE C on C.Merged_Regimen_ID = R.Merged_Regimen_ID
    left join SACT.AT_DRUG_DETAIL D on D.Merged_Cycle_ID = C.Merged_Cycle_ID
    left join SACT.AT_OUTCOME O on O.Merged_Regimen_ID = R.Merged_Regimen_ID
    left join DIAGNOSIS_SUBGROUP_SACT@CAS1901 DSG on DSG.ICD_Code = T.Primary_Diagnosis
    left join BENCHMARK_ANALYSIS_LOOKUP@CAS1901 BAL on BAL.Mapped_regimen = R.Mapped_regimen
    left join DRUG_LOOKUP@CAS1901 DL on DL.Drug_name = D.Drug_name
    left join CONSULTANT_LOOKUP@CAS1901 CO on T.Consultant_gmc_code = CO.Consultant_gmc_code
where (r.Start_date_of_regimen >= '01-APR-18'
 or c.Start_Date_of_Cycle >= '01-APR-18'
 or d.Administration_Date >= '01-APR-18') 
 )
 
 /* **************************************** End of part one **************************************** */ 

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
    ,Activity.Weekday
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
    ,Activity.Merged_Drug_Detail_ID
    ,Activity.Administration_Date
    ,Activity.Administration_Route
    ,Activity.Drug_Group
    ,Activity.Drug_Name
    ,Activity.Actual_Dose_per_Administration
    ,Activity.Org_Code_of_Drug_Provider
    ,Activity.Trust_of_Drug_Provider
    ,Activity.Regimen_Outcome_Summary
    ,Activity.Consultant_Code_Name
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
from SACT_SAS_DrugLevel_April2018 ACTIVITY
left join CDF_EXCLUSIONS@CAS1901 CDFEX ON CDFEX.merged_regimen_id = activity.merged_regimen_id;
        
    
    
    
    

