/* Written by Edward Pearce - 9th September 2019 */

/* This creates the CTYA (Children, Teenagers, and Young Adults) SAS extract from Simulacrum datasets (simulated SACT and AV tables) */

/* User Instructions */
/* 1. Set the extract start date in the Extract_Dates table below - to be regularly updated when producing new extracts/snapshots of the datasets */
/* 2. Run the code as a procedure or copy and paste directly into SQL Developer (or your favourite SQL IDE) */

/* Code Explanation */
/* The code is split up into three sections: */
/* In Part One, the 'Derived_Regimen_Fields' table derives the 'Provider' AND 'Trust' (initiating treatment) fields from 'Org_Code_of_Drug_Provider' */
/* In Part Two, we create the tables 'Patient_Tumour_table' and 'Birch_Class_ICCC3_Group' in order to derive the fields 'Birch_Classification' and 'ICCC3_Paed_Grouping' */
/* In Part Three, the table 'SIM_SACT_CTYA' is defined, joining various Simulacrum data sources and constructing several derived fields, including 'ICCC3_Site_Group' */
/* The 'SIM_SACT_CTYA' table is constrained according to the user-input extract start date from the Extract_Dates table */

/* **************************************** Part One **************************************** */

WITH
/* Change this date at your leisure/when creating a new monthly update */
/* Note: Version 1 of the Simulacrum is based on legacy SACT tables and will only require updating with new versions */
/* We report the following extract sizes (number of rows) for given extract start dates when running the code on Simulacrum Version 1 (SIM_SACT_FINAL tables): */
/* Extract_Start = 01-04-2018 returns 151 rows; Extract_Start = 01-04-2017 returns 1,421 rows; Extract_Start = 01-01-2013 returns 72,353 rows */
/* We report the following extract sizes (number of rows) for given extract start dates when running the code on Simulacrum Version 2 (SIM_SACT_X_SimII tables): */
/* Extract_Start = 01-04-2017 returns 31,901 rows; */
Extract_Dates AS
(SELECT 
	TO_DATE('01-04-2017','DD-MM-YYYY') AS Extract_Start
FROM DUAL), 


Derived_Regimen_Fields AS
(SELECT
	SIM_SACT_C.Merged_Regimen_ID AS Merged_Regimen_ID,
/*  The Simulacrum currently does NOT contain the tumour-level field 'Organisation_Code_of_Provider' (initiating treatment) */
/*  We obtain estimates for the 'Provider' AND 'Trust' (initiating treatment) fields using the drug-level field 'Org_Code_of_Drug_Provider' with the earliest 'Administration_Date' in the regimen */
/*  This means that the 'Provider/Trust that initiated treatment' may change between regimens for the same Merged_Tumour_ID where they would not in the real SACT data */
	MAX(SIM_SACT_D.Org_Code_of_Drug_Provider) KEEP (DENSE_RANK FIRST ORDER BY SIM_SACT_D.Administration_Date, SIM_SACT_D.Merged_Drug_Detail_ID) AS Provider,
	MAX(SUBSTR(SIM_SACT_D.Org_Code_of_Drug_Provider, 1, 3)) KEEP (DENSE_RANK FIRST ORDER BY SIM_SACT_D.Administration_Date, SIM_SACT_D.Merged_Drug_Detail_ID) AS Trust
FROM ANALYSISPAULCLARKE.SIM_SACT_CYCLE_SimII SIM_SACT_C
LEFT JOIN ANALYSISPAULCLARKE.SIM_SACT_DRUG_DETAIL_SimII SIM_SACT_D
ON SIM_SACT_D.Merged_Cycle_ID = SIM_SACT_C.Merged_Cycle_ID
GROUP BY SIM_SACT_C.Merged_Regimen_ID),

/* **************************************** Part Two **************************************** */

Patient_Tumour_table AS
(SELECT
/*  Patient-level fields */
	SIM_SACT_P.Merged_Patient_ID AS Merged_Patient_ID,
/*  Patient level data is extracted from the linked simulated AV tables, as absent from the simulated SACT tables */
	SIM_AV_P.Sex AS Gender_Current,

/*  Tumour-level fields */
	SIM_SACT_T.Merged_Tumour_ID AS Merged_Tumour_ID, 
	SIM_SACT_T.Primary_Diagnosis AS Primary_Diagnosis,
    SUBSTR(SIM_SACT_T.Primary_Diagnosis, 1, 3) as Primary_Diagnosis_3char,	
	SIM_SACT_T.Morphology_clean AS Morphology_clean,
	SUBSTR(SIM_SACT_T.Morphology_clean, 1, 4) AS Morphology_code,
	SUBSTR(SIM_SACT_T.Morphology_clean, -1, 1) AS Behaviour,
	
/*  Regimen-level fields */
    SIM_SACT_R.Merged_Regimen_ID AS Merged_Regimen_ID,
/*  The Age field defined below is based on age at regimen start date, whilst the Age field in SIM_AV_TUMOUR denotes age at diagnosis */
/*  Therefore we add the difference in time between DiagnosisDateBest and Start_Date_of_Regimen to calculate age at regimen start date */
	TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) AS Age_at_Regimen_Start
FROM 
/*  SIM_SACT_P is used as a link between SIM_SACT tables and SIM_AV tables to derive regimen-level fields 'Age' AND 'AgeGroup' (at regimen start date) */
ANALYSISPAULCLARKE.SIM_SACT_PATIENT_SimII SIM_SACT_P
INNER JOIN ANALYSISPAULCLARKE.SIM_SACT_TUMOUR_SimII SIM_SACT_T
ON SIM_SACT_T.Merged_Patient_ID = SIM_SACT_P.Merged_Patient_ID
INNER JOIN ANALYSISPAULCLARKE.SIM_SACT_REGIMEN_SimII SIM_SACT_R
ON SIM_SACT_R.Merged_Tumour_ID = SIM_SACT_T.Merged_Tumour_ID
/*  Used to derive patient-level field 'Gender_Current' */
LEFT JOIN ANALYSISPAULCLARKE.SIM_AV_PATIENT_SimII SIM_AV_P
ON SIM_AV_P.LinkNumber = SIM_SACT_P.Link_Number
/*  Used to derive regimen-level field 'Age' (at regimen start date) */
LEFT JOIN ANALYSISPAULCLARKE.SIM_AV_TUMOUR_SimII SIM_AV_T
ON SIM_AV_T.LinkNumber = SIM_SACT_P.Link_Number
/*  Restrict our cohort to patients whose Age_at_Regimen_Start is less than 25 */
WHERE TRUNC(SIM_AV_T.Age + (MONTHS_BETWEEN(SIM_SACT_R.Start_Date_of_Regimen, SIM_AV_T.DiagnosisDateBest)/12)) < 25),


Birch_Class_ICCC3_Group AS
(SELECT
	Merged_Patient_ID,
	Merged_Tumour_ID, 
	Primary_Diagnosis,
	Primary_Diagnosis_3char,	
	Morphology_clean,
	Morphology_code,
	Behaviour,
	Merged_Regimen_ID,
	Age_at_Regimen_Start,
/*  Birch_Classification is a derived field for patients aged over 15 based on Primary_Diagnosis AND Morphology_clean (Morphology_code and Behaviour) */
	CASE
    WHEN Age_at_Regimen_Start > 15 THEN
        CASE 
		/*I LEUKAEMIAS*/
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('9821','9826','9827','9831','9832','9833','9834','9835','9836','9837','9948')) 
			THEN 'Acute lymphoid leukaemia'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('9840','9861','9866','9867','9871','9872','9873','9874','9891','9895','9896','9897','9910','9942')) 
			THEN 'Acute myeloid leukaemia'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('9863','9875','9876')) 
			THEN 'Chronic myeloid leukaemia'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('9820','9822','9823','9824','9825','9830','9831')) 
			THEN 'Other and unspecified lymphoid leukaemias'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('9860','9862','9864','9865')) 
			THEN 'Other and unspecified myeloid leukaemias'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('9722','9733','9742','9805','9810','9830','9841', '9842','9850','9868','9870','9880','9890','9892','9893','9894','9900','9920','9930','9931','9932','9940','9941','9945','9946','9963','9964', '9950','9970','9975')) 
			THEN 'Other specified leukaemias, NEC'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('9800','9801','9802','9803','9804','BLLX','TLLX')) 
			THEN 'Unspecified leukaemias'
        /*II LYMPHOMAS*/
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('9723','9727','9728','9729','9750','9755') OR Morphology_code BETWEEN '9593' AND '9649' OR Morphology_code BETWEEN '9670' AND '9719')
            THEN 'Non-Hodgkin lymphoma, specified subtype'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code BETWEEN '9590' AND '9592')
            THEN 'Non-Hodgkin lymphoma, subtype not specified'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code BETWEEN '9651' AND '9667')
            THEN 'Hodgkin lymphoma, specified subtype'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code = '9650')
            THEN 'Hodgkin lymphoma, subtype not specified'
		/*III CNS*/ 
        WHEN ((Primary_Diagnosis IN ('C723','D333','D433') AND Morphology_code = '9380') OR (Behaviour IN ('0','1','3','6','9') AND Morphology_code = '9421'))
            THEN 'Pilocytic astrocytoma'
        WHEN ((Primary_Diagnosis_3char IN ('C70','C71','C72','D32','D33','D42','D43') OR Primary_Diagnosis IN ('D352','D353','D354','D443','D444','D445')) AND (Morphology_code BETWEEN '9410' AND '9420' OR Morphology_code BETWEEN '9422' AND '9425'))
            THEN 'Other specified low grade astrocytic tumours'
        WHEN (Behaviour IN ('0','1','3','6','9') AND Morphology_code IN ('9401','9440','9441','9442','9481'))
            THEN 'Glioblastoma and anaplastic astrocytoma'
        WHEN (Behaviour IN ('0','1','3','6','9') AND Morphology_code = '9400')
            THEN 'Astrocytoma, NOS'
        WHEN (Behaviour IN ('0','1','3','6','9') AND Morphology_code IN ('9450','9451'))
            THEN 'Oligodendroglioma'
        WHEN (Behaviour IN ('0','1','3','6','9') AND Morphology_code IN ('9381','9382','9383','9384','9430','9443','9444','9460','9505','9509'))
            THEN 'Other specified glioma'
        WHEN (Behaviour IN ('0','1','3','6','9') AND Primary_Diagnosis NOT IN ('C723','D333','D433') AND Morphology_code = '9380')
            THEN 'Glioma, NOS'
        WHEN (Behaviour IN ('0','1','3','6','9') AND Morphology_code IN ('9391','9392','9393','9394'))
            THEN 'Ependymoma'
        WHEN (Primary_Diagnosis IN ('C716','D331','D431') AND Morphology_code IN ('9260','9364','9365','9470','9471','9472','9473','9474','9480'))
            THEN 'Medulloblastoma'
        WHEN ((Primary_Diagnosis_3char IN ('C70','C71','C72','D32','D33','D42','D43') AND Primary_Diagnosis NOT IN ('C716','D331','D431')) AND Morphology_code IN ('9260','9364','9365','9470','9471','9472','9473','9474','9480'))
            THEN 'Supratentorial primitive neuroectodermal tumours (PNET) '
        WHEN ((Behaviour IN ('0','1','3','6','9') AND Morphology_code IN ('9508')) OR ((Primary_Diagnosis_3char IN ('C70','C71','C72','D32','D33','D42','D43') OR Primary_Diagnosis IN ('D352','D353','D354','D443','D444','D445')) AND Morphology_code IN ('8963')))  	
            THEN 'Atypical Teratoid / Rhabdoid Tumour (ATRT)'
        WHEN (Behaviour IN ('0','1','3','6','9') AND Morphology_code = '9350')
            THEN 'Craniopharyngioma'
        WHEN (Primary_Diagnosis IN ('C751','C752','D352','D353','D443','D444') AND Morphology_code BETWEEN '8000' AND '8589')
            THEN 'Other Pituitary tumours'
        WHEN ((Primary_Diagnosis IN ('C753','D354','D445') AND Morphology_code BETWEEN '8000' AND '8589') OR ((Primary_Diagnosis_3char IN ('C70','C71','C72','D32','D33','D42','D43') OR Primary_Diagnosis IN ('C753','D354','D445')) AND Morphology_code IN ('9360','9361','9362')))
            THEN 'Pineal tumours'
        WHEN (Behaviour IN ('0','1','3','6','9') AND Morphology_code = '9390')
            THEN 'Choroid plexus tumours'
        WHEN (Behaviour IN ('0','1','3','6','9') AND Morphology_code BETWEEN '9530' AND '9539')
            THEN 'Meningioma'
        WHEN (Primary_Diagnosis_3char IN ('C70','C71','C72','D32','D33','D42','D43') AND Morphology_code BETWEEN '9540' AND '9571')
            THEN 'Nerve sheath tumours of CNS'
        WHEN (Primary_Diagnosis_3char IN ('C70','C71','C72','D32','D33','D42','D43') AND (Morphology_code NOT BETWEEN '8000' AND '8589' AND Morphology_code NOT IN ('8963','9260','9350','9360','9361','9362','9364','9365','9380','9381','9382','9383','9384','9391','9392','9393','9394','9400','9401','9410','9411','9412','9413','9414','9415','9416','9417','9418','9419','9420','9421','9422','9423','9424','9425','9430','9440','9441','9442','9450','9451','9460','9460','9470','9471','9472','9473','9474','9481','9508','9530','9531','9532','9533','9534','9535','9536','9537','9538','9539','9540','9541','9542','9543','9544','9545','9546','9547','9548','9549','9550','9551','9552','9553','9554','9555','9556','9557','9558','9559','9560','9561','9562','9563','9564','9565','9566','9567','9568','9569','9570','9571','9443','9444')))
            THEN 'Other specified intracranial and intraspinal neoplasms'
        WHEN (Primary_Diagnosis_3char BETWEEN 'C70' AND 'C72' AND Morphology_code IN ('8000','8001','8002','8003','8004','8010','9990'))
            THEN 'Unspecified malignant intracranial and  intraspinal neoplasms (Behaviour code 3) '
        WHEN (Primary_Diagnosis_3char IN ('D32','D33','D42','D43') AND Morphology_code IN ('8000','8001','8002','8003','8004','8010','9990'))
            THEN 'Unspecified benign and borderline intracranial and intraspinal neoplasms (Behaviour code < 3)'
		/*IV BONE TUMOURS*/
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code BETWEEN '9180' AND '9200')
            THEN 'Osteosarcoma'
        WHEN (Behaviour IN ('3','6','9') AND (Morphology_code BETWEEN '9210' AND '9240' or Morphology_code IN ('9242','9243')))
            THEN 'Chondrosarcoma'
        WHEN (Behaviour IN ('3','6','9') AND Primary_Diagnosis_3char NOT BETWEEN 'C70' AND 'C72' AND Morphology_code IN ('9260','9364','9365','9470','9471','9472','9473','9474'))
            THEN 'Ewing sarcoma'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('8812','9250','9261','9370','9371','9372'))
            THEN 'Other specified bone tumours'
        WHEN (Primary_Diagnosis_3char IN ('C40','C41') AND Morphology_code IN ('8000','8001','8002','8003','8004','8800','8801','8802','8803','8805','8806'))
            THEN 'Unspecified bone tumours' 
		/*SOFT TISSUE SARCOMAS*/
        WHEN (Behaviour IN ('3','6','9') AND (Morphology_code IN ('8810','8811','8813','8814','8815') OR Morphology_code BETWEEN '8820' AND '8827'))
            THEN 'Fibrosarcoma'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('8830','8831','8835','8836'))
            THEN 'Malignant fibrous histiocytoma'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('8832','8833'))
            THEN 'Dermatofibrosarcoma'
        WHEN (Behaviour IN ('3','6','9') AND (Morphology_code BETWEEN '8900' AND '8921' or Morphology_code = '8991'))
            THEN 'Rhabdomyosarcoma'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code BETWEEN '8850' AND '8881')
            THEN 'Liposarcoma'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code BETWEEN '8890' AND '8896')
            THEN 'Leiomyosarcoma'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code BETWEEN '9040' AND '9043')
            THEN 'Synovial sarcoma'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code = '9044')
            THEN 'Clear cell sarcoma'
        WHEN ((Behaviour IN ('3','6','9') AND Morphology_code BETWEEN '9120' AND '9160') OR (Behaviour IN ('3','6','9') AND Primary_Diagnosis_3char NOT BETWEEN 'C70' AND 'C72' AND Morphology_code = '9161'))
            THEN 'Blood vessel tumours'
        WHEN (Behaviour IN ('3','6','9') AND Primary_Diagnosis_3char NOT BETWEEN 'C70' AND 'C72' AND Morphology_code BETWEEN '9540' AND '9571')
            THEN 'Nerve sheath tumours'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code = '9581')
            THEN 'Alveolar soft part sarcoma'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('8804','8840','8841','8842','8990','9014','9015','9170','9171','9172','9173','9174','9175','9251','9252','9561','9580','9582'))
            THEN 'Other Specified'
        WHEN (Behaviour IN ('3','6','9') AND Primary_Diagnosis_3char NOT IN ('C40','C41') AND Morphology_code IN ('8800','8801','8802','8803','8805','8806'))
            THEN 'Unspecified soft tissue sarcoma'
		/*VI GERM CELL TUMOURS*/
        WHEN (Primary_Diagnosis_3char IN ('C56','C62') AND Morphology_code BETWEEN '9060' AND '9105')
            THEN 'Germ cell and  trophoblastic neoplasms of gonads'
        WHEN (Primary_Diagnosis_3char = 'C62' AND (Morphology_code BETWEEN '8010' AND '8239' OR Morphology_code BETWEEN '8246' AND '8580'))
            THEN 'Germ cell and  trophoblastic neoplasms of gonads'
        WHEN (((Primary_Diagnosis_3char IN ('C70','C71','C72','D32','D33','D42','D43') OR Primary_Diagnosis IN ('C751','C752','C753','D352','D353','D354','D443','D444','D445')) AND Morphology_code BETWEEN '9060' AND '9105') OR (Primary_Diagnosis IN ('D443') AND Morphology_code IN ('9054'))) 
            THEN 'Intracranial'
        WHEN (Behaviour IN ('3','6','9') AND (Primary_Diagnosis_3char BETWEEN 'C00' AND 'C55' OR Primary_Diagnosis_3char BETWEEN 'C57' AND 'C61' OR Primary_Diagnosis_3char BETWEEN 'C63' AND 'C69' OR Primary_Diagnosis_3char BETWEEN 'C73' AND 'C74' OR Primary_Diagnosis_3char BETWEEN 'C76' AND 'C97' OR Primary_Diagnosis IN ('C750','C754','C755','C758','C759')) AND Morphology_code BETWEEN '9060' AND '9105')
            THEN 'Other non-gonadal sites'
		/*VII MELANOMA AND SKIN*/
        WHEN (Behaviour IN ('3','5','6','9') AND Morphology_code BETWEEN '8720' AND '8790')
            THEN 'Melanoma'
        WHEN (Primary_Diagnosis_3char = 'C44' AND Morphology_code BETWEEN '8010' AND '8589')
            THEN 'Skin carcinoma'     
		/*VIII CARCINOMAS*/
        WHEN (Primary_Diagnosis_3char = 'C73' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Thyroid carcinoma'
        WHEN (Primary_Diagnosis_3char = 'C11' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Nasopharyngeal carcinoma'
        WHEN ((Primary_Diagnosis_3char BETWEEN 'C00' AND 'C10' OR Primary_Diagnosis_3char BETWEEN 'C12' AND 'C14') AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Other sites in lip, oral cavity and pharynx'
        WHEN ((Primary_Diagnosis_3char BETWEEN 'C30' AND 'C32' OR Primary_Diagnosis = 'C760') AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Nasal cavity, middle ear, sinuses, larynx and other and ill-defined head and neck'
        WHEN (Primary_Diagnosis_3char IN ('C33','C34') AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Carcinomas of trachea, bronchus and lung'
        WHEN (Primary_Diagnosis_3char = 'C50' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Carcinoma of breast'
        WHEN (Primary_Diagnosis_3char = 'C64' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Carcinoma of kidney'
        WHEN (Primary_Diagnosis_3char = 'C67' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Carcinoma bladder'
        WHEN (Primary_Diagnosis_3char = 'C56' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982' AND Morphology_code NOT IN ('8442','8451','8462','8472','8473')))
            THEN 'Carcinoma of ovary'
        WHEN (Primary_Diagnosis_3char = 'C53' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Carcinoma of cervix' 
        WHEN (Primary_Diagnosis_3char IN ('C51','C52','C54','C55','C57','C58','C60','C61','C63','C65','C66','C68') AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Carcinoma of other and ill-defined sites in GU tract'
        WHEN (Primary_Diagnosis_3char BETWEEN 'C18' AND 'C21' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Carcinoma of colon and rectum'
        WHEN (Primary_Diagnosis_3char = 'C16' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Carcinoma of stomach'
        WHEN (Primary_Diagnosis_3char = 'C22' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Carcinoma of liver and intrahepatic bile ducts'
        WHEN (Primary_Diagnosis_3char = 'C25' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Carcinoma of pancreas'
        WHEN (Primary_Diagnosis_3char IN ('C15','C17','C23','C24','C26') AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Carcinoma of other and ill-defined sites in  GI tract'
        WHEN (Primary_Diagnosis_3char = 'C74' AND (Morphology_code BETWEEN '8010' AND '8589' or Morphology_code = '8982'))
            THEN 'Adrenocortical carcinoma'
        WHEN ((Primary_Diagnosis_3char IN ('C37','C38','C39','C40','C41','C43','C45','C46','C47','C48','C49','C62','C69','C77','C78','C79','C80','C81','C82','C83','C84','C85','C86','C87','C88','C89','C90','C91','C92','C93','C94','C95','C96','C97') OR Primary_Diagnosis IN ('C750','C752','C754','C755','C756','C757','C758','C759','C761','C762','C763','C764','C765','C766','C767','C768')) AND (Morphology_code BETWEEN '8010' AND '8589' OR Morphology_code = '8982'))
            THEN 'Carcinoma of other and ill-defined sites, NEC'
		/*IX MISC SPECIFIED*/
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code BETWEEN '8959' AND '8962')
            THEN 'Wilms tumours'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code IN ('9490','9500'))
            THEN 'Neuroblastoma'
        WHEN ((Behaviour IN ('3','6','9') AND (Morphology_code IN ('8964','8970','8971','8972','8973','8981') OR Morphology_code BETWEEN '9501' AND '9523')) OR ((Primary_Diagnosis_3char NOT IN ('C70','C71','C72','D32','D33','D42','D43') OR Primary_Diagnosis NOT IN ('D352','D353','D354','D443','D444','D445')) AND Morphology_code IN ('8963')))  
            THEN 'Other paediatric and embryonal, NEC'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code BETWEEN '8680' AND '8711')
            THEN 'Paraganglioma and glomus'
        WHEN ((Behaviour IN ('3','6','9') AND (Morphology_code BETWEEN '8590' AND '8650' or Morphology_code IN ('8670','9000'))) OR (Primary_Diagnosis_3char = 'C62' AND Morphology_code BETWEEN '8240' AND '8245'))             
            THEN 'Other specified gonadal tumours'
        WHEN (Behaviour IN ('3','6','9') AND Morphology_code BETWEEN '9720' AND '9764')
            THEN 'Myeloma, mast cell tumours and miscellaneous lymphoreticular neoplasms NEC'
        WHEN (Behaviour IN ('3','6','9') AND (Morphology_code IN ('8980','9020','9050','9051','9052','9053','9110','9342') OR Morphology_code BETWEEN '8930' AND '8951' OR Morphology_code BETWEEN '9270' AND '9330'))
            THEN 'Other specified neoplasms NEC'
		/*X UNSPECIFIED*/
        WHEN (Behaviour IN ('3','6','9') AND (Primary_Diagnosis_3char NOT IN ('C40','C41','C70','C71','C72') AND Primary_Diagnosis NOT IN ('C751','C752','C753')) AND (Morphology_code BETWEEN '8000' AND '8005' or Morphology_code = '9990'))
            THEN 'Unspecifed malignant neoplasms NEC'
		/*XI MYLOPROLIFERATIVE*/
        WHEN (Morphology_code IN ('9950','9960','9961','9962','9964','9980','9982','9983','9984','9985','9986','9987','9989'))
            THEN 'Myloproliferative'
        ELSE 'Unable to derive Birch code' END 
	ELSE NULL END as Birch_Classification,
/*  ICCC3_Paed_Grouping is a derived field for patients aged 15 and under based on Primary_Diagnosis AND Morphology_clean (Morphology_code and Behaviour) */
    CASE
    WHEN Age_at_Regimen_Start <= 15 THEN 
        CASE 
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9820', '9823', '9826', '9827', '9831', '9832', '9833', '9834', '9835', '9836', '9837', '9940', '9948'))
            THEN '1a Lymphoid leukemias'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9840', '9861', '9866', '9867', '9870', '9871', '9872', '9873', '9874', '9891', '9895', '9896', '9897', '9910', '9920', '9931'))
            THEN '1b Acute myeloid leukemias'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9863', '9875', '9876', '9950', '9960', '9961', '9962', '9963', '9964'))
            THEN '1c Chronic myeloproliferative diseases'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9945', '9946', '9975', '9980', '9982', '9983', '9984', '9985', '9986', '9987', '9989'))
            THEN '1d Myelodysplastic syndrome and other myeloproliferative diseases'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9800', '9801', '9805', '9860', '9930'))
            THEN '1e Unspecified and other specified leukemias'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9650', '9651', '9652', '9653', '9654', '9655', '9659', '9661', '9662', '9663', '9664', '9665', '9667'))
            THEN '2a Hodgkin lymphoma'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9591', '9670', '9671', '9673', '9675', '9678', '9679', '9680', '9684', '9689', '9690', '9691', '9695', '9698', '9699', '9700', '9701', '9702', '9705', '9708', '9709', '9714', '9716', '9717', '9718', '9719', '9727', '9728', '9729', '9731', '9732', '9733', '9734', '9760', '9761', '9762', '9764', '9765', '9766', '9767', '9768', '9769', '9970'))
            THEN '2b Non-Hodgkin lymphomas (except Burkitt lymphoma)'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code      = '9687')
            THEN '2c Burkitt lymphoma'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9740', '9741', '9742', '9750', '9754', '9755', '9756', '9757', '9758'))
            THEN '2d Miscellaneous lymphoreticular neoplasms'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9590', '9596'))
            THEN '2e Unspecified lymphomas'
        WHEN (Behaviour IN ('0', '1', '3', '6', '9') AND Morphology_code IN ('9383', '9390', '9391', '9392', '9393', '9394'))
            THEN '3a Ependymomas and choroid plexus tumor'
        WHEN ((Behaviour IN ('0', '1', '3', '6', '9') AND Primary_Diagnosis  = 'C723' AND Morphology_code = '9380') OR (Behaviour IN ('0', '1', '3', '6', '9') AND Morphology_code IN ('9384', '9400', '9401', '9402', '9403', '9404', '9405', '9406', '9407', '9408', '9409', '9410', '9411', '9420', '9421', '9422', '9423', '9424', '9440', '9441', '9442')))
            THEN '3b Astrocytomas'
        WHEN ((Behaviour IN ('0', '1', '3', '6', '9') AND (Primary_Diagnosis_3char IN ('C70', 'C71', 'C72','D42','D43') OR Primary_Diagnosis BETWEEN 'C700' AND 'C729') AND Morphology_code IN ('9501', '9502', '9503', '9504')) OR (Behaviour IN ('0', '1', '3', '6', '9') AND Morphology_code IN ('9470', '9471', '9472', '9473', '9474', '9480', '8963', '9508')))
            THEN '3c Intracranial and intraspinal embryonal tumors'
        WHEN ((Behaviour IN ('0', '1', '3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C700' AND 'C722' OR Primary_Diagnosis BETWEEN 'C724' AND 'C729' OR Primary_Diagnosis  IN ('C751', 'C753')) AND Morphology_code = '9380') OR (Behaviour IN ('0', '1', '3', '6', '9') AND Morphology_code IN ('9381', '9382', '9430', '9444', '9450', '9451', '9460')))
            THEN '3d Other gliomas'
        WHEN (Behaviour IN ('0', '1', '3', '6', '9') AND Morphology_code IN ('8270', '8271', '8272', '8273', '8274', '8275', '8276', '8277', '8278', '8279', '8280', '8281', '8300', '9350', '9351', '9352', '9360', '9361', '9362', '9412', '9413', '9492', '9493', '9505', '9506', '9507', '9530', '9531', '9532', '9533', '9534', '9535', '9536', '9537', '9538', '9539', '9582'))
            THEN '3e Other specified intracranial and intraspinal neoplasms'
        WHEN (Behaviour IN ('0', '1', '3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C700' AND 'C729' OR Primary_Diagnosis BETWEEN 'C751' AND 'C753' OR Primary_Diagnosis_3char='D43') AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005'))
            THEN '3f Unspecified intracranial and intraspinal neoplasms'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9490', '9500'))
            THEN '4a Neuroblastoma and ganglioneuroblastoma'
        WHEN ((Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C000' AND 'C699' OR Primary_Diagnosis BETWEEN 'C739' AND 'C768' OR Primary_Diagnosis = 'C809' OR Primary_Diagnosis_3char= 'C80') AND Morphology_code IN ('9501', '9502', '9503', '9504')) OR (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('8680', '8681', '8682', '8683', '8690', '8691', '8692', '8693', '8700', '9520', '9521', '9522', '9523')))
            THEN '4b Other peripheral nervous cell tumors'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9510', '9511', '9512', '9513', '9514'))
            THEN '5 Retinoblastoma'
        WHEN ((Primary_Diagnosis = 'C649' OR Primary_Diagnosis_3char = 'C64' AND Morphology_code IN ('8963', '9364')) OR (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('8959', '8960', '8964', '8965', '8966', '8967')))
            THEN '6a Nephroblastoma and other nonepithelial renal tumors'
        WHEN ((Behaviour IN ('3', '6', '9') AND Primary_Diagnosis = 'C649' OR Primary_Diagnosis_3char ='C64' AND (Morphology_code BETWEEN '8010' AND '8041' OR Morphology_code BETWEEN '8050' AND '8075' OR Morphology_code BETWEEN '8130' AND '8141' OR Morphology_code BETWEEN '8190' AND '8201' OR Morphology_code BETWEEN '8221' AND '8231' OR Morphology_code BETWEEN '8480' AND '8490' OR Morphology_code BETWEEN '8560' AND '8576' OR Morphology_code IN ('8082', '8120', '8121', '8122', '8143', '8155', '8210', '8211', '8240', '8241', '8244', '8245', '8246', '8260', '8261', '8262', '8263', '8290', '8310', '8320', '8323', '8401', '8430', '8440', '8504', '8510', '8550'))) OR (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('8311', '8312', '8316', '8317', '8318', '8319', '8361')))
            THEN '6b Renal carcinomas' 
        WHEN (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis = 'C649' OR Primary_Diagnosis_3char = 'C64' AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005')))
            THEN '6c Unspecified malignant renal tumours'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code = '8970')
            THEN '7a Hepatoblastoma'
        WHEN ((Behaviour IN ('3', '6', '9') AND Primary_Diagnosis IN ('C220', 'C221') AND (Morphology_code BETWEEN '8010' AND '8041' OR Morphology_code BETWEEN '8050' AND '8075' OR Morphology_code BETWEEN '8190' AND '8201' OR Morphology_code BETWEEN '8480' AND '8490' OR Morphology_code BETWEEN '8560' AND '8576' OR Morphology_code IN ('8082', '8120', '8121', '8122', '8140', '8141', '8143', '8155', '8210', '8211', '8230', '8231', '8240', '8241', '8244', '8245', '8246', '8260', '8261', '8262', '8263', '8264', '8310', '8320', '8323', '8401', '8430', '8440', '8504', '8510', '8550'))) OR (Behaviour IN ('3', '6', '9') AND Morphology_code BETWEEN '8160' AND '8180'))
            THEN '7b Hepatic carcinomas'
        WHEN (Behaviour IN ('3', '6', '9') AND Primary_Diagnosis IN ('C220', 'C221') AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005'))
            THEN '7c Unspecified malignant hepatic tumors'
        WHEN (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C400' AND 'C419' OR Primary_Diagnosis BETWEEN 'C760' AND 'C768' OR Primary_Diagnosis_3char = 'C80' OR Primary_Diagnosis = 'C809') AND Morphology_code IN ('9180', '9181', '9182', '9183', '9184', '9185', '9186', '9187', '9191', '9192', '9193', '9194', '9195', '9200'))
            THEN '8a Osteosarcomas'
        WHEN ((Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C400' AND 'C419' OR Primary_Diagnosis BETWEEN 'C760' AND 'C768' OR Primary_Diagnosis_3char = 'C80' OR Primary_Diagnosis = 'C809') AND Morphology_code IN ('9210', '9220', '9240')) OR (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('9221', '9230', '9241', '9242', '9243')))
            THEN '8b Chondrosarcomas'
        WHEN ((Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C400' AND 'C419') AND Morphology_code IN ('9363', '9364', '9365')) OR (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C400' AND 'C419' OR Primary_Diagnosis BETWEEN 'C760' AND 'C768' OR Primary_Diagnosis_3char= 'C80' OR Primary_Diagnosis = 'C809') AND Morphology_code = '9260'))
            THEN '8c Ewing tumor and related sarcomas of bone'
        WHEN ((Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C400' AND 'C419') AND Morphology_code IN ('8810', '8811', '8823', '8830')) OR (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('8812', '9250', '9261', '9262', '9270', '9271', '9272', '9273', '9274', '9275', '9280', '9281', '9282', '9290', '9300', '9301', '9302', '9310', '9311', '9312', '9320', '9321', '9322', '9330', '9340', '9341', '9342', '9370', '9371', '9372')))
            THEN '8d Other specified malignant bone tumors'
        WHEN (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C400' AND 'C419') AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005', '8800', '8801', '8803', '8804', '8805'))
            THEN '8e Unspecified malignant bone tumors'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code IN ('8900', '8901', '8902', '8903', '8904', '8905', '8910', '8912', '8920', '8991'))
            THEN '9a Rhabdomyosarcomas'
        WHEN ((Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis_3char BETWEEN 'C00' AND 'C39' OR Primary_Diagnosis BETWEEN 'C000' AND 'C399' OR Primary_Diagnosis BETWEEN 'C440' AND 'C768' OR Primary_Diagnosis_3char= 'C80' OR Primary_Diagnosis = 'C809') AND Morphology_code IN ('8810', '8811', '8813', '8814', '8815', '8821', '8823', '8834', '8835')) OR (Behaviour IN ('3', '6', '9') AND (Morphology_code    IN ('8820', '8822', '8824', '8825', '8826', '8827', '9150', '9160', '9491', '9580') OR Morphology_code BETWEEN '9540' AND '9571')))
            THEN '9b Fibrosarcomas, peripheral nerve sheath tumors, and other fibrous neoplasms'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code      = '9140')
            THEN '9c Kaposi sarcoma'
        WHEN ((Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C000' AND 'C399' OR Primary_Diagnosis BETWEEN 'C440' AND 'C768' OR Primary_Diagnosis_3char IN ('C80', 'C809', 'C52 ', 'C56', 'C64', 'C61')) AND Morphology_code = '8830') OR (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis_3char IN ('C63', 'C61', 'C73', 'C80', 'C809', 'C52', 'C56') OR Primary_Diagnosis BETWEEN 'C000' AND 'C639' OR Primary_Diagnosis BETWEEN 'C659' AND 'C699' OR Primary_Diagnosis BETWEEN 'C739' AND 'C768') AND Morphology_code IN ('8963')) OR (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C490' AND 'C499' OR Primary_Diagnosis_3char= 'C49') AND Morphology_code IN ('9180', '9210', '9220', '9240')) OR (Behaviour IN('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C000' AND 'C399' OR Primary_Diagnosis BETWEEN 'C470' AND 'C759') AND Morphology_code = '9260') OR (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis_3char BETWEEN 'C00' AND 'C39' OR Primary_Diagnosis BETWEEN 'C000' AND 'C399' OR Primary_Diagnosis BETWEEN 'C470' AND 'C639' OR Primary_Diagnosis BETWEEN 'C659' AND 'C699' OR Primary_Diagnosis_3char= 'C73' OR Primary_Diagnosis BETWEEN 'C739' AND 'C768' OR Primary_Diagnosis_3char= 'C80' OR Primary_Diagnosis = 'C809' OR Primary_Diagnosis_3char= 'C61' OR Primary_Diagnosis_3char= 'C63') AND Morphology_code = '9364') OR (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C000' AND 'C399' OR Primary_Diagnosis BETWEEN 'C470' AND 'C639' OR Primary_Diagnosis BETWEEN 'C659' AND 'C768' OR Primary_Diagnosis_3char= 'C80' OR Primary_Diagnosis = 'C809' OR Primary_Diagnosis_3char= 'C63' OR Primary_Diagnosis_3char = 'C61' OR Primary_Diagnosis_3char = 'C73') AND Morphology_code = '9365') OR (Behaviour IN ('3', '6', '9') AND (Morphology_code    IN ('8587', '8710', '8711', '8712', '8713', '8806', '8831', '8832', '8833', '8836', '8840', '8841', '8842', '8860', '8861', '8862', '8870', '8880', '8881', '8921', '8982', '8990', '9040', '9041', '9042', '9043', '9044', '9120', '9121', '9122', '9123', '9124', '9125', '9130', '9131', '9132', '9133', '9135', '9136', '9141', '9142', '9161', '9170', '9171', '9172', '9173', '9174', '9175', '9231', '9251', '9252', '9373', '9581') OR Morphology_code BETWEEN '8850' AND '8858' OR Morphology_code BETWEEN '8890' AND '8898')))
            THEN '9d Other specified soft tissue sarcomas'
        WHEN (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C000' AND 'C399' OR Primary_Diagnosis BETWEEN 'C440' AND 'C768' OR Primary_Diagnosis_3char= 'C80' OR Primary_Diagnosis_3char = 'C61' OR Primary_Diagnosis = 'C809') AND Morphology_code IN ('8800', '8801', '8802', '8803', '8804', '8805'))
            THEN '9e Unspecified soft tissue sarcomas'
        WHEN (Behaviour IN ('0', '1', '3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C700' AND 'C729' OR Primary_Diagnosis BETWEEN 'C751' AND 'C753') AND Morphology_code IN ('9060', '9061', '9062', '9063', '9064', '9065', '9070', '9071', '9072', '9080', '9081', '9082', '9083', '9084', '9085', '9100', '9101'))
            THEN '10a Intracranial and intraspinal germ cell tumors'
        WHEN (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis ='C809' OR Primary_Diagnosis_3char IN ('C73', 'C80') OR Primary_Diagnosis BETWEEN 'C000' AND 'C559' OR Primary_Diagnosis BETWEEN 'C570' AND 'C619' OR Primary_Diagnosis BETWEEN 'C630' AND 'C699' OR Primary_Diagnosis BETWEEN 'C739' AND 'C750' OR Primary_Diagnosis BETWEEN 'C754' AND 'C768') AND Morphology_code IN ('9060', '9061', '9062', '9063', '9064', '9065', '9070', '9071', '9072', '9080', '9081', '9082', '9083', '9084', '9085', '9100', '9101', '9102', '9103', '9104', '9105'))
            THEN '10b Malignant extracranial and extragonadal germ cell tumors'
        WHEN (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis = 'C569' OR Primary_Diagnosis BETWEEN 'C620' AND 'C629') AND Morphology_code IN ('9060', '9061', '9062', '9063', '9064', '9065', '9070', '9071', '9072', '9073', '9080', '9081', '9082', '9083', '9084', '9085', '9090', '9091', '9100', '9101'))
            THEN '10c Malignant gonadal germ cell tumors'
        WHEN ((Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis = 'C569' OR Primary_Diagnosis BETWEEN 'C620' AND 'C629') AND (Morphology_code IN ('8082', '8120', '8121', '8122', '8143', '8210', '8211', '8244', '8245', '8246', '8260', '8261', '8262', '8263', '8290', '8310', '8313', '8320', '8323', '8380', '8381', '8382', '8383', '8384', '8430', '8440', '8504', '8510', '8550', '9000', '9014', '9015') OR Morphology_code BETWEEN '8010' AND '8041' OR Morphology_code BETWEEN '8050' AND '8075' OR Morphology_code BETWEEN '8130' AND '8141' OR Morphology_code BETWEEN '8190' AND '8201' OR Morphology_code BETWEEN '8221' AND '8241' OR Morphology_code BETWEEN '8480' AND '8490' OR Morphology_code BETWEEN '8560' AND '8573')) OR (Behaviour IN ('3', '6', '9') AND (Morphology_code    IN ('8441', '8442', '8443', '8444', '8450', '8451', '8462', '8461') OR Morphology_code BETWEEN '8460' AND '8473')))
            THEN '10d Gonadal carcinomas'
        WHEN ((Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis = 'C569' OR Primary_Diagnosis BETWEEN 'C620' AND 'C629') AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005')) OR (Behaviour IN ('3', '6', '9') AND Morphology_code BETWEEN '8590' AND '8671'))
            THEN '10e Other and unspecified malignant gonadal tumors'
        WHEN (Behaviour IN ('3', '6', '9') AND Morphology_code BETWEEN '8370' AND '8375')
            THEN '11a Adrenocortical carcinomas'
        WHEN ((Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis_3char = 'C73' OR Primary_Diagnosis = 'C739') AND (Morphology_code IN ('8082', '8120', '8121', '8122', '8190', '8200', '8201', '8211', '8230', '8231', '8244', '8245', '8246', '8260', '8261', '8262', '8263', '8290', '8310', '8320', '8323', '8430', '8440', '8480', '8481', '8510') OR Morphology_code BETWEEN '8010' AND '8041' OR Morphology_code BETWEEN '8050' AND '8075' OR Morphology_code BETWEEN '8130' AND '8141' OR Morphology_code BETWEEN '8560' AND '8573')) OR (Behaviour IN ('3', '6', '9') AND Primary_Diagnosis_3char BETWEEN 'C00' AND 'C97' AND (Morphology_code BETWEEN '8330' AND '8337' OR Morphology_code BETWEEN '8340' AND '8347' OR Morphology_code = '8350')))
            THEN '11b Thyroid carcinomas'
        WHEN (Behaviour IN ('3', '6', '9') AND Primary_Diagnosis BETWEEN 'C110' AND 'C119' AND (Morphology_code IN ('8082', '8083', '8120', '8121', '8122', '8190', '8200', '8201', '8211', '8230', '8231', '8244', '8245', '8246', '8260', '8261', '8262', '8263', '8290', '8310', '8320', '8323', '8430', '8440', '8480', '8481') OR Morphology_code BETWEEN '8010' AND '8041' OR Morphology_code BETWEEN '8050' AND '8075' OR Morphology_code BETWEEN '8130' AND '8141' OR Morphology_code BETWEEN '8500' AND '8576'))
            THEN '11c Nasopharyngeal carcinomas'
        WHEN (Behaviour IN ('3', '6', '9') AND (Morphology_code BETWEEN '8720' AND '8780' OR Morphology_code = '8790'))
            THEN '11d Malignant melanomas'
        WHEN (Behaviour IN ('3', '6', '9') AND Primary_Diagnosis BETWEEN 'C440' AND 'C449' AND (Morphology_code IN ('8078', '8082', '8140', '8143', '8147', '8190', '8200', '8240', '8246', '8247', '8260', '8310', '8320', '8323', '8430', '8480', '8542', '8560', '8940', '8941') OR Morphology_code BETWEEN '8010' AND '8041' OR Morphology_code BETWEEN '8050' AND '8075' OR Morphology_code BETWEEN '8090' AND '8110' OR Morphology_code BETWEEN '8390' AND '8420' OR Morphology_code BETWEEN '8570' AND '8573'))
            THEN '11e Skin carcinomas'
        WHEN (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C000' AND 'C109' OR Primary_Diagnosis BETWEEN 'C129' AND 'C218' OR Primary_Diagnosis BETWEEN 'C239' AND 'C399' OR Primary_Diagnosis BETWEEN 'C480' AND 'C488' OR Primary_Diagnosis BETWEEN 'C500' AND 'C559' OR Primary_Diagnosis BETWEEN 'C570' AND 'C619' OR Primary_Diagnosis BETWEEN 'C630' AND 'C639' OR Primary_Diagnosis BETWEEN 'C659' AND 'C729' OR Primary_Diagnosis BETWEEN 'C750' AND 'C768' OR Primary_Diagnosis_3char = 'C80' OR Primary_Diagnosis     = 'C809') AND (Morphology_code IN ('8290', '8310', '8313', '8314', '8315', '8320', '8321', '8322', '8323', '8324', '8325', '8360', '8380', '8381', '8382', '8383', '8384', '8452', '8453', '8454', '8588', '8589', '8940', '8941', '8983', '9000', '9020', '9030') OR Morphology_code BETWEEN '8010' AND '8084' OR Morphology_code BETWEEN '8120' AND '8157' OR Morphology_code BETWEEN '8190' AND '8264' OR Morphology_code BETWEEN '8430' AND '8440' OR Morphology_code BETWEEN '8480' AND '8586' OR Morphology_code BETWEEN '9010' AND '9016'))
            THEN '11f Other and unspecified carcinomas'
        WHEN ((Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C000' AND 'C399' OR Primary_Diagnosis BETWEEN 'C470' AND 'C759' OR Primary_Diagnosis_3char IN ('C64', 'C61', 'C52')) AND Morphology_code = '9363') OR (Behaviour IN ('3', '6', '9') AND (Morphology_code    IN ('8930', '8931', '8932', '8933', '8934', '8935', '8936', '8950', '8951', '9050', '9051', '9052', '9053', '9054', '9055', '9110') OR Morphology_code BETWEEN '8971' AND '8981')))
            THEN '12a Other specified malignant tumors'
        WHEN (Behaviour IN ('3', '6', '9') AND (Primary_Diagnosis BETWEEN 'C000' AND 'C218' OR Primary_Diagnosis BETWEEN 'C239' AND 'C399' OR Primary_Diagnosis BETWEEN 'C420' AND 'C559' OR Primary_Diagnosis BETWEEN 'C570' AND 'C619' OR Primary_Diagnosis BETWEEN 'C630' AND 'C639' OR Primary_Diagnosis BETWEEN 'C659' AND 'C699' OR Primary_Diagnosis BETWEEN 'C739' AND 'C750' OR Primary_Diagnosis BETWEEN 'C754' AND 'C809' OR Primary_Diagnosis_3char IN ('C64', 'C61', 'C52')) AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005'))
            THEN '12b Other unspecified malignant tumors'
		ELSE 'Other childhood tumour' END 
	ELSE NULL END AS ICCC3_Paed_Grouping
/*  ICCC3_Site_Group is a derived field for patients aged 15 and sorts ICCC3_Paed_Grouping into broader categories */
FROM Patient_Tumour_table
WHERE Age_at_Regimen_Start < 25),

/* **************************************** Part Three **************************************** */

SIM_SACT_CTYA AS
(SELECT
/*  Patient-level fields */
	PT.Merged_Patient_ID AS Merged_Patient_ID,
/*  Patient level data is extracted from the linked simulated AV tables, as absent from the simulated SACT tables */
	PT.Gender_Current AS Gender_Current,

/*  Tumour-level fields */
	PT.Merged_Tumour_ID AS Merged_Tumour_ID, 
	PT.Primary_Diagnosis AS Primary_Diagnosis,
    PT.Primary_Diagnosis_3char as Primary_Diagnosis_3char,	
	PT.Morphology_clean AS Morphology_clean,
	PT.Morphology_code AS Morphology_code,
	PT.Behaviour AS Behaviour,
/*  The field 'GroupDescription2' is derived from Primary Diagnosis using the Diagnosis Subgroup lookup */	
	DSG.Group_Description2 AS Group_Description2,
	
/*  The Simulacrum currently does NOT contain the tumour-level field 'Organisation_Code_of_Provider' (initiating treatment) */
/*  We obtain estimates for the 'Provider' AND 'Trust' (initiating treatment) fields using the drug-level field 'Org_Code_of_Drug_Provider' with the earliest 'Administration_Date' in the regimen */
/*  This means that the 'Provider/Trust that initiated treatment' may change between regimens for the same Merged_Tumour_ID where they would not in the real SACT data */
    R1.Provider AS Provider,
    R1.Trust as Trust,
/*  The Simulacrum currently does NOT contain the tumour-level field 'Consultant_gmc_code' */
/*  Consultant_gmc_code is used to derive 'ref_no_with_c' */	
/*  Therefore the field 'ref_no_with_c' cannot be extracted from the Simulacrum */

/*  Regimen-level fields */
    SIM_SACT_R.Merged_Regimen_ID AS Merged_Regimen_ID,
	TO_CHAR(SIM_SACT_R.Start_Date_of_Regimen, 'MON/YYYY') AS Start_Month_of_Regimen,

/*  The AgeGroup field defined below is based on age at regimen start date, whilst the Age field in SIM_AV_TUMOUR denotes age at diagnosis */
/*  Therefore we add the difference in time between DiagnosisDateBest and Start_Date_of_Regimen to calculate age at regimen start date */
/*  The AgeGroup field is grouped differently for the CTYA extract compared to the others */
	PT.Age_at_Regimen_Start AS Age_at_Regimen_Start,
	CASE
	WHEN PT.Age_at_Regimen_Start BETWEEN 0 AND 4 THEN '0-4'
    WHEN PT.Age_at_Regimen_Start BETWEEN 5 AND 9 THEN '5-9' 
	WHEN PT.Age_at_Regimen_Start BETWEEN 10 AND 15 THEN '10-15'			
	WHEN PT.Age_at_Regimen_Start BETWEEN 16 AND 19 THEN '16-19'			 
	WHEN PT.Age_at_Regimen_Start BETWEEN 20 AND 24 THEN '20-24' 
	WHEN PT.Age_at_Regimen_Start >= 25 THEN '25+' 
	ELSE 'Missing' END AS AgeGroup,
	
    SIM_SACT_R.Mapped_Regimen AS Mapped_Regimen,
/*  The fields 'Benchmark' AND 'Analysis' are derived from MappedRegimen using the Benchmark Analysis Lookup */	
    BAL.Benchmark as Benchmark,
    BAL.Analysis as Analysis,
	
/*  Cycle-level fields */	
    SIM_SACT_C.Merged_Cycle_ID AS Merged_Cycle_ID,
    TO_CHAR(SIM_SACT_C.Start_Date_of_Cycle, 'MON/YYYY') as Start_Month_of_Cycle,

/*  Drug-level fields */	
    SIM_SACT_D.Merged_Drug_Detail_ID AS Merged_Drug_Detail_ID,
    TO_CHAR(SIM_SACT_D.Administration_Date, 'MON/YYYY') as Administration_Month,
	TO_CHAR(SIM_SACT_D.Administration_Date, 'DAY') as Weekday,
    SIM_SACT_D.Administration_Route AS Administration_Route,
/*  The Simulacrum currently does NOT contain the field 'Drug_Name', but does contain 'Drug_Group' */
    SIM_SACT_D.Drug_Group AS Drug_Group,
    SIM_SACT_D.Org_Code_of_Drug_Provider AS Org_Code_of_Drug_Provider,
    SUBSTR(SIM_SACT_D.Org_Code_of_Drug_Provider, 1, 3) as Trust_of_Drug_Provider,
	
/*  Outcome-level fields */	
    SIM_SACT_O.Regimen_Outcome_Summary AS Regimen_Outcome_Summary,

/*  Exclusions - A field primarily based on regimen-level field Mapped_Regimen, though the E5 sometimes also depends on tumour-level field Primary_Diagnosis */
/*  The CDF exclusions depend not only on the type of treatment, but also the tumour being treated and treatment dates */
/*  Note: An exclusions lookup table for all types of exclusions may be useful in the future */
    CASE
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) = 'NOT CHEMO' OR UPPER(BAL.Benchmark) = 'NOT CHEMO') THEN 'E1'
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) IN ('PAMIDRONATE','ZOLEDRONIC ACID') OR UPPER(BAL.Benchmark) IN ('PAMIDRONATE','ZOLEDRONIC ACID')) THEN 'E2'
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) = 'DENOSUMAB' OR UPPER(BAL.Benchmark) = 'DENOSUMAB') THEN 'E3'	
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) = 'HORMONES' OR UPPER(BAL.Benchmark) = 'HORMONES') THEN 'E4'
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) IN ('BCG INTRAVESICAL','MITOMYCIN INTRAVESICAL','EPIRUBICIN INTRAVESICAL'))
      OR (UPPER(SIM_SACT_R.Mapped_Regimen) IN ('MITOMYCIN', 'EPIRUBICIN') AND (PT.Primary_Diagnosis LIKE 'C67%' OR PT.Primary_Diagnosis LIKE 'D41%')) THEN 'E5'														
	WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) LIKE '%TRIAL%' OR UPPER(BAL.Benchmark) LIKE '%TRIAL%') THEN 'E6'
    WHEN (UPPER(SIM_SACT_R.Mapped_Regimen) = 'NOT MATCHED' OR UPPER(BAL.Benchmark) = 'NOT MATCHED') THEN 'E7'
/*  CDF Exclusions (coded as 'E8' exclusions) are currently excluded from this extract since they will require extra work to be derived from Simulacrum data */
	ELSE SIM_SACT_R.Mapped_Regimen END
	AS Exclusion,

/*  Birch_Classification is a derived field for patients aged over 15 based on Primary_Diagnosis AND Morphology_clean (Morphology_code and Behaviour) */
	Birch_ICCC3.Birch_Classification AS Birch_Classification,
/*  ICCC3_Paed_Grouping is a derived field for patients aged 15 and under based on Primary_Diagnosis AND Morphology_clean (Morphology_code and Behaviour) */
    Birch_ICCC3.ICCC3_Paed_Grouping AS ICCC3_Paed_Grouping,
/*  ICCC3_Site_Group is a derived field for patients aged 15 and sorts ICCC3_Paed_Grouping into broader categories */
    CASE 
	WHEN PT.Age_at_Regimen_Start <= 15 THEN
        CASE
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 2) IN ('1a','1b','1c','1d','1e') THEN 'Leukemias, myeloproliferative diseases, and myelodysplastic diseases'
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 2) IN ('2a','2b','2c','2d','2e') THEN 'Lymphomas and reticuloendothelial neoplasms'
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 2) IN ('3a','3b','3c','3d','3e','3f') THEN 'CNS and miscellaneous intracranial and intraspinal neoplasms'
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 2) IN ('4a','4b') THEN 'Neuroblastoma and other peripheral nervous cell tumors'
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 1) = '5' THEN 'Retinoblastoma'
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 2) IN ('6a','6b','6c') THEN 'Renal tumors'
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 2) IN ('7a','7b','7c') THEN 'Hepatic tumors'
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 2) IN ('8a','8b','8c','8d','8e') THEN 'Malignant bone tumors'
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 2) IN ('9a','9b','9c','9d','9e') THEN 'Soft tissue and other extraosseous sarcomas'
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 3) IN ('10a','10b','10c','10d','10e') THEN 'Germ cell tumors, trophoblastic tumors, and neoplasms of gonads'
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 3) IN ('11a','11b','11c','11d','11e','11f') THEN 'Other malignant epithelial neoplasms and malignant melanomas'
        WHEN SUBSTR(Birch_ICCC3.ICCC3_Paed_Grouping,1, 3) IN ('12a','12b') THEN 'Other and unspecified malignant neoplasms' 
		ELSE 'Not classified by ICCC3' END 
	ELSE NULL END AS ICCC3_Site_Group
FROM 
Patient_Tumour_table PT
INNER JOIN Birch_Class_ICCC3_Group Birch_ICCC3
ON Birch_ICCC3.Merged_Regimen_ID = PT.Merged_Regimen_ID
INNER JOIN ANALYSISPAULCLARKE.SIM_SACT_REGIMEN_SimII SIM_SACT_R
ON SIM_SACT_R.Merged_Regimen_ID = PT.Merged_Regimen_ID
/*  Used to derive cycle-level field 'Start_Month_of_Cycle' */
INNER JOIN ANALYSISPAULCLARKE.SIM_SACT_CYCLE_SimII SIM_SACT_C
ON SIM_SACT_C.Merged_Regimen_ID = SIM_SACT_R.Merged_Regimen_ID
LEFT JOIN ANALYSISPAULCLARKE.SIM_SACT_DRUG_DETAIL_SimII SIM_SACT_D
ON SIM_SACT_D.Merged_Cycle_ID = SIM_SACT_C.Merged_Cycle_ID
/*  Used to obtain regimen outcome-level field 'Regimen_Outcome_Summary' */
LEFT JOIN ANALYSISPAULCLARKE.SIM_SACT_OUTCOME_SimII SIM_SACT_O 
ON SIM_SACT_O.Merged_Regimen_ID = SIM_SACT_R.Merged_Regimen_ID
/*  Used to derive tumour-level field 'GroupDescription2' */
LEFT JOIN ANALYSISBUKKYJUWA.DIAGNOSIS_SUBGROUP_SACT DSG
ON DSG.ICD_Code = PT.Primary_Diagnosis
/*  Used to derive regimen-level fields 'Benchmark' AND 'Analysis' */
LEFT JOIN ANALYSISBUKKYJUWA.BENCHMARK_ANALYSIS_LOOKUP_NEW BAL
ON BAL.Mapped_Regimen = SIM_SACT_R.Mapped_Regimen
/*  Used to derive 'tumour-level' fields 'Provider' AND 'Trust' */
LEFT JOIN Derived_Regimen_Fields R1
ON R1.Merged_Regimen_ID = SIM_SACT_R.Merged_Regimen_ID, 
Extract_dates
WHERE (SIM_SACT_R.Start_Date_of_Regimen >= Extract_Start OR SIM_SACT_C.Start_Date_of_Cycle >= Extract_Start OR SIM_SACT_D.Administration_Date >= Extract_Start))

SELECT * FROM SIM_SACT_CTYA;