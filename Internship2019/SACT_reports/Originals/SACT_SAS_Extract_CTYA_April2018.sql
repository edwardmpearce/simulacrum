/* SACT_SAS_Extract_CTYA from April 2018 onward */

-- USING 1905 SNAPSHOT

--CREATE TABLE SACT_SAS_CTYA_April2018 AS
WITH SACT_SAS_CTYA_April2018 AS
(
Select distinct 
    Encore_Patient_ID
    ,Gender_Current
    ,AgeGroup
    ,Sact_tumour_id
    ,Primary_diagnosis
    ,primary_diagnosis_3char
    ,concat(morphology_code, behaviour) as Morphology_code
    ,provider
    ,trust
    ,merged_regimen_id
    ,start_date_of_regimen
    ,weekday
    ,intent_of_treatment
    ,benchmark
    ,mapped_regimen
    ,merged_cycle_id
    ,start_date_of_cycle
    ,merged_drug_detail_id
    ,administration_date
    ,administration_route
    ,drug_group
    ,drug_name
    ,Org_Code_of_Drug_Provider
    ,trust_of_drug_provider
    --,merged_outcome_id
    ,regimen_outcome_summary
    ,NVL(Birch_Classification, '') as Birch_Classification
    ,NVL(ICCC3_Paed_Grouping,'') as ICCC3_Paed_Grouping
    ,Ethnicity
    ,Group_Description2
    ,ref_no_with_c --Added by Bukky 14/06/2019
    ,CASE
        WHEN Age <= 15 THEN
        CASE
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 2) in ('1a','1b','1c','1d','1e') then 'Leukemias, myeloproliferative diseases, and myelodysplastic diseases'
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 2) in ('2a','2b','2c','2d','2e') then 'Lymphomas and reticuloendothelial neoplasms'
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 2) in ('3a','3b','3c','3d','3e','3f') then 'CNS and miscellaneous intracranial and intraspinal neoplasms'
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 2) in ('4a','4b') then 'Neuroblastoma and other peripheral nervous cell tumors'
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 1) = '5' then 'Retinoblastoma'
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 2) in ('6a','6b','6c') then 'Renal tumors'
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 2) in ('7a','7b','7c') then 'Hepatic tumors'
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 2) in ('8a','8b','8c','8d','8e') then 'Malignant bone tumors'
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 2) in ('9a','9b','9c','9d','9e') then 'Soft tissue and other extraosseous sarcomas'
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 3) in ('10a','10b','10c','10d','10e') then 'Germ cell tumors, trophoblastic tumors, and neoplasms of gonads'
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 3) in ('11a','11b','11c','11d','11e','11f') then 'Other malignant epithelial neoplasms and malignant melanomas'
        WHEN SUBSTR(ICCC3_Paed_Grouping,1, 3) in ('12a','12b') then 'Other and unspecified malignant neoplasms' else 'Not classified by ICCC3' end end ICCC3_Site_Group
FROM
(
select distinct
    Encore_Patient_ID, Gender_Current, AgeGroup, Sact_tumour_id, Primary_diagnosis, primary_diagnosis_3char
    ,Morphology_code, behaviour, provider, trust, merged_regimen_id, start_date_of_regimen
    ,weekday, intent_of_treatment, benchmark, mapped_regimen, merged_cycle_id, start_date_of_cycle, merged_drug_detail_id
    ,administration_date, administration_route, drug_group, drug_name, Org_Code_of_Drug_Provider, trust_of_drug_provider--,merged_outcome_id
    ,regimen_outcome_summary, age, Ethnicity, Group_Description2, Ref_no_with_c,       
CASE
        WHEN Age >15 THEN
            CASE /*I LEUKAEMIAS*/
        when (behaviour in ('3','6','9')
            and Morphology_code in ('9821','9826','9827','9831','9832','9833','9834',
            '9835','9836','9837','9948'))
            Then 'Acute lymphoid leukaemia'
        when (behaviour in ('3','6','9')
            and Morphology_code in ('9840','9861','9866','9867','9871','9872','9873',
            '9874','9891','9895','9896','9897','9910','9942')) Then
            'Acute myeloid leukaemia'
        when (behaviour in ('3','6','9')
            and Morphology_code in ('9863','9875','9876'))
            then 'Chronic myeloid leukaemia'
        when (behaviour in ('3','6','9')
            and Morphology_code in ('9820','9822','9823','9824','9825','9830','9831'))
            then 'Other and unspecified lymphoid leukaemias'
        when (behaviour in ('3','6','9')
            and Morphology_code in ('9860','9862','9864','9865'))
            then 'Other and unspecified myeloid leukaemias'
        when (behaviour in ('3','6','9')
            and Morphology_code in ('9722','9733','9742','9805','9810','9830','9841',
            '9842','9850','9868','9870','9880','9890','9892','9893','9894','9900',
            '9920','9930','9931','9932','9940','9941','9945','9946','9963','9964',
            '9950','9970','9975'))
            then 'Other specified leukaemias, NEC'
        when (behaviour in ('3','6','9')
            and Morphology_code in ('9800','9801','9802','9803','9804','BLLX','TLLX'))
            then 'Unspecified leukaemias'
            
/*II LYMPHOMAS*/
        when (behaviour in ('3','6','9')
            and Morphology_code in ('9723','9727','9728','9729','9750','9755')
            or Morphology_code between '9593' and '9649'
            or Morphology_code between '9670' and '9719')
            then 'Non-Hodgkin lymphoma, specified subtype'
        when (behaviour in ('3','6','9')
            and Morphology_code between '9590' and '9592')
            then 'Non-Hodgkin lymphoma, subtype not specified'
        when (behaviour in ('3','6','9')
            and Morphology_code between '9651' and '9667')
            then 'Hodgkin lymphoma, specified subtype'
        when (behaviour in ('3','6','9')
            and Morphology_code = '9650')
            then 'Hodgkin lymphoma, subtype not specified'

/*III CNS*/ 
        when ((Primary_diagnosis in ('C723','D333','D433')
            and Morphology_code = '9380')
            or (behaviour in ('0','1','3','6','9')
            and Morphology_code = '9421'))
            then 'Pilocytic astrocytoma'
        when ((Primary_diagnosis_3char in ('C70','C71','C72','D32','D33','D42','D43') or
            Primary_diagnosis in ('D352','D353','D354','D443','D444','D445'))
            and (Morphology_code between '9410' and '9420' or Morphology_code between '9422' and '9425'))
            then 'Other specified low grade astrocytic tumours'
        when (behaviour in ('0','1','3','6','9')
            and Morphology_code in ('9401','9440','9441','9442','9481'))
            then 'Glioblastoma and anaplastic astrocytoma'
        when (behaviour in ('0','1','3','6','9')
            and Morphology_code = '9400')
            then 'Astrocytoma, NOS'
        when (behaviour in ('0','1','3','6','9')
            and Morphology_code in ('9450','9451'))
            then 'Oligodendroglioma'
        when (behaviour in ('0','1','3','6','9')
            and Morphology_code in ('9381','9382','9383','9384','9430','9443','9444','9460','9505','9509'))
            then 'Other specified glioma'
        when (behaviour in ('0','1','3','6','9')
            and Primary_diagnosis not in ('C723','D333','D433')
            and Morphology_code = '9380')
            then 'Glioma, NOS'
        when (behaviour in ('0','1','3','6','9')
            and Morphology_code in ('9391','9392','9393','9394'))
            then 'Ependymoma'
        when (Primary_diagnosis in ('C716','D331','D431')
            and Morphology_code in ('9260','9364','9365','9470','9471','9472','9473','9474','9480'))
            then 'Medulloblastoma'
        when ((Primary_diagnosis_3char in ('C70','C71','C72','D32','D33','D42','D43') 
            and Primary_diagnosis not in ('C716','D331','D431'))
            and Morphology_code in ('9260','9364','9365','9470','9471','9472','9473','9474','9480'))
            then 'Supratentorial primitive neuroectodermal tumours (PNET) '
        when ((behaviour in ('0','1','3','6','9') and Morphology_code in ('9508'))
            or ((Primary_diagnosis_3char in ('C70','C71','C72','D32','D33','D42','D43') 
            or Primary_diagnosis in ('D352','D353','D354','D443','D444','D445')) 
            and Morphology_code in ('8963')))  	
            then 'Atypical Teratoid / Rhabdoid Tumour (ATRT) '
        when (behaviour in ('0','1','3','6','9')
            and Morphology_code = '9350')
            then 'Craniopharyngioma'
        when (Primary_diagnosis in ('C751','C752','D352','D353','D443','D444')
            and Morphology_code between '8000' and '8589')
            then 'Other Pituitary tumours'
        when ((Primary_diagnosis in ('C753','D354','D445')
            and Morphology_code between '8000' and '8589')
            or ((Primary_diagnosis_3char in ('C70','C71','C72','D32','D33','D42','D43')
            or Primary_diagnosis in ('C753','D354','D445'))
            and Morphology_code in ('9360','9361','9362')))
            then 'Pineal tumours'
        when (behaviour in ('0','1','3','6','9')
            and Morphology_code = '9390')
            then 'Choroid plexus tumours'
        when (behaviour in ('0','1','3','6','9')
            and Morphology_code between '9530' and '9539')
            then 'Meningioma'
        when (Primary_diagnosis_3char in ('C70','C71','C72','D32','D33','D42','D43')
            and Morphology_code between '9540' and '9571')
            then 'Nerve sheath tumours of CNS'
        when (Primary_diagnosis_3char in ('C70','C71','C72','D32','D33','D42','D43')
            and (Morphology_code not between '8000' AND '8589'
            and Morphology_code not in ('8963','9260','9350','9360','9361','9362',
            '9364','9365','9380','9381','9382','9383','9384','9391','9392','9393',
            '9394','9400','9401','9410','9411','9412','9413','9414','9415','9416',
            '9417','9418','9419','9420','9421','9422','9423','9424','9425','9430',
            '9440','9441','9442','9450','9451','9460','9460','9470','9471','9472',
            '9473','9474','9481','9508','9530','9531','9532','9533','9534','9535',
            '9536','9537','9538','9539','9540','9541','9542','9543','9544','9545',
            '9546','9547','9548','9549','9550','9551','9552','9553','9554','9555',
            '9556','9557','9558','9559','9560','9561','9562','9563','9564','9565',
            '9566','9567','9568','9569','9570','9571','9443','9444')))
            then 'Other specified intracranial and intraspinal neoplasms'
        when (Primary_diagnosis_3char between 'C70' and 'C72'
            and Morphology_code in ('8000','8001','8002','8003','8004','8010','9990'))
            then 'Unspecified malignant intracranial and  intraspinal neoplasms (behaviour code 3) '
        when (Primary_diagnosis_3char in ('D32','D33','D42','D43')
            and Morphology_code in ('8000','8001','8002','8003','8004','8010','9990'))
            then 'Unspecified benign and borderline intracranial and intraspinal neoplasms (behaviour code < 3)'

/*IV BONE TUMOURS*/
        when (behaviour in ('3','6','9')
            and Morphology_code between '9180' and '9200')
            then 'Osteosarcoma'
        when (behaviour in ('3','6','9')
            and (Morphology_code between '9210' and '9240' or Morphology_code in ('9242','9243')))
            then 'Chondrosarcoma'
        when (behaviour in ('3','6','9')
            and Primary_diagnosis_3char not between 'C70' and 'C72'
            and Morphology_code in ('9260','9364','9365','9470','9471','9472','9473','9474'))
            then 'Ewing sarcoma'
        when (behaviour in ('3','6','9')
            and Morphology_code in ('8812','9250','9261','9370','9371','9372'))
            then 'Other specified bone tumours'
        when (Primary_diagnosis_3char in ('C40','C41')
            and Morphology_code in ('8000','8001','8002','8003','8004','8800','8801','8802','8803','8805','8806'))
            then 'Unspecified bone tumours' 

/*SOFT TISSUE SARCOMAS*/
        when (behaviour in ('3','6','9')
            and (Morphology_code in ('8810','8811','8813','8814','8815')
            or Morphology_code between '8820' and '8827'))
            then 'Fibrosarcoma'
        when (behaviour in ('3','6','9')
            and Morphology_code in ('8830','8831','8835','8836'))
            then 'Malignant fibrous histiocytoma'
        when (behaviour in ('3','6','9')
            and Morphology_code in ('8832','8833'))
            then 'Dermatofibrosarcoma'
        when (behaviour in ('3','6','9')
            and (Morphology_code between '8900' and '8921' or Morphology_code = '8991'))
            then 'Rhabdomyosarcoma'
        when (behaviour in ('3','6','9')
            and Morphology_code between '8850' and '8881')
            then 'Liposarcoma'
        when (behaviour in ('3','6','9')
            and Morphology_code between '8890' and '8896')
            then 'Leiomyosarcoma'
        when (behaviour in ('3','6','9')
            and Morphology_code between '9040' and '9043')
            then 'Synovial sarcoma'
        when (behaviour in ('3','6','9')
            and Morphology_code = '9044')
            then 'Clear cell sarcoma'
        when ((behaviour in ('3','6','9')
            and Morphology_code between '9120' and '9160')
            or (behaviour in ('3','6','9')
            and Primary_diagnosis_3char not between 'C70' and 'C72'
            and Morphology_code = '9161'))
            then 'Blood vessel tumours'
        when (behaviour in ('3','6','9')
            and Primary_diagnosis_3char not between 'C70' and 'C72'
            and Morphology_code between '9540' and '9571')
            then 'Nerve sheath tumours'
        when (behaviour in ('3','6','9')
            and Morphology_code = '9581')
            then 'Alveolar soft part sarcoma'
        when (behaviour in ('3','6','9')
            and Morphology_code in ('8804','8840','8841','8842','8990','9014','9015','9170',
            '9171','9172','9173','9174','9175','9251','9252','9561','9580','9582'))
            then 'Other Specified'
        when (behaviour in ('3','6','9')
            and Primary_diagnosis_3char not in ('C40','C41')
            and Morphology_code in ('8800','8801','8802','8803','8805','8806'))
            then 'Unspecified soft tissue sarcoma'
            
/*VI GERM CELL TUMOURS*/
        when (Primary_diagnosis_3char in ('C56','C62')
            and Morphology_code between '9060' and '9105')
            then 'Germ cell and  trophoblastic neoplasms of gonads'
        when (Primary_diagnosis_3char = 'C62'
            and (Morphology_code between '8010' and '8239' or Morphology_code between '8246' and '8580'))
            then 'Germ cell and  trophoblastic neoplasms of gonads'
        when (((Primary_diagnosis_3char in ('C70','C71','C72','D32','D33','D42','D43')
            or Primary_diagnosis in ('C751','C752','C753','D352','D353','D354','D443','D444','D445'))
            and Morphology_code between '9060' and '9105')
            or (Primary_diagnosis in ('D443') and Morphology_code in ('9054'))) 
            then 'Intracranial'
        when (behaviour in ('3','6','9')
            and (Primary_diagnosis_3char between 'C00' and 'C55' or Primary_diagnosis_3char between 'C57' and 'C61'
            or Primary_diagnosis_3char between 'C63' and 'C69' or Primary_diagnosis_3char between 'C73' and 'C74'
            or Primary_diagnosis_3char between 'C76' and 'C97'
            or Primary_diagnosis in ('C750','C754','C755','C758','C759'))
            and Morphology_code between '9060' and '9105')
            then 'Other non-gonadal sites'

/*VII MELANOMA AND SKIN*/
        when (behaviour in ('3','5','6','9')
            and Morphology_code between '8720' and '8790')
            then 'Melanoma'
        when (Primary_diagnosis_3char = 'C44'
            and Morphology_code between '8010' and '8589')
            then 'Skin carcinoma'     

/*VIII CARCINOMAS*/
        when (Primary_diagnosis_3char = 'C73'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Thyroid carcinoma'
        when (Primary_diagnosis_3char = 'C11'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Nasopharyngeal carcinoma'
        when ((Primary_diagnosis_3char between 'C00' and 'C10' or Primary_diagnosis_3char between 'C12' and 'C14')
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Other sites in lip, oral cavity and pharynx'
        when ((Primary_diagnosis_3char between 'C30' and 'C32' or Primary_diagnosis = 'C760')
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Nasal cavity, middle ear, sinuses, larynx and other and ill-defined head and neck'
        when (Primary_diagnosis_3char in ('C33','C34')
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinomas of trachea, bronchus and lung'
        when (Primary_diagnosis_3char = 'C50'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinoma of breast'
        when (Primary_diagnosis_3char = 'C64'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinoma of kidney'
        when (Primary_diagnosis_3char = 'C67'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinoma bladder'
        when (Primary_diagnosis_3char = 'C56'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'
            and Morphology_code not in ('8442','8451','8462','8472','8473')))
            then 'Carcinoma of ovary'
        when (Primary_diagnosis_3char = 'C53'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinoma of cervix' 
        when (Primary_diagnosis_3char in ('C51','C52','C54','C55','C57','C58','C60','C61',
            'C63','C65','C66','C68')
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinoma of other and ill-defined sites in GU tract'
        when (Primary_diagnosis_3char between 'C18' and 'C21'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinoma of colon and rectum'
        when (Primary_diagnosis_3char = 'C16'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinoma of stomach'
        when (Primary_diagnosis_3char = 'C22'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinoma of liver and intrahepatic bile ducts'
        when (Primary_diagnosis_3char = 'C25'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinoma of pancreas'
        when (Primary_diagnosis_3char in ('C15','C17','C23','C24','C26')
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinoma of other and ill-defined sites in  GI tract'
        when (Primary_diagnosis_3char = 'C74'
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Adrenocortical carcinoma'
        when ((Primary_diagnosis_3char in ('C37','C38','C39','C40','C41','C43','C45','C46',
            'C47','C48','C49','C62','C69','C77','C78','C79','C80','C81','C82','C83',
             'C84','C85','C86','C87','C88','C89','C90','C91','C92','C93','C94','C95',
            'C96','C97')
            or Primary_diagnosis in ('C750','C752','C754','C755','C756','C757','C758','C759',
            'C761','C762','C763','C764','C765','C766','C767','C768'))
            and (Morphology_code between '8010' and '8589' or Morphology_code = '8982'))
            then 'Carcinoma of other and ill-defined sites, NEC'

/*IX MISC SPECIFIED*/
        when (behaviour in ('3','6','9')
            and Morphology_code between '8959' and '8962')
            then 'Wilms tumours'
        when (behaviour in ('3','6','9')
            and Morphology_code in ('9490','9500'))
            then 'Neuroblastoma'
        when ((behaviour in ('3','6','9')
            and (Morphology_code in ('8964','8970','8971','8972','8973','8981')
            or Morphology_code between '9501' and '9523'))
            or ((Primary_diagnosis_3char not in ('C70','C71','C72','D32','D33','D42','D43') 
            or Primary_diagnosis not in ('D352','D353','D354','D443','D444','D445')) and Morphology_code in ('8963')))  
            then 'Other paediatric and embryonal, NEC'
        when (behaviour in ('3','6','9')
            and Morphology_code between '8680' and '8711')
            then 'Paraganglioma and glomus'
        when ((behaviour in ('3','6','9')
            and (Morphology_code between '8590' and '8650' or Morphology_code in ('8670','9000')))
            or (Primary_diagnosis_3char = 'C62'
            and Morphology_code between '8240' and '8245'))             
            then 'Other specified gonadal tumours'
        when (behaviour in ('3','6','9')
            and Morphology_code between '9720' and '9764')
            then 'Myeloma, mast cell tumours and miscellaneous lymphoreticular neoplasms NEC'
        when (behaviour in ('3','6','9')
            and (Morphology_code in ('8980','9020','9050','9051','9052','9053','9110','9342')
            or Morphology_code between '8930' and '8951' or Morphology_code between '9270' and '9330'))
            then 'Other specified neoplasms NEC'

/*X UNSPECIFIED*/
        when (behaviour in ('3','6','9')
            and (Primary_diagnosis_3char not in ('C40','C41','C70','C71','C72')
            and Primary_diagnosis not in ('C751','C752','C753'))
            and (Morphology_code between '8000' and '8005' or Morphology_code = '9990'))
            then 'Unspecifed malignant neoplasms NEC'

/*XI MYLOPROLIFERATIVE*/
        when (Morphology_code in ('9950','9960','9961','9962','9964','9980','9982','9983',
            '9984','9985','9986','9987','9989'))
            then 'Myloproliferative'
            ELSE 'Unable to derive Birch code'
            END END as Birch_Classification,
CASE 				
        WHEN Age <=15 THEN 
        CASE 
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9820', '9823', '9826', '9827', '9831', '9832', '9833', '9834', '9835', '9836', '9837', '9940', '9948'))
            THEN '1a Lymphoid leukemias'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9840', '9861', '9866', '9867', '9870', '9871', '9872', '9873', '9874', '9891', '9895', '9896', '9897', '9910', '9920', '9931'))
            THEN '1b Acute myeloid leukemias'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9863', '9875', '9876', '9950', '9960', '9961', '9962', '9963', '9964'))
            THEN '1c Chronic myeloproliferative diseases'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9945', '9946', '9975', '9980', '9982', '9983', '9984', '9985', '9986', '9987', '9989'))
            THEN '1d Myelodysplastic syndrome and other myeloproliferative diseases'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9800', '9801', '9805', '9860', '9930'))
            THEN '1e Unspecified and other specified leukemias'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9650', '9651', '9652', '9653', '9654', '9655', '9659', '9661', '9662', '9663', '9664', '9665', '9667'))
            THEN '2a Hodgkin lymphoma'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9591', '9670', '9671', '9673', '9675', '9678', '9679', '9680', '9684', '9689', '9690', '9691', '9695', '9698', '9699', '9700', '9701', '9702', '9705', '9708', '9709', '9714', '9716', '9717', '9718', '9719', '9727', '9728', '9729', '9731', '9732', '9733', '9734', '9760', '9761', '9762', '9764', '9765', '9766', '9767', '9768', '9769', '9970'))
            THEN '2b Non-Hodgkin lymphomas (except Burkitt lymphoma)'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code      = '9687')
            THEN '2c Burkitt lymphoma'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9740', '9741', '9742', '9750', '9754', '9755', '9756', '9757', '9758'))
            THEN '2d Miscellaneous lymphoreticular neoplasms'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9590', '9596'))
            THEN '2e Unspecified lymphomas'
        WHEN (BEHAVIOUR IN ('0', '1', '3', '6', '9')
            AND Morphology_code     IN ('9383', '9390', '9391', '9392', '9393', '9394'))
            THEN '3a Ependymomas and choroid plexus tumor'
        WHEN ((BEHAVIOUR IN ('0', '1', '3', '6', '9')
            AND primary_diagnosis  = 'C723'
            AND Morphology_code = '9380')
            OR (BEHAVIOUR IN ('0', '1', '3', '6', '9')
            AND Morphology_code     IN ('9384', '9400', '9401', '9402', '9403', '9404', '9405', '9406', '9407', '9408', '9409', '9410', '9411', '9420', '9421', '9422', '9423', '9424', '9440', '9441', '9442')))
            THEN '3b Astrocytomas'
        WHEN ((BEHAVIOUR IN ('0', '1', '3', '6', '9')
            AND (primary_diagnosis_3char IN ('C70', 'C71', 'C72','D42','D43')
            OR primary_diagnosis BETWEEN 'C700' AND 'C729')
            AND Morphology_code IN ('9501', '9502', '9503', '9504'))
            OR (BEHAVIOUR IN ('0', '1', '3', '6', '9')
            AND Morphology_code     IN ('9470', '9471', '9472', '9473', '9474', '9480', '8963', '9508')))
            THEN '3c Intracranial and intraspinal embryonal tumors'
        WHEN ((BEHAVIOUR IN ('0', '1', '3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C700' AND 'C722'
            OR primary_diagnosis BETWEEN 'C724' AND 'C729'
            OR primary_diagnosis  IN ('C751', 'C753'))
            AND Morphology_code = '9380')
            OR (BEHAVIOUR IN ('0', '1', '3', '6', '9')
            AND Morphology_code     IN ('9381', '9382', '9430', '9444', '9450', '9451', '9460')))
            THEN '3d Other gliomas'
        WHEN (BEHAVIOUR IN ('0', '1', '3', '6', '9')
            AND Morphology_code     IN ('8270', '8271', '8272', '8273', '8274', '8275', '8276', '8277', '8278', '8279', '8280', '8281', '8300', '9350', '9351', '9352', '9360', '9361', '9362', '9412', '9413', '9492', '9493', '9505', '9506', '9507', '9530', '9531', '9532', '9533', '9534', '9535', '9536', '9537', '9538', '9539', '9582'))
            THEN '3e Other specified intracranial and intraspinal neoplasms'
        WHEN (BEHAVIOUR IN ('0', '1', '3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C700' AND 'C729'
            OR primary_diagnosis BETWEEN 'C751' AND 'C753'
            OR primary_diagnosis_3char='D43')
            AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005'))
            THEN '3f Unspecified intracranial and intraspinal neoplasms'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9490', '9500'))
            THEN '4a Neuroblastoma and ganglioneuroblastoma'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C000' AND 'C699'
            OR primary_diagnosis BETWEEN 'C739' AND 'C768'
            OR primary_diagnosis = 'C809'
            OR primary_diagnosis_3char= 'C80')
            AND Morphology_code IN ('9501', '9502', '9503', '9504'))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('8680', '8681', '8682', '8683', '8690', '8691', '8692', '8693', '8700', '9520', '9521', '9522', '9523')))
            THEN '4b Other peripheral nervous cell tumors'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9510', '9511', '9512', '9513', '9514'))
            THEN '5 Retinoblastoma'
        WHEN ((primary_diagnosis = 'C649'
            OR primary_diagnosis_3char='C64'
            AND Morphology_code IN ('8963', '9364'))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('8959', '8960', '8964', '8965', '8966', '8967')))
            THEN '6a Nephroblastoma and other nonepithelial renal tumors'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND primary_diagnosis ='C649'
            OR primary_diagnosis_3char ='C64' 
            AND (Morphology_code BETWEEN '8010' AND '8041'
            OR Morphology_code BETWEEN '8050' AND '8075'
            OR Morphology_code BETWEEN '8130' AND '8141'
            OR Morphology_code BETWEEN '8190' AND '8201'
            OR Morphology_code BETWEEN '8221' AND '8231'
            OR Morphology_code BETWEEN '8480' AND '8490'
            OR Morphology_code BETWEEN '8560' AND '8576'
            OR Morphology_code IN ('8082', '8120', '8121', '8122', '8143', '8155', '8210', '8211', '8240', '8241', '8244', '8245', '8246', '8260', '8261', '8262', '8263', '8290', '8310', '8320', '8323', '8401', '8430', '8440', '8504', '8510', '8550')))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('8311', '8312', '8316', '8317', '8318', '8319', '8361')))
            THEN '6b Renal carcinomas' 
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis ='C649'
            or primary_diagnosis_3char = 'C64'
            AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005')))
            THEN '6c Unspecified malignant renal tumours'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code      = '8970')
            THEN '7a Hepatoblastoma'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND primary_diagnosis IN ('C220', 'C221')
            AND (Morphology_code BETWEEN '8010' AND '8041'
            OR Morphology_code BETWEEN '8050' AND '8075'
            OR Morphology_code BETWEEN '8190' AND '8201'
            OR Morphology_code BETWEEN '8480' AND '8490'
            OR Morphology_code BETWEEN '8560' AND '8576'
            OR Morphology_code IN ('8082', '8120', '8121', '8122', '8140', '8141', '8143', '8155', '8210', '8211', '8230', '8231', '8240', '8241', '8244', '8245', '8246', '8260', '8261', '8262', '8263', '8264', '8310', '8320', '8323', '8401', '8430', '8440', '8504', '8510', '8550')))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code BETWEEN '8160' AND '8180'))
            THEN '7b Hepatic carcinomas'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND primary_diagnosis IN ('C220', 'C221')
            AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005'))
            THEN '7c Unspecified malignant hepatic tumors'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C400' AND 'C419'
            OR primary_diagnosis BETWEEN 'C760' AND 'C768'
            OR primary_diagnosis_3char = 'C80'
            OR primary_diagnosis = 'C809')
            AND Morphology_code IN ('9180', '9181', '9182', '9183', '9184', '9185', '9186', '9187', '9191', '9192', '9193', '9194', '9195', '9200'))
            THEN '8a Osteosarcomas'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C400' AND 'C419'
            OR primary_diagnosis BETWEEN 'C760' AND 'C768'
            OR primary_diagnosis_3char = 'C80'
            OR primary_diagnosis = 'C809')
            AND Morphology_code IN ('9210', '9220', '9240'))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('9221', '9230', '9241', '9242', '9243')))
            THEN '8b Chondrosarcomas'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C400' AND 'C419')
            AND Morphology_code IN ('9363', '9364', '9365'))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C400' AND 'C419'
            OR primary_diagnosis BETWEEN 'C760' AND 'C768'
            OR primary_diagnosis_3char= 'C80'
            OR primary_diagnosis = 'C809')
            AND Morphology_code = '9260'))
            THEN '8c Ewing tumor and related sarcomas of bone'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C400' AND 'C419')
            AND Morphology_code IN ('8810', '8811', '8823', '8830'))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('8812', '9250', '9261', '9262', '9270', '9271', '9272', '9273', '9274', '9275', '9280', '9281', '9282', '9290', '9300', '9301', '9302', '9310', '9311', '9312', '9320', '9321', '9322', '9330', '9340', '9341', '9342', '9370', '9371', '9372')))
            THEN '8d Other specified malignant bone tumors'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C400' AND 'C419')
            AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005', '8800', '8801', '8803', '8804', '8805'))
            THEN '8e Unspecified malignant bone tumors'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code     IN ('8900', '8901', '8902', '8903', '8904', '8905', '8910', '8912', '8920', '8991'))
            THEN '9a Rhabdomyosarcomas'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis_3char BETWEEN 'C00' AND 'C39'
            OR primary_diagnosis BETWEEN 'C000' AND 'C399'
            OR primary_diagnosis BETWEEN 'C440' AND 'C768'
            OR primary_diagnosis_3char= 'C80'
            OR primary_diagnosis = 'C809')
            AND Morphology_code IN ('8810', '8811', '8813', '8814', '8815', '8821', '8823', '8834', '8835'))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND (Morphology_code    IN ('8820', '8822', '8824', '8825', '8826', '8827', '9150', '9160', '9491', '9580')
            OR Morphology_code BETWEEN '9540' AND '9571')))
            THEN '9b Fibrosarcomas, peripheral nerve sheath tumors, and other fibrous neoplasms'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code      = '9140')
            THEN '9c Kaposi sarcoma'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C000' AND 'C399'
            OR primary_diagnosis BETWEEN 'C440' AND 'C768'
            OR primary_diagnosis_3char IN ('C80', 'C809', 'C52 ', 'C56', 'C64', 'C61'))
            AND Morphology_code = '8830')
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis_3char IN ('C63', 'C61', 'C73', 'C80', 'C809', 'C52', 'C56')
            OR primary_diagnosis BETWEEN 'C000' AND 'C639'
            OR primary_diagnosis BETWEEN 'C659' AND 'C699'
            OR primary_diagnosis BETWEEN 'C739' AND 'C768')
            AND Morphology_code IN ('8963'))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C490' AND 'C499'
            OR primary_diagnosis_3char= 'C49')
            AND Morphology_code IN ('9180', '9210', '9220', '9240'))
            OR (BEHAVIOUR IN('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C000' AND 'C399'
            OR primary_diagnosis BETWEEN 'C470' AND 'C759')
            AND Morphology_code = '9260')
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis_3char BETWEEN 'C00' AND 'C39'
            OR primary_diagnosis BETWEEN 'C000' AND 'C399'
            OR primary_diagnosis BETWEEN 'C470' AND 'C639'
            OR primary_diagnosis BETWEEN 'C659' AND 'C699'
            OR primary_diagnosis_3char= 'C73'
            OR primary_diagnosis BETWEEN 'C739' AND 'C768'
            OR primary_diagnosis_3char= 'C80'
            OR primary_diagnosis = 'C809'
            OR primary_diagnosis_3char= 'C61'
            OR primary_diagnosis_3char= 'C63')
            AND Morphology_code = '9364')
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C000' AND 'C399'
            OR primary_diagnosis BETWEEN 'C470' AND 'C639'
            OR primary_diagnosis BETWEEN 'C659' AND 'C768'
            OR primary_diagnosis_3char= 'C80'
            OR primary_diagnosis = 'C809'
            OR primary_diagnosis_3char= 'C63'
            OR primary_diagnosis_3char = 'C61'
            OR primary_diagnosis_3char = 'C73')
            AND Morphology_code = '9365')
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND (Morphology_code    IN ('8587', '8710', '8711', '8712', '8713', '8806', '8831', '8832', '8833', '8836', '8840', '8841', '8842', '8860', '8861', '8862', '8870', '8880', '8881', '8921', '8982', '8990', '9040', '9041', '9042', '9043', '9044', '9120', '9121', '9122', '9123', '9124', '9125', '9130', '9131', '9132', '9133', '9135', '9136', '9141', '9142', '9161', '9170', '9171', '9172', '9173', '9174', '9175', '9231', '9251', '9252', '9373', '9581')
            OR Morphology_code BETWEEN '8850' AND '8858'
            OR Morphology_code BETWEEN '8890' AND '8898')))
            THEN '9d Other specified soft tissue sarcomas'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C000' AND 'C399'
            OR primary_diagnosis BETWEEN 'C440' AND 'C768'
            OR primary_diagnosis_3char= 'C80'
            OR primary_diagnosis_3char = 'C61'
            OR primary_diagnosis = 'C809')
            AND Morphology_code IN ('8800', '8801', '8802', '8803', '8804', '8805'))
            THEN '9e Unspecified soft tissue sarcomas'
        WHEN (BEHAVIOUR IN ('0', '1', '3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C700' AND 'C729'
            OR primary_diagnosis BETWEEN 'C751' AND 'C753')
            AND Morphology_code IN ('9060', '9061', '9062', '9063', '9064', '9065', '9070', '9071', '9072', '9080', '9081', '9082', '9083', '9084', '9085', '9100', '9101'))
            THEN '10a Intracranial and intraspinal germ cell tumors'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis ='C809'
            OR primary_diagnosis_3char IN ('C73', 'C80')
            OR primary_diagnosis BETWEEN 'C000' AND 'C559'
            OR primary_diagnosis BETWEEN 'C570' AND 'C619'
            OR primary_diagnosis BETWEEN 'C630' AND 'C699'
            OR primary_diagnosis BETWEEN 'C739' AND 'C750'
            OR primary_diagnosis BETWEEN 'C754' AND 'C768')
            AND Morphology_code IN ('9060', '9061', '9062', '9063', '9064', '9065', '9070', '9071', '9072', '9080', '9081', '9082', '9083', '9084', '9085', '9100', '9101', '9102', '9103', '9104', '9105'))
            THEN '10b Malignant extracranial and extragonadal germ cell tumors'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis = 'C569'
            OR primary_diagnosis BETWEEN 'C620' AND 'C629')
            AND Morphology_code IN ('9060', '9061', '9062', '9063', '9064', '9065', '9070', '9071', '9072', '9073', '9080', '9081', '9082', '9083', '9084', '9085', '9090', '9091', '9100', '9101'))
            THEN '10c Malignant gonadal germ cell tumors'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis = 'C569'
            OR primary_diagnosis BETWEEN 'C620' AND 'C629')
            AND (Morphology_code IN ('8082', '8120', '8121', '8122', '8143', '8210', '8211', '8244', '8245', '8246', '8260', '8261', '8262', '8263', '8290', '8310', '8313', '8320', '8323', '8380', '8381', '8382', '8383', '8384', '8430', '8440', '8504', '8510', '8550', '9000', '9014', '9015')
            OR Morphology_code BETWEEN '8010' AND '8041'
            OR Morphology_code BETWEEN '8050' AND '8075'
            OR Morphology_code BETWEEN '8130' AND '8141'
            OR Morphology_code BETWEEN '8190' AND '8201'
            OR Morphology_code BETWEEN '8221' AND '8241'
            OR Morphology_code BETWEEN '8480' AND '8490'
            OR Morphology_code BETWEEN '8560' AND '8573'))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND (Morphology_code    IN ('8441', '8442', '8443', '8444', '8450', '8451', '8462', '8461')
            OR Morphology_code BETWEEN '8460' AND '8473')))
            THEN '10d Gonadal carcinomas'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis = 'C569'
            OR primary_diagnosis BETWEEN 'C620' AND 'C629')
            AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005'))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code BETWEEN '8590' AND '8671'))
            THEN '10e Other and unspecified malignant gonadal tumors'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND Morphology_code BETWEEN '8370' AND '8375')
            THEN '11a Adrenocortical carcinomas'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis_3char = 'C73'
            OR primary_diagnosis = 'C739')
            AND (Morphology_code IN ('8082', '8120', '8121', '8122', '8190', '8200', '8201', '8211', '8230', '8231', '8244', '8245', '8246', '8260', '8261', '8262', '8263', '8290', '8310', '8320', '8323', '8430', '8440', '8480', '8481', '8510')
            OR Morphology_code BETWEEN '8010' AND '8041'
            OR Morphology_code BETWEEN '8050' AND '8075'
            OR Morphology_code BETWEEN '8130' AND '8141'
            OR Morphology_code BETWEEN '8560' AND '8573'))
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND primary_diagnosis_3char BETWEEN 'C00' AND 'C97'
            AND (Morphology_code BETWEEN '8330' AND '8337'
            OR Morphology_code BETWEEN '8340' AND '8347'
            OR Morphology_code = '8350')))
            THEN '11b Thyroid carcinomas'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND primary_diagnosis BETWEEN 'C110' AND 'C119'
            AND (Morphology_code IN ('8082', '8083', '8120', '8121', '8122', '8190', '8200', '8201', '8211', '8230', '8231', '8244', '8245', '8246', '8260', '8261', '8262', '8263', '8290', '8310', '8320', '8323', '8430', '8440', '8480', '8481')
            OR Morphology_code BETWEEN '8010' AND '8041'
            OR Morphology_code BETWEEN '8050' AND '8075'
            OR Morphology_code BETWEEN '8130' AND '8141'
            OR Morphology_code BETWEEN '8500' AND '8576'))
            THEN '11c Nasopharyngeal carcinomas'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND (Morphology_code BETWEEN '8720' AND '8780'
            OR Morphology_code = '8790'))
            THEN '11d Malignant melanomas'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND primary_diagnosis BETWEEN 'C440' AND 'C449'
            AND (Morphology_code IN ('8078', '8082', '8140', '8143', '8147', '8190', '8200', '8240', '8246', '8247', '8260', '8310', '8320', '8323', '8430', '8480', '8542', '8560', '8940', '8941')
            OR Morphology_code BETWEEN '8010' AND '8041'
            OR Morphology_code BETWEEN '8050' AND '8075'
            OR Morphology_code BETWEEN '8090' AND '8110'
            OR Morphology_code BETWEEN '8390' AND '8420'
            OR Morphology_code BETWEEN '8570' AND '8573'))
            THEN '11e Skin carcinomas'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C000' AND 'C109'
            OR primary_diagnosis BETWEEN 'C129' AND 'C218'
            OR primary_diagnosis BETWEEN 'C239' AND 'C399'
            OR primary_diagnosis BETWEEN 'C480' AND 'C488'
            OR primary_diagnosis BETWEEN 'C500' AND 'C559'
            OR primary_diagnosis BETWEEN 'C570' AND 'C619'
            OR primary_diagnosis BETWEEN 'C630' AND 'C639'
            OR primary_diagnosis BETWEEN 'C659' AND 'C729'
            OR primary_diagnosis BETWEEN 'C750' AND 'C768'
            OR primary_diagnosis_3char = 'C80'
            OR primary_diagnosis     = 'C809')
            AND (Morphology_code IN ('8290', '8310', '8313', '8314', '8315', '8320', '8321', '8322', '8323', '8324', '8325', '8360', '8380', '8381', '8382', '8383', '8384', '8452', '8453', '8454', '8588', '8589', '8940', '8941', '8983', '9000', '9020', '9030')
            OR Morphology_code BETWEEN '8010' AND '8084'
            OR Morphology_code BETWEEN '8120' AND '8157'
            OR Morphology_code BETWEEN '8190' AND '8264'
            OR Morphology_code BETWEEN '8430' AND '8440'
            OR Morphology_code BETWEEN '8480' AND '8586'
            OR Morphology_code BETWEEN '9010' AND '9016'))
            THEN '11f Other and unspecified carcinomas'
        WHEN ((BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C000' AND 'C399'
            OR primary_diagnosis BETWEEN 'C470' AND 'C759'
            OR primary_diagnosis_3char IN ('C64', 'C61', 'C52'))
            AND Morphology_code = '9363')
            OR (BEHAVIOUR IN ('3', '6', '9')
            AND (Morphology_code    IN ('8930', '8931', '8932', '8933', '8934', '8935', '8936', '8950', '8951', '9050', '9051', '9052', '9053', '9054', '9055', '9110')
            OR Morphology_code BETWEEN '8971' AND '8981')))
            THEN '12a Other specified malignant tumors'
        WHEN (BEHAVIOUR IN ('3', '6', '9')
            AND (primary_diagnosis BETWEEN 'C000' AND 'C218'
            OR primary_diagnosis BETWEEN 'C239' AND 'C399'
            OR primary_diagnosis BETWEEN 'C420' AND 'C559'
            OR primary_diagnosis BETWEEN 'C570' AND 'C619'
            OR primary_diagnosis BETWEEN 'C630' AND 'C639'
            OR primary_diagnosis BETWEEN 'C659' AND 'C699'
            OR primary_diagnosis BETWEEN 'C739' AND 'C750'
            OR primary_diagnosis BETWEEN 'C754' AND 'C809'
            OR primary_diagnosis_3char IN ('C64', 'C61', 'C52'))
            AND Morphology_code IN ('8000', '8001', '8002', '8003', '8004', '8005'))
            THEN '12b Other unspecified malignant tumors'
ELSE 'Other childhood tumour'
END END AS ICCC3_Paed_Grouping

FROM 
(
select distinct
    p.Encore_Patient_ID as Encore_Patient_ID,
    trunc((months_between(r.start_date_of_regimen, p.date_of_birth))/12) as Age,
    NVL(p.Gender_Current, '') as Gender_Current,
    NVL(p.Ethnicity, '') as Ethnicity,
    NVL(
        (case when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 0 and 4 then '0-4'
       when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 5 and 9 then '5-9' 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 10 and 15 then '10-15'			
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 16 and 19 then '16-19'			 
	   when trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) between 20 and 24 then '20-24' 
       else 'missing' end), '') as AgeGroup,
    NVL(p.Date_Of_Birth,'')as date_of_birth,
    NVL(t.SACT_Tumour_ID, '') as SACT_Tumour_ID,
    NVL(t.Primary_Diagnosis, '') as Primary_Diagnosis,
    SUBSTR(t.primary_diagnosis,1, 3) as primary_diagnosis_3char,
    NVL(dsg.Group_Description2, '') as Group_Description2,
    NVL(t.Morphology_code, '') as Morphology,
    SUBSTR(t.Morphology_code, 1, 4) as Morphology_code,
    SUBSTR(t.Morphology_code, -1, 1) as Behaviour,
    NVL(t.Organisation_Code_of_Provider, '') as Provider, 
    NVL(SUBSTR(t.Organisation_Code_of_Provider,1, 3), '') as Trust, 
    NVL(r.Merged_Regimen_ID, '') as Merged_Regimen_ID,
    NVL(TO_CHAR(r.Start_Date_of_Regimen,'MON/YYYY'), '') as Start_Date_of_Regimen,
    NVL(TO_CHAR(d.Administration_Date, 'DAY'), '') as Weekday,
    NVL(r.Intent_of_Treatment, '') as Intent_of_Treatment,
    NVL(BAL.Benchmark, '') as Benchmark,
    NVL(r.Mapped_Regimen, '') as Mapped_Regimen,
    NVL(c.Merged_Cycle_ID, '') as Merged_Cycle_ID,
    NVL(TO_CHAR(c.Start_Date_of_Cycle,'MON/YYYY'), '') as Start_Date_of_Cycle,
    NVL(d.Merged_Drug_Detail_ID, '') as Merged_Drug_Detail_ID,
    NVL(TO_CHAR(d.Administration_Date,'MON/YYYY'), '') as Administration_Date,
    NVL(d.Administration_Route, '') as Administration_Route,
    NVL(dl.Drug_Group, '') as Drug_Group,
    replace(NVL(d.Drug_Name, ''), '|', ',') as Drug_Name,        
    NVL(d.Org_Code_of_Drug_Provider, '') as Org_Code_of_Drug_Provider,  
    NVL(SUBSTR(d.Org_Code_of_Drug_Provider,1, 3), '') as Trust_of_Drug_Provider, 
    --NVL(o.Merged_Outcome_ID, '') as Merged_Outcome_ID,
    NVL(o.Regimen_Outcome_Summary, '') as Regimen_Outcome_Summary,
    NVL(co.ref_no_with_c, '') as ref_no_with_c --Consultant_code  --Added by Bukky 14/06/2019
from SACT.AT_PATIENT@CAS1905 P            
    inner join SACT.AT_TUMOUR T on T.Encore_Patient_ID = P.Encore_Patient_ID
    inner join SACT.AT_REGIMEN R on R.SACT_Tumour_ID = T.SACT_Tumour_ID            
    inner join SACT.AT_CYCLE C on C.Merged_Regimen_ID = R.Merged_Regimen_ID
    left join SACT.AT_DRUG_DETAIL D on D.Merged_Cycle_ID = C.Merged_Cycle_ID
    left join SACT.AT_OUTCOME O on O.Merged_Regimen_ID = R.Merged_Regimen_ID
    left join DIAGNOSIS_SUBGROUP_SACT@CAS1905 DSG on DSG.ICD_Code = T.Primary_Diagnosis
    left join BENCHMARK_ANALYSIS_LOOKUP_NEW@CAS1905 BAL on BAL.Mapped_regimen = R.Mapped_regimen
    left join DRUG_LOOKUP_NEW@CAS1905 DL on DL.Drug_name = D.Drug_name
    left join official_consultant_22022019@CAS1905 co on  co.ref_no_with_c = T.Consultant_GMC_Code 
where 
    trunc((months_between(r.start_date_of_regimen,p.date_of_birth))/12) < 25
    and (r.Start_date_of_regimen >= '01-APR-18'
    or c.Start_Date_of_Cycle >= '01-APR-18'
    or d.Administration_Date >= '01-APR-18') 
)a
)b
)
--;
--select *
--from SACT_SAS_CTYA_April2018;

            
SELECT 
    Activity.Encore_patient_id as Merged_Patient_ID,
    Activity.Gender_Current,
    Activity.AgeGroup,
    Activity.Sact_tumour_id as Merged_Tumour_ID,
    Activity.Group_Description2,
    Activity.Morphology_code,
    Activity.Trust,
    Activity.Merged_Regimen_ID,
    Activity.Start_Date_of_Regimen,
    Activity.Weekday,
    Activity.Benchmark,
    Activity.Mapped_Regimen,
    Activity.Merged_Cycle_ID,
    Activity.Start_Date_of_Cycle,
    Activity.Merged_Drug_Detail_ID,
    Activity.Administration_Date,
    Activity.Administration_Route,
    Activity.Drug_Group,
    Activity.Drug_Name,
    Activity.Org_Code_of_Drug_Provider,
    Activity.Trust_of_Drug_Provider,
    --Activity.Merged_Outcome_ID
    Activity.Regimen_Outcome_Summary,
    Activity.ref_no_with_c, --Consultant_code, --Added by Bukky 14/06/2019
    case 
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
        end as Exclusion,
    Activity.Birch_Classification,
    Activity.ICCC3_Paed_Grouping,
    NVL(Activity.ICCC3_Site_Group,'') as ICCC3_Site_Group    
from SACT_SAS_CTYA_April2018 ACTIVITY
left join CDF_EXCLUSIONS@CAS1905 CDFEX ON CDFEX.merged_regimen_id = activity.merged_regimen_id
;    

            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
            
         
        
        
        
        
        