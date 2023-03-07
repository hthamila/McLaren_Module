\set ON_ERROR_STOP ON;

DROP TABLE intermediate_stage_high_emeto_antiemetic_cases IF EXISTS; 
CREATE table intermediate_stage_high_emeto_antiemetic_cases AS 
with chrg_fct_J9070_J9080_only
AS 
(select Z.fcy_nm as fcy_nm, Z.encntr_num as encntr_num, Z.cpt_Code, SUM(Z.quantity) as qty
, CASE when Z.cpt_code = 'J9070' THEN 1 END AS code1
, CASE when Z.cpt_code = 'J9080' THEN 1 END AS code2
FROM prd_chrg_fct Z WHERE Z.cpt_Code IN ('J9070','J9080')
GROUP BY 1,2,3
)
select EF.fcy_nm,EF.fcy_num,  EF.encntr_num ,DATE(EF.dschrg_dt) as dschrg_dt,
     MAX(CASE when Z.cpt_code in ('J9050') THEN 1 
     when Z.cpt_code in ('J9060') THEN 1 
     WHEN Z.cpt_Code in('J9070')  AND X.cpt_code = 'J9070' AND X.qty >=23 THEN 1
	 when Z.cpt_code in ('J9080') AND X.cpt_code = 'J9080' AND  X.qty >=12 THEN 1 
     when Z.cpt_code in ('J9120') THEN 1 
	 when Z.cpt_code in ('J9000') AND X.cpt_code = 'J9070' and X.qty < 23 THEN 1 
     when Z.cpt_code in ('J9178') AND X.cpt_code = 'J9070' and X.qty < 23 THEN 1 
	 when Z.cpt_code in ('J9130') THEN 1 
     when Z.cpt_code in ('J9230') THEN 1 
	 when Z.cpt_code in ('J9320') THEN 1
    ELSE 0 END)  as high_emeto_cases,
	MAX(case when Z.cpt_code ='J0185' THEN 1
	         WHEN Z.cpt_code ='J8501' THEN 1
			 WHEN Z.cpt_code ='J1453' THEN 1 
			 WHEN UPPER(Z.persp_clncl_dtl_descr) = 'OLANZAPINE INJ 10MG' THEN 1 
			 WHEN UPPER(Z.persp_clncl_dtl_descr) = 'OLANZAPINE TAB 5MG' THEN 1 ELSE 0 END) as antimetic_cases,
    high_emeto_cases as highemeto_antiecases_denominator,  -- grand_Total
	case when high_emeto_cases = 1 AND antimetic_cases =1 THEN 1 ELSE 0 END as cases_with_antiemetic , --- numerator,
    case when high_emeto_cases = 1 AND  antimetic_cases =0  THEN 1 ELSE 0 END as cases_without_antiemetic --numerator_2
from prd_chrg_fct Z
INNER JOIN prd_encntr_anl_fct EF
on EF.encntr_num = Z.encntr_num AND EF.fcy_nm = Z.fcy_nm 
LEFT JOIN chrg_fct_J9070_J9080_only X
ON X.fcy_nm = Z.fcy_nm and X.encntr_num = Z.encntr_num 
WHERE
(UPPER(Z.cpt_code ) IN ('J9050','J9060','J9070','J9080','J9120','J9000','J9178','J9130','J9230','J9320', 'J0185','J8501','J1453') 
OR UPPER(Z.persp_clncl_dtl_descr) IN ('OLANZAPINE INJ 10MG','OLANZAPINE TAB 5MG'))
GROUP By 1,2 ,3,4;

--July 2020 : Outpatient Chemotherapy logic added into SLP encntr_anl_Fct
------Chemotherapy Patients meeting inclusion criteria From Oct 1st 2016
--Encounter LEvel (Inclusion)
--select 'processing table: intermediate_stage_op_chemo_visits_all_inclusions ' as table_processing;
DROP TABLE intermediate_stage_op_chemo_visits_all_inclusions IF EXISTS; 
CREATE TABLE intermediate_stage_op_chemo_visits_all_inclusions AS 
select fcy_nm, encntr_num , dschrg_dt,adm_ts,
--1 as inclusion_ind , 
max(chemo_proc_ind)  as  chemo_denom_incl_proc_ind,
max(chemo_dgns_ind) as chemo_denom_incl_dgns_ind, 
max(chemo_encntr_ind) as chemo_denom_incl_encntr_ind,
max(chemo_med_ind) as chemo_denom_incl_med_ind,
CASE 
 WHEN ( (max(chemo_proc_ind) =1 AND max(chemo_dgns_ind)   =1 ) AND ( max(chemo_encntr_ind) >=0 OR max(chemo_med_ind) >=0 ) ) THEN 1    -- (Tab 13 & Tab 14) c
 WHEN ( (max(chemo_dgns_ind) =1 AND max(chemo_med_ind)    =1 ) ) THEN 1    -- (Tab 13 & Tab 16) 
 WHEN ( (max(chemo_dgns_ind) =1 AND max(chemo_encntr_ind) =1 ) ) THEN 1    -- (Tab 13 & Tab 15) 
ELSE 0 END AS inclusion_ind
FROM 
(  
--Tab # 13 - Denominator - Cancer Diagnosis 
select distinct Z.fcy_nm, Z.encntr_num , Z.dschrg_dt , Z.adm_ts,  0 as chemo_proc_ind , 1 as chemo_dgns_ind, 0 as chemo_encntr_ind ,0 as chemo_med_ind
FROM prd_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18  AND Z.prim_Dgns_cd in (
select cd from chemo_val_set_dim WHERE cohrt_id IN ('cancer_icd10') ) UNION ALL 

select distinct Z.fcy_nm, Z.encntr_num , Z.dschrg_dt ,Z.adm_ts,   0 as chemo_proc_ind , 1 as chemo_dgns_ind ,0 as chemo_encntr_ind ,0 as chemo_med_ind
FROM prd_encntr_anl_fct Z
INNER JOIN prd_encntr_dgns_fct DF
on Z.fcy_nm = DF.fcy_nm AND Z.encntr_num = DF.encntr_num 
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18  AND DF.icd_version ='ICD10' AND DF.icd_code  IN (
select cd from chemo_val_set_dim WHERE cohrt_id IN ('cancer_icd10') ) UNION ALL 

--Tab # 14 - Denominator - Chemo Procedure (CPT) 
select  distinct CF.fcy_nm as fcy_nm, CF.encntr_num as encntr_num, Z.dschrg_dt , Z.adm_ts,  1 as chemo_proc_ind , 0 as chemo_dgns_ind, 0 as chemo_encntr_ind  ,0 as chemo_med_ind 
FROM prd_chrg_fct CF 
INNER JOIN prd_encntr_anl_fct Z
on Z.encntr_num = CF.fcy_nm AND Z.fcy_nm = CF.fcy_nm 
where Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18 AND  CF.cpt_code IN  
(select cd from chemo_val_set_dim where cohrt_id ='chemo_proc_cpt')   UNION ALL
 
--Tab # 14 - Denominator - Chemo Procedure (ICD-10) 
select distinct Z.fcy_nm, Z.encntr_num, Z.dschrg_dt,Z.adm_ts,  1 as chemo_proc_ind , 0 as chemo_dgns_ind, 0 as chemo_encntr_ind ,0 as chemo_med_ind
FROM prd_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18 AND Z.prim_pcd_cd in (
select cd from chemo_val_set_dim WHERE cohrt_id IN ('chemo_proc_icd10') ) UNION ALL 

select distinct Z.fcy_nm, Z.encntr_num , Z.dschrg_dt , Z.adm_ts,   1 as chemo_proc_ind , 0 as chemo_dgns_ind, 0 as chemo_encntr_ind  ,0 as chemo_med_ind
FROM prd_encntr_anl_fct Z
INNER JOIN prd_encntr_pcd_fct DF
on Z.fcy_nm = DF.fcy_nm AND Z.encntr_num = DF.encntr_num 
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18  AND DF.icd_version ='ICD10' AND DF.icd_code  IN (
select cd from chemo_val_set_dim WHERE cohrt_id IN ('chemo_proc_icd10') ) UNION ALL 

--Tab # 14 - Denominator - Chemo Procedure (Revenue Code) 
select  distinct CF.fcy_nm as fcy_nm, CF.encntr_num as encntr_num ,  Z.dschrg_dt, Z.adm_ts, 1 as chemo_proc_ind , 0 as chemo_dgns_ind, 0 as chemo_encntr_ind   ,0 as chemo_med_ind
FROM prd_chrg_fct CF 
INNER JOIN prd_encntr_anl_fct Z
on Z.encntr_num = CF.encntr_num AND Z.fcy_nm = CF.fcy_nm 
where Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18 AND  CF.revenue_code IN   
(select cd from chemo_val_set_dim where cohrt_id = 'chemo_proc_revcd')  UNION ALL 

--Tab # 15 - Denominator - Chemo Encouneter (ICD 10 Code)   
select distinct Z.fcy_nm, Z.encntr_num , Z.dschrg_dt , Z.adm_ts, 0 as chemo_proc_ind , 0 as chemo_dgns_ind, 1 as chemo_encntr_ind ,0 as chemo_med_ind
FROM prd_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18  AND Z.prim_Dgns_cd in (
select cd from chemo_val_set_dim WHERE cohrt_id IN ( 'chemo_encntr_cpt') ) UNION ALL 

select distinct Z.fcy_nm, Z.encntr_num , Z.dschrg_dt ,Z.adm_ts,  0 as chemo_proc_ind , 0 as chemo_dgns_ind, 1 as chemo_encntr_ind  ,0 as chemo_med_ind
FROM prd_encntr_anl_fct Z
INNER JOIN prd_encntr_dgns_fct DF
on Z.fcy_nm = DF.fcy_nm AND Z.encntr_num = DF.encntr_num 
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18  AND DF.icd_version ='ICD10' AND DF.icd_code  IN (
select cd from chemo_val_set_dim WHERE cohrt_id IN ('chemo_encntr_cpt') ) UNION ALL 

--Tab # 16 - Denominator - Chemo Mediciene (HCPCS Code)
select  distinct CF.fcy_nm as fcy_nm, CF.encntr_num as encntr_num, Z.dschrg_dt , Z.adm_ts, 0 as chemo_proc_ind , 0 as chemo_dgns_ind, 0 as chemo_encntr_ind  ,1 as chemo_med_ind 
FROM prd_chrg_fct CF 
INNER JOIN prd_encntr_anl_fct Z
on Z.encntr_num = CF.encntr_num AND Z.fcy_nm = CF.fcy_nm 
where Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18 AND  CF.cpt_code IN  
(select cd from chemo_val_set_dim where cohrt_id ='chemo_medicine_cpt')


) X
GROUP BY 1, 2,3,4;

--Cohrt Exclusions --Encounter Level (Exclusion)  
--Encounter with Non-cancer  intermediate_stage_op_chemo_visits_exclusions_noncancer
--Encounters with Lekumia intermediate_stage_op_chemo_visits_exclusions_lekumia
--Encounters with AutoImmune  intermediate_stage_op_chemo_visits_exclusions_autoimmune
--select 'processing table:  intermediate_stage_op_chemo_visits_exclusions_lekumia' as table_processing;
DROP TABLE intermediate_stage_op_chemo_visits_exclusions_lekumia IF EXISTS; 
CREATE  TABLE intermediate_stage_op_chemo_visits_exclusions_lekumia AS 
select distinct fcy_nm, encntr_num , 1 as lekumia_ind FROM 
(
select distinct Z.fcy_nm, Z.encntr_num  FROM prd_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.prim_Dgns_cd in (
select cd from chemo_val_set_dim WHERE cohrt_id IN ('lekumia_icd10') )
UNION ALL
select distinct EF.fcy_nm, EF.encntr_num  FROM prd_encntr_anl_fct EF
INNER JOIN prd_encntr_dgns_fct PF
on EF.fcy_nm= PF.fcy_nm and EF.encntr_num = PF.encntr_num 
WHERE EF.in_or_out_patient_ind = 'O' AND DATE(EF.dschrg_dt) >= '2016-10-01' AND PF.icd_version ='ICD10' AND PF.icd_code  IN 
(select cd from chemo_val_set_dim WHERE cohrt_id IN ('lekumia_icd10') and opr_typ_nm = 'NOT IN')) X; 

--Encounters with AutoImmune  intermediate_stage_op_chemo_visits_exclusions_autoimmune
--select 'processing table:  intermediate_stage_op_chemo_visits_exclusions_autoimmune' as table_processing;
DROP TABLE intermediate_stage_op_chemo_visits_exclusions_autoimmune IF EXISTS; 
CREATE  TABLE intermediate_stage_op_chemo_visits_exclusions_autoimmune AS 
select distinct fcy_nm, encntr_num , 1 as autoimmune_ind  FROM 
(
select distinct Z.fcy_nm, Z.encntr_num  FROM prd_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.prim_Dgns_cd in (
select cd from chemo_val_set_dim WHERE cohrt_id IN ('autoImmune_icd10') and opr_typ_nm = 'NOT IN' )
UNION ALL
select distinct EF.fcy_nm, EF.encntr_num  FROM prd_encntr_anl_fct EF
INNER JOIN prd_encntr_dgns_fct PF
on EF.fcy_nm= PF.fcy_nm and EF.encntr_num = PF.encntr_num 
WHERE EF.in_or_out_patient_ind = 'O' AND DATE(EF.dschrg_dt) >= '2016-10-01' AND PF.icd_version ='ICD10' AND PF.icd_code  IN 
(select cd from chemo_val_set_dim WHERE cohrt_id IN ('autoImmune_icd10') and opr_typ_nm = 'NOT IN')) X; 

--Encounters with Non-Cancer
--select 'processing table: intermediate_stage_op_chemo_visits_exclusions_noncancer ' as table_processing;
DROP TABLE intermediate_stage_op_chemo_visits_exclusions_noncancer IF EXISTS; 
CREATE  TABLE intermediate_stage_op_chemo_visits_exclusions_noncancer AS 
--'chemoNonCancer_icd10'
select distinct fcy_nm, encntr_num , 1 as noncancer_ind FROM 
(
select distinct Z.fcy_nm, Z.encntr_num   FROM prd_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' 
AND Z.prim_Dgns_cd in ( select cd from chemo_val_set_dim WHERE cohrt_id IN ('chemoNonCancer_icd10') and opr_typ_nm = 'NOT IN' )
UNION ALL
select distinct EF.fcy_nm, EF.encntr_num  FROM prd_encntr_anl_fct EF
INNER JOIN prd_encntr_dgns_fct PF
on EF.fcy_nm= PF.fcy_nm and EF.encntr_num = PF.encntr_num 
WHERE EF.in_or_out_patient_ind = 'O' AND DATE(EF.dschrg_dt) >= '2016-10-01' 
AND PF.icd_version ='ICD10' AND PF.icd_code IN (select cd from chemo_val_set_dim WHERE cohrt_id IN ('chemoNonCancer_icd10') and opr_typ_nm = 'NOT IN')
UNION ALL
select  distinct CF.fcy_nm as fcy_nm, CF.encntr_num as encntr_num  FROM prd_chrg_fct CF 
INNER JOIN prd_encntr_anl_fct Z
on Z.encntr_num = CF.encntr_num AND Z.fcy_nm = CF.fcy_nm 
where Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' 
AND CF.cpt_code IN  (select cd from chemo_val_set_dim where cohrt_id IN ('chemoNonCancer_hcpcs',  'chemoNonCancer_cpt'))
) X;
------Chemotherapy Patients meeting Exclusion criteria From Oct 1st 2016
--select 'processing table: intermediate_stage_op_chemo_visits_all_exclusions ' as table_processing;
DROP TABLE intermediate_stage_op_chemo_visits_all_exclusions IF EXISTS; 
CREATE TABLE intermediate_stage_op_chemo_visits_all_exclusions AS 
select 
NON.fcy_nm, NON.encntr_num,
NON.noncancer_ind as chemo_denom_excl_noncancer_ind, 
LK.lekumia_ind as  chemo_denom_excl_lekumia_ind , 
AI.autoimmune_ind as  chemo_denom_excl_autoimmune_ind,
--CASE WHEN (NON.noncancer_ind=1 OR LK.lekumia_ind =1 OR  AI.autoimmune_ind=1) THEN 1 ELSE 0 END as exclusion_ind
CASE WHEN ((NON.noncancer_ind=1 AND AI.autoimmune_ind=1) OR LK.lekumia_ind =1 ) THEN 1 ELSE 0 END as exclusion_ind
FROM intermediate_stage_op_chemo_visits_exclusions_noncancer NON 
LEFT JOIN intermediate_stage_op_chemo_visits_exclusions_lekumia LK
on LK.fcy_nm = NON.fcy_nm AND LK.encntr_num  = NON.encntr_num 
LEFT JOIN intermediate_stage_op_chemo_visits_exclusions_autoimmune AI
on NON.fcy_nm = AI.fcy_nm AND NON.encntr_num  = AI.encntr_num ;

--Logic : Combine all-inclusions and all-exclusions Encounters and baseline with Encntr_anl_Fct Table 
--select 'processing table:  intermediate_stage_op_chemo_visits_denominator' as table_processing;
DROP TABLE intermediate_stage_op_chemo_visits_denominator IF EXISTS; 
CREATE  TABLE intermediate_stage_op_chemo_visits_denominator AS 
select distinct EF.fcy_nm, EF.encntr_num, EF.dschrg_dt,EF.adm_ts,  EF.medical_record_number, nvl(EF.empi,EF.medical_record_number) as empi, EF.in_or_out_patient_ind, 
CASE WHEN EF.in_or_out_patient_ind = 'O' THEN nvl(ALLIN.inclusion_ind,0) else ALLIN.inclusion_ind END as chemo_denom_incl_ind ,
CASE WHEN EF.in_or_out_patient_ind = 'O' THEN nvl(ALLEX.exclusion_ind,0) else ALLEX.exclusion_ind END as chemo_denom_excl_ind ,
CASE WHEN (EF.in_or_out_patient_ind ='O' AND (ALLIN.inclusion_ind =1 AND  ALLEX.exclusion_ind=1)) THEN 0
     WHEN (EF.in_or_out_patient_ind ='O' AND (ALLIN.inclusion_ind =1 AND  ALLEX.exclusion_ind=0)) THEN 1
	 WHEN (EF.in_or_out_patient_ind ='O' AND (ALLIN.inclusion_ind =0 AND  ALLEX.exclusion_ind=0)) THEN 0
	 WHEN (EF.in_or_out_patient_ind ='O' AND (ALLIN.inclusion_ind =0 AND  ALLEX.exclusion_ind=1)) THEN 0
	 WHEN EF.in_or_out_patient_ind ='I' THEN NULL 
     ELSE 0 END as chemo_denom_ind,
--CODE CHANGE: 09/10/2020 Modified 
ALLIN.chemo_denom_incl_proc_ind, 
ALLIN.chemo_denom_incl_dgns_ind, 
ALLIN.chemo_denom_incl_encntr_ind, 
ALLIN.chemo_denom_incl_med_ind,
ALLEX.chemo_denom_excl_noncancer_ind,
ALLEX.chemo_denom_excl_lekumia_ind,
ALLEX.chemo_denom_excl_autoimmune_ind
FROM prd_encntr_anl_fct EF
LEFT JOIN intermediate_stage_op_chemo_visits_all_inclusions ALLIN
on ALLIN.fcy_nm = EF.fcy_nm AND ALLIN.encntr_num = EF.encntr_num
LEFT JOIN intermediate_stage_op_chemo_visits_all_exclusions ALLEX
on EF.fcy_nm = ALLEX.fcy_nm AND EF.encntr_num = ALLEX.encntr_num
--Added 11/23/2020
WHERE EF.dschrg_tot_chrg_amt > 0;



--Logic : Identify all the IP visits based on the OP chemo visits Patient's MRN 
--select 'processing table:  intermediate_stage_chemo_ip_visits_for_op_chemo' as table_processing;
DROP TABLE intermediate_stage_chemo_ip_visits_for_op_chemo IF EXISTS; 
CREATE  TABLE intermediate_stage_chemo_ip_visits_for_op_chemo AS 
with op_mrn as 
(select  
--Z.medical_record_number as mrn 
Z.empi as empi
FROM intermediate_stage_op_chemo_visits_denominator Z
WHERE Z.in_or_out_patient_ind = 'O' AND Z.chemo_denom_ind = 1  ),
ip_records as 
(select EF.* from prd_encntr_anl_fct EF 
INNER JOIN op_mrn 
--ON op_mrn.mrn = EF.medical_record_number
ON op_mrn.empi = EF.empi
WHERE EF.in_or_out_patient_ind = 'I' AND EF.prim_dgns_Cd in (select cd from chemo_val_set_dim where val_set_nm = 'Inpatient Chemo' )
)
select * FROM 
(select ip_records.fcy_nm, ip_records.encntr_num, ip_records.empi, ip_records.in_or_out_patient_ind, ip_records.dschrg_dt, ip_records.adm_ts, 0 as chemo_denom_ind, 1 as chemo_numer_ind, ip_records.ed_case_ind
FROM ip_records
UNION 
select Z.fcy_nm, Z.encntr_num, Z.empi, Z.in_or_out_patient_ind, Z.dschrg_dt, Z.adm_ts, Z.chemo_denom_ind, 0 as chemo_numer_ind, 0 as ed_case_ind
FROM intermediate_stage_op_chemo_visits_denominator Z 
INNER JOIN ip_records ON Z.empi = ip_records.empi
WHERE Z.in_or_out_patient_ind = 'O' aND  Z.chemo_denom_ind = 1 )  X 
ORDER BY X.empi, X.in_or_out_patient_ind desc, X.dschrg_dt; 
 
--11/03/2020: Modified version 
--------------------------------------------------------------------
--CODE CHANGE : OCT 2020: MLH-592: Add new oncology related measures
--------------------------------------------------------------------																	
DROP TABLE intermediate_stage_onc_region_agg_fct IF EXISTS; 
CREATE TABLE intermediate_stage_onc_region_agg_fct AS 
with
 chrgt_fct_encntrs_filtered as 
 (select  distinct EF.fcy_nm,EF.encntr_num, EF.in_or_out_patient_ind, EF.dschrg_dt,
 cpt_code ,
 betos_cd, 
  CASE WHEN betos_cd = 'M1A' THEN 1 ELSE 0 END AS new_consults_ind,
  CASE WHEN betos_cd = 'M1B' THEN 1 ELSE 0 END as outpt_visits_ind,
  CASE WHEN cpt_code IN ( '96401','96402','96409','96411','96413','96415','96416','96417','G0498',
--11/02/2020: Added based on new requirements
'95990','96369','93370','96377','96405','96521','96522','96542'
) THEN 1 ELSE 0 END as chemo_infusion_ind,
CASE WHEN cpt_code IN ( '96360','96361','96365','96366','96367','96368','96372','96374','96375','96376') THEN 1 ELSE 0 END as non_chemo_infusion_ind,
CASE WHEN cpt_code IN ( '99441','99442','99443','G2010','G2012','99423'
--11/02/2020: Added based on new requirements 
,'G0406','Q3014'
) THEN 1 ELSE 0 END as telemed_ind,
--11/02/2020: Added based on new requirements 
CASE WHEN cpt_code IN ('36415','36416','36591','36592','36593','36600','90471','90472','99195','G0008','G0009','G0010','96523') THEN 1 ELSE 0 END as nurse_visits_ind,
--11/06/2020: Added based on new requirements 
--CASE WHEN cpt_Code IN ('99223','99232','99233','99238','99239','99220','99219','99225','99226','99236','99245','99244','99222','99231','99218','99224','G0378','G0379') END AS hsptl_visits_ind,
CASE WHEN betos_cd IN ('M2A','M2B') THEN 1 ELSE 0 END as hsptl_visits_ind,
--11/03/2020: Added Hierarchy 
CASE WHEN chemo_infusion_ind =1 THEN 1
     WHEN non_chemo_infusion_ind = 1 THEN 2 
	 WHEN nurse_visits_ind = 1 THEN 3 
	 ELSE 100 END as rnk_num,
Y.dept as dept,
Y.department_description as dept_descr,
Y.department_group, 
nvl(Z.oncology_region1,'ZZZZ')  as oncology_region1,
nvl(Z.oncology_region2,'ZZZZ')  as oncology_region2
 FROM prd_chrg_fct Y
 --LEFT JOIN onc_region_data Z 
 --on Y.fcy_nm = Z.company_id AND Z.department_code = Y.dept
 LEFT JOIN pce_qe16_slp_prd_dm.dept_dim Z
on Y.fcy_nm = Z.company_id AND Z.department_code = Y.dept
 INNER JOIN intermediate_stage_op_chemo_visits_denominator EF
 on EF.encntr_num = Y.encntr_num AND EF.fcy_nm = Y.fcy_nm
 WHERE  ( 
 (Y.cpt_code 
 IN (--Chemo Infusions 2019
'96401','96402','96409','96411','96413','96415','96416','96417','G0498',
--11/02/2020: Added based on new requirements
'95990','96369','93370','96377','96405','96521','96522','96523','96542',
--Non-Chemo Infusions 2019
'96360','96361','96365','96366','96367','96368','96372','96374','96375','96376',
--Telemedicine 2020
'99441','99442','99443','G2010','G2012','99423',
--11/02/2020: Added based on new requirements 
'G0406','Q3014'
--Nurse Visits 11/02/2020: Added based on new requirements 
,'36415','36416','36591','36592','36593','36600','90471','90472','99195','G0008','G0009','G0010','96542'
--Hospital Visists 11/06/2020 : Added based on new requirements
--,'99223','99232','99233','99238','99239','99220','99219','99225','99226','99236','99245','99244','99222','99231','99218','99224','G0378','G0379'
 )) OR
 (Y.betos_cd IN ('M1A','M1B','M2A','M2B'))
       )
 )
,
chrgt_fct_encntrs_filtered_x as (
select fcy_nm, encntr_num, in_or_out_patient_ind, dschrg_dt, cpt_code, betos_cd, new_consults_ind, outpt_visits_ind, chemo_infusion_ind, non_chemo_infusion_ind, telemed_ind, 
--11/02/2020: Added based on new requirements 
nurse_visits_ind,
--11/06/2020: Added based on new requirements 
hsptl_visits_ind,
dept, dept_descr, 
--CASE WHEN ONC.department_code is NOT NULL AND ONC.oncology_region1 IS NOT NULL THEN ONC.oncology_region1 ELSE 'ZZZZ' END as oncology_region1,
--CASE WHEN ONC.department_code is NOT NULL AND ONC.oncology_region2 IS NOT NULL THEN ONC.oncology_region2 ELSE 'ZZZZ' END as oncology_region2,
 oncology_region1,
 oncology_region2,
department_group
,row_num
--,rnk_num
FROM 
(
select *,
 row_number() over (partition by fcy_nm, encntr_num, in_or_out_patient_ind, dschrg_dt order by oncology_region1, oncology_region2 ) as row_num
from chrgt_fct_encntrs_filtered
WHERE (oncology_region1 is NOT NULL AND oncology_region2 IS NOT NULL)
) X
)
,chrg_fct_encntrs_agg as
(select fcy_nm, encntr_num,in_or_out_patient_ind,date(dschrg_dt) as dschrg_dt,
--dept,
oncology_region1, oncology_region2,
--rnk_num,
max(new_consults_ind) as new_consults_ind,
max(outpt_visits_ind) as outpt_visits_ind, 
max(chemo_infusion_ind) as chemo_infusion_ind, 
max(non_chemo_infusion_ind) as non_chemo_infusion_ind, 
max(telemed_ind) as telemed_ind
--11/02/2020: Added based on new requirements 
,max(nurse_visits_ind) as nurse_visits_ind
--11/02/2020: Added based on new requirements 
,max(hsptl_visits_ind) as hsptl_visits_ind
from chrgt_fct_encntrs_filtered_x
--WHERE (oncology_region1 is NOT NULL AND oncology_region2 IS NOT NULL)
GROUP BY 1,2,3,4,5,6
--,7
),
chrg_fct_encntrs_agg_first_row_only as 
(
  select fcy_nm, encntr_num, in_or_out_patient_ind, dschrg_dt, oncology_region1, oncology_region2, new_consults_ind, outpt_visits_ind, chemo_infusion_ind, non_chemo_infusion_ind, telemed_ind, nurse_visits_ind, hsptl_visits_ind
  FROM 
  (
  select *,
  row_number() over (partition by fcy_nm, encntr_num, in_or_out_patient_ind, dschrg_dt order by oncology_region1, oncology_region2 ) as row_num
  from chrg_fct_encntrs_agg
  ) Z 
  WHERE Z.row_num =1 
)
select * from chrg_fct_encntrs_agg_first_row_only
;
---
--CODE CHANGE : OCT 2020 MLH-602:  ORGAN DONOR LOGIC 
DROP TABLE intermediate_stage_organ_donor_encntrs_only IF EXISTS; 
CREATE TABLE intermediate_stage_organ_donor_encntrs_only as 
with prim_dgns_cd_organ_donor as
(select distinct Z.fcy_nm, Z.encntr_num, Z.dschrg_dt,Z.adm_ts, nvl(Z.empi,Z.medical_record_number) as empi , 1 as organ_donor_ind 
FROM prd_encntr_anl_fct Z 
WHERE Z.prim_dgns_cd IN ('Z52.3') )
select * from prim_dgns_cd_organ_donor;

--CODE CHANGE : OCT 2020 MLH-602:  ORGAN DONOR LOGIC 
 DROP TABLE intermediate_stage_chrg_fct_data IF EXISTS; 
CREATE TABLE intermediate_stage_chrg_fct_data as 
 with intermediate_stage_chrg_fct_data_for_fcy_lvl_data_only as 
 (
select  CF.fcy_nm, CF.encntr_num,  nvl(EF.empi,EF.medical_record_number) as empi,EF.dschrg_dt, EF.adm_ts
, case when (ONCREGN.oncology_region1 ='ZZZZ' OR ONCREGN.oncology_region1 is NULL) THEN 'UNKNOWN' ELSE ONCREGN.oncology_region1 END as onc_region1 
, case when (ONCREGN.oncology_region2 ='ZZZZ' OR ONCREGN.oncology_region2 is NULL) THEN 'UNKNOWN' ELSE ONCREGN.oncology_region2 END as onc_region2
,max(CASE WHEN CF.department_group IN ('Lab','Mammography') THEN 1 ELSE 0 END) as max_lab_or_mammo_ind,
max(CASE WHEN CF.department_group NOT IN ('Lab','Mammography') THEN 1 ELSE 0 END) as max_others_ind,
max(nvl(organ_donor_ind,0)) as max_organ_donor_ind,
CASE WHEN (max_organ_donor_ind =1 OR max_lab_or_mammo_ind = 1) AND max_others_ind =0 THEN 0  
     WHEN (max_organ_donor_ind =0 ) AND max_others_ind =1 THEN 1
ELSE 0 END  as  qualify_visit
--Added on 11/12/2020: MLH-608 New Cancer Patient (Facility/Enterprise)
,max(EF.cancer_case_ind) as max_cancer_case_ind
,CASE WHEN max_cancer_case_ind =1 AND qualify_visit =1 THEN 1 ELSE 0 END as qualify_visit_cancer
--Added on 11/12/2020: MLH-608 New Cancer Patient (Facility/Enterprise)
--Updatd on 12/02/2020: MLH-608: Based on client request i.e Removed cancer case = 1 clause 
--,CASE WHEN qualify_visit_cancer =1 AND (onc_region2 <> 'UNKNOWN') THEN 1 ELSE 0 END as qualify_medonc
,CASE WHEN qualify_visit =1 AND (onc_region2 <> 'UNKNOWN') THEN 1 ELSE 0 END as qualify_medonc
FROM prd_chrg_fct CF
INNER JOIN prd_encntr_anl_fct EF
ON CF.fcy_nm = EF.fcy_nm AND EF.encntr_num = CF.encntr_num
LEFT JOIN intermediate_stage_organ_donor_encntrs_only ORGAN
on CF.fcy_nm = ORGAN.fcy_nm AND ORGAN.encntr_num = CF.encntr_num
--Added 11/23/2020 
LEFT JOIN intermediate_stage_onc_region_agg_fct ONCREGN
on ONCREGN.encntr_num = CF.encntr_num AND ONCREGN.fcy_nm = CF.fcy_nm
--CODE CHANGE: Feb 16th 2021: As Per McLaren request removed MMG filter logic 
--WHERE EF.fcy_nm != 'MMG' and CF.department_Group is NOT NULL and EF.dschrg_dt is NOT NULL and EF.dschrg_tot_chrg_amt > 0.0
WHERE 
--EF.fcy_nm != 'MMG' and
CF.department_Group is NOT NULL and EF.dschrg_dt is NOT NULL and EF.dschrg_tot_chrg_amt > 0.0
GROUP BY 1,2,3,4,5,6,7
 )
 select * FROM intermediate_stage_chrg_fct_data_for_fcy_lvl_data_only ;

-----Added on 11/12/2020;: (Start)
DROP TABLE intermediate_stage_fcy_and_entrprise_lvl_new_cancer_ptnts IF EXISTS; 
CREATE TABLE intermediate_stage_fcy_and_entrprise_lvl_new_cancer_ptnts as 
with fcy_lvl_not_qualify_data as
(   select
fcy_nm, encntr_num, empi, dschrg_dt,adm_ts, max_lab_or_mammo_ind, max_organ_donor_ind, max_others_ind, max_cancer_case_ind, 0 as fcy_lvl_new_cncr_ptnt_ind, 0 as entrprise_lvl_new_cncr_ptnt_ind
   FROM intermediate_stage_chrg_fct_data
   WHERE qualify_visit_cancer=0
)
,fcy_lvl_data as
(
select fcy_nm, encntr_num, empi, dschrg_dt,adm_ts, max_lab_or_mammo_ind, max_organ_donor_ind, max_others_ind, max_cancer_case_ind,
CASE WHEN (((max_lab_or_mammo_ind = 1) AND (max_others_ind=0)) OR (max_organ_donor_ind =1 )) THEN  0 
WHEN ( max_others_ind = 1 AND max_organ_donor_ind <> 1 and max_cancer_case_ind = 1 and visit_num=1) THEN 1 
ELSE 0 END as fcy_lvl_new_cncr_ptnt_ind
FROM 
(
   select *, 
   row_number() over (partition by empi , fcy_nm order by dschrg_dt asc,adm_ts asc, encntr_num ) as visit_num
   FROM intermediate_stage_chrg_fct_data
   WHERE qualify_visit_cancer=1
)Z

),
entrprise_lvl_data as
( select fcy_nm, encntr_num, empi, dschrg_dt,adm_ts, max_lab_or_mammo_ind as labs_or_mammo_chrg_ind, max_organ_donor_ind, max_others_ind as other_chrg_ind,max_cancer_case_ind, fcy_lvl_new_cncr_ptnt_ind,
CASE WHEN (((max_lab_or_mammo_ind = 1) AND (max_others_ind=0)) OR (max_organ_donor_ind =1 )) THEN  0 
WHEN (visit_num =1 AND max_others_ind = 1 AND max_organ_donor_ind <> 1 and max_cancer_case_ind =1) THEN 1 
ELSE 0 END as entrprise_lvl_new_cncr_ptnt_ind
FROM 
(
   select *, 
   row_number() over (partition by empi order by dschrg_dt asc, adm_ts asc ,encntr_num asc ) as visit_num
   FROM fcy_lvl_data
) Z
)
select * From entrprise_lvl_data UNION 
select * From fcy_lvl_not_qualify_data; 

----(End)
--CODE CHANGE : OCT 2020 MLH-602:  ORGAN DONOR LOGIC 
DROP TABLE intermediate_stage_fcy_and_entrprise_lvl_new_ptnts IF EXISTS; 
CREATE TABLE intermediate_stage_fcy_and_entrprise_lvl_new_ptnts as 
with fcy_lvl_not_qualify_data as
(   select
fcy_nm, encntr_num, empi, dschrg_dt,adm_ts, max_lab_or_mammo_ind, max_organ_donor_ind, max_others_ind, 0 as fcy_lvl_new_ptnt_ind, 0 as entrprise_lvl_new_ptnt_ind
   FROM intermediate_stage_chrg_fct_data
   WHERE qualify_visit=0
)
,fcy_lvl_data as
(
select fcy_nm, encntr_num, empi, dschrg_dt,adm_ts, max_lab_or_mammo_ind, max_organ_donor_ind, max_others_ind, 
CASE WHEN (((max_lab_or_mammo_ind = 1) AND (max_others_ind=0)) OR (max_organ_donor_ind =1 )) THEN  0 
WHEN ( max_others_ind = 1 AND max_organ_donor_ind <> 1 and visit_num=1) THEN 1 
ELSE 0 END as fcy_lvl_new_ptnt_ind
FROM 
(
   select *, 
   row_number() over (partition by empi , fcy_nm order by dschrg_dt asc, adm_ts asc, encntr_num asc ) as visit_num
   FROM intermediate_stage_chrg_fct_data
   WHERE qualify_visit=1
)Z
--WHERE Z.empi = '300001342170'
--AND Z.row_num = 1
),
entrprise_lvl_data as
( select fcy_nm, encntr_num, empi, dschrg_dt,adm_ts , max_lab_or_mammo_ind as labs_or_mammo_chrg_ind, max_organ_donor_ind, max_others_ind as other_chrg_ind, fcy_lvl_new_ptnt_ind,
CASE WHEN (((max_lab_or_mammo_ind = 1) AND (max_others_ind=0)) OR (max_organ_donor_ind =1 )) THEN  0 
WHEN (visit_num =1 AND max_others_ind = 1 AND max_organ_donor_ind <> 1) THEN 1 
ELSE 0 END as entrprise_lvl_new_ptnt_ind
FROM 
(
   select *, 
   row_number() over (partition by empi order by dschrg_dt asc , adm_ts asc,encntr_num asc) as visit_num
   FROM fcy_lvl_data
) Z
)
select * From entrprise_lvl_data UNION 
select * From fcy_lvl_not_qualify_data; 

--MLH-618
-----Added on 11/23/2020;: (Start)


DROP TABLE intermediate_stage_fcy_and_entrprise_lvl_new_medonc_ptnts IF EXISTS; 
CREATE TABLE intermediate_stage_fcy_and_entrprise_lvl_new_medonc_ptnts as 
with fcy_lvl_not_qualify_data as
(   select
fcy_nm, encntr_num, empi, dschrg_dt,adm_ts, max_lab_or_mammo_ind, max_organ_donor_ind, max_others_ind, max_cancer_case_ind, 0 as fcy_lvl_new_medonc_ptnt_ind, 0 as entrprise_lvl_new_medonc_ptnt_ind
,onc_region1 as oncology_region1
,onc_region2 as oncology_region2
   FROM intermediate_stage_chrg_fct_data
   WHERE qualify_medonc=0
)
,fcy_lvl_data as 
(
select fcy_nm, encntr_num, empi, dschrg_dt,adm_ts, max_lab_or_mammo_ind, max_organ_donor_ind, max_others_ind, max_cancer_case_ind,
CASE WHEN (((max_lab_or_mammo_ind = 1) AND (max_others_ind=0)) OR (max_organ_donor_ind =1 )) THEN  0 
WHEN ( max_others_ind = 1 AND max_organ_donor_ind <> 1 
--and max_cancer_case_ind = 1
and visit_num=1) THEN 1 
ELSE 0 END as fcy_lvl_new_medonc_ptnt_ind
,onc_region1 as oncology_region1
,onc_region2 as oncology_region2

FROM 
(
   select *, 
   row_number() over (partition by empi , fcy_nm order by dschrg_dt asc,adm_ts asc, encntr_num ) as visit_num
   FROM intermediate_stage_chrg_fct_data
   WHERE qualify_medonc=1
)Z

)
--select * FROM fcy_lvl_data LIMIT 100 
,entrprise_lvl_data as
( select fcy_nm, encntr_num, empi, dschrg_dt,adm_ts, max_lab_or_mammo_ind as labs_or_mammo_chrg_ind, max_organ_donor_ind, max_others_ind as other_chrg_ind,max_cancer_case_ind, fcy_lvl_new_medonc_ptnt_ind,
CASE WHEN (((max_lab_or_mammo_ind = 1) AND (max_others_ind=0)) OR (max_organ_donor_ind =1 )) THEN  0 
WHEN (visit_num =1 AND max_others_ind = 1 AND max_organ_donor_ind <> 1 
--and max_cancer_case_ind =1
) THEN 1 
ELSE 0 END as entrprise_lvl_new_medonc_ptnt_ind
, oncology_region1
, oncology_region2
FROM 
(
   select *, 
   row_number() over (partition by empi order by dschrg_dt asc, adm_ts asc ,encntr_num asc ) as visit_num
   FROM fcy_lvl_data
) Z
)
select * From entrprise_lvl_data UNION 
select * From fcy_lvl_not_qualify_data; 

----(End)

--Logic to tie Op visits with the Ip visits / ED visits which are occurred after on or  30 days of Op chemo visits 
--select 'processing table:  intermediate_stage_encntr_oncology_anl_fct' as table_processing;
DROP TABLE intermediate_stage_encntr_oncology_anl_fct IF EXISTS; 
CREATE  TABLE intermediate_stage_encntr_oncology_anl_fct AS 
with op_chemo_30_days_ip_or_ed_visits as 
(select fcy_nm , encntr_num , empi, in_or_out_patient_ind, dschrg_dt, chemo_denom_ind, chemo_numer_ind, ed_case_ind, 
case when nxt_encntr_dschrg_dt-dschrg_dt <= 30 and nxt_encntr_type='I' then 1 else 0 end as ip_visit_after_30_days_of_op_chemo_ind,
case when nxt_encntr_dschrg_dt-dschrg_dt <= 30 and nxt_encntr_type='I' and nxt_encntr_ed_case_ind =1 then 1 else 0 end as ed_visit_after_30_days_of_op_chemo_ind 
from
(
SELECT *,
lead(dschrg_dt,1) over (partition by empi order by dschrg_dt, adm_ts, encntr_num  )as nxt_encntr_dschrg_dt,
lead(in_or_out_patient_ind,1) over (partition by empi order by dschrg_dt, adm_ts, encntr_num  )as nxt_encntr_type,
lead(ed_case_ind,1) over (partition by empi order by dschrg_dt, adm_ts, encntr_num  )as nxt_encntr_ed_case_ind
FROM intermediate_stage_chemo_ip_visits_for_op_chemo Z 
)
a
)
select X.*,
CASE WHEN X.in_or_out_patient_ind = 'O' THEN nvl(chemo_numer_ind,0) ELSE 0 END  as chemo_numer_ind,
nvl(ed_case_ind, 0) as ed_case_ind ,
CASE WHEN X.in_or_out_patient_ind = 'O' THEN nvl(ip_visit_after_30_days_of_op_chemo_ind,0) ELSE 0 END as ip_visit_after_30_days_of_op_chemo_ind,
CASE WHEN X.in_or_out_patient_ind = 'O' THEN nvl(ed_visit_after_30_days_of_op_chemo_ind,0) ELSE 0 END as ed_visit_after_30_days_of_op_chemo_ind,
nvl(Z.cases_without_antiemetic,0) as cases_without_antiemetic_ind,
nvl(Z.cases_with_antiemetic,0) as cases_with_antiemetic_ind,
nvl(Z.highemeto_antiecases_denominator,0) as highemeto_antiecases_denom_ind
--Adding New Measures w.r.to OncRegion
, nvl(ONCREGN.new_consults_ind,0) as new_consults_ind
, nvl(ONCREGN.outpt_visits_ind,0) as outpt_visits_ind
, nvl(ONCREGN.chemo_infusion_ind,0 ) as chemo_infusion_ind
, nvl(ONCREGN.non_chemo_infusion_ind,0 ) as non_chemo_infusion_ind 
, nvl(ONCREGN.telemed_ind,0)  as telemed_ind
--11/02/2020: Added based on new requirements 
, nvl(ONCREGN.nurse_visits_ind,0)  as nurse_visits_ind
--11/06/2020: Added based on new requirements 
, nvl(ONCREGN.hsptl_visits_ind,0)  as hsptl_visits_ind
, case when (ONCREGN.oncology_region1 ='ZZZZ' OR ONCREGN.oncology_region1 is NULL) THEN 'UNKNOWN' ELSE ONCREGN.oncology_region1 END as oncology_region1 
, case when (ONCREGN.oncology_region2 ='ZZZZ' OR ONCREGN.oncology_region2 is NULL) THEN 'UNKNOWN' ELSE ONCREGN.oncology_region2 END as oncology_region2
--Adding New measures w.r.to MLH-602
,nvl(NEWPTNT.labs_or_mammo_chrg_ind, 0) as labs_or_mammo_chrg_ind
,nvl(ORGAN.organ_donor_ind,0) as organ_donor_ind
,nvl(NEWPTNT.other_chrg_ind,0) as other_chrg_ind
,nvl(NEWPTNT.fcy_lvl_new_ptnt_ind,0) as fcy_lvl_new_ptnt_ind
,nvl(NEWPTNT.entrprise_lvl_new_ptnt_ind, 0) as entrprise_lvl_new_ptnt_ind
--Adding New Measures w.r.to MLH-608 New Cancer Patients (Facility & Enterprise)
,nvl(NEWCNCRPTNT.fcy_lvl_new_cncr_ptnt_ind,0) as fcy_lvl_new_cncr_ptnt_ind
,nvl(NEWCNCRPTNT.entrprise_lvl_new_cncr_ptnt_ind, 0) as entrprise_lvl_new_cncr_ptnt_ind
--Adding New Measures w.r.to MLH-612 MEd Onc Patients (Facility & Enterprise)
,nvl(MEDONCPTNT.fcy_lvl_new_medonc_ptnt_ind,0) as fcy_lvl_new_medonc_ptnt_ind
,nvl(MEDONCPTNT.entrprise_lvl_new_medonc_ptnt_ind, 0) as entrprise_lvl_new_medonc_ptnt_ind
FROm intermediate_stage_op_chemo_visits_denominator X
LEFT JOIN op_chemo_30_days_ip_or_ed_visits Y
on X.fcy_nm = Y.fcy_nm  AND X.encntr_num = Y.encntr_num AND X.empi = Y.empi 
--Adding Highly Emeto Geneic and Anti-emetic Case INDICATOR details
LEFT JOIN intermediate_stage_high_emeto_antiemetic_cases Z
on X.fcy_nm = Z.fcy_nm and X.encntr_num = Z.encntr_num 
--Adding New Measures w.r.to OncRegion
LEFT JOIN intermediate_stage_onc_region_agg_fct ONCREGN
on ONCREGN.fcy_nm = X.fcy_nm and ONCREGN.encntr_num = X.encntr_num
--Adding New Measures w.r.to New Patients
LEFT JOIN intermediate_stage_fcy_and_entrprise_lvl_new_ptnts NEWPTNT
on NEWPTNT.fcy_nm = X.fcy_nm and NEWPTNT.encntr_num = X.encntr_num
--Adding New Measures w.r.to Organ Donor
LEFT JOIN intermediate_stage_organ_donor_encntrs_only ORGAN
on X.fcy_nm = ORGAN.fcy_nm AND ORGAN.encntr_num = X.encntr_num
--Adding New Measures w.r.to MLH-608 New Cancer Patients (Facility & Enterprise)
LEFT JOIN intermediate_stage_fcy_and_entrprise_lvl_new_cancer_ptnts NEWCNCRPTNT
on NEWCNCRPTNT.fcy_nm = X.fcy_nm and NEWCNCRPTNT.encntr_num = X.encntr_num
--Adding New Measures w.r.to MLH-612 Med-Onc Patients (Facility & Enterprise)
LEFT JOIN intermediate_stage_fcy_and_entrprise_lvl_new_medonc_ptnts MEDONCPTNT
on MEDONCPTNT.fcy_nm = X.fcy_nm and MEDONCPTNT.encntr_num = X.encntr_num
;
----------------------
--Added 11/24/2020 : Prisoner Flag (Legacy)
DROP TABLE intermediate_stage_encntr_anl_pqsd_fct IF EXISTS;
CREATE TABLE intermediate_stage_encntr_anl_pqsd_fct AS
SELECT EF.fcy_nm, EF.encntr_num, LE.discharge, LE.discharge_disposition,LE.discharge_to, LE.facility, LE.financial_class, LE.medical_service, LE.encounter_type, nvl(LE.exclusion_ind,0)
as prsnr_excln_ind
--,case when (EF.fcy_nm in ('Oakland','Port Huron') and EF.dschrg_svc in ('BEH','GERI','REHAB','PSYCH'))then 0 else 1 end as dschrg_svc_excl
--,case when (EF.dschrg_dt >= '06/01/2018' and EF.fcy_nm = 'Lansing' and EF.dschrg_svc in ('Behavioral Medicine','Rehabilitation')) then 0 else 1 end as lnsg_dschrg_svc_excl
--,case when (EF.dschrg_dt >= '10/01/2018' and EF.upd_id = 'Incarcerated' and EF.fcy_nm ='Lansing') then 0 else 1 end as crnr_lnsg_prsnr_excl_ind
--,case when (dschrg_svc_excl = 0 or crnr_lnsg_prsnr_excl_ind = 0 or prsnr_excln_ind <> 1 or lnsg_dschrg_svc_excl =0) then 0 else 1 end as dsc_prsnr_excl_ind
FROM prd_encntr_anl_fct EF
LEFT JOIN pce_qe16_prd..lansing_prisoner_encounters LE
on EF.encntr_num = LE.encntr_num ;

DROP TABLE intermediate_stage_encntr_anl_pqsd_fct_prev IF EXISTS;
ALTER TABLE intermediate_encntr_anl_pqsd_fct RENAME TO intermediate_stage_encntr_anl_pqsd_fct_prev;
ALTER TABLE intermediate_stage_encntr_anl_pqsd_fct  RENAME TO intermediate_encntr_anl_pqsd_fct;

select 'processing table:  prd_encntr_anl_pqsd_fct' as table_processing;
DROP TABLE prd_encntr_anl_pqsd_fct if EXISTS;
CREATE TABLE prd_encntr_anl_pqsd_fct AS SELECT *,now() as rcrd_isrt_ts FROM intermediate_encntr_anl_pqsd_fct;
---------------------
select 'processing table: intermediate_encntr_oncology_anl_fct_prev ' as table_processing;
DROP TABLE intermediate_encntr_oncology_anl_fct_prev IF EXISTS;
ALTER TABLE intermediate_encntr_oncology_anl_fct RENAME TO intermediate_encntr_oncology_anl_fct_prev;
ALTER TABLE intermediate_stage_encntr_oncology_anl_fct  RENAME TO intermediate_encntr_oncology_anl_fct;

select 'processing table:  prd_encntr_oncology_anl_fct' as table_processing;
DROP TABLE prd_encntr_oncology_anl_fct if EXISTS;
CREATE TABLE prd_encntr_oncology_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM intermediate_encntr_oncology_anl_fct;

\unset ON_ERROR STOP
