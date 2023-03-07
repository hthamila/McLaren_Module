DROP TABLE pce_qe16_inst_bill_prd_lnd..encntr_rcc_cost_fct IF EXISTS; 

CREATE TABLE pce_qe16_inst_bill_prd_lnd..encntr_rcc_cost_fct AS 
SELECT distinct 
EF.fcy_nm as company_id,
EF.fcy_num,
EF.encntr_num as patient_id,
EF.in_or_out_patient_ind as inpatient_outpatient_flag, 
EF.medical_record_number,
EF.adm_dt,
EF.dschrg_dt,
EF.dschrg_tot_chrg_amt,
EF.agg_rcc_based_direct_cst_amt,
EF.agg_rcc_based_indirect_cst_amt,
EF.agg_rcc_based_total_cst_amt,
--11/05/2021: 
CASE 
--Inpatient Check 
--2022-04-08 : MLH-991 - Added Logic to Handle Swing Beds for Thumb and Caro

WHEN EF.in_or_out_patient_ind ='I' AND EF.dschrg_svc = 'SB' THEN '10'
WHEN EF.in_or_out_patient_ind ='I' AND (EF.iptnt_encntr_type IN ('Newborn','Acute') ) THEN '08'
WHEN EF.in_or_out_patient_ind ='I' AND (EF.iptnt_encntr_type = 'Specialty Care' ) THEN '22'
WHEN EF.in_or_out_patient_ind ='I' AND (EF.iptnt_encntr_type = 'Rehab' ) THEN '23'
WHEN EF.in_or_out_patient_ind ='I' AND (EF.iptnt_encntr_type = 'Psych' ) THEN '24'
WHEN EF.in_or_out_patient_ind ='I' AND (EF.iptnt_encntr_type = 'Hospice' ) THEN '25'
--Outpatient Check 
--11/15/2021: Updated the Observation and Same Day Surgery
WHEN EF.in_or_out_patient_ind ='O' AND (EF.obsrv_stay_ind = 1 ) THEN '29'
WHEN EF.in_or_out_patient_ind ='O' AND (EF.srgl_case_ind = 1 ) THEN '27'
WHEN EF.in_or_out_patient_ind ='O' AND (EF.ed_case_ind = 1 ) THEN '28'
WHEN EF.in_or_out_patient_ind = 'O' AND EF.rcurrng_case_ind = 1 THEN '31'
ELSE
'90'
END AS qa_ptnt_typ_cd,
--11/10/2021
CASE 
--Inpatient Check 
--2022-04-08 : MLH-991 - Added Logic to Handle Swing Beds for Thumb and Caro
--2022-09-16 : PCENB-96 - Used Inpatient Encounter Type for the derivation of Patient Type Code
WHEN EF.in_or_out_patient_ind ='I' AND EF.dschrg_svc = 'SB' THEN 'Skilled Nursing'
WHEN EF.in_or_out_patient_ind ='I' AND (EF.iptnt_encntr_type IN ('Newborn','Acute') ) THEN 'Acute Inpatient'
WHEN EF.in_or_out_patient_ind ='I' AND (EF.iptnt_encntr_type = 'Specialty Care' ) THEN 'Long Term Care'
WHEN EF.in_or_out_patient_ind ='I' AND (EF.iptnt_encntr_type = 'Rehab' ) THEN 'Rehabiliation'
WHEN EF.in_or_out_patient_ind ='I' AND (EF.iptnt_encntr_type = 'Psych') THEN 'Psyhiatric'
WHEN EF.in_or_out_patient_ind ='I' AND (EF.iptnt_encntr_type = 'Hospice') THEN 'Hospice'
--Outpatient Check 
--11/15/2021: Updated the Observation and Same Day Surgery
WHEN EF.in_or_out_patient_ind ='O' AND (EF.obsrv_stay_ind = 1 ) THEN 'Observation' 
WHEN EF.in_or_out_patient_ind ='O' AND (EF.srgl_case_ind = 1 ) THEN 'Same Day Surgery'
WHEN EF.in_or_out_patient_ind ='O' AND (EF.ed_case_ind = 1 ) THEN 'Emergency'
WHEN EF.in_or_out_patient_ind = 'O' AND EF.rcurrng_case_ind = 1 THEN 'Recurring/Series'
ELSE
'Other'
END AS qa_ptnt_typ_descr,
CASE WHEN EF.ed_case_ind = 1 THEN 'Y' ELSE 'N' END  as  ed_case_ind,
--2022-03-02 : MLH-947,MLH-954 - Adding NPI's for QA Data submissions
EF.adm_pract_npi as admitting_practitioner_npi,
EF.attnd_pract_npi as  attending_practitioner_npi,
EF.cnslt_pract_1_npi as consulting_practitioner_npi_1,
EF.cnslt_pract_2_npi as consulting_practitioner_npi_2,
EF.cnslt_pract_3_npi as consulting_practitioner_npi_3,
EF.rcrd_isrt_ts
FROM pce_qe16_slp_prd_dm..prd_encntr_anl_fct EF
where
dschrg_tot_chrg_amt > 0
DISTRIBUTE ON (company_id, patient_id);

DROP TABLE pce_qe16_inst_bill_prd_lnd..chrg_rcc_cost_fct IF EXISTS;

CREATE TABLE pce_qe16_inst_bill_prd_lnd..chrg_rcc_cost_fct AS
(SELECT
 CF.fcy_nm as company_id,
 CF.fcy_num,
 CF.encntr_num as patient_id,
 CF.service_date,
 CF.charge_code,
 CF.quantity,
 CF.total_charge,
 CF.total_variable_cost,
 CF.total_fixed_cost,
 CF.cpt_code,
 CF.revenue_code,
 CF.client_revenue_code_group,
 CF.dept,
 CF.postdate,
 CF.direct_cost_ratio,
 CF.indirect_cost_ratio,
 CF.total_cost_ratio,
 CF.rcc_based_direct_cst_amt,
 CF.rcc_based_indirect_cst_amt,
 CF.rcc_based_total_cst_amt,
 CF.crline,
 --11/03/21: Added the following
CF.cpt_modifier_1,
CF.cpt_modifier_2,
CF.cpt_modifier_3,
CF.cpt_modifier_4,
CF.ordering_practitioner_code,
--2022-03-02 : MLH-947,MLH-954 - Adding NPI's for QA Data submissions
CF.encntr_pcd_ordering_npi as ordering_practitioner_npi,
CF.rcrd_isrt_ts
 FROM pce_qe16_slp_prd_dm..prd_chrg_fct CF
)
DISTRIBUTE ON (company_id, patient_id);


--2022-03-02 : MLH-947,MLH-954 - Adding NPI's for QA Data submissions
DROP TABLE pce_qe16_inst_bill_prd_lnd..slp_prd_cpt_fct IF EXISTS;
CREATE TABLE pce_qe16_inst_bill_prd_lnd..slp_prd_cpt_fct AS
(SELECT
DISTINCT
cpt.fcy_nm as company_id,
cpt.fcy_num,
cpt.encntr_num as patient_id,
cpt.cpt_code,
DATE(cpt.cpt_code_ts) as cpt_code_date,
cpt.cpt_modifier_1,
cpt.cpt_modifier_2,
cpt.cpt_modifier_3,
cpt.cpt_modifier_4,
cpt.cpt4seq,
cpt.pcd_pract_npi as procedure_practitioner_npi,
cpt.encntr_pcd_ordering_npi as ordering_practitioner_npi,
cpt.rcrd_isrt_ts
FROM pce_qe16_slp_prd_dm..prd_cpt_fct cpt
)
DISTRIBUTE ON (company_id, patient_id);


DROP TABLE pce_qe16_inst_bill_prd_lnd..slp_prd_encntr_pcd_fct IF EXISTS;
CREATE TABLE pce_qe16_inst_bill_prd_lnd..slp_prd_encntr_pcd_fct AS
(SELECT
DISTINCT
pcd.fcy_nm as company_id,
pcd.fcy_num,
pcd.encntr_num as patient_id,
pcd.procedure_date,
pcd.icd_code,
pcd.procedureseq,
pcd.encntr_pcd_surgeon_npi as surgeon_npi,
pcd.encntr_pcd_ordering_npi as orderingphysician_npi,
pcd.rcrd_isrt_ts
FROM pce_qe16_slp_prd_dm..prd_encntr_pcd_fct pcd
)
DISTRIBUTE ON (company_id, patient_id);


DROP TABLE pce_qe16_inst_bill_prd_lnd..slp_prd_encntr_pract_fct IF EXISTS;
CREATE TABLE pce_qe16_inst_bill_prd_lnd..slp_prd_encntr_pract_fct AS
(SELECT
DISTINCT 
pract.fcy_nm as company_id,
pract.encntr_num as patient_id,
pract.service_start_date,
pract.service_end_date,
pract.practitioner_role,
pract.raw_role,
pract.encntr_pract_npi as practitioner_npi,
pract.rcrd_isrt_ts
FROM pce_qe16_slp_prd_dm..prd_encntr_pract_fct pract
)
DISTRIBUTE ON (company_id, patient_id);

