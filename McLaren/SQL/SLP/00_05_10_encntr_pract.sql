--select 'processing table:  intermediate_stage_encntr_pract_fct' as table_processing;
DROP TABLE intermediate_stage_encntr_pract_fct IF EXISTS ;
CREATE TABLE intermediate_stage_encntr_pract_fct AS
(
  SELECT
		Z.company_id as fcy_nm
	   ,Z.patient_id as encntr_num
       ,CH.company_id
       ,CH.patient_id
       ,CH.practitioner_role
       ,CH.practitioner_code
	   ,DATE (to_timestamp((CH.service_start_date || ' ' || nvl(substr(CH.service_start_date, 1, 2), '00') || ':' || nvl(substr(CH.service_start_date, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS service_start_date
	   ,DATE (to_timestamp((CH.service_end_date || ' ' || nvl(substr(CH.service_end_date, 1, 2), '00') || ':' || nvl(substr(CH.service_end_date, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS service_end_date
       ,CH.raw_role
	   ,SPCL.npi as encntr_pract_npi
       ,SPCL.practitioner_name as encntr_pract_nm
       ,SPCL.practitioner_spclty_description as encntr_pract_splcy_descr
       ,SPCL.mcare_spcly_cd as encntr_pract_splcy_cd
--	   ,SPCL.hcare_pvdr_txnmy_cd as prim_txnmy_cd
--	   ,SPCL.hcare_pvdr_txnmy_cl_nm as prim_txnmy_cl_nm
	   ,SPCL.npi_dactv_dt
	   ,row_number() over(partition by Z.company_id, Z.patient_id
Order by  CH.service_start_date) as rec_num
  FROM intermediate_stage_temp_eligible_encntr_data Z
  LEFT JOIN cv_patprac CH
  on Z.company_id = CH.company_id and Z.patient_id = CH.patient_id
  LEFT JOIN intermediate_stage_temp_physician_npi_spclty SPCL
on SPCL.company_id = CH.company_id and SPCL.practitioner_code = CH.practitioner_code
)
-- DISTRIBUTE ON (fcy_nm_hash, encntr_num_hash);
 DISTRIBUTE ON (fcy_nm, encntr_num);

--Code Change :  Logic to mark specl_valid_ind for Inpatient (Medical DRG's)
--select 'processing table:  intermediate_stage_temp_specl_valid_ind' as table_processing;
DROP TABLE intermediate_stage_temp_specl_valid_ind IF EXISTS ;
CREATE  TABLE  intermediate_stage_temp_specl_valid_ind AS
select distinct P.fcy_nm, P.encntr_num, 1 as specl_valid_ind
FROM intermediate_stage_encntr_pract_fct P
INNER JOIN intermediate_stage_temp_eligible_encntr_data A
on A.company_id = P.fcy_nm and A.patient_id = P.encntr_num and A.inpatient_outpatient_flag = 'I'
INNER JOIN intermediate_stage_temp_physician_npi_spclty S
on S.company_id = P.fcy_nm and S.practitioner_code = P.practitioner_code
INNER JOIN ms_drg_dim MSDRG
on MSDRG.ms_drg_cd = CAST(LPAD(CAST(coalesce(A.msdrg_code,'000') as INTEGER), 3,0 ) as Varchar(3)) and MSDRG.ms_drg_type_cd = 'MED'
WHERE A.inpatient_outpatient_flag = 'I' AND
 Upper(S.practitioner_spclty_description) in
(
'CARDIOVASCULAR DISEASE',
'INTERVENTIONAL CARDIOLOGY',
'CARDIAC SURGERY',
'THORACIC SURGERY (CARDIOTHORACIC VASCULAR SURGERY)',
'CLINICAL CARDIAC ELECTROPHYSIOLOGY',
'VASCULAR SURGERY',
'PSYCHIATRY & NEUROLOGY',
'NEUROLOGY',
'NEUROSURGERY',
'NEUROLOGICAL SURGERY',
'NEUROMUSCULOSKELETAL MEDICINE & OMM'
);