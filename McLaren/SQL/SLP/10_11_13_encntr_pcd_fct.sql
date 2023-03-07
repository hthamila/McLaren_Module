-----intermediate_stage_encntr_pcd_fct Table  creation based on Net 3 years Of patient Account Number
--select 'processing table:  intermediate_stage_encntr_pcd_fct' as table_processing;
DROP TABLE intermediate_stage_encntr_pcd_fct IF EXISTS ;
CREATE TABLE intermediate_stage_encntr_pcd_fct AS
SELECT
         Z.company_id as fcy_nm
	   , z.patient_id AS encntr_num
       , VSET_FCY.alt_cd as fcy_num
       , DF.company_id
       , DF.patient_id
       , nvl(DF.icd_code, '-100') as icd_code
       , DF.icd_type
       , nvl(DF.surgeon_code, '-100') as surgeon_code

	   ---Srujan Update Start----------------
	   /*Start PCD CCS Attributes*/
	   ,NVL(PCDD.icd_pcd_ccs_cgy_cd,'-100') AS icd_pcd_ccs_cgy_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(pcdd.icd_pcd_ccs_cgy_descr,'UNKNOWN') AS icd_pcd_ccs_cgy_descr
	   ,NVL(PCDD.icd_pcd_ccs_lvl_1_cd,'-100') AS icd_pcd_ccs_lvl_1_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,NVL(PCDD.icd_pcd_ccs_lvl_1_descr,'UNKNOWN') AS icd_pcd_ccs_lvl_1_descr
	   ,NVL(PCDD.icd_pcd_ccs_lvl_2_cd,'-100') AS icd_pcd_ccs_lvl_2_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,NVL(PCDD.icd_pcd_ccs_lvl_2_descr,'UNKNOWN') AS icd_pcd_ccs_lvl_2_descr
	   /*End PCD CCS Attributes*/
	    ---Srujan Update End----------------


	   ,SURGEON.npi as encntr_pcd_surgeon_npi
       ,SURGEON.practitioner_name as encntr_pcd_surgeon_pract_nm
       ,SURGEON.practitioner_spclty_description as encntr_pcd_surgeon_splcy_descr
       ,SURGEON.mcare_spcly_cd as encntr_pcd_surgeon_splcy_cd
       ,DATE (to_timestamp((DF.procedure_date || ' ' || nvl(substr(DF.procedure_date, 1, 2), '00') || ':' || nvl(substr(DF.procedure_date, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS procedure_date
--       , DF.diagnosis_code_present_on_admission_flag
       , DF.icd_version
       , DF.procedureseq
       , DF.proceduretype
       , nvl(DF.orderingphysician, '-100') as orderingphysician
	   ,ORDERING.npi as encntr_pcd_ordering_npi
       ,ORDERING.practitioner_name as encntr_pcd_ordering_pract_nm
       ,ORDERING.practitioner_spclty_description as encntr_pcd_ordering_splcy_descr
       ,ORDERING.mcare_spcly_cd as encntr_pcd_ordering_splcy_cd
       , DF.procedurestarttime
       ,DATE (to_timestamp((DF.procedureenddate || ' ' || nvl(substr(DF.procedureenddate, 1, 2), '00') || ':' || nvl(substr(DF.procedureenddate, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS procedureenddate
       , DF.procedureendtime
       --, DF.updatedateproc
       , DF.sourcesystemproc
	   	   , row_number() over(partition by Z.company_id, Z.patient_id
Order by  DF.procedureseq) as rec_num
--       , DF.diagnosisseq
--       , DF.diagnosistype
  FROM intermediate_stage_temp_eligible_encntr_data Z
  LEFT JOIN pcd_fct DF
  on Z.company_id = DF.company_id and Z.patient_id = DF.patient_id

  -----------Srujan Update Start---------------------------
  /*Start Join for PCD CCS Attributes*/
  LEFT JOIN pcd_dim pcdd on replace(df.icd_code,'.','') = replace(pcdd.icd_pcd_cd,'.','') and df.icd_version = pcdd.icd_ver
  /*End Join for PCD CCS Attributes*/
   ---Srujan Update End----------------


  LEFT JOIN val_set_dim VSET_FCY
  ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN intermediate_stage_temp_physician_npi_spclty SURGEON
  on SURGEON.company_id = DF.company_id and SURGEON.practitioner_code = DF.surgeon_code
    LEFT JOIN intermediate_stage_temp_physician_npi_spclty ORDERING
  on ORDERING.company_id = DF.company_id and ORDERING.practitioner_code = DF.orderingphysician
  -- DISTRIBUTE ON (fcy_nm_hash, encntr_num_hash);
  DISTRIBUTE ON (fcy_nm, encntr_num);
