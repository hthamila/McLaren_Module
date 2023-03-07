DROP TABLE tmp_lung_cancer_fct IF EXISTS;
create table tmp_lung_cancer_fct as
( SELECT distinct ef.medical_record_number, ef.fcy_num, ef.fcy_nm, ef.encntr_num, 1 as lung_cancer_scrn_ind
  FROM  intermediate_stage_encntr_anl_fct ef
  inner join  intermediate_stage_chrg_fct cf on ef.encntr_num = cf.patient_id
  inner join val_set_dim vd on cf.charge_code = vd.cd and cf.fcy_num=vd.val_set_nm and vd.cohrt_id = 'LUNG_SCREEN_CHRG_CD'

  UNION

SELECT distinct ef.medical_record_number, ef.fcy_num, ef.fcy_nm, ef.encntr_num, 1 as lung_cancer_scrn_ind
  FROM  intermediate_stage_encntr_anl_fct ef
  inner join  intermediate_stage_chrg_fct cf on ef.encntr_num = cf.patient_id
  inner join val_set_dim vd on cf.cpt_code = vd.cd  and vd.cohrt_id = 'LUNG_SCREEN_CPT_CD'

--CODE CHANGE: JAN 24th 2020 AS per JIRA - MLH :
 UNION
  SELECT distinct ef.medical_record_number, ef.fcy_num, ef.fcy_nm, ef.encntr_num, 1 as lung_cancer_scrn_ind
  FROM  intermediate_stage_encntr_anl_fct ef
  inner join  intermediate_stage_cpt_fct cpf on ef.encntr_num = cpf.patient_id
  inner join val_set_dim vd on cpf.cpt_code = vd.cd and vd.cohrt_id='LUNG_SCREEN_CPT_CD'
  );

DROP TABLE tmp_robo_encntr_fct IF EXISTS;
create table tmp_robo_encntr_fct as
(SELECT distinct epf.patient_id as encntr_num
FROM  intermediate_stage_encntr_pcd_fct epf
inner join val_set_dim vd on epf.icd_code = vd.cd and  vd.cohrt_id = 'ROBO_SURG_PCD_CD'
left join pce_qe16_prd..icd_pcd_cd_dim ipd on epf.icd_code = ipd.icd_pcd_cd

union

SELECT distinct ef.encntr_num
  FROM  intermediate_stage_encntr_anl_fct ef
  inner join  intermediate_stage_cpt_fct cpf on ef.encntr_num = cpf.patient_id
  inner join val_set_dim vd on cpf.cpt_code = vd.cd and vd.cohrt_id='ROBO_SURG_CPT_CD'

 union

  SELECT distinct ef.encntr_num
  FROM  intermediate_stage_encntr_anl_fct ef
  inner join  intermediate_stage_chrg_fct cf on ef.encntr_num = cf.patient_id
  inner join val_set_dim vd on cf.persp_clncl_smy_cd = vd.cd and vd.cohrt_id='ROBO_SURG_SPL_SMY_CD'


  union

  SELECT distinct ef.encntr_num
  FROM  intermediate_stage_encntr_anl_fct ef
  inner join  intermediate_stage_chrg_fct cf on ef.encntr_num = cf.patient_id
  inner join val_set_dim vd on cf.charge_code = vd.cd and vd.cohrt_id='ROBO_SURG_CHRG_CD'  and cf.fcy_num=vd.val_set_nm
  WHERE upper(vd.cd_descr) = upper(cf.chargecodedesc)
  );
