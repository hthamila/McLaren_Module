--CODE CHANGE: MLH-591

DROP TABLE intermediate_stage_temp_ptnt_prim_n_second_dgns_with_cancer IF EXISTS;
	CREATE TABLE intermediate_stage_temp_ptnt_prim_n_second_dgns_with_cancer AS (
with prim_dgns_data AS
(
	SELECT Z.company_id
		,Z.patient_id
		,Z.icd_code
		,DGNS.dgns_descr
		,Z.icd_type
		,Z.icd_version
		,Z.diagnosisseq
		,Z.diagnosistype
		,Z.diagnosis_code_present_on_admission_flag,
		 Z.non_cancer_case_dgns_ind,
		 Z.cancer_case_dgns_ind,
		 Z.prim_dgns_non_cancer_case_ind,
		 Z.sec_dgns_cancer_case_ind,
		 Z.prim_dgns_cancer_case_ind,
		 Z.cancer_dgns_cd,
		 Z.cancer_case_code_descr
		,VSETPOA.cd_descr
		,DGNS.dgns_descr_long
		,DGNS.dgns_alt_cd
		,DGNS.dgns_3_dgt_cd
		,DGNS.dgns_3_dgt_descr
		,DGNS.dgns_4_dgt_cd
		,DGNS.dgns_4_dgt_descr
		,DGNS.dgns_5_dgt_cd
		,DGNS.dgns_5_dgt_descr
		,DGNS.dgns_6_dgt_cd
		,DGNS.dgns_6_dgt_descr FROM (
		SELECT X.*
			,row_number() OVER (
				PARTITION BY X.company_id
				,X.patient_id ORDER BY diagnosisseq, sourcesystemdiag_rnk
				) AS row_num
		FROM  intermediate_stage_encntr_dgns_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(diagnosistype) = 'primary'
		) Z LEFT JOIN dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = 'ICD10'
		AND Z.icd_version = 'ICD10' LEFT JOIN val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 1 and (Z.cancer_case_dgns_ind = 1 OR Z.non_cancer_case_dgns_ind =1)),
sec_dgns_data AS
(
	SELECT Z.company_id
		,Z.patient_id
		,Z.icd_code
		,DGNS.dgns_descr
		,Z.icd_type
		,Z.icd_version
		,Z.diagnosisseq
		,Z.diagnosistype
		,Z.diagnosis_code_present_on_admission_flag,
		 Z.non_cancer_case_dgns_ind,
		 Z.cancer_case_dgns_ind,
		 Z.prim_dgns_non_cancer_case_ind,
		 Z.sec_dgns_cancer_case_ind,
		 Z.prim_dgns_cancer_case_ind,
		 Z.cancer_dgns_cd,
		 Z.cancer_case_code_descr
		,VSETPOA.cd_descr
		,DGNS.dgns_descr_long
		,DGNS.dgns_alt_cd
		,DGNS.dgns_3_dgt_cd
		,DGNS.dgns_3_dgt_descr
		,DGNS.dgns_4_dgt_cd
		,DGNS.dgns_4_dgt_descr
		,DGNS.dgns_5_dgt_cd
		,DGNS.dgns_5_dgt_descr
		,DGNS.dgns_6_dgt_cd
		,DGNS.dgns_6_dgt_descr FROM (
		SELECT X.*
			,row_number() OVER (
				PARTITION BY X.company_id
				,X.patient_id ORDER BY diagnosisseq, sourcesystemdiag_rnk
				) AS row_num
		FROM  intermediate_stage_encntr_dgns_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(diagnosistype) = 'secondary'
		--ADDED on 10/30/2020:
		AND X.cancer_case_dgns_ind = 1
		) Z LEFT JOIN dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = 'ICD10'
		AND Z.icd_version = 'ICD10' LEFT JOIN val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 1
		--and Z.cancer_case_dgns_ind = 1
		)
select * from prim_dgns_data
UNION
select * from sec_dgns_data
		);

--Code Change : Cancer Patient Identification
--select 'processing table: intermediate_stage_temp_encntr_dgns_fct_with_cancer_case ' as table_processing;
--CODE CHANGE : MLH-591 (Commenting the old Code )
DROP TABLE intermediate_stage_temp_encntr_dgns_fct_with_cancer_case IF EXISTS;
CREATE TABLE intermediate_stage_temp_encntr_dgns_fct_with_cancer_case AS
  with encntr_dgns_agg as
  ( select
    company_id as fcy_nm, patient_id as encntr_num
  , max(non_cancer_case_dgns_ind) as non_cancer_case_dgns_ind
  , max(cancer_case_dgns_ind) as cancer_case_dgns_ind
  , max(prim_dgns_non_cancer_case_ind) as prim_dgns_non_cancer_case_ind
  , max(sec_dgns_cancer_case_ind) as sec_dgns_cancer_case_ind
  , max(prim_dgns_cancer_case_ind) as prim_dgns_cancer_case_ind
  --FROM intermediate_stage_encntr_dgns_fct
  FROM intermediate_stage_temp_ptnt_prim_n_second_dgns_with_cancer
  GROUP BY 1,2
  )     -- select * from encntr_dgns_agg where encntr_num IN ('60015167197','60015169656');
  ,enctr_prim_maint_chemo_only as
  ( Select distinct fcy_nm, encntr_num from encntr_dgns_agg Z
    WHERE prim_dgns_non_cancer_case_ind =1 AND sec_dgns_cancer_case_ind =0 AND  prim_dgns_cancer_case_ind =0
  )  --select * from enctr_prim_maint_chemo_only where encntr_num IN ('60015167197','60015169656');
  ,encntr_prim_or_sec_chemo_only as
  (
   Select distinct fcy_nm, encntr_num from encntr_dgns_agg Z
    WHERE cancer_case_dgns_ind =1 AND (sec_dgns_cancer_case_ind =1 OR prim_dgns_cancer_Case_ind =1) AND (prim_dgns_non_cancer_case_ind =1 OR prim_dgns_non_cancer_case_ind =0)
  )  --select * from encntr_prim_or_sec_chemo_only where encntr_num IN ('60015167197','60015169656');
  ,encntr_cancer_dgns_cd as
  (
     select * from
	 (select distinct company_id as fcy_nm, patient_id as encntr_num, cancer_dgns_cd,  cancer_case_code_descr
	-- , ccs_dgns_cgy_cd, ccs_dgns_cgy_descr, ccs_dgns_lvl_1_cd, ccs_dgns_lvl_1_descr, ccs_dgns_lvl_2_cd, ccs_dgns_lvl_2_descr,
   ,row_number() over(partition by fcy_nm, encntr_num ORDER BY diagnosistype , diagnosisseq ) as rank_num
  --FROM intermediate_stage_encntr_dgns_fct
  FROM intermediate_stage_temp_ptnt_prim_n_second_dgns_with_cancer X
  INNER JOIN encntr_prim_or_sec_chemo_only Y
  on X.company_id = Y.fcy_nm AND X.patient_id = Y.encntr_num
  WHERE cancer_case_dgns_ind =1 and (sec_dgns_cancer_case_ind =1 OR prim_dgns_cancer_Case_ind =1) and diagnosistype IN ('Primary','Secondary')) X
  WHERE X.rank_num =1 --and encntr_num IN ('60015167197','60015169656')
  UNION
       select * from
	 (select distinct company_id as fcy_nm, patient_id as encntr_num, cancer_dgns_cd,  cancer_case_code_descr
	-- , ccs_dgns_cgy_cd, ccs_dgns_cgy_descr, ccs_dgns_lvl_1_cd, ccs_dgns_lvl_1_descr, ccs_dgns_lvl_2_cd, ccs_dgns_lvl_2_descr,
   ,row_number() over(partition by fcy_nm, encntr_num ORDER BY diagnosistype , diagnosisseq ) as rank_num
  --FROM intermediate_stage_encntr_dgns_fct
  FROM intermediate_stage_temp_ptnt_prim_n_second_dgns_with_cancer X
  INNER JOIN enctr_prim_maint_chemo_only Y
  on X.company_id = Y.fcy_nm AND X.patient_id = Y.encntr_num
  WHERE diagnosistype IN ('Primary')) X
  WHERE X.rank_num =1
  )
  --select COUNT(*) from encntr_cancer_dgns_cd
--   select * from encntr_cancer_dgns_cd where encntr_num in ('60013481756','60012756885','60013151201','60010992862')
    select Z.*,
  CASE WHEN non_cancer_case_dgns_ind =1 AND prim_dgns_non_cancer_case_ind =1 AND sec_dgns_cancer_case_ind =0 AND prim_dgns_cancer_Case_ind =0 THEN 1 ELSE 0 END as maint_cancer_case_ind,
  CASE   WHEN cancer_case_dgns_ind =1 AND prim_dgns_non_cancer_case_ind =1 AND sec_dgns_cancer_case_ind =0 AND prim_dgns_cancer_Case_ind =0 THEN 0
   		 WHEN cancer_case_dgns_ind = 1 AND prim_dgns_non_cancer_case_ind =1 AND (sec_dgns_cancer_case_ind =1 OR prim_dgns_cancer_Case_ind =1) THEN 1
  		 WHEN cancer_case_dgns_ind =1 AND prim_dgns_non_cancer_case_ind =0 AND (sec_dgns_cancer_case_ind =1 OR prim_dgns_cancer_Case_ind =1) THEN 1
	--Added on 10/30/2020:
	    WHEN non_cancer_case_dgns_ind =1 AND prim_dgns_non_cancer_case_ind =1 AND sec_dgns_cancer_case_ind =0 AND prim_dgns_cancer_Case_ind =0 THEN 1
  ELSE 0 END as cancer_case_ind,
  cancer_dgns_cd,
  cancer_case_code_descr
--  , ccs_dgns_cgy_cd,
--  ccs_dgns_cgy_descr,
--  ccs_dgns_lvl_1_cd,
--  ccs_dgns_lvl_1_descr,
--  ccs_dgns_lvl_2_cd,
--  ccs_dgns_lvl_2_descr
  FROM encntr_dgns_agg Z
  LEFT JOIN encntr_cancer_dgns_cd X
  on Z.fcy_nm = X.fcy_nm AND Z.encntr_num = X.encntr_num;

--select * FROM intermediate_stage_temp_encntr_dgns_fct_with_cancer_case WHERE fcy_nm ='Karmanos' and encntr_num IN ('60007748723','60007799627') ;


-- select * from intermediate_stage_temp_encntr_dgns_fct_with_cancer_case where encntr_num IN ('60015167197','60015169656');


 --validation of MLH-591
select maint_cancer_case_ind , 'maint_cancer_case_ind' as t, COUNT(*)
from intermediate_stage_temp_encntr_dgns_fct_with_cancer_case GROUP BY 1 UNION
select cancer_case_ind,'cancer_case_ind_new' as t, COUNT(*)
from intermediate_stage_temp_encntr_dgns_fct_with_cancer_case GROUP BY 1;
