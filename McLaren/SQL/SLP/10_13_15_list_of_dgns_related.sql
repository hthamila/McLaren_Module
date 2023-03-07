--select 'processing table:  intermediate_stage_temp_ptnt_adm_dgns' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_adm_dgns IF EXISTS;
	CREATE TABLE intermediate_stage_temp_ptnt_adm_dgns AS (
		SELECT Z.company_id
		,Z.patient_id
		,Z.icd_code AS adm_icd_code
		,DGNS.dgns_descr AS adm_icd_descr
		,Z.icd_type AS adm_icd_type
		,Z.icd_version AS adm_icd_version
		,Z.diagnosisseq AS adm_diagnosisseq
		,Z.diagnosistype AS adm_diagnosistype
		,Z.diagnosis_code_present_on_admission_flag AS adm_diagnosis_code_present_on_admission_flag
		,VSETPOA.cd_descr AS adm_dgns_poa_flg_descr
		,DGNS.dgns_descr_long AS adm_dgns_descr_long
		,DGNS.dgns_alt_cd AS adm_dgns_alt_cd
		,DGNS.dgns_3_dgt_cd AS adm_dgns_3_dgt_cd
		,DGNS.dgns_3_dgt_descr AS adm_dgns_3_dgt_descr
		,DGNS.dgns_4_dgt_cd AS adm_dgns_4_dgt_cd
		,DGNS.dgns_4_dgt_descr AS adm_dgns_4_dgt_descr
		,DGNS.dgns_5_dgt_cd AS adm_dgns_5_dgt_cd
		,DGNS.dgns_5_dgt_descr AS adm_dgns_5_dgt_descr
		,DGNS.dgns_6_dgt_cd AS adm_dgns_6_dgt_cd
		,DGNS.dgns_6_dgt_descr AS adm_dgns_6_dgt_descr FROM (
		SELECT X.company_id
			,X.patient_id
			,X.icd_code
			,X.icd_type
			,X.icd_version
			,X.diagnosisseq
			,X.diagnosistype
			,X.diagnosis_code_present_on_admission_flag
			,row_number() OVER (
				PARTITION BY X.company_id
				,X.patient_id ORDER BY diagnosisseq
				) AS row_num
		FROM  intermediate_stage_encntr_dgns_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id AND ENCNTR.patient_id = X.patient_id
		WHERE lower(diagnosistype) = 'admitting'
		) Z LEFT JOIN dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = '10'
		AND Z.icd_version = 'ICD10' LEFT JOIN val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 1
		);

--select 'processing table:  intermediate_stage_temp_ptnt_prim_dgns' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_prim_dgns IF EXISTS;
	CREATE TABLE intermediate_stage_temp_ptnt_prim_dgns AS (
			SELECT Z.company_id
		,Z.patient_id
		,Z.icd_code AS prim_icd_code
		,DGNS.dgns_descr AS prim_icd_descr
		,Z.icd_type AS prim_icd_type
		,Z.icd_version AS prim_icd_version
		,Z.diagnosisseq AS prim_diagnosisseq
		,Z.diagnosistype AS prim_diagnosistype
		,Z.diagnosis_code_present_on_admission_flag AS prim_diagnosis_code_present_on_admission_flag
		,VSETPOA.cd_descr AS prim_dgns_poa_flg_descr
		,DGNS.dgns_descr_long AS prim_dgns_descr_long
		,DGNS.dgns_alt_cd AS prim_dgns_alt_cd
		,DGNS.dgns_3_dgt_cd AS prim_dgns_3_dgt_cd
		,DGNS.dgns_3_dgt_descr AS prim_dgns_3_dgt_descr
		,DGNS.dgns_4_dgt_cd AS prim_dgns_4_dgt_cd
		,DGNS.dgns_4_dgt_descr AS prim_dgns_4_dgt_descr
		,DGNS.dgns_5_dgt_cd AS prim_dgns_5_dgt_cd
		,DGNS.dgns_5_dgt_descr AS prim_dgns_5_dgt_descr
		,DGNS.dgns_6_dgt_cd AS prim_dgns_6_dgt_cd
		,DGNS.dgns_6_dgt_descr AS prim_dgns_6_dgt_descr FROM (
		SELECT X.company_id
			,X.patient_id
			,X.icd_code
			,X.icd_type
			,X.icd_version
			,X.diagnosisseq
			,X.diagnosistype
			,X.diagnosis_code_present_on_admission_flag
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
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 1
		);

--select 'processing table:  intermediate_stage_temp_ptnt_second_dgns' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_second_dgns IF EXISTS;
	CREATE TABLE intermediate_stage_temp_ptnt_second_dgns AS (
	SELECT Z.company_id
		,Z.patient_id
		,Z.icd_code AS scdy_icd_code
		,DGNS.dgns_descr AS scdy_icd_descr
		,Z.icd_type AS scdy_icd_type
		,Z.icd_version AS scdy_icd_version
		,Z.diagnosisseq AS scdy_diagnosisseq
		,Z.diagnosistype AS scdy_diagnosistype
		,Z.diagnosis_code_present_on_admission_flag AS scdy_diagnosis_code_present_on_admission_flag
		,VSETPOA.cd_descr AS scdy_dgns_poa_flg_descr
		,DGNS.dgns_descr_long AS scdy_dgns_descr_long
		,DGNS.dgns_alt_cd AS scdy_dgns_alt_cd
		,DGNS.dgns_3_dgt_cd AS scdy_dgns_3_dgt_cd
		,DGNS.dgns_3_dgt_descr AS scdy_dgns_3_dgt_descr
		,DGNS.dgns_4_dgt_cd AS scdy_dgns_4_dgt_cd
		,DGNS.dgns_4_dgt_descr AS scdy_dgns_4_dgt_descr
		,DGNS.dgns_5_dgt_cd AS scdy_dgns_5_dgt_cd
		,DGNS.dgns_5_dgt_descr AS scdy_dgns_5_dgt_descr
		,DGNS.dgns_6_dgt_cd AS scdy_dgns_6_dgt_cd
		,DGNS.dgns_6_dgt_descr AS scdy_dgns_6_dgt_descr FROM (
		SELECT X.company_id
			,X.patient_id
			,X.icd_code
			,X.icd_type
			,X.icd_version
			,X.diagnosisseq
			,X.diagnosistype
			,X.diagnosis_code_present_on_admission_flag
			,row_number() OVER (
				PARTITION BY X.company_id
				,X.patient_id ORDER BY diagnosisseq, sourcesystemdiag_rnk
				) AS row_num
		FROM  intermediate_stage_encntr_dgns_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(diagnosistype) = 'secondary'
		) Z LEFT JOIN dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = 'ICD10'
		AND Z.icd_version = 'ICD10' LEFT JOIN val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 1
		);

SELECT count(*)
FROM intermediate_stage_temp_ptnt_second_dgns;--4,432,358


--select 'processing table: intermediate_stage_temp_ptnt_trty_dgns ' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_trty_dgns IF EXISTS;
	CREATE TABLE intermediate_stage_temp_ptnt_trty_dgns AS (
		SELECT Z.company_id
		,Z.patient_id
		,Z.icd_code AS trty_icd_code
		,DGNS.dgns_descr AS trty_icd_descr
		,Z.icd_type AS trty_icd_type
		,Z.icd_version AS trty_icd_version
		,Z.diagnosisseq AS trty_diagnosisseq
		,Z.diagnosistype AS trty_diagnosistype
		,Z.diagnosis_code_present_on_admission_flag AS trty_diagnosis_code_present_on_admission_flag
		,VSETPOA.cd_descr AS trty_dgns_poa_flg_descr
		,DGNS.dgns_descr_long AS trty_dgns_descr_long
		,DGNS.dgns_alt_cd AS trty_dgns_alt_cd
		,DGNS.dgns_3_dgt_cd AS trty_dgns_3_dgt_cd
		,DGNS.dgns_3_dgt_descr AS trty_dgns_3_dgt_descr
		,DGNS.dgns_4_dgt_cd AS trty_dgns_4_dgt_cd
		,DGNS.dgns_4_dgt_descr AS trty_dgns_4_dgt_descr
		,DGNS.dgns_5_dgt_cd AS trty_dgns_5_dgt_cd
		,DGNS.dgns_5_dgt_descr AS trty_dgns_5_dgt_descr
		,DGNS.dgns_6_dgt_cd AS trty_dgns_6_dgt_cd
		,DGNS.dgns_6_dgt_descr AS trty_dgns_6_dgt_descr FROM (
		SELECT X.company_id
			,X.patient_id
			,X.icd_code
			,X.icd_type
			,X.icd_version
			,X.diagnosisseq
			,X.diagnosistype
			,X.diagnosis_code_present_on_admission_flag
			,row_number() OVER (
				PARTITION BY X.company_id
				,X.patient_id ORDER BY diagnosisseq, sourcesystemdiag_rnk
				) AS row_num
		FROM  intermediate_stage_encntr_dgns_fct X
		WHERE lower(diagnosistype) = 'secondary'
		) Z LEFT JOIN dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = 'ICD10'
		AND Z.icd_version = 'ICD10' LEFT JOIN val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 2
		);--5,781,165