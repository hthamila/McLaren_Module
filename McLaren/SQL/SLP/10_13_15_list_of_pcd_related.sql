--select 'processing table: intermediate_stage_temp_ptnt_prim_proc ' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_prim_proc

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_ptnt_prim_proc AS (
		SELECT Z.company_id
		,Z.patient_id
		,Z.icd_code AS prim_proc_icd_code
		,PCDDIM.icd_pcd_descr AS prim_proc_icd_pcd_descr
		,Z.icd_type AS prim_proc_icd_type
		,Z.icd_version AS prim_proc_icd_version
		,Z.procedureseq AS prim_proc_procedureseq
		,Z.proceduretype AS prim_proc_proceduretype FROM (
		SELECT X.company_id
			,X.patient_id
			,X.icd_code
			,X.icd_type
			,X.icd_version
			,X.procedureseq
			,X.proceduretype
			,row_number() OVER (
				PARTITION BY X.company_id
				,X.patient_id ORDER BY procedureseq
				) AS row_num
		FROM  intermediate_stage_encntr_pcd_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(proceduretype) = 'primary'
		) Z LEFT JOIN pcd_dim PCDDIM ON PCDDIM.icd_pcd_cd = Z.icd_code WHERE Z.row_num = 1
		);

SELECT count(*)
FROM intermediate_stage_temp_ptnt_prim_proc;--332,451

--select 'processing table:  intermediate_stage_temp_ptnt_scdy_proc' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_scdy_proc

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_ptnt_scdy_proc AS (
		SELECT Z.company_id
		,Z.patient_id
		,Z.icd_code AS scdy_proc_icd_code
		,PCDDIM.icd_pcd_descr AS scdy_proc_icd_pcd_descr
		,Z.icd_type AS scdy_proc_icd_type
		,Z.icd_version AS scdy_proc_icd_version
		,Z.procedureseq AS scdy_proc_procedureseq
		,Z.proceduretype AS scdy_proc_proceduretype FROM (
		SELECT X.company_id
			,X.patient_id
			,X.icd_code
			,X.icd_type
			,X.icd_version
			,X.procedureseq
			,X.proceduretype
			,row_number() OVER (
				PARTITION BY X.company_id
				,X.patient_id ORDER BY procedureseq
				) AS row_num
		FROM  intermediate_stage_encntr_pcd_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(proceduretype) = 'secondary'
		) Z LEFT JOIN pcd_dim PCDDIM ON PCDDIM.icd_pcd_cd = Z.icd_code WHERE Z.row_num = 1
		);

SELECT count(*)
FROM intermediate_stage_temp_ptnt_scdy_proc;--145,743

--select 'processing table:  intermediate_stage_temp_ptnt_trty_proc' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_trty_proc

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_ptnt_trty_proc AS (
		SELECT Z.company_id
		,Z.patient_id
		,Z.icd_code AS trty_proc_icd_code
		,PCDDIM.icd_pcd_descr AS trty_proc_icd_pcd_descr
		,Z.icd_type AS trty_proc_icd_type
		,Z.icd_version AS trty_proc_icd_version
		,Z.procedureseq AS trty_proc_procedureseq
		,Z.proceduretype AS trty_proc_proceduretype FROM (
		SELECT X.company_id
			,X.patient_id
			,X.icd_code
			,X.icd_type
			,X.icd_version
			,X.procedureseq
			,X.proceduretype
			,row_number() OVER (
				PARTITION BY X.company_id
				,X.patient_id ORDER BY procedureseq
				) AS row_num
		FROM  intermediate_stage_encntr_pcd_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(proceduretype) = 'secondary'
		) Z LEFT JOIN pcd_dim PCDDIM ON PCDDIM.icd_pcd_cd = Z.icd_code WHERE Z.row_num = 2
		);--84961
