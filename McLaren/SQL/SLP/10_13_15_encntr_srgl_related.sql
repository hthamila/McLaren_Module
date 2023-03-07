--code change : Added logic to calculate Surgercy Cases based on SPL Dimension

--select 'processing table: intermediate_stage_temp_srgl_case  ' as table_processing;
DROP TABLE intermediate_stage_temp_srgl_case

IF EXISTS;
	CREATE  TABLE intermediate_stage_temp_srgl_case AS (
		SELECT DISTINCT patient_id
		,company_id FROM  intermediate_stage_chrg_fct CF
		INNER JOIN  intermediate_stage_spl_dim SP
		on CF.charge_code = SP.cdm_cd and CF.fcy_num = SP.fcy_num
		WHERE SP.persp_clncl_smy_descr in ('SURGERY TIME', 'AMBULATORY SURGERY SERVICES')
		GROUP BY patient_id
		,company_id
		);

--code change : Added logic to calculate  Lithotripsy  Cases based on SPL Dimension
   --select 'processing table: intermediate_stage_temp_lithotripsy_case ' as table_processing;
DROP TABLE intermediate_stage_temp_lithotripsy_case IF EXISTS;
	CREATE  TABLE intermediate_stage_temp_lithotripsy_case AS (
		SELECT DISTINCT patient_id
		,company_id FROM  intermediate_stage_chrg_fct CF
		INNER JOIN  intermediate_stage_spl_dim SP
		on CF.charge_code = SP.cdm_cd and CF.fcy_num = SP.fcy_num
		WHERE UPPER(SP.persp_clncl_dtl_descr) in ('PF LITHOLAPAXY COMPLICATED > 2.5 CM','LITHOTRIPSY KIDNEY','PERC NEPHROLITHOTOMY W/WO DILATION <2 CM')
		GROUP BY patient_id
		,company_id
		);

 --code change : Added logic to calculate  CathLab Cases based on intermediate_stage_svc_ln_anl_fct
DROP TABLE temp_cathlab_case IF EXISTS;
CREATE  TABLE temp_cathlab_case	AS
(	SELECT DISTINCT patient_id, company_id
	FROM  intermediate_stage_chrg_fct cf
	WHERE cf.cpt_code in ('93451','93452','93453','93454','93455','93456','93457','93458','93459','93460','93461','93462','93501','93508','93510',
                            '93511','93514','93524','93526','93527','93528','93529','93530','93531','93532','93533','93542','93543','93544','93545',
                            '93555','93556','93561','93562','93566','93567','93568')
UNION
	SELECT DISTINCT patient_id, company_id
	FROM  intermediate_stage_cpt_fct cpf
	WHERE cpf.cpt_code in ('93451','93452','93453','93454','93455','93456','93457','93458','93459','93460','93461','93462','93501','93508','93510',
                            '93511','93514','93524','93526','93527','93528','93529','93530','93531','93532','93533','93542','93543','93544','93545',
                            '93555','93556','93561','93562','93566','93567','93568'));

	--select 'processing table: intermediate_stage_temp_cathlab_case ' as table_processing;
DROP TABLE intermediate_stage_temp_cathlab_case IF EXISTS;

	CREATE  TABLE intermediate_stage_temp_cathlab_case AS
	(
	  SELECT DISTINCT patient_id, company_id
	  FROM temp_cathlab_case);

