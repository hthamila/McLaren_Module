--select 'processing table: intermediate_stage_temp_dschrg_inpatient_ltcsnf ' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient_ltcsnf

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_dschrg_inpatient_ltcsnf AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_ltcsnf_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
		AND Z.patient_id = ENCNTR.patient_id WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I'
		AND (
			lower(Z.primary_payer_code) in ('select','selec')
			OR lower(Z.patient_type) = lower('bsch')
			) GROUP BY 1
		,2
		);--1437

--select 'processing table:  intermediate_stage_temp_dschrg_inpatient_nbrn' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient_nbrn

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_dschrg_inpatient_nbrn AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_nbrn_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
		AND Z.patient_id = ENCNTR.patient_id WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I'
		AND (
			lower(Z.patient_type) = 'nb'
			OR lower(Z.dischargeservice) IN (
				'nbn'
				,'oin'
				,'scn'
				,'l1n'
				,'bbn'
				,'nb'
				,'newborn'
--CODE CHANGE : 03/24/2021 ADDED FOR THUMB REGION
                                ,'nurs', 'n'
				)
			OR lower(admitservice) IN (
				'nbn'
				,'oin'
				,'scn'
				,'l1n'
				,'bbn'
				,'nb'
				,'newborn'
--CODE CHANGE : 03/24/2021 ADDED FOR THUMB REGION
                                ,'nurs'
				)
			) GROUP BY 1
		,2
		);--16347

--select 'processing table: intermediate_stage_temp_dschrg_inpatient_rehab ' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient_rehab

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_dschrg_inpatient_rehab AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_rehab_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
		AND Z.patient_id = ENCNTR.patient_id WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I'
		AND (
			lower(admitservice) IN ('rehab','rehabilitation')
			OR lower(dischargeservice) IN ('rehab','rehabilitation')
			OR lower(patient_type) IN (
				'rehab'
				,'3'
             ,'tcu'
				)
			) GROUP BY 1
		,2
		);--3328

--select 'processing table: intermediate_stage_temp_dschrg_inpatient_psych ' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient_psych

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_dschrg_inpatient_psych AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_psych_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
		AND Z.patient_id = ENCNTR.patient_id WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I'
		AND (
			lower(admitservice) IN (
				'beh'
				,'geri'
				,'ipsyc'
				,'behavioral medicine'
				)
			OR lower(dischargeservice) IN (
				'beh'
				,'geri'
				,'ipsyc'
				,'behavioral medicine'
				)
			OR lower(patient_type) IN ('psych')
			) GROUP BY 1
		,2
		);--12564

--select 'processing table: intermediate_stage_temp_dschrg_inpatient_spclcare ' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient_spclcare

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_dschrg_inpatient_spclcare AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_spclcare_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I'
		AND (
			lower(Z.primary_payer_code) in ('select','selec')
			OR lower(Z.patient_type) = lower('bsch')
			OR lower(Z.subfacility) = lower('Bay Special Care')

			) GROUP BY 1
		,2
		);--1440

--CODE CHANGE : Discharge - Hospice Old Logic
----select 'processing table: intermediate_stage_temp_dschrg_inpatient_hospice ' as table_processing;
--DROP TABLE intermediate_stage_temp_dschrg_inpatient_hospice;
--
--IF EXISTS;
--	CREATE TABLE intermediate_stage_temp_dschrg_inpatient_hospice AS (
--		SELECT DISTINCT Z.patient_id
--		,Z.company_id
--		,1 AS dschrg_hospice_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
--		AND Z.patient_id = ENCNTR.patient_id INNER JOIN pce_qe16_prd_qadv..val_set_dim VSET_HOSPICE ON VSET_HOSPICE.cohrt_nm = 'Sepsis Mortality'
--		AND Z.primary_payer_code = VSET_HOSPICE.cd WHERE Z.discharge_date IS NOT NULL
--		AND Z.inpatient_outpatient_flag = 'I'
--		AND Z.discharge_total_charges > 0 GROUP BY 1
--		,2
--		);--4577

--select 'processing table: intermediate_stage_temp_dschrg_inpatient_hospice ' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient_hospice

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_dschrg_inpatient_hospice AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_hospice_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
		AND Z.patient_id = ENCNTR.patient_id
		INNER JOIN intermediate_stage_temp_payer_fcy_std_code VSET_HOSPICE
        ON VSET_HOSPICE.payer_code = Z.primary_payer_code
	    WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I' AND VSET_HOSPICE.payor_group3 = 'Hospice'
		AND Z.discharge_total_charges > 0 GROUP BY 1
		,2
		);--4577



--select 'processing table: intermediate_stage_temp_dschrg_inpatient_lipmip ' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient_lipmip

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_dschrg_inpatient_lipmip AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_lipmip_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I'
		AND (
			lower(patient_type) IN (
				'lip'
				,'mip'
				)
			) GROUP BY 1
		,2
		);--14467

--select 'processing table: intermediate_stage_temp_dschrg_inpatient_acute ' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient_acute

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_dschrg_inpatient_acute AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,CASE
			WHEN (
					NB.dschrg_nbrn_ind = 1
					OR REHAB.dschrg_rehab_ind = 1
					OR PSYCH.dschrg_psych_ind = 1
					OR LIPMIP.dschrg_lipmip_ind = 1
					OR LTCSNF.dschrg_ltcsnf_ind = 1
					OR HOSPICE.dschrg_hospice_ind = 1
					)
				THEN NULL
			ELSE 1
			END AS dschrg_acute_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
		AND Z.patient_id = ENCNTR.patient_id LEFT JOIN intermediate_stage_temp_dschrg_inpatient_nbrn NB ON NB.patient_id = Z.patient_id
		AND NB.company_id = Z.company_id LEFT JOIN intermediate_stage_temp_dschrg_inpatient_lipmip LIPMIP ON LIPMIP.patient_id = Z.patient_id
		AND LIPMIP.company_id = Z.company_id LEFT JOIN intermediate_stage_temp_dschrg_inpatient_rehab REHAB ON REHAB.patient_id = Z.patient_id
		AND REHAB.company_id = Z.company_id LEFT JOIN intermediate_stage_temp_dschrg_inpatient_psych PSYCH ON PSYCH.patient_id = Z.patient_id
		AND PSYCH.company_id = Z.company_id LEFT JOIN intermediate_stage_temp_dschrg_inpatient_ltcsnf LTCSNF ON LTCSNF.patient_id = Z.patient_id
		AND LTCSNF.company_id = Z.company_id LEFT JOIN intermediate_stage_temp_dschrg_inpatient_hospice HOSPICE ON HOSPICE.patient_id = Z.patient_id
		AND HOSPICE.company_id = Z.company_id WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I'
		AND Z.discharge_total_charges > 0
		);--323191

--select 'processing table: intermediate_stage_temp_dschrg_inpatient ' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_dschrg_inpatient AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
		AND Z.patient_id = ENCNTR.patient_id WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I' GROUP BY 1
		,2
		);--323191

--select 'processing table: intermediate_stage_temp_derived_ptnt_days ' as table_processing;
DROP TABLE intermediate_stage_temp_derived_ptnt_days

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_derived_ptnt_days AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN (sum(ZOOM.quantity) =0) THEN NULL ELSE sum(ZOOM.quantity) END  AS ptnt_days
		FROM  intermediate_stage_chrg_fct ZOOM INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id WHERE (
			(
				revenue_code BETWEEN '0100'
					AND '0138'
				OR revenue_code BETWEEN '0140'
					AND '0179'
				OR revenue_code BETWEEN '0181'
					AND '0235'
				)
			AND (
				charge_code != '36636630019'
				AND revenue_code = '0120'
				)
			AND revenue_code NOT IN (
				'0139'
				,'0180'
				)
			AND charge_code NOT IN (
				'401014150199'
				,'401014145199'
				,'401008125198'
				,'401008125199'
				,'401026133199'
				,'401500150292'
				,'401019141199'
				,'401019141198'
				,'401500150291'
				,'401900435199'
				,'401500435201'
				)
			) GROUP BY ZOOM.patient_id
		,ZOOM.company_id
		);--187665
