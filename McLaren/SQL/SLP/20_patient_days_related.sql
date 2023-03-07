--select 'processing table:  intermediate_stage_temp_obsrv' as table_processing;
DROP TABLE intermediate_stage_temp_obsrv

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_obsrv AS (
		SELECT pb.patient_id
		,pb.company_id
		,sum(pb.quantity) AS qty FROM  intermediate_stage_chrg_fct pb WHERE pb.revenue_code = '0762' GROUP BY pb.patient_id
		,pb.company_id
		);--215561

--Code Change : Modified the existing logic (Rev Code) based on SPL Dimension

--select 'processing table:  intermediate_stage_temp_icu' as table_processing;
DROP TABLE intermediate_stage_temp_icu

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_icu AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS icu_days
		FROM  intermediate_stage_chrg_fct ZOOM
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id
		 INNER JOIN  intermediate_stage_spl_dim SP
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --ICU
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B ICU' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B ICU','R&B NURSERY INTENSIVE LEVEL III(NICU)','R&B NURSERY INTENSIVE LEVEL IV (NICU)',
		   'R&B TRAUMA ICU'))
			) GROUP BY 1,2
		);


--Code Change : Modified the existing logic (Rev Code) based on SPL Dimension
--select 'processing table:  intermediate_stage_temp_ccu' as table_processing;
DROP TABLE intermediate_stage_temp_ccu

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_ccu AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ccu_days
		FROM  intermediate_stage_chrg_fct ZOOM
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id
		 INNER JOIN  intermediate_stage_spl_dim SP
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --CCU
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B ICU' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B CICU/CCU (CORONARY CARE)'))
			) GROUP BY 1,2
		);

--select 'processing table:  intermediate_stage_temp_nrs' as table_processing;
DROP TABLE intermediate_stage_temp_nrs

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_nrs AS (
		SELECT patient_id
		,company_id
		,count(DISTINCT pb.service_date) AS nrs_days FROM  intermediate_stage_chrg_fct pb WHERE pb.revenue_code BETWEEN '0170'
			AND '0179' GROUP BY patient_id
		,company_id
		);--32134

--select 'processing table: intermediate_stage_temp_rtne ' as table_processing;
DROP TABLE intermediate_stage_temp_rtne

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_rtne AS (
		SELECT patient_id
		,company_id
		,count(DISTINCT pb.service_date) AS rtne_days FROM  intermediate_stage_chrg_fct pb WHERE (
			(
				pb.revenue_code NOT BETWEEN '0170'
					AND '0179'
				)
			AND (
				(
					pb.revenue_code BETWEEN '0210'
						AND '0219'
					)
				OR (
					pb.revenue_code BETWEEN '0200'
						AND '0209'
					)
				)
			) GROUP BY patient_id
		,company_id
		);--173479

--select 'processing table:  intermediate_stage_temp_ed_case' as table_processing;
DROP TABLE intermediate_stage_temp_ed_case

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_ed_case AS (
		SELECT DISTINCT patient_id
		,company_id FROM  intermediate_stage_chrg_fct PB INNER JOIN val_set_dim VSET_ED_CPT ON VSET_ED_CPT.cd = PB.cpt_code
		AND VSET_ED_CPT.cohrt_nm = 'ED_VISIT'
		);--2136885


--Code Change : Added logic to calculate patient_days_StepDown based on SPL Dimension
--select 'processing table: intermediate_stage_temp_derived_ptnt_days_stepdown ' as table_processing;
DROP TABLE intermediate_stage_temp_derived_ptnt_days_stepdown

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_derived_ptnt_days_stepdown AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum( ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_stepdown
		FROM  intermediate_stage_chrg_fct ZOOM
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id
		 INNER JOIN  intermediate_stage_spl_dim SP
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --StepDown
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B STEP DOWN' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B TCU PRIVATE','R&B TCU SEMI PRIVATE','R&B TCU DELUXE',
		   'R&B STEP DOWN PRIVATE (PCU)',
		   'R&B STEP DOWN SEMI PRIVATE (PCU)','R&B STEP DOWN ISOLATION'))
		   OR
		 --Telemetry
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B TELEMETRY' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B TELEMETRY PRIVATE','R&B TELEMETRY SEMI PRIVATE' ))
			) GROUP BY 1,2
		);
--CODE change: Modified the existing logic (Rev Code) based on SPL Dimension

--Adding New Logic for Telemetry (Patient Routine) - 08/24/2020
--select 'processing table: intermediate_stage_temp_derived_ptnt_days_rtne ' as table_processing;
DROP TABLE intermediate_stage_temp_derived_ptnt_days_rtne IF EXISTS;
CREATE TABLE intermediate_stage_temp_derived_ptnt_days_rtne AS (
                SELECT ZOOM.patient_id
                ,ZOOM.company_id
                ,CASE WHEN(sum( ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_rtne
                FROM  intermediate_stage_chrg_fct ZOOM
                 INNER JOIN  intermediate_stage_spl_dim SP
                 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
                 WHERE (
                 --Telemetry
                   (SP.persp_clncl_smy_cd='110109')
                        ) GROUP BY 1,2
                );

--select 'processing table: intermediate_stage_temp_derived_ptnt_days_nbrn ' as table_processing;
DROP TABLE intermediate_stage_temp_derived_ptnt_days_nbrn

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_derived_ptnt_days_nbrn AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_nbrn
		FROM  intermediate_stage_chrg_fct ZOOM
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id
		 INNER JOIN  intermediate_stage_spl_dim SP
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --NewBorn
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B NURSERY' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B NURSERY','R&B NURSERY INTERMEDIATE LEVEL II'))
			) GROUP BY 1,2
		);
--CODE change: May 2020 Modified the existing logic (Rev Code) based on SPL Dimension

--select 'processing table:  intermediate_stage_temp_derived_ptnt_days_rnb_only' as table_processing;
DROP TABLE intermediate_stage_temp_derived_ptnt_days_rnb_only

IF EXISTS;
	CREATE TABLE  intermediate_stage_temp_derived_ptnt_days_rnb_only AS (
		SELECT ZOOM.patient_id
	        	,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_rb_only
		FROM intermediate_stage_chrg_fct ZOOM
		INNER JOIN pce_qe16_oper_prd_zoom..cv_patdisch EF
		ON ZOOM.fcy_nm  = EF.company_id AND ZOOM.encntr_num = EF.patient_id
		LEFT JOIN intermediate_stage_temp_payer_fcy_std_code PRIMPAYER ON PRIMPAYER.company_id = EF.company_id
	    AND PRIMPAYER.fcy_payer_code = EF.primary_payer_code
		 WHERE --Room and Board Only
		   (UPPER(ZOOM.persp_clncl_std_dept_descr_v10) = 'ROOM AND BOARD' AND ZOOM.total_charge <> 0.0000
		   AND ( nvl(UPPER(EF.patient_Type),'UNKNOWN') NOT IN ('BSCH','BSCHO')
           AND nvl(upper(PRIMPAYER.fcy_payer_code),'UNKNOWN') not in ('SELECT','SELEC')
           AND nvl(upper(PRIMPAYER.payor_group3),'UNKNOWN') not in ('HOSPICE')
           AND nvl(upper(EF.dischargeservice),'UNKNOWN') not in ('NB','NBN','OIN','SCN','L1N','BBN','NURS'))
           AND EF.inpatient_outpatient_flag ='I'
			) GROUP BY 1,2
		);


--CODE change: Modified the existing logic (Rev Code) based on SPL Dimension

--select 'processing table: intermediate_stage_temp_derived_ptnt_days_psych ' as table_processing;
DROP TABLE intermediate_stage_temp_derived_ptnt_days_psych

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_derived_ptnt_days_psych AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_psych
		FROM  intermediate_stage_chrg_fct ZOOM
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id
		 INNER JOIN  intermediate_stage_spl_dim SP
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --Psych
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B PSYCH' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B PSYCH ISOLATION','R&B PSYCH PRIVATE','R&B PSYCH SEMI PRIVATE'))
		   OR
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B DETOX' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B DETOX SEMI PRIVATE'))
			) GROUP BY 1,2
		);

--CODE change: Modified the existing logic (REv Code) based on SPL Dimension
--select 'processing table:  intermediate_stage_temp_derived_ptnt_days_rehab' as table_processing;
DROP TABLE intermediate_stage_temp_derived_ptnt_days_rehab

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_derived_ptnt_days_rehab AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_rehab
		FROM  intermediate_stage_chrg_fct ZOOM
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id
		 INNER JOIN  intermediate_stage_spl_dim SP
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --Rehab
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B REHAB' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B REHAB ISOLATION','R&B REHAB PRIVATE','R&B REHAB SEMI PRIVATE'))
			) GROUP BY 1,2
		);

--CODE change: Modified the existing logic (REv Code) based on SPL Dimension

--select 'processing table: intermediate_stage_temp_derived_ptnt_days_acute ' as table_processing;
DROP TABLE intermediate_stage_temp_derived_ptnt_days_acute IF EXISTS;


--Code Change: Modified the logic to calculate ptnt_days_acute
--select 'processing table: intermediate_stage_temp_derived_ptnt_days_acute ' as table_processing;
DROP TABLE intermediate_stage_temp_derived_ptnt_days_acute

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_derived_ptnt_days_acute AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_acute
		FROM  intermediate_stage_chrg_fct ZOOM
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id
		 INNER JOIN  intermediate_stage_spl_dim SP
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --Acute
		  -- (UPPER(SP.persp_clncl_smy_descr) = 'R&B MED/SURG' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B ISOLATION PRIVATE','R&B MED/SURG DELUXE','R&B MED/SURG PRIVATE',
		  -- 'R&B MED/SURG SEMI PRIVATE','R&B OB','R&B ONCOLOGY','R&B PEDIATRIC'))
		  --  OR
		  --  (UPPER(SP.persp_clncl_smy_descr) = 'R&B MISC' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B MISC'))
		    SP.persp_clncl_smy_cd in ('110103','110109','110999') --09/14 : Changing this due to descriptions might have slight changes
			) GROUP BY 1,2
		);

--Old version of logic to calculate ptnt_days_acute
--CREATE TABLE intermediate_stage_temp_derived_ptnt_days_acute AS (
--    SELECT ZOOM.patient_id
--		,ZOOM.company_id
--		,count(DISTINCT ZOOM.service_date) AS ptnt_days_acute
--		 FROM  intermediate_stage_chrg_fct ZOOM
--		INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id WHERE (
--			--NOT Rehab / Psych /NewBorn / Hospice/LTC/SNF
--			revenue_code NOT IN ('0170','0171','0172''0173','0174','0175','0179','0114','0124','0134','0144','0154','0204','0118','0128','0138','0148','0158',
--			'0650','0651','0652','0653','0654','0655','0656','0657','0659')
--			) GROUP BY 1 ,2
--);
