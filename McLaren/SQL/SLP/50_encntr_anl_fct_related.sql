---------------------------------------------------------------------------------------

--select 'processing table:  intermediate_stage_encntr_anl_fct' as table_processing;
DROP TABLE intermediate_stage_encntr_anl_fct_temp IF EXISTS;

CREATE TABLE intermediate_stage_encntr_anl_fct_temp AS

--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
SELECT Distinct
     ZOOM.company_id AS fcy_nm
	,VSET_FCY.alt_cd AS fcy_num
	,ZOOM.inpatient_outpatient_flag AS in_or_out_patient_ind
	,ZOOM.medical_record_number
	,ZOOM.patient_id AS encntr_num
	,ZOOM.admission_ts AS adm_ts
	,DATE (to_timestamp((ZOOM.admissionarrival_date || ' ' || nvl(substr(ZOOM.admissionarrival_date, 1, 2), '00') || ':' || nvl(substr(ZOOM.admissionarrival_date, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS adm_dt
	,ZOOM.discharge_ts AS src_dschrg_ts
	,ZOOM3YRS.discharge_ts AS dschrg_ts
	,ZOOM.admit_time AS adm_tm
	--CODE CHANGE: 07/07/2021 : using derived dschrg_ts (i.e based on patient type code I/O)
	,DATE(ZOOM3YRS.discharge_ts) AS dschrg_dt
	--,DATE (to_timestamp((ZOOM.discharge_date || ' ' || nvl(substr(ZOOM.discharge_date, 1, 2), '00') || ':' || nvl(substr(ZOOM.discharge_date, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS dschrg_dt
	,ZOOM.discharge_time AS dschrg_tm
	,ZOOM.length_of_stay AS los
	--,NVL(ZOOM.msdrg_code,'-100') AS ms_drg_cd     --- CODE Changed for outpatient
	,CASE WHEN ZOOM.inpatient_outpatient_flag = 'I' THEN NVL(ZOOM.msdrg_code,'-100')
              WHEN ZOOM.inpatient_outpatient_flag = 'O' THEN NVL(ZOOM.msdrg_code,'0')
        else null end AS ms_drg_cd
	--CODE CHANGE : AUG 2019 (a) Ms_Drg_Dim CMI Historical Weights
	,ZOOM3YRS.ms_drg_wght AS case_mix_idnx_num
	,ZOOM3YRS.ms_drg_geo_mean_los_num
        ,ZOOM3YRS.ms_drg_arthm_mean_los_num
	,ACO_MSDRG.drg_fam_nm
--   ,ACO_MSDRG.case_mix_idnx_num
	,ACO_MSDRG.geo_mean_los_num
	,ACO_MSDRG.arthm_mean_los_num
	,nvl(QADV.apr_drg_cd,'-100') AS apr_cd
	,QADV.apr_svry_of_ill
	,QADV.apr_rsk_of_mrtly
	,ZOOM.discharge_total_charges AS dschrg_tot_chrg_amt
	,ZOOM.discharge_variable_cost AS dschrg_var_cst_amt
	,ZOOM.discharge_fixed_cost AS dschrg_fix_cst_amt
	,ZOOM.reimbursement_amount AS rmbmt_amt
	,ZOOM.age_in_years AS age_in_yr
	,DATE (to_timestamp((ZOOM.birth_date || ' ' || nvl(substr(ZOOM.birth_date, 1, 2), '00') || ':' || nvl(substr(ZOOM.birth_date, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS brth_dt
	,ZOOM.babys_patient_number AS babys_encntr_num
	,VSET_GENDER.cd_descr AS ptnt_gnd
	,ZOOM.employer_code AS empr
	,ZOOM.state_of_patient_origin AS ste_of_ptnt_orig
	,ZOOM.county_of_patient_origin AS cnty_of_ptnt_orig
	--07/13/2021: Added race_cd to perform look up with QADV Race_cd
	,ZOOM.race as race_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,QADV_RACE.race_descr AS race_descr
	,VSET_MARITAL.cd_descr AS mar_status
	,ZOOM.birth_weight_in_grams AS brth_wght_in_grm
	,ZOOM.days_on_mechanical_ventilator AS day_on_mchnc_vntl
	,ZOOM.smoker_flag AS smk_flag
	,ZOOM.weight_in_lbs AS wght_in_lb
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,VSET_ETHCTY.cd_descr AS ethcty_descr
	,ZOOM.ed_visit AS ed_vst_ind
	,ZOOM.ccn_care_setting AS ccn_care_setting
	,ZOOM.patient_hic_number AS ptnt_hic_num
	,ZOOM.tin
	,ZOOM.patient_first_name AS ptnt_frst_nm
	,ZOOM.patient_middle_name AS ptnt_mid_nm
	,ZOOM.patient_last_name AS ptnt_lst_nm
  --,ZOOM.subfacility AS sub_fcy
	,ZOOM.accountstatus AS acct_sts
	,ZOOM.readmissionflag AS readm_flag
	,DATE (to_timestamp((ZOOM.previousdischargedate || ' ' || nvl(substr(ZOOM.previousdischargedate, 1, 2), '00') || ':' || nvl(substr(ZOOM.previousdischargedate, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS prev_dschg_dt
	,ZOOM.namesuffix AS ptnt_nm_sfx
	,ZOOM.admitservice AS adm_svc
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,CVADMSVC.fielddescription as adm_svc_descr
	,ZOOM.dischargeservice AS dschrg_svc
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,CVDSCHRGSVC.fielddescription as dschrg_svc_descr
	,ZOOM.nursingstation AS nrg_stn
	,ZOOM.financialclass AS fnc_cls
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,CVFNCCLS.fielddescription as fnc_cls_descr
	,ZOOM.financialclassoriginal AS fnc_cls_orig
	,ZOOM.finalbillflag AS fnl_bill_flag
	,CASE WHEN length(finalbilldate)=8  THEN to_Date(ZOOM.finalbilldate,'mmddyyyy') else DATE(ZOOM.finalbilldate) END AS fnl_bill_dt
	--    ,DATE (ZOOM.finalbilldate) AS fnl_bill_dt
	,ZOOM.totaladjustments AS tot_adj_amt
	,ZOOM.accountbalance AS acct_bal_amt
	,ZOOM.expectedpayment AS expt_pymt_amt
	,DATE (to_timestamp((ZOOM.updatedate || ' ' || nvl(substr(ZOOM.updatedate, 1, 2), '00') || ':' || nvl(substr(ZOOM.updatedate, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS upd_dt
	,ZOOM.updateid AS upd_id
	--,ZOOM.sourcesystem AS src_sys
	,ZOOM.total_charges_ind AS tot_chrg_ind
	,ZOOM.admitdate_yr_ind AS admdt_yr_ind
	,FCY_REF.bed_cnt
	,CASE
		WHEN intermediate_stage_temp_dschrg_inpatient_nbrn.patient_id IS NULL
			THEN NULL
	--CODE CHANGE: MLH - 666 : PQSD Discharge Defintion change request
	    WHEN ( intermediate_stage_temp_dschrg_inpatient_spclcare.dschrg_spclcare_ind =1 OR nvl(PFAGG.excld_trnsfr_encntr_ind,0) = 1) AND
		intermediate_stage_temp_dschrg_inpatient_nbrn.dschrg_nbrn_ind =1
		THEN 0
		ELSE intermediate_stage_temp_dschrg_inpatient_nbrn.dschrg_nbrn_ind
		END AS dschrg_nbrn_ind
	,CASE
		WHEN intermediate_stage_temp_dschrg_inpatient_rehab.patient_id IS NULL
			THEN NULL
	--CODE CHANGE: MLH - 666 : PQSD Discharge Defintion change request
	    WHEN ( intermediate_stage_temp_dschrg_inpatient_spclcare.dschrg_spclcare_ind =1 OR nvl(PFAGG.excld_trnsfr_encntr_ind,0) = 1) AND
		intermediate_stage_temp_dschrg_inpatient_rehab.dschrg_rehab_ind = 1
		THEN 0
		ELSE intermediate_stage_temp_dschrg_inpatient_rehab.dschrg_rehab_ind
		END AS dschrg_rehab_ind
	,CASE
		WHEN intermediate_stage_temp_dschrg_inpatient_psych.patient_id IS NULL
			THEN NULL
	--CODE CHANGE: MLH - 666 : PQSD Discharge Defintion change request
	    WHEN ( intermediate_stage_temp_dschrg_inpatient_spclcare.dschrg_spclcare_ind =1 OR nvl(PFAGG.excld_trnsfr_encntr_ind,0) = 1) AND
		intermediate_stage_temp_dschrg_inpatient_psych.dschrg_psych_ind =1
		THEN 0
		ELSE intermediate_stage_temp_dschrg_inpatient_psych.dschrg_psych_ind
		END AS dschrg_psych_ind
	,CASE
		WHEN intermediate_stage_temp_dschrg_inpatient_ltcsnf.patient_id IS NULL
			THEN NULL
	--CODE CHANGE: MLH - 666 : PQSD Discharge Defintion change request
	    WHEN ( intermediate_stage_temp_dschrg_inpatient_spclcare.dschrg_spclcare_ind =1 OR nvl(PFAGG.excld_trnsfr_encntr_ind,0) = 1) AND
		intermediate_stage_temp_dschrg_inpatient_ltcsnf.dschrg_ltcsnf_ind =1
		THEN 0
		ELSE intermediate_stage_temp_dschrg_inpatient_ltcsnf.dschrg_ltcsnf_ind
		END AS dschrg_ltcsnf_ind
	,CASE
		WHEN intermediate_stage_temp_dschrg_inpatient_hospice.patient_id IS NULL
			THEN NULL
	--CODE CHANGE: MLH - 666 : PQSD Discharge Defintion change request
	     WHEN ( intermediate_stage_temp_dschrg_inpatient_spclcare.dschrg_spclcare_ind =1 OR nvl(PFAGG.excld_trnsfr_encntr_ind,0) = 1) AND
		intermediate_stage_temp_dschrg_inpatient_hospice.dschrg_hospice_ind =1
		THEN 0
		ELSE intermediate_stage_temp_dschrg_inpatient_hospice.dschrg_hospice_ind
		END AS dschrg_hospice_ind
	,CASE
		WHEN intermediate_stage_temp_dschrg_inpatient_spclcare.patient_id IS NULL
			THEN NULL
		WHEN ( intermediate_stage_temp_dschrg_inpatient_spclcare.dschrg_spclcare_ind =1 OR nvl(PFAGG.excld_trnsfr_encntr_ind,0) = 1) AND
		intermediate_stage_temp_dschrg_inpatient_lipmip.dschrg_lipmip_ind=1
		THEN 0
        ELSE intermediate_stage_temp_dschrg_inpatient_spclcare.dschrg_spclcare_ind
		END AS dschrg_spclcare_ind
	,CASE
		WHEN intermediate_stage_temp_dschrg_inpatient_lipmip.patient_id IS NULL
			THEN NULL
	--CODE CHANGE: MLH - 666 : PQSD Discharge Defintion change request
	    WHEN ( intermediate_stage_temp_dschrg_inpatient_spclcare.dschrg_spclcare_ind =1 OR nvl(PFAGG.excld_trnsfr_encntr_ind,0) = 1) AND
		intermediate_stage_temp_dschrg_inpatient_lipmip.dschrg_lipmip_ind=1
		THEN 0
		ELSE intermediate_stage_temp_dschrg_inpatient_lipmip.dschrg_lipmip_ind
		END AS dschrg_lipmip_ind
	,CASE
		WHEN intermediate_stage_temp_dschrg_inpatient_acute.patient_id IS NULL
			THEN NULL
	--CODE CHANGE: MLH - 666 : PQSD Discharge Defintion change request
	    WHEN ( intermediate_stage_temp_dschrg_inpatient_spclcare.dschrg_spclcare_ind =1 OR nvl(PFAGG.excld_trnsfr_encntr_ind,0) = 1) AND
		intermediate_stage_temp_dschrg_inpatient_acute.dschrg_acute_ind = 1
		THEN 0
		ELSE intermediate_stage_temp_dschrg_inpatient_acute.dschrg_acute_ind
		END AS dschrg_acute_ind
	,CASE
		WHEN intermediate_stage_temp_dschrg_inpatient.patient_id IS NOT NULL
			AND ZOOM.patient_type NOT IN (
				'LIP'
				,'MIP'
				)
			AND (
				dschrg_acute_ind = 1
				OR dschrg_rehab_ind = 1
				OR dschrg_psych_ind = 1
				OR dschrg_ltcsnf_ind = 1
				OR dschrg_hospice_ind = 1
				OR dschrg_spclcare_ind = 1
				OR dschrg_nbrn_ind = 1
				)
			THEN 1
		ELSE NULL
		END AS dschrg_ind
	,CASE
		WHEN intermediate_stage_temp_obsrv.qty > 0
			THEN intermediate_stage_temp_obsrv.qty
		ELSE intermediate_stage_temp_obsrv.qty
		END AS obsrv_hours
	,CASE
		WHEN obsrv_hours > 0
			THEN obsrv_hours / 24
		ELSE NULL
		END AS obsrv_days
	,CASE
		WHEN obsrv_days > 0
			THEN 1
		ELSE 0
		END AS obsrv_stay_ind
	,CASE
		WHEN obsrv_days > 0
			AND dschrg_psych_ind = 1
			THEN 1
		ELSE NULL
		END AS obsrv_psych_ind
	, ( nvl(intermediate_stage_temp_derived_ptnt_days_acute.ptnt_days_acute ,0)+
	   nvl(intermediate_stage_temp_ccu.ccu_days, 0) +
	   nvl(intermediate_stage_temp_icu.icu_days, 0) +
	   nvl(intermediate_stage_temp_derived_ptnt_days_stepdown.ptnt_days_stepdown, 0) +
	   nvl(intermediate_stage_temp_derived_ptnt_days_nbrn.ptnt_days_nbrn, 0) +
	   nvl(intermediate_stage_temp_derived_ptnt_days_rehab.ptnt_days_rehab, 0) +
	   nvl(intermediate_stage_temp_derived_ptnt_days_psych.ptnt_days_psych, 0)) AS ptnt_days
	,CASE
		WHEN intermediate_stage_temp_derived_ptnt_days_psych.patient_id IS NOT NULL
			THEN intermediate_stage_temp_derived_ptnt_days_psych.ptnt_days_psych
		ELSE NULL
		END AS ptnt_days_pysch
	,CASE
		WHEN intermediate_stage_temp_derived_ptnt_days_rehab.patient_id IS NOT NULL
			THEN intermediate_stage_temp_derived_ptnt_days_rehab.ptnt_days_rehab
		ELSE NULL
		END AS ptnt_days_rehab
	,CASE
		WHEN intermediate_stage_temp_derived_ptnt_days_nbrn.patient_id IS NOT NULL
			THEN intermediate_stage_temp_derived_ptnt_days_nbrn.ptnt_days_nbrn
		ELSE NULL
		END AS ptnt_days_nbrn
	,CASE
		WHEN intermediate_stage_temp_derived_ptnt_days_stepdown.patient_id IS NOT NULL
			THEN intermediate_stage_temp_derived_ptnt_days_stepdown.ptnt_days_stepdown
		ELSE NULL
		END AS ptnt_days_stepdown
        --,CASE
        --        WHEN intermediate_stage_temp_derived_ptnt_days_rtne.patient_id IS NOT NULL
        --                THEN intermediate_stage_temp_derived_ptnt_days_rtne.ptnt_days_rtne
        --        ELSE NULL
        --        END AS ptnt_days_rtne
	,CASE
		WHEN intermediate_stage_temp_derived_ptnt_days_acute.patient_id IS NOT NULL
			THEN intermediate_stage_temp_derived_ptnt_days_acute.ptnt_days_acute
		ELSE NULL
		END AS ptnt_days_acute
	,CASE
		WHEN intermediate_stage_temp_icu.patient_id IS NOT NULL
			THEN intermediate_stage_temp_icu.icu_days
		ELSE NULL
		END AS icu_days
	,CASE
		WHEN intermediate_stage_temp_ccu.patient_id IS NOT NULL
			THEN intermediate_stage_temp_ccu.ccu_days
		ELSE NULL
		END AS ccu_days
	,CASE
		WHEN intermediate_stage_temp_nrs.patient_id IS NOT NULL
			THEN intermediate_stage_temp_nrs.nrs_days
		ELSE NULL
		END AS nrs_days
	,CASE
		WHEN intermediate_stage_temp_rtne.patient_id IS NOT NULL
			THEN intermediate_stage_temp_rtne.rtne_days
		ELSE NULL
		END AS rtne_days
	,CASE
		WHEN intermediate_stage_temp_ed_case.patient_id IS NOT NULL
			THEN 1
		ELSE NULL
		END AS ed_case_ind
	,nvl(PRIMPAYER.fcy_payer_code,'-100') AS src_prim_pyr_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(PRIMPAYER.fcy_payer_description,'UNKNOWN') AS src_prim_pyr_descr
	,nvl(PRIMPAYER.std_payer_code,'-100') AS qadv_prim_pyr_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(PRIMPAYER.std_payer_descr,'UNKNOWN') AS qadv_prim_pyr_descr
	,PRIMPAYER.payor_group1 as src_prim_payor_grp1
	,PRIMPAYER.payor_group2 as src_prim_payor_grp2
	,PRIMPAYER.payor_group3 as src_prim_payor_grp3
	,nvl(SECONPAYER.fcy_payer_code,'-100') AS src_scdy_pyr_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(SECONPAYER.fcy_payer_description,'UNKNOWN') AS src_scdy_pyr_descr
	,nvl(SECONPAYER.std_payer_code,'-100') AS qadv_scdy_pyr_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(SECONPAYER.std_payer_descr,'UNKNOWN') AS qadv_scdy_pyr_descr
	,SECONPAYER.payor_group1 as src_scdy_payor_grp1
	,SECONPAYER.payor_group2 as src_scdy_payor_grp2
	,SECONPAYER.payor_group3 as src_scdy_payor_grp3
	--Adding Tertiary Payer
	,nvl(TRTYPAYER.fcy_payer_code,'-100') AS src_trty_pyr_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(TRTYPAYER.fcy_payer_description,'UNKNOWN') AS src_trty_pyr_descr
	,nvl(TRTYPAYER.std_payer_code,'-100') AS qadv_trty_pyr_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(TRTYPAYER.std_payer_descr,'UNKNOWN') AS qadv_trty_pyr_descr
	,TRTYPAYER.payor_group1 as src_trty_payor_grp1
	,TRTYPAYER.payor_group2 as src_trty_payor_grp2
	,TRTYPAYER.payor_group3 as src_trty_payor_grp3
	--Adding Quarternary Payer
	,nvl(QTRPAYER.fcy_payer_code,'-100') AS src_qtr_pyr_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(QTRPAYER.fcy_payer_description,'UNKNOWN') AS src_qtr_pyr_descr
	,nvl(QTRPAYER.std_payer_code,'-100') AS qadv_qtr_pyr_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(QTRPAYER.std_payer_descr,'UNKNOWN') AS qadv_qtr_pyr_descr
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,QTRPAYER.payor_group1 as src_qtr_payor_grp1
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,QTRPAYER.payor_group2 as src_qtr_payor_grp2
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,QTRPAYER.payor_group3 as src_qtr_payor_grp3
	,CASE
		WHEN intermediate_stage_temp_endoscopy_case.patient_id IS NULL
			THEN NULL
		ELSE 1
		END AS endoscopy_case_ind
	,CASE
		WHEN intermediate_stage_temp_srgl_case.patient_id IS NULL
			THEN NULL
		ELSE 1
		END AS srgl_case_ind
	,CASE
		WHEN intermediate_stage_temp_lithotripsy_case.patient_id IS NULL
			THEN NULL
		ELSE 1
		END AS lithotripsy_case_ind
	,CASE
		WHEN intermediate_stage_temp_cathlab_case.patient_id IS NULL
			THEN NULL
		ELSE 1
		END AS cathlab_case_ind
	,nvl(ZOOM.admission_type_visit_type,'-100') AS adm_tp_cd
	,nvl(ZOOM.point_of_origin_for_admission_or_visit,'-100') AS pnt_of_orig_cd
	,nvl(ZOOM.discharge_status,'-100') AS dschrg_sts_cd
--Code Change : Zoom gets from Encntr but Integrated Mart gets from intermediate_stage_encntr_dgns_fct so commenting Integrated Version
--  ,nvl(ADMDGNS.adm_icd_code,'-100') AS adm_dgns_cd
--	,nvl(ADMDGNS.adm_icd_descr,'UNKNOWN') AS adm_dgns_descr
--	,nvl(ADMDGNS.adm_diagnosis_code_present_on_admission_flag,'-100') AS adm_dgns_poa_flg_cd
	,nvl(ADMDGNS.dgns_cd,'-100') AS adm_dgns_cd
        ,nvl(ADMDGNS.dgns_descr,'UNKNOWN') AS adm_dgns_descr
	,'-100' AS adm_dgns_poa_flg_cd
	,nvl(DGNSDIM.dgns_cd,'-100') AS prim_dgns_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(DGNSDIM.dgns_descr,'UNKNOWN') AS prim_dgns_descr

	-------Srujan Update Start-----------------
	/*Start Primary Diagnosis CCS Attributes*/
	,nvl(DGNSDIM.ccs_dgns_cgy_cd,'-100') AS prim_ccs_dgns_cgy_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(DGNSDIM.ccs_dgns_cgy_descr,'UNKNOWN') AS prim_ccs_dgns_cgy_descr
	,nvl(DGNSDIM.ccs_dgns_lvl_1_cd,'-100') AS prim_ccs_dgns_lvl_1_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(DGNSDIM.ccs_dgns_lvl_1_descr,'UNKNOWN') AS prim_ccs_dgns_lvl_1_descr
	,nvl(DGNSDIM.ccs_dgns_lvl_2_cd,'-100') AS prim_ccs_dgns_lvl_2_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(DGNSDIM.ccs_dgns_lvl_2_descr,'UNKNOWN') AS prim_ccs_dgns_lvl_2_descr
	/*End Primary Diagnosis CCS Attributes*/
	-------Srujan Update End-----------------

	,'-100' AS prim_dgns_poa_flg_cd
--	,nvl(PRIMDGNS.prim_icd_code,'-100') AS prim_dgns_cd
--  ,nvl(PRIMDGNS.prim_icd_descr,'UNKNOWN') AS prim_dgns_descr
--	,nvl(PRIMDGNS.prim_diagnosis_code_present_on_admission_flag,'-100') AS prim_dgns_poa_flg_cd
	--------------------------------------------------------------------------------------------------
	,nvl(SCDYDGNS.scdy_icd_code,'-100') AS scdy_dgns_cd
	,nvl(SCDYDGNS.scdy_diagnosis_code_present_on_admission_flag,'-100') AS scdy_dgns_poa_flg_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
	,nvl(SCDYDGNS.scdy_dgns_descr_long,'UNKNOWN') as scdy_dgns_descr_long
	,nvl(TRTYDGNS.trty_icd_code,'-100') AS trty_dgns_cd
	,nvl(TRTYDGNS.trty_diagnosis_code_present_on_admission_flag,'-100') AS trty_dgns_poa_flg_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
	,nvl(TRTYDGNS.trty_dgns_descr_long,'UNKNOWN') as trty_dgns_descr_long
--Code Change: Zoom gets from Encntr but Integrated Mart gets from intermediate_stage_encntr_pcd_fct so commenting Integrated Version
--	,nvl(PRIMPROC.prim_proc_icd_code,'-100') AS prim_pcd_cd
--	,nvl(PRIMPROC.prim_proc_icd_pcd_descr,'UNKNOWN') as prim_pcd_descr
        ,nvl(PCDDIM.icd_pcd_cd,'-100') as prim_pcd_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(PCDDIM.icd_pcd_descr,'UNKNOWN') as prim_pcd_descr

		-------Srujan Update Start-----------------
	/*Start Primary Procedure CCS Attributes*/
	,nvl(PCDDIM.icd_pcd_ccs_cgy_cd,'-100') as prim_pcd_ccs_cgy_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(PCDDIM.icd_pcd_ccs_cgy_descr,'UNKNOWN') as prim_pcd_ccs_cgy_descr
	,nvl(PCDDIM.icd_pcd_ccs_lvl_1_cd,'-100') as prim_pcd_ccs_lvl_1_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(PCDDIM.icd_pcd_ccs_lvl_1_descr,'UNKNOWN') as prim_pcd_ccs_lvl_1_descr
	,nvl(PCDDIM.icd_pcd_ccs_lvl_2_cd,'-100') as prim_pcd_ccs_lvl_2_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(PCDDIM.icd_pcd_ccs_lvl_2_descr,'UNKNOWN') as prim_pcd_ccs_lvl_2_descr

		-------Srujan Update End-----------------
	/*End Primary Procedure CCS Attributes*/

	,nvl(SCDYPROC.scdy_proc_icd_code,'-100') AS scdy_pcd_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(SCDYPROC.scdy_proc_icd_pcd_descr,'UNKNOWN') as scdy_pcd_descr
	,nvl(TRTYPROC.trty_proc_icd_code,'-100') AS trty_pcd_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(TRTYPROC.trty_proc_icd_pcd_descr,'UNKNOWN') as trty_pcd_descr
	,nvl(PATTYPE.patient_type_code,'-100') AS ptnt_tp_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,nvl(PATTYPE.patient_type_description,'UNKNOWN') AS ptnt_tp_descr
	,nvl(PATTYPE.standard_patient_type_code,'-100') AS std_ptnt_tp_cd
	,ADMITPRACTSPEC.npi AS adm_pract_npi
	,ATTENDPRACTSPEC.npi AS attnd_pract_npi
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,ATTENDPRACTSPEC.practitioner_spclty_description as attnd_pract_spclty_descr
        ,ATTENDPRACTSPEC.mcare_spcly_cd as attnd_pract_spclty_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,ADMITPRACTSPEC.practitioner_spclty_description as adm_pract_spclty_descr
        ,ADMITPRACTSPEC.mcare_spcly_cd as adm_pract_spclty_cd
	,nvl(ADMITPRACTSPEC.practitioner_code,'-100') AS adm_pract_cd
	,nvl(ATTENDPRACTSPEC.practitioner_code,'-100') AS attnd_pract_cd
	,ADMITPRACTSPEC.practitioner_name AS adm_pract_nm
	,ATTENDPRACTSPEC.practitioner_name AS attnd_pract_nm
	,ZOOM.address1 AS adr1
	,ZOOM.address2 AS adr2
	,ZOOM.city AS cty
	,nvl(ZIPCODE.ptnt_zip_cd,'-100') AS ptnt_zip_cd
	,ZIPCODE.mjr_cty_ste_nm AS ptnt_mjr_cty_ste_nm
	,ZIPCODE.mjr_cty_nm AS ptnt_mjr_cty_nm
	,ZIPCODE.cnty_fips_ste_nm AS ptnt_cnty_fips_ste_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,ACO_FIPSADR.fips_ste_descr
	,ZIPCODE.cnty_fips_nm AS ptnt_cnty_fips_cd
	,ZIPCODE.cnty_nm AS ptnt_cnty_nm
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,ACO_FIPSADR.fips_cnty_descr
	,ZIPCODE.ste_cd AS std_ste_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,ZIPCODE.ste_descr AS std_ste_descr
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--,ZIPCODE.rgon_descr AS std_rgon_descr
        ,SVCRNK.svc_cgy as e_svc_cgy
	,SVCRNK.svc_ln  as e_svc_ln_nm
	,SVCRNK.sub_svc_ln as e_sub_svc_ln_nm
	,SVCRNK.svc_nm as e_svc_nm
        ,SVCRNK.svc_cgy_rnk as e_svc_cgy_rnk
	,SVCRNK.svc_ln_rnk as e_svc_ln_rnk
	,SVCRNK.sub_svc_ln_rnk as e_sub_svc_ln_rnk
	,SVCRNK.svc_rnk as e_svc_rnk
	,SVCRNK.mclaren_major_slp_grouping as e_mclaren_major_slp_grouping
	,SURGEON.prim_srgn_cd
	,SURGEON.prim_srgn_nm
	,SURGEON.prim_srgn_npi
	,SURGEON.prim_srgn_spclty
	,SURGEON.prim_srgn_mcare_spcly_cd
	,CNSLT.cnslt_pract_1_cd
	,CNSLT.cnslt_pract_1_nm
	,CNSLT.cnslt_pract_1_npi
	,CNSLT.cnslt_pract_1_spclty
	,CNSLT.cnslt_pract_1_mcare_spcly_cd
	,CNSLT.cnslt_pract_2_cd
	,CNSLT.cnslt_pract_2_nm
	,CNSLT.cnslt_pract_2_npi
	,CNSLT.cnslt_pract_2_spclty
 	,CNSLT.cnslt_pract_2_mcare_spcly_cd
        ,CNSLT.cnslt_pract_3_cd
	,CNSLT.cnslt_pract_3_nm
	,CNSLT.cnslt_pract_3_npi
	,CNSLT.cnslt_pract_3_spclty
	,CNSLT.cnslt_pract_3_mcare_spcly_cd
	,NETREV.est_acct_paid_ind
	,CASE WHEN NETREV.est_net_rev_amt > 0 THEN
            ROUND(NETREV.est_net_rev_amt, 2)
	 ELSE
            ROUND(0,2) END as est_net_rev_amt
        ,NETREV.prof_chrg_ind
	,ZOOM3YRS.fiscal_yr
	,CHRGRCC.agg_rcc_based_direct_cst_amt
	,CHRGRCC.agg_rcc_based_indirect_cst_amt
	,CHRGRCC.agg_rcc_based_total_cst_amt
        ,CHRGRCC.agg_calculated_or_hrs
        ,ZOOM3YRS.fiscal_yr_tp

	--Code Change : Physician Attributions Data Elements (Below code changed as requested on 04/06/2020

	,CASE WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG' THEN SURGEON.prim_srgn_cd
	 ELSE nvl(ATTENDPRACTSPEC.practitioner_code,'-100') END AS attrb_physcn_cd

	,CASE WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG' THEN SURGEON.prim_srgn_nm
	 ELSE ATTENDPRACTSPEC.practitioner_name END AS attrb_physcn_nm

	,CASE WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG' THEN SURGEON.prim_srgn_npi
	 ELSE ATTENDPRACTSPEC.npi END as attrb_physn_npi

	,CASE WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG' THEN SURGEON.prim_srgn_mcare_spcly_cd
	 ELSE ATTENDPRACTSPEC.mcare_spcly_cd END AS attrb_physcn_spcl_cd

	,CASE WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG' THEN SURGEON.prim_srgn_spclty
	 ELSE ATTENDPRACTSPEC.practitioner_spclty_description END AS attrb_physcn_spcl_cd_descr

--	,CASE WHEN ZOOM.inpatient_outpatient_flag = 'O' THEN
--	       nvl(ATTENDPRACTSPEC.practitioner_code,'-100')
--		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG'     THEN
--	        SURGEON.prim_srgn_cd
--		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND (MSDRGDIM.ms_drg_type_cd = 'OTH' OR   MSDRGDIM.ms_drg_type_cd = 'MED')   THEN
--	        nvl(ATTENDPRACTSPEC.practitioner_code,'-100')
--		END AS attrb_physcn_cd,
--        CASE WHEN ZOOM.inpatient_outpatient_flag = 'O' THEN
--	    ATTENDPRACTSPEC.practitioner_name
--		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG'     THEN
--	        SURGEON.prim_srgn_nm
--		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND (MSDRGDIM.ms_drg_type_cd = 'OTH' OR   MSDRGDIM.ms_drg_type_cd = 'MED')     THEN
--	        ATTENDPRACTSPEC.practitioner_name
--		END AS attrb_physcn_nm,
--	CASE WHEN ZOOM.inpatient_outpatient_flag = 'O' THEN
--	    ATTENDPRACTSPEC.npi
--		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG'     THEN
--	        SURGEON.prim_srgn_npi
--		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND (MSDRGDIM.ms_drg_type_cd = 'OTH' OR   MSDRGDIM.ms_drg_type_cd = 'MED')     THEN
--	        ATTENDPRACTSPEC.npi
--		END as attrb_physn_npi,
--	CASE WHEN ZOOM.inpatient_outpatient_flag = 'O' THEN
--	    ATTENDPRACTSPEC.mcare_spcly_cd
--		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG'     THEN
--	        SURGEON.prim_srgn_mcare_spcly_cd
--		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND (MSDRGDIM.ms_drg_type_cd = 'OTH' OR   MSDRGDIM.ms_drg_type_cd = 'MED')     THEN
--	        ATTENDPRACTSPEC.mcare_spcly_cd
--		END AS attrb_physcn_spcl_cd,
--	CASE WHEN ZOOM.inpatient_outpatient_flag = 'O' THEN
--	    ATTENDPRACTSPEC.practitioner_spclty_description
--		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG'     THEN
--	        SURGEON.prim_srgn_spclty
--		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND (MSDRGDIM.ms_drg_type_cd = 'OTH' OR   MSDRGDIM.ms_drg_type_cd = 'MED')     THEN
--	        ATTENDPRACTSPEC.practitioner_spclty_description
--		END AS attrb_physcn_spcl_cd_descr,
	    , nvl(SPECLVALID.specl_valid_ind, 0 ) as specl_valid_ind
        , nvl(CANCER.cancer_dgns_cd,'-100') as cancer_dgns_cd
        , nvl(CANCER.cancer_case_ind , 0) as cancer_case_ind
	    , nvl(CANCER.cancer_case_code_descr,'UNKNOWN') as cancer_case_code_descr
        , CLIENTDRG.mclaren_major_slp_grouping as client_mjr_slp_grp
        , CLIENTDRG.mclaren_service_line as client_drg_svc_line_grp
        , CLIENTDRG.mclaren_sub_service_line as client_drg_sub_svc_line_grp
--CODE CHANGE : AUG 2019 Blood and Lab Utilization
        ,BLOOD.blood_util_qty as blood_util_qty
        ,LAB.lab_util_qty  as lab_util_qty
--CODE CHANGE : Added Sub_facility
        ,ZOOM.subfacility as sub_fcy
--CODE CHANGE : Added Ptnt_Days (ROOM and BOARD Only) excluding Hospice/Newborn/SELECT
	,CASE
		WHEN intermediate_stage_temp_derived_ptnt_days_rnb_only.patient_id IS NOT NULL
			THEN intermediate_stage_temp_derived_ptnt_days_rnb_only.ptnt_days_rb_only
		ELSE NULL
		END AS ptnt_days_room_n_board_fin_cases
--CODE CHANGE : Added Covid Admit Indicator
--	,CASE when ZOOM.primaryicd10diagnosiscode in ('U07.1') then 1
--		ELSE NULL END as covid_adm_ind
	,covid_tst_ind
	,covid_ptnt_ind as covid_adm_ind
	,covid_ptnt_ind
	,covid_ssp_ind
--CODE CHANGE: 08/31/2020 MLH-581
       , PFAGG.fcy_pymt_amt
       , PFAGG.fcy_adj_amt
       , CFAGG.prfssnl_chrg_amt
       , CFAGG.fcy_chrg_amt
       , CFAGG.prfssnl_direct_cst_amt
       , CFAGG.fcy_direct_cst_amt
       , CFAGG.prfssnl_indirect_cst_amt
       , CFAGG.fcy_indirect_cst_amt
       , CFAGG.prfssnl_total_cst_amt
       , CFAGG.fcy_total_cst_amt
       , empi.empi
       , empi.empi_ind
	,ephf.cpt_code prim_hcpcs_cd
	,ephf.prim_hcpcs_descr
	,ephf.prim_ccs_hcpcs_cgy_cd
	,ephf.prim_ccs_hcpcs_cgy_descr
	,ephf.prim_betos_cd
	,ephf.prim_betos_descr
	,ephf.prim_betos_cgy_nm
	--CODE CHANGE : MLH-591: Added Maintenance Cancer Service Indicator
        ,CANCER.maint_cancer_case_ind
	,hol.hol_ind as dschrg_dt_hol_ind
	,coalesce(CHRGRCC.clnscpy_ind,acptf.cpt_clnscpy_ind) as clnscpy_ind
	,CHRGRCC.mamgrphy_ind
--	, CASE WHEN ZOOM.inpatient_outpatient_flag = 'I' AND UPPER(practitioner_spclty_description) in
--	(
--	'CARDIOVASCULAR DISEASE (CARDIOLOGY)',
--	'INTERVENTIONAL CARDIOLOGY',
--	 'CARDIAC SURGERY',
--	 'THORACIC SURGERY',
--	 'CARDIAC ELECTROPHYSIOLOGY'
--	) THEN 1 ELSE 0 END AS  phys_specl_valid_ind
--07/13/2021 : Added ZOOM.ethnicity_code
,ZOOM.ethnicity_code as ethncty_cd
--08/30/2021: MLH-723: Added a new measure
,nvl(PFAGG.excld_trnsfr_encntr_ind,0) as excld_trnsfr_encntr_ind,
--09/14/2021: MLH-723: Added new measures based on members request
 ZOOM.mothersaccount as mthrs_accnt,
 ZOOM.mothersname as mthrs_nm,
 ZOOM.patientssn as ptnt_ssn,
 ZOOM.fin,
 ZOOM.fin_string as fin_str,
 ZOOM.fin_with_recur_sequence as fin_w_rcur_seq,
 ZOOM.recur_sequence as rcr_seq,
 ZOOM.recur_service_month as rcr_svc_mnth,
 ZOOM.recur_service_year as rcr_svc_yr,
 ZOOM.preregistration_date_time as pre_rgstr_dt_tm,
 ZOOM.combined_into_encounter_id as cmbnd_encntr_num,
 ZOOM.bad_dept_balance as bad_dbt_balnc,
 ZOOM.bad_dept_date_time as bad_dbt_dt,
 ZOOM.coding_status as cdng_sts,
 ZOOM.coding_last_updated_by_username as cdng_lst_updtd_by_usrnm,
 ZOOM.patient_email as ptnt_email,
 ZOOM.working_drg as wrkng_drg
FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM
INNER JOIN  intermediate_stage_temp_eligible_encntr_data ZOOM3YRS
on ZOOM.company_id = ZOOM3YRS.company_id and ZOOM.patient_id = ZOOM3YRS.patient_id
LEFT JOIN  intermediate_stage_chrg_cost_fct CHRGRCC
on ZOOM.company_id = CHRGRCC.fcy_nm and ZOOM.patient_id = CHRGRCC.encntr_num
LEFT JOIN  intermediate_stage_encntr_qly_anl_fct QADV ON ZOOM.patient_id = QADV.encntr_num AND QADV.ptnt_cl_cd = ZOOM.inpatient_outpatient_flag--AND ZOOM.company_id = QADV.fcy_num
LEFT JOIN pce_qe16_oper_prd_zoom..cv_empi empi on ZOOM.company_id = empi.company_id and ZOOM.patient_id=empi.patient_id
--Added on 11/02/2020
AND ZOOM.medical_record_number = empi.medical_record_number
LEFT JOIN intermediate_stage_encntr_prim_cpt_fct ephf on ZOOM.company_id = ephf.fcy_nm and ZOOM.patient_id = ephf.encntr_num
LEFT JOIN intermediate_aggr_cpt_fct acptf on ZOOM.company_id = acptf.fcy_nm and ZOOM.patient_id = acptf.encntr_num
LEFT JOIN val_set_dim VSET_FCY ON VSET_FCY.cd = ZOOM.company_id
	AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
LEFT JOIN stnd_fcy_demog_dim FCY_REF ON VSET_FCY.alt_cd = FCY_REF.fcy_num
LEFT JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_dschrg_inpatient.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_physician_npi_spclty ATTENDPRACTSPEC ON ZOOM.company_id = ATTENDPRACTSPEC.company_id
	AND ZOOM.attending_practitioner_code = ATTENDPRACTSPEC.practitioner_code
LEFT JOIN intermediate_stage_temp_physician_npi_spclty ADMITPRACTSPEC ON ZOOM.company_id = ADMITPRACTSPEC.company_id
	AND ZOOM.admitting_practitioner_code = ADMITPRACTSPEC.practitioner_code
LEFT JOIN intermediate_stage_temp_dschrg_inpatient_hospice ON intermediate_stage_temp_dschrg_inpatient_hospice.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_dschrg_inpatient_hospice.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_obsrv ON intermediate_stage_temp_obsrv.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_obsrv.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_icu ON intermediate_stage_temp_icu.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_icu.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_ccu ON intermediate_stage_temp_ccu.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_ccu.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_nrs ON intermediate_stage_temp_nrs.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_nrs.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_rtne ON intermediate_stage_temp_rtne.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_rtne.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_ed_case ON intermediate_stage_temp_ed_case.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_ed_case.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_dschrg_inpatient_nbrn ON intermediate_stage_temp_dschrg_inpatient_nbrn.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_dschrg_inpatient_nbrn.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_dschrg_inpatient_rehab ON intermediate_stage_temp_dschrg_inpatient_rehab.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_dschrg_inpatient_rehab.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_dschrg_inpatient_psych ON intermediate_stage_temp_dschrg_inpatient_psych.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_dschrg_inpatient_psych.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_dschrg_inpatient_ltcsnf ON intermediate_stage_temp_dschrg_inpatient_ltcsnf.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_dschrg_inpatient_ltcsnf.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_dschrg_inpatient_spclcare ON intermediate_stage_temp_dschrg_inpatient_spclcare.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_dschrg_inpatient_spclcare.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_dschrg_inpatient_lipmip ON intermediate_stage_temp_dschrg_inpatient_lipmip.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_dschrg_inpatient_lipmip.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_dschrg_inpatient_acute ON intermediate_stage_temp_dschrg_inpatient_acute.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_dschrg_inpatient_acute.company_id = ZOOM.company_id
--Code Change: Commenting the following since ptnt_days_total would be based on SPL dimension
--LEFT JOIN intermediate_stage_temp_derived_ptnt_days ON intermediate_stage_temp_derived_ptnt_days.patient_id = ZOOM.patient_id
--AND intermediate_stage_temp_derived_ptnt_days.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_srgl_case ON intermediate_stage_temp_srgl_case.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_srgl_case.company_id = ZOOM.company_id
--Code Change : To add LITHOTRIPSY, Endoscopy and Cath Lab Case INDICATOR
LEFT JOIN intermediate_stage_temp_cathlab_case ON intermediate_stage_temp_cathlab_case.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_cathlab_case.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_lithotripsy_case ON intermediate_stage_temp_lithotripsy_case.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_lithotripsy_case.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_endoscopy_case ON intermediate_stage_temp_endoscopy_case.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_endoscopy_case.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_derived_ptnt_days_nbrn ON intermediate_stage_temp_derived_ptnt_days_nbrn.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_derived_ptnt_days_nbrn.company_id = ZOOM.company_id
--Code Change : To add Ptnt_Days_Room_And_Board_With_Financial Cases Only
LEFT JOIN intermediate_stage_temp_derived_ptnt_days_rnb_only ON intermediate_stage_temp_derived_ptnt_days_rnb_only.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_derived_ptnt_days_rnb_only.company_id = ZOOM.company_id
--Code Change : To add Ptnt_Days_stepdown
LEFT JOIN intermediate_stage_temp_derived_ptnt_days_stepdown ON intermediate_stage_temp_derived_ptnt_days_stepdown.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_derived_ptnt_days_stepdown.company_id = ZOOM.company_id
--Code Change : 08/24/2020 To Add R&B Telemetry to Patient Days Routine
LEFT JOIN intermediate_stage_temp_derived_ptnt_days_rehab ON intermediate_stage_temp_derived_ptnt_days_rehab.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_derived_ptnt_days_rehab.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_derived_ptnt_days_acute ON intermediate_stage_temp_derived_ptnt_days_acute.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_derived_ptnt_days_acute.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_derived_ptnt_days_psych ON intermediate_stage_temp_derived_ptnt_days_psych.patient_id = ZOOM.patient_id
	AND intermediate_stage_temp_derived_ptnt_days_psych.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_payer_fcy_std_code PRIMPAYER ON PRIMPAYER.company_id = ZOOM.company_id
	AND PRIMPAYER.fcy_payer_code = ZOOM.primary_payer_code
LEFT JOIN intermediate_stage_temp_payer_fcy_std_code SECONPAYER ON SECONPAYER.company_id = ZOOM.company_id
	AND SECONPAYER.fcy_payer_code = ZOOM.secondary_payer_code
LEFT JOIN intermediate_stage_temp_payer_fcy_std_code TRTYPAYER ON TRTYPAYER.company_id = ZOOM.company_id
	AND TRTYPAYER.fcy_payer_code = ZOOM.tertiarypayorplan
LEFT JOIN intermediate_stage_temp_payer_fcy_std_code QTRPAYER ON QTRPAYER.company_id = ZOOM.company_id
	AND QTRPAYER.fcy_payer_code = ZOOM.quaternarypayorplan
LEFT JOIN cdr_dim hol on to_date(ZOOM.discharge_date,'mmddyyyy')=hol.cdr_dt
--LEFT JOIN stnd_ptnt_type_dim STNDPTNTTYPE ON STNDPTNTTYPE.std_encntr_type_Cd = PATTYPEMAP.standard_patient_type_code
LEFT JOIN stnd_adm_type_dim ADMTYPE ON ADMTYPE.adm_type_cd = ZOOM.admission_type_visit_type
LEFT JOIN stnd_adm_src_dim ADMSRC ON ADMSRC.adm_src_cd = ZOOM.point_of_origin_for_admission_or_visit
LEFT JOIN intermediate_stage_temp_discharge_fcy_std_status_code DISSTATUS ON DISSTATUS.discharge_status = ZOOM.discharge_status
LEFT JOIN stnd_ptnt_zip_dim ZIPCODE ON ZIPCODE.ptnt_zip_cd = substr(ZOOM.residential_zip_code, 1, 5)
LEFT JOIN intermediate_stage_temp_ptnt_type_fcy_std_cd PATTYPE ON PATTYPE.patient_type_code = ZOOM.patient_type
	AND PATTYPE.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_ptnt_prim_dgns PRIMDGNS ON PRIMDGNS.patient_id = ZOOM.patient_id
	AND PRIMDGNS.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_ptnt_second_dgns SCDYDGNS ON SCDYDGNS.patient_id = ZOOM.patient_id
	AND SCDYDGNS.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_ptnt_trty_dgns TRTYDGNS ON TRTYDGNS.patient_id = ZOOM.patient_id
	AND TRTYDGNS.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_ptnt_prim_proc PRIMPROC ON PRIMPROC.patient_id = ZOOM.patient_id
	AND PRIMPROC.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_ptnt_scdy_proc SCDYPROC ON SCDYPROC.patient_id = ZOOM.patient_id
	AND SCDYPROC.company_id = ZOOM.company_id
LEFT JOIN intermediate_stage_temp_ptnt_trty_proc TRTYPROC ON TRTYPROC.patient_id = ZOOM.patient_id
	AND TRTYPROC.company_id = ZOOM.company_id
LEFT JOIN val_set_dim VSET_MARITAL ON VSET_MARITAL.cd = ZOOM.marital_status
	AND VSET_MARITAL.cohrt_id = 'MARITAL_STATUS'
LEFT JOIN val_set_dim VSET_GENDER ON VSET_GENDER.cd = ZOOM.sex
	AND VSET_GENDER.cohrt_id = 'GENDER'
LEFT JOIN pce_qe16_prd_qadv..race_cd_ref QADV_RACE ON QADV_RACE.race_cd = ZOOM.race
LEFT JOIN ms_drg_dim ACO_MSDRG ON ACO_MSDRG.ms_drg_cd = ZOOM.msdrg_code
LEFT JOIN fips_adr_dim ACO_FIPSADR ON ACO_FIPSADR.fips_cnty_cd = substr(ZIPCODE.cnty_fips_ste_nm, 3, 3)
	AND ACO_FIPSADR.fips_ste_cd = substr(ZIPCODE.cnty_fips_ste_nm, 1, 2)
LEFT JOIN stnd_pnt_of_orig_ref QADV_POO ON QADV_POO.pnt_of_orig_cd = ZOOM.point_of_origin_for_admission_or_visit
LEFT JOIN dgns_dim DGNSDIM ON DGNSDIM.dgns_alt_cd = replace(ZOOM.primaryicd10diagnosiscode,'.','') and DGNSDIM.dgns_icd_ver ='ICD10'
LEFT JOIN dgns_dim ADMDGNS ON ADMDGNS.dgns_alt_cd = replace(ZOOM.admitdiagnosiscode,'.','') AND ADMDGNS.dgns_icd_ver ='ICD10'
LEFT JOIN pcd_dim PCDDIM ON PCDDIM.icd_pcd_cd = ZOOM.primaryicd10procedurecode and PCDDIM.icd_ver='ICD10'
LEFT JOIN val_set_dim VSET_ETHCTY ON VSET_ETHCTY.cd = ZOOM.ethnicity_code
	AND VSET_ETHCTY.cohrt_id = 'ETHNICITY'
LEFT JOIN intermediate_stage_temp_encntr_svc_hier SVCRNK
on SVCRNK.company_id = ZOOM.company_id and SVCRNK.patient_id = ZOOM.patient_id
LEFT JOIN  intermediate_stage_temp_surgeon_pract SURGEON
on SURGEON.company_id = ZOOM.company_id and SURGEON.patient_id = ZOOM.patient_id
LEFT JOIN  intermediate_stage_encntr_cnslt_pract_fct CNSLT
on CNSLT.fcy_nm = ZOOM.company_id and CNSLT.encntr_num = ZOOM.patient_id
LEFT JOIN  intermediate_stage_encntr_net_rvu_fct NETREV
on NETREV.fcy_nm = ZOOM.company_id and NETREV.encntr_num = ZOOM.patient_id
LEFT JOIN ms_drg_dim MSDRGDIM
on MSDRGDIM.ms_drg_cd = CAST(LPAD(CAST(coalesce(ZOOM.msdrg_code,'000') as INTEGER), 3,0 ) as Varchar(3)) AND MSDRGDIM.ms_drg_type_cd IN ('SURG','MED','OTH')
LEFT JOIN   intermediate_stage_temp_specl_valid_ind SPECLVALID
on SPECLVALID.fcy_nm = ZOOM.company_id and ZOOM.patient_id = SPECLVALID.encntr_num
LEFT JOIN  intermediate_stage_temp_encntr_dgns_fct_with_cancer_case CANCER
on CANCER.encntr_num = ZOOM.patient_id AND CANCER.fcy_nm = ZOOM.company_id
--CODE CHANGE : AUG 2019 Blood and Lab Utilization
LEFT JOIN  intermediate_stage_temp_blood_util_qty BLOOD
on BLOOD.encntr_num = ZOOM.patient_id AND BLOOD.fcy_nm = ZOOM.company_id
LEFT JOIN  intermediate_stage_temp_lab_util_qty LAB
on LAB.encntr_num = ZOOM.patient_id AND LAB.fcy_nm = ZOOM.company_id
---------
---LEFT JOIN client_drg_svc_line_grouper_new CLIENTDRG
LEFT JOIN pce_qe16_oper_prd_zoom..cv_drgmap CLIENTDRG
on CAST(LPAD(CAST(coalesce(ZOOM.msdrg_code,'000') as INTEGER), 3,0 ) as Varchar(3)) = CLIENTDRG.ms_drg_code
----
LEFT JOIN encntr_covid_test cvdt on ZOOM.company_id=cvdt.company_id and ZOOM.patient_id=cvdt.patient_id
LEFT JOIN covid_patient cvd ON ZOOM.company_id=cvd.company_id and ZOOM.patient_id=cvd.patient_id
----
LEFT JOIN pce_qe16_oper_prd_zoom..cv_admitservice CVADMSVC
on CVADMSVC.code = ZOOM.admitservice AND CVADMSVC.facility = ZOOM.company_id
LEFT JOIN pce_qe16_oper_prd_zoom..cv_dischservice CVDSCHRGSVC
on CVDSCHRGSVC.code = ZOOM.dischargeservice AND CVDSCHRGSVC.facility = ZOOM.company_id AND CVDSCHRGSVC.fieldname='DischargeService'
LEFT JOIN pce_qe16_oper_prd_zoom..cv_financialclass CVFNCCLS
on CVFNCCLS.code = ZOOM.financialclass AND CVFNCCLS.facility = ZOOM.company_id
--MLH-581
lEFT JOIN  intermediate_stage_chrg_agg_fct CFAGG
on  CFAGG.fcy_nm = ZOOM.company_id AND CFAGG.encntr_num = ZOOM.patient_id
lEFT JOIN  intermediate_stage_ptnt_fnc_txn_agg_fct PFAGG
on  PFAGG.fcy_nm = ZOOM.company_id AND PFAGG.encntr_num = ZOOM.patient_id
--code change : 08/11/2021: fixed the JOB failure by changing the where clause NOT LIKE 'V%'
--WHERE coalesce(ZOOM.msdrg_code ,'000') NOT IN ('V45','V70','V67','V04')
WHERE coalesce(ZOOM.msdrg_code ,'000') NOT LIKE '%V%'
--WHERE ZOOM.discharge_total_charges > 0
--	AND cast(ZOOM.admission_ts AS DATE) BETWEEN add_months(CURRENT_DATE, - 36)
--		AND CURRENT_DATE
--DISTRIBUTE ON (fcy_nm_hash,encntr_num_hash);
DISTRIBUTE ON (fcy_nm, encntr_num);

-------------------------------------- SERVICE LINE Updates--------------------

DROP TABLE intermediate_stage_encntr_anl_fct IF EXISTS;
CREATE TABLE intermediate_stage_encntr_anl_fct AS
SELECT ef.*,
case when ef.in_or_out_patient_ind = 'I ' then ef.e_mclaren_major_slp_grouping ELSE ef.e_svc_ln_nm END as cal_svc_ln,
case when cal_svc_ln is null then null else
COALESCE(case
when ef.in_or_out_patient_ind = 'I ' and (ef.e_mclaren_major_slp_grouping <> ef.e_svc_ln_nm) then 'Other'
ELSE ef.e_sub_svc_ln_nm END,'Other') end as cal_sub_svc_ln,
case when ef.in_or_out_patient_ind = 'I ' and (ef.e_mclaren_major_slp_grouping <> nvl(ef.e_svc_ln_nm,' ')) then 'Other' ELSE ef.e_svc_nm END as cal_svc_nm

FROM  intermediate_stage_encntr_anl_fct_temp ef
DISTRIBUTE ON (fcy_nm, encntr_num);