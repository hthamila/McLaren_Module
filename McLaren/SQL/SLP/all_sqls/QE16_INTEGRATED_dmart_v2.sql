\set ON_ERROR_STOP ON;
--Qualifiers 

--select 'processing table:  intermediate_stage_temp_dates_tbl' as table_processing;
DROP TABLE intermediate_stage_temp_dates_tbl IF EXISTS; 
CREATE TEMP TABLE intermediate_stage_temp_dates_tbl 
AS 
select 
CASE WHEN DATE_PART('day',now()) >=15 THEN last_day(now() - INTERVAL '1 MONTH') ELSE last_day(now() - INTERVAL '2 MONTH') END as curr_year_end_dt;

--select 'processing table:  intermediate_stage_temp_fiscal_year_tbl' as table_processing;
DROP TABLE intermediate_stage_temp_fiscal_year_tbl IF EXISTS; 
CREATE TEMP Table intermediate_stage_temp_fiscal_year_tbl AS 
with fiscal_year_tbl AS 
(select distinct 
CASE WHEN 
MONTH(date_trunc('quarter',date(discharge_ts))) >= 10
THEN (YEAR(discharge_ts) + 1) ELSE YEAR(discharge_ts) END as FY_num, 
CASE WHEN 
MONTH(date_trunc('quarter',date(discharge_ts))) >= 10
THEN 'FY' || (YEAR(discharge_ts) + 1) ELSE 'FY' || YEAR(discharge_ts) END as FY,
CASE WHEN 
MONTH(date_trunc('quarter',date(discharge_ts))) >= 10
THEN  YEAR(date(discharge_ts)) || '-10-01' 
ELSE
      (YEAR(date(discharge_ts)) -1) || '-10-01'
END AS Fiscal_start,
CASE WHEN 
MONTH(date_trunc('quarter',date(discharge_ts))) >= 10
THEN  (YEAR(date(discharge_ts)) + 1) || '-09-30' 
ELSE
      YEAR(date(discharge_ts)) || '-09-30' 
END AS Fiscal_end
from  pce_qe16_oper_prd_zoom..cv_patdisch ZOOM
WHERE 
date(discharge_ts) <= Now() - Day(Now()))

select * from fiscal_year_tbl 
where fiscal_start <= (select curr_year_end_dt from intermediate_stage_temp_dates_tbl)
and fiscal_end >= (select curr_year_end_dt from intermediate_stage_temp_dates_tbl);

--select 'processing table:  intermediate_stage_temp_eligible_encntr_data_inpatient' as table_processing;
DROP TABLE intermediate_stage_temp_eligible_encntr_data_inpatient IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_eligible_encntr_data_inpatient AS (
		SELECT DISTINCT ZOOM.company_id
		,ZOOM.patient_id , ZOOM.inpatient_outpatient_flag ,
		ZOOM.admission_ts 
		,case WHEN ZOOM.inpatient_outpatient_flag ='O' THEN COALESCE(Date(zoom.discharge_ts), Date(zoom.admission_ts)) ELSE Date(zoom.discharge_ts) END AS discharge_ts
		,ZOOM.msdrg_code 
		,CAST(NULL as NUMERIC(14,4)) as ms_drg_wght
        ,CAST(NULL as NUMERIC(14,4)) as ms_drg_geo_mean_los_num
        ,CAST(NULL as NUMERIC(14,4)) as ms_drg_arthm_mean_los_num
                ,CASE WHEN  MONTH(date_trunc('quarter',date(discharge_ts))) >= 10  THEN 'FY' || (YEAR(discharge_ts) + 1) 
					   WHEN  MONTH(date_trunc('quarter',date(discharge_ts))) < 10   THEN 'FY' || YEAR(discharge_ts) 
					   WHEN  ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND MONTH(date_trunc('quarter',date(admission_ts))) >= 10  THEN  'FY' || (YEAR(admission_ts) + 1) 
					   WHEN  ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND MONTH(date_trunc('quarter',date(admission_ts))) < 10  THEN   'FY' || YEAR(admission_ts) 
					   ELSE
					   NULL END as fiscal_yr
		,CASE WHEN Date(zoom.discharge_ts) BETWEEN  (Select Fiscal_start from intermediate_stage_temp_fiscal_year_tbl) and --now()- Day(now()) THEN 'C'
		 (select curr_year_end_dt from intermediate_stage_temp_dates_tbl) THEN 'C' 
				      WHEN Date(zoom.discharge_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '1 year' FROM intermediate_stage_temp_dates_tbl) THEN 'P'
				      WHEN Date(zoom.discharge_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '2 year' from intermediate_stage_temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '2 year' FROM intermediate_stage_temp_dates_tbl) THEN 'P-1'
					  WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) BETWEEN  (Select Fiscal_start from intermediate_stage_temp_fiscal_year_tbl) and 
                       (select curr_year_end_dt	FROM intermediate_stage_temp_dates_tbl)				  THEN 'C'
				      WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND
                                           Date(zoom.admission_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '1 year' FROM intermediate_stage_temp_dates_tbl)  THEN 'P'
				      WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND
                                           Date(zoom.admission_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '2 year' from intermediate_stage_temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '2 year' FROM intermediate_stage_temp_dates_tbl)  THEN 'P-1'                       
					   ELSE NULL END AS fiscal_yr_tp
		FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM 
		WHERE ZOOM.inpatient_outpatient_flag ='I' AND 
 coalesce(ZOOM.msdrg_code,'000') NOT IN ('V45','V70') AND 
 ((cast(ZOOM.admission_ts AS DATE) BETWEEN DATE('2015-10-01') AND now()) OR (cast(ZOOM.discharge_ts AS DATE) BETWEEN DATE('2015-10-01') AND now())));



--select 'processing table:  intermediate_stage_temp_eligible_encntr_data_outpatient' as table_processing;
DROP TABLE intermediate_stage_temp_eligible_encntr_data_outpatient IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_eligible_encntr_data_outpatient AS (
		SELECT DISTINCT ZOOM.company_id
		,ZOOM.patient_id , ZOOM.inpatient_outpatient_flag ,
		ZOOM.admission_ts
		,case WHEN ZOOM.inpatient_outpatient_flag ='O' THEN COALESCE(Date(zoom.discharge_ts), Date(zoom.admission_ts)) ELSE Date(zoom.discharge_ts) END AS discharge_ts
		,ZOOM.msdrg_code 
		,CAST(NULL as NUMERIC(14,4)) as ms_drg_wght
        ,CAST(NULL as NUMERIC(14,4)) as ms_drg_geo_mean_los_num
        ,CAST(NULL as NUMERIC(14,4)) as ms_drg_arthm_mean_los_num
                ,CASE WHEN  MONTH(date_trunc('quarter',date(discharge_ts))) >= 10  THEN 'FY' || (YEAR(discharge_ts) + 1) 
					   WHEN  MONTH(date_trunc('quarter',date(discharge_ts))) < 10   THEN 'FY' || YEAR(discharge_ts) 
					   WHEN  ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND MONTH(date_trunc('quarter',date(admission_ts))) >= 10  THEN  'FY' || (YEAR(admission_ts) + 1) 
					   WHEN  ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND MONTH(date_trunc('quarter',date(admission_ts))) < 10  THEN   'FY' || YEAR(admission_ts) 
					   ELSE
					   NULL END as fiscal_yr
		,CASE WHEN Date(zoom.discharge_ts) BETWEEN  (Select Fiscal_start from intermediate_stage_temp_fiscal_year_tbl) and (select curr_year_end_dt from intermediate_stage_temp_dates_tbl) THEN 'C' 
				      WHEN Date(zoom.discharge_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '1 year' FROM intermediate_stage_temp_dates_tbl) THEN 'P'
	                  WHEN Date(zoom.discharge_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '2 year' from intermediate_stage_temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '2 year' FROM intermediate_stage_temp_dates_tbl) THEN 'P-1'
	WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) BETWEEN  (Select Fiscal_start from intermediate_stage_temp_fiscal_year_tbl) and (select curr_year_end_dt	FROM intermediate_stage_temp_dates_tbl) THEN 'C'
				      WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND
                                           Date(zoom.admission_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '1 year' FROM intermediate_stage_temp_dates_tbl)  THEN 'P'
                      WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND
                                           Date(zoom.admission_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '2 year' from intermediate_stage_temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '2 year' FROM intermediate_stage_temp_dates_tbl)  THEN 'P-1'                                     
									 ELSE NULL END AS fiscal_yr_tp

		FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM 
		WHERE ZOOM.inpatient_outpatient_flag ='O' AND 
 coalesce(ZOOM.msdrg_code,'000') NOT IN ('V45','V70') AND 
 (cast(ZOOM.admission_ts AS DATE) BETWEEN DATE('2015-10-01') AND now()) 
		);


--CODE CHANGE : AUG 2019 (a) Ms_Drg_Dim Historical CMI Weights
--select 'processing table:  intermediate_stage_temp_ms_drg_dim_hist' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_ms_drg_dim_hist  IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_ms_drg_dim_hist AS 
select ms_drg_cd, 
CAST(case_mix_idnx_num as NUMERIC(14,4)) as drg_wght,
CAST(geo_mean_los_num as NUMERIC(14,4)) as ms_drg_geo_mean_los_num,
CAST(arthm_mean_los_num as NUMERIC(14,4)) as ms_drg_arthm_mean_los_num,
drg_vrsn, vld_fm_dt, nvl(vld_to_dt, now()) as vld_to_dt
  FROM pce_ae00_aco_prd_cdr..ms_drg_dim_h 
  WHERE case_mix_idnx_num NOT IN ('UNKNOWN');
  
   
--CODE CHANGE : AUG 2019 (a) Ms_Drg_Dim Historical CMI Weights   intermediate_stage_temp_eligible_encntr_data
--select 'processing table:  intermediate_stage_temp_eligible_encntrs' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_eligible_encntrs IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_eligible_encntrs 
AS 
SELECT X.*
FROM intermediate_stage_temp_eligible_encntr_data_inpatient X
UNION 
SELECT Y.*
FROM intermediate_stage_temp_eligible_encntr_data_outpatient Y; 


--select 'processing table: intermediate_stage_temp_encntr_with_ms_drg_wghts' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_encntr_with_ms_drg_wghts IF EXISTS; 

CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_encntr_with_ms_drg_wghts AS 
SELECT 
X.company_id, X.patient_id,  X.inpatient_outpatient_flag,  X.admission_ts,  X.discharge_ts,  X.msdrg_code, 
nvl(DRGWGHT.drg_wght , X.ms_drg_wght) as ms_drg_wght, 
nvl(DRGWGHT.ms_drg_geo_mean_los_num, X.ms_drg_geo_mean_los_num) as ms_drg_geo_mean_los_num,
nvl(DRGWGHT.ms_drg_arthm_mean_los_num, X.ms_drg_arthm_mean_los_num) as ms_drg_arthm_mean_los_num, 
X.fiscal_yr,  X.fiscal_yr_tp
FROM pce_qe16_slp_prd_dm..intermediate_stage_temp_eligible_encntrs X 
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_temp_ms_drg_dim_hist DRGWGHT
ON X.msdrg_code= DRGWGHT.ms_drg_cd  AND date(X.discharge_ts)  BETWEEN DRGWGHT.vld_fm_dt AND DRGWGHT.vld_to_dt;


--select 'processing table:  intermediate_stage_temp_encntr_without_ms_drg_wghts ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_encntr_without_ms_drg_wghts IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_encntr_without_ms_drg_wghts AS 
with recs_with_weights AS 
(select distinct patient_id , company_id FROM pce_qe16_slp_prd_dm..intermediate_stage_temp_encntr_with_ms_drg_wghts)
select * FROM pce_qe16_slp_prd_dm..intermediate_stage_temp_eligible_encntrs X  
WHERE (patient_id || company_id) NOT IN (select (patient_id || company_id) from recs_with_weights );

--select 'processing table: intermediate_stage_temp_eligible_encntr_data ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_eligible_encntr_data IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_eligible_encntr_data 
AS 
SELECT X.*
FROM intermediate_stage_temp_encntr_with_ms_drg_wghts X
UNION 
SELECT Y.*
FROM intermediate_stage_temp_encntr_without_ms_drg_wghts Y; 
		

--Adding ED Encounter Analysis Fact --------------------

--select 'processing table:  intermediate_stage_encntr_ed_anl_fct' as table_processing;
DROP TABLE intermediate_stage_encntr_ed_anl_fct if exists;
create table intermediate_stage_encntr_ed_anl_fct AS
select distinct
       ZOOM.company_id AS fcy_nm
       ,ZOOM.inpatient_outpatient_flag AS in_or_out_patient_ind
       ,ZOOM.medical_record_number
       ,ZOOM.patient_id AS encntr_num
       ,nvl(DGNSDIM.dgns_cd,'-100') AS prim_dgns_cd
       ,nvl(DGNSDIM.dgns_descr,'UNKNOWN') AS prim_dgns_descr
       ,DGNSDIM.alc_rel_pct
       ,DGNSDIM.drug_rel_pct 
       ,DGNSDIM.ed_care_needed_not_prvntable_pct
       ,DGNSDIM.ed_care_needed_prvntable_avoidable_pct
       ,DGNSDIM.injry_rel_pct
       ,DGNSDIM.non_emrgnt_rel_pct
       ,DGNSDIM.psychology_rel_pct 
       ,DGNSDIM.treatable_emrgnt_ptnt_care_pct
       ,DGNSDIM.unclsfd_pct
FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_temp_eligible_encntr_data ZOOM3YRS
on ZOOM.company_id = ZOOM3YRS.company_id and ZOOM.patient_id = ZOOM3YRS.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNSDIM ON DGNSDIM.dgns_alt_cd = replace(ZOOM.primaryicd10diagnosiscode,'.','') 
       and DGNSDIM.dgns_icd_ver ='ICD10'
--      DISTRIBUTE ON (fcy_nm_hash, encntr_num_hash);
DISTRIBUTE ON (fcy_nm, encntr_num); 
----ED Encounter Analysis Fact --------------------------

--select 'processing table:  intermediate_stage_temp_physician_npi_spclty' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_physician_npi_spclty

IF EXISTS;
	CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_physician_npi_spclty AS (
		SELECT DISTINCT PT.company_id
		,PT.practitioner_code
		,NPIREG.npi
--Code Change : Source NPI's are tied with More than one Physician so using NPI 
--Registry detaiils
 --		,PT.practitioner_name
--,coalesce(NPIREG.pvdr_lgl_last_nm ||  ', ' || NPIREG.pvdr_frst_nm || ' ' || NPIREG.pvdr_mid_nm, NPIREG.pvdr_lgl_org_nm)   as practitioner_name
         --       ,NPIREG.pvdr_lgl_last_nm ||  ', ' || NPIREG.pvdr_frst_nm  as practitioner_name
--,coalesce(coalesce(NPIREG.pvdr_lgl_last_nm,'') ||  ', ' || coalesce(NPIREG.pvdr_frst_nm,'') || ' ' || coalesce(NPIREG.pvdr_mid_nm,''), coalesce(NPIREG.pvdr_lgl_org_nm,''))   as practitioner_name
--,decode(coalesce(NPIREG.pvdr_lgl_last_nm,'') ||  ', ' || coalesce(NPIREG.pvdr_frst_nm,'') || ' ' || coalesce(NPIREG.pvdr_mid_nm,''), ',', coalesce(NPIREG.pvdr_lgl_org_nm,'')) as practitioner_name
,CASE WHEN trim(coalesce(NPIREG.pvdr_lgl_last_nm,'') ||  ', ' || coalesce(NPIREG.pvdr_frst_nm,'') || ' ' || coalesce(NPIREG.pvdr_mid_nm,'')) = ',' THEN 
coalesce(NPIREG.pvdr_lgl_org_nm,'')
ELSE
   trim(coalesce(NPIREG.pvdr_lgl_last_nm,'') ||  ', ' || coalesce(NPIREG.pvdr_frst_nm,'') || ' ' || coalesce(NPIREG.pvdr_mid_nm,''))
END as practitioner_name
--After CHANGE
		--,coalesce(NPIREG.hcare_pvdr_txnmy_cl_nm , NPIREG.hcare_scdy_pvdr_txnmy_cl_nm)  AS practitioner_spclty_description 
		,replace(coalesce(NPIREG.hcare_pvdr_txnmy_descr,NPIREG.hcare_scdy_pvdr_txnmy_descr),'-',' ') as practitioner_spclty_description
		,coalesce(NPIREG.hcare_pvdr_txnmy_cd, NPIREG.hcare_scdy_pvdr_txnmy_cd) as mcare_spcly_cd
--Before Change
--		,coalesce(coalesce(CWALK.pvdr_spclty_descr,'') , coalesce(NPIREG.mcare_spcly_descr,''))  AS practitioner_spclty_description 
--		,NPIREG.mcare_spcly_cd
--		,coalesce(NPIREG.hcare_pvdr_txnmy_cd, NPIREG.hcare_scdy_pvdr_txnmy_cd) as hcare_pvdr_txnmy_cd
--		,coalesce(NPIREG.hcare_pvdr_txnmy_cl_nm , NPIREG.hcare_scdy_pvdr_txnmy_cl_nm) as hcare_pvdr_txnmy_cl_nm
		,NPIREG.npi_dactv_dt
		FROM pce_qe16_slp_prd_dm..phys_dim PT 
		INNER JOIN pce_qe16_slp_prd_dm..pvdr_dim NPIREG 
		ON PT.npi = NPIREG.npi
--                LEFT JOIN pce_qe16_slp_prd_dm..manual_txny_pvdr_spcl_dim CWALK
--		on trim(NPIREG.hcare_pvdr_txnmy_cd) = Trim(CWALK.pvdr_txny_cd)
--      WHERE initcap(PT.company_id) <> 'Lansing'
		);

----select 'processing table:svc_hier_dim_consolidated  ' as table_processing;
DROP TABLE svc_hier_dim_consolidated if exists; 
--create table svc_hier_dim_consolidated as sElect * from svc_hier_dim_consolidated; 

--CODE Change : Adding intermediate_stage_intermediate_stage_spl_dim to fix the Lansing encounters with Charge Code but SPL Code is NULL issue 
--select 'processing table:  intermediate_stage_spl_dim' as table_processing;
DROP TABLE intermediate_stage_spl_dim IF EXISTS;
CREATE TABLE intermediate_stage_spl_dim as
with zoom_uniq_chrg_codes as 
(
  SELECT distinct cf.company_id, VSET_FCY.alt_cd as fcy_num, cf.charge_code, spl.cdm_cd
  FROM pce_qe16_oper_prd_zoom..cv_patbill cf
    LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
  ON VSET_FCY.cd = cf.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN  pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl spl on cf.charge_code = spl.cdm_cd
  and VSET_FCY.alt_cd = spl.fcy_num and cf.charge_Code = spl.cdm_cd
  WHERE spl.cdm_cd is NULL
)
SELECT pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.fcy_num, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_cd, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_strt_cdr_dk, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_strt_dt, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_end_cdr_dk, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_end_dt, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_descr, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_procedure_cd_v10 AS persp_clncl_dtl_pcd_cd_v10,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_procedure_descr_v10 AS persp_clncl_dtl_pcd_descr_v10,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.spl_unit_conv AS spl_unit_cnvr, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_cd AS persp_clncl_dtl_cd,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_descr AS persp_clncl_dtl_descr,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_unit AS persp_clncl_dtl_unit, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcs_cd AS persp_clncl_smy_cd,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcs_descr AS persp_clncl_smy_descr, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_cd_v10 AS persp_clncl_std_dept_cd_v10,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_descr_v10 AS persp_clncl_std_dept_descr_v10, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_v10_rollup_cat_cd AS persp_clncl_std_dept_v10_rollup_cgy_cd,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_v10_rollup_cat_descr AS persp_clncl_std_dept_v10_rollup_cgy_descr,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_spl_modfr_cd AS persp_clncl_dtl_spl_modfr_cd, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_spl_modfr_descr AS persp_clncl_dtl_spl_modfr_descr
FROM pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl
UNION
select 
'-100',
'-100',
19000101,
'1900-01-01',
29000101,
'2900-01-01',
'UNKNOWN',
'-100',
'UNKNOWN',
0.0,
0.0,
'UNKNOWN',
0.0,
'-100',
'UNKNOWN',
-100,
'UNKNOWN',
'-100',
'UNKNOWN',
'-100',
'UNKNOWN'
UNION 
select 
fcy_num,
charge_code,
19000101,
'1900-01-01',
29000101,
'2900-01-01',
'UNKNOWN',
'-100',
'UNKNOWN',
0.0,
0.0,
'UNKNOWN',
0.0,
'-100',
'UNKNOWN',
-100,
'UNKNOWN',
'-100',
'UNKNOWN',
'-100',
'UNKNOWN'
FROM zoom_uniq_chrg_codes; 

--QADV Table creation based on Net 3 years Of patient Account Number
--select 'processing table:  intermediate_stage_encntr_qly_anl_fct' as table_processing;
DROP TABLE intermediate_stage_encntr_qly_anl_fct IF EXISTS;

CREATE TABLE intermediate_stage_encntr_qly_anl_fct as
select 
Z.company_id as fcy_nm
, Z.patient_id as encntr_num 
       , dschrg_cdr_dk
       , ptnt_cl_cd
       , dschrg_dt
       , adm_cdr_dk
       , adm_dt
       , pbls_type_ind
       , apr_drg_cd
       , apr_rom_cd
       , otlr_cd
       , gnd_cd
       , mar_sts_cd
       , los_cnt
       , tot_chrg_amt
       , tot_cst_amt
       , tot_fix_cst_amt
       , tot_var_cst_amt
       , tot_pmnt_amt
       , compl_cnt
       , mrtly_cnt
       , wi_cst_amt
       , wi_var_cst_amt
       , wi_chrg_amt
       , prim_diag_icd_poa_cnt
       , prim_diag_icd_pst_adm_cnt
       , csa_expc_mrtly_cnt
       , csa_expc_morbid_compl_cnt
       , csa_expc_compl_cnt
       , csa_expc_chrg_amt
       , csa_expc_los_cnt
       , csa_expc_cst_amt
       , csa_expc_svr_comp_rsk_cnt
       , csa_obs_readmit_rsk_adj_cnt
       , csa_expc_prs_readm_30dy_rsk
       , expc_mrtly_outc_case_cnt
       , expc_morbid_compl_outc_case_cnt
       , expc_compl_outc_case_cnt
       , expc_chrg_outc_case_cnt
       , expc_los_outc_case_cnt
       , expc_cst_outc_case_cnt
       , day_of_mech_vent_cnt
       , apr_expc_chrg_amt
       , apr_expc_cst_amt
       , apr_expc_fix_cst_amt
       , apr_expc_var_cst_amt
       , apr_expc_day_cnt
       , apr_expc_mrtly_cnt
       , apr_expc_compl_cnt
       , apr_expc_prev_c_section_cnt
       , apr_expc_prim_c_section_cnt
       , apr_expc_rpet_c_section_cnt
       , apr_expc_dlv_cnt
       , apr_expc_readmit_cnt
       , apr_readmit_cnt
       , drg_readmit_cnt
       , re_adm_day_cnt
       , prs_readm_30dy_rsk_out_case_cnt
       , prs_comp_out_case_cnt
       , prs_comp_rsk_out_case_cnt
       , prs_svr_comp_out_case_cnt
       , prs_svr_comp_rsk_out_case_cnt
       , acute_readmit_days_key
       , readmit_diag_ind
       , acute_readmit_diag_ind
       , csa_cmp_cst_scl_fctr
       , csa_cmp_scl_fctr
       , csa_los_scl_fctr
       , csa_mort_scl_fctr
       , csa_readm_30dy_scl_fctr
       , csa_tot_chg_scl_fctr
       , csa_ln_readm_30dy_los_stderr
       , csa_readm_30dy_stderr
       , readmit_cnt_30dy_diag
       , readmit_den
       , readmit_dtl_ind
       , readmit_risk_adj_den
       , readmit_unpln_pln_ind
       , csa_hwr4_readm_rsk_adj_cnt
       , csa_hwr4_30d_readm_out_case_cnt
       , csa_hwr4_expc_readm
       , csa_hwr4_expc_30day_readm_scl_fctr
       , csa_hwr4_readm_unpln2pln_ind
       , ln_los
       , csa_ln_exp_los
       , ln_total_cost
       , csa_ln_exp_comp_cost
       , apr_svry_of_ill
       , apr_rsk_of_mrtly
from intermediate_stage_temp_eligible_encntr_data Z
INNER JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
LEFT JOIN pce_qe16_slp_prd_dm..encntr_fct QADV
on Z.patient_id = QADV.encntr_num and QADV.fcy_num = VSET_FCY.alt_cd
--DISTRIBUTE ON (fcy_nm_hash, encntr_num_hash);
DISTRIBUTE ON (fcy_nm, encntr_num);

--intermediate_stage_chrg_fct Table creation based on Net 3 years Of patient Account Number
--select 'processing table:  intermediate_stage_chrg_fct_temp' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct_temp IF EXISTS ; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct_temp AS 
(
  SELECT Z.company_id as fcy_nm
        ,VSET_FCY.alt_cd as fcy_num
        ,Z.patient_id as encntr_num
       	,CH.company_id
       	,CH.patient_id
       	,DATE (to_timestamp((CH.service_date || ' ' || nvl(substr(CH.service_date, 1, 2), '00') || ':' || nvl(substr(CH.service_date, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS service_date
       	,nvl(CH.charge_code, '-100') as charge_code
       	,CH.quantity
       	,CH.total_charge
       	,CH.total_variable_cost
       	,CH.total_fixed_cost
	   --CODE CHANGE : AUG 2019 Populate CPT_CODE From cdm_dim when cv_patbill.cpt_code is NULL
      -- ,nvl(CH.cpt_code,'-100') as cpt_code
	,nvl(nvl(CH.cpt_code,CHRGCD.cpt_code),'-100') as cpt_code
	,nvl(CH.revenue_code,'-100') as revenue_code
	   /* Start Srujan Update for  Adding Revenue Code Grouping Attributes*/
	,nvl(crev.revenue_code_description,'UNKNOWN') AS revenue_code_description 
	,nvl(crev.client_revenue_code_group,'UNKNOWN') as client_revenue_code_group
	   /* End Srujan Update for  Adding Revenue Code Grouping Attributes*/
       	,nvl(CH.ordering_practitioner_code,'-100') as ordering_practitioner_code
       	,CH.cpt_modifier_1
       	,CH.cpt_modifier_2
       	,CH.cpt_modifier_3
       	,CH.cpt_modifier_4
	   /*Start CPT CCS AND BETOS */
	,hccs.ccs_hcpcs_cgy_cd
	,hccs.ccs_hcpcs_cgy_descr
	,hbt.betos_cd
	,hbt.betos_descr
	   /*END CPT CCS AND BETOS */
       	,CH.dept
	,DATE (to_timestamp((CH.postdate || ' ' || nvl(substr(CH.postdate, 1, 2), '00') || ':' || nvl(substr(CH.postdate, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS postdate
       	,CH.unitcharge
       	,CH.invoiceid
       	,CH.performingphysician
      -- ,CH.cpt4full
       	,CH.subaccount
       	,CHRGCD.charge_code_description as chargecodedesc
       	,CH.financialclass
       	,nvl(CH.payorplancode, '-100') as payorplancode
       	,DATE (to_timestamp((CH.updatedate || ' ' || nvl(substr(CH.updatedate, 1, 2), '00') || ':' || nvl(substr(CH.updatedate, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS updatedate
       	,CH.sourcesystem
       	,CH.raw_chargcode
	,CH.ndc
	,Z.fiscal_yr
       	,nvl(RCC.directrcc,0) as direct_cost_ratio
       	,nvl(RCC.indirectrcc,0) as indirect_cost_ratio
       	,nvl(RCC.totalrcc,0) as total_cost_ratio
	,Round(nvl(CH.total_charge * RCC.directrcc,  0),2) as rcc_based_direct_cst_amt
	,ROUND(nvl(CH.total_charge * RCC.indirectrcc, 0),2) as rcc_based_indirect_cst_amt
	,Round(nvl(CH.total_charge * RCC.totalrcc, 0),2) as rcc_based_total_cst_amt
--Code Change: 03/06 Added crline as per McLaren's Request
        ,nvl(RCC.crline,'0') as crline
--	   ,RAWCHRGCD.charge_code_description as raw_chrg_cd_descr
  FROM intermediate_stage_temp_eligible_encntr_data Z
  LEFT JOIN pce_qe16_slp_prd_dm..cv_patbill CH 
  on Z.company_id = CH.company_id and Z.patient_id = CH.patient_id
  LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
  ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN pce_qe16_slp_prd_dm..cdm_dim CHRGCD
  on CHRGCD.company_id = CH.company_id and CHRGCD.charge_code = CH.charge_code
  LEFT JOIN pce_qe16_slp_prd_dm..hcpcs_ccs_dim hccs
  on UPPER(CH.cpt_code)=hccs.hcpcs_cd
  LEFT JOIN pce_qe16_slp_prd_dm..hcpcs_betos_dim hbt
  on UPPER(CH.cpt_code)=hbt.hcpcs_cd
  LEFT JOIN pce_qe16_oper_prd_zoom..cv_rccmap RCC
  on RCC.fy = Z.fiscal_yr and CH.charge_code = RCC.charge_code and CH.company_id = RCC.company_id
  /* Start Srujan Update - Join for revenue code attributes*/
  LEFT JOIN pce_qe16_oper_prd_zoom..cv_revcodemap crev on ch.revenue_code = crev.revenue_code
  /* End Srujan Update - Join for revenue code attributes*/
  
)
--DISTRIBUTE ON (fcy_nm, encntr_num,charge_code);
DISTRIBUTE ON (fcy_nm, encntr_num);




----select 'processing table: intermediate_encntr_cst_fct' as table_processing;
DROP TABLE intermediate_encntr_cst_fct if exists;
CREATE TABLE intermediate_encntr_cst_fct as
select 
 	 fcy_nm as fcy_nm
     	,encntr_num as encntr_num
    	,client_revenue_code_group
     	,sum(quantity) qty
	,sum(rcc_based_direct_cst_amt) drct_cst_amt
	,sum(rcc_based_indirect_cst_amt) indrct_cst_amt
	,sum(rcc_based_total_cst_amt) ttl_cst_amt
from intermediate_stage_chrg_fct_temp
	where strright(fiscal_yr,4)::int>=2016
group by 1,2,3;


-- Table with hash values on key columns

--select 'processing table:  intermediate_stage_chrg_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct as 
SELECT
intermediate_stage_chrg_fct_temp.*
,intermediate_stage_spl_dim.persp_clncl_dtl_pcd_cd_v10
,intermediate_stage_spl_dim.persp_clncl_dtl_pcd_descr_v10
,intermediate_stage_spl_dim.spl_unit_cnvr
,intermediate_stage_spl_dim.persp_clncl_dtl_cd
,intermediate_stage_spl_dim.persp_clncl_dtl_descr
,intermediate_stage_spl_dim.persp_clncl_dtl_unit
,intermediate_stage_spl_dim.persp_clncl_smy_cd
,intermediate_stage_spl_dim.persp_clncl_smy_descr
,intermediate_stage_spl_dim.persp_clncl_std_dept_cd_v10
,intermediate_stage_spl_dim.persp_clncl_std_dept_descr_v10
,intermediate_stage_spl_dim.persp_clncl_std_dept_v10_rollup_cgy_cd
,intermediate_stage_spl_dim.persp_clncl_std_dept_v10_rollup_cgy_descr
,intermediate_stage_spl_dim.persp_clncl_dtl_spl_modfr_cd
,intermediate_stage_spl_dim.persp_clncl_dtl_spl_modfr_descr
,rev_cl_dim.prn_rev_cd
,rev_cl_dim.prn_rev_descr
,rev_cl_dim.rev_descr
,rev_cl_dim.rev_cd_grp_nm
,rev_cl_dim.rev_cd_num_fmt_nm
,rev_cl_dim.rev_cd_shrt_descr
,hcpcs_dim.hcpcs_descr as cpt_descr
,hcpcs_dim.hcpcs_descr
,hcpcs_dim.hcpcs_descr_long
,dept_dim.department_description
,dept_dim.department_group
--CODE Change : 06/19 OR Time Calculation 
,CASE WHEN intermediate_stage_spl_dim.persp_clncl_smy_descr in ('SURGERY TIME','AMBULATORY SURGERY SERVICES') AND 
UPPER(intermediate_stage_spl_dim.persp_clncl_dtl_descr) <> 'OR MINOR FLAT RATE' AND 
(UPPER(intermediate_stage_spl_dim.persp_clncl_dtl_descr) IN ('OR MINOR 1 HR','OR MAJOR 1 HR','ROBOTIC OR TIME 1 HOUR') OR (intermediate_stage_spl_dim.cdm_cd in ('3001458200100','3001458200101','4041502857023','4041502857024','4041502857025','4041502857026')))
THEN 
   ROUND(intermediate_stage_chrg_fct_temp.quantity * intermediate_stage_spl_dim.spl_unit_cnvr * intermediate_stage_spl_dim.persp_clncl_dtl_unit,2) 
   ELSE 
   0  END as calculated_or_hrs
--CODE CHANGE: 08/24/2020 MLH-581 Adding Professional and Facility Charge Indicators
, CASE WHEN intermediate_stage_chrg_fct_temp.subaccount like '12011420%' THEN 1
       WHEN intermediate_stage_chrg_fct_temp.subaccount like '13130720%' THEN 1
       WHEN intermediate_stage_chrg_fct_temp.subaccount like '03142020%' THEN 1
       ELSE 0 END as prfssnl_chrg_ind
, CASE when prfssnl_chrg_ind =0 THEN 1 ELSE 0 END  fcy_chrg_ind
,row_number() over(partition by intermediate_stage_chrg_fct_temp.fcy_nm, intermediate_stage_chrg_fct_temp.encntr_num
Order by  intermediate_stage_chrg_fct_temp.service_date) as rec_num
FROM intermediate_stage_chrg_fct_temp
LEFT JOIN prmretlp.intermediate_stage_spl_dim on intermediate_stage_chrg_fct_temp.charge_code=intermediate_stage_spl_dim.cdm_cd and intermediate_stage_chrg_fct_temp.fcy_num=intermediate_stage_spl_dim.fcy_num
LEFT JOIN pce_qe16_slp_prd_dm..rev_cl_dim on intermediate_stage_chrg_fct_temp.revenue_code = rev_cl_dim.rev_cd 
LEFT JOIN pce_qe16_slp_prd_dm..hcpcs_dim  on intermediate_stage_chrg_fct_temp.cpt_code = hcpcs_dim.hcpcs_cd
LEFT JOIN pce_qe16_slp_prd_dm..dept_dim on  intermediate_stage_chrg_fct_temp.company_id = dept_dim.company_id and intermediate_stage_chrg_fct_temp.dept = dept_dim.department_code

--DISTRIBUTE ON (fcy_nm_hash,encntr_num_hash,charge_code_hash);
DISTRIBUTE ON (fcy_nm,encntr_num);


--Cost Model:  From the Charge Fact, do a sum total of the Indirect Cost for each encounter and add it in the Encounter Analysis Fact. Ditto Direct Cost and Total Cost Amt

--select 'processing table:  intermediate_stage_chrg_cost_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_cost_fct IF EXISTS ; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_cost_fct AS 
(
select 
  fcy_nm
, encntr_num 
, sum(rcc_based_direct_cst_amt)   as  agg_rcc_based_direct_cst_amt
, sum(rcc_based_indirect_cst_amt) as agg_rcc_based_indirect_cst_amt
, sum(rcc_based_total_cst_amt)      as agg_rcc_based_total_cst_amt
, sum(calculated_or_hrs) as agg_calculated_or_hrs
FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct
GROUP BY 1,2
);

--select 'Total in intermediate_stage_chrg_cost_fct' , count(*) from  pce_qe16_slp_prd_dm..intermediate_stage_chrg_cost_fct;
--intermediate_stage_encntr_pract_fct Table  creation based on Net 3 years Of patient Account Number


--select 'processing table:  intermediate_stage_encntr_pract_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_pract_fct IF EXISTS ; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_pract_fct AS 
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
  LEFT JOIN pce_qe16_slp_prd_dm..cv_patprac CH
  on Z.company_id = CH.company_id and Z.patient_id = CH.patient_id
  LEFT JOIN intermediate_stage_temp_physician_npi_spclty SPCL
on SPCL.company_id = CH.company_id and SPCL.practitioner_code = CH.practitioner_code
)
-- DISTRIBUTE ON (fcy_nm_hash, encntr_num_hash);
 DISTRIBUTE ON (fcy_nm, encntr_num);
 
 
--Code Change :  Logic to mark specl_valid_ind for Inpatient (Medical DRG's) 
--select 'processing table:  intermediate_stage_temp_specl_valid_ind' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_specl_valid_ind IF EXISTS ; 
CREATE  TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_specl_valid_ind AS 
select distinct P.fcy_nm, P.encntr_num, 1 as specl_valid_ind
FROM intermediate_stage_encntr_pract_fct P 
INNER JOIN intermediate_stage_temp_eligible_encntr_data A 
on A.company_id = P.fcy_nm and A.patient_id = P.encntr_num and A.inpatient_outpatient_flag = 'I'
INNER JOIN intermediate_stage_temp_physician_npi_spclty S
on S.company_id = P.fcy_nm and S.practitioner_code = P.practitioner_code
INNER JOIN pce_qe16_slp_prd_dm..ms_drg_dim MSDRG
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

--intermediate_stage_cpt_fct tABLE 
--select 'processing table:  intermediate_stage_cpt_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_cpt_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_cpt_fct AS 
SELECT 
 Z.company_id as fcy_nm
,z.patient_id AS encntr_num
,VSET_FCY.alt_cd as fcy_num
,CF.company_id
,CF.patient_id
,CF.cpt_code
,CPT_DIM.hcpcs_descr as cpt_descr
,CPT_DIM.hcpcs_descr_long as cpt_descr_long
,hccs.ccs_hcpcs_cgy_cd
,hccs.ccs_hcpcs_cgy_descr
,hbt.betos_cd
,hbt.betos_descr
,CF.cpt_modifier_1
,CF.cpt_modifier_2
,CF.cpt_modifier_3
,CF.cpt_modifier_4
,CF.procedure_practitioner_code as pcd_pract_cd
,SPCL.npi as pcd_pract_npi
,SPCL.practitioner_name as pcd_pract_nm
,SPCL.practitioner_spclty_description as pcd_pract_splcy_descr
,SPCL.mcare_spcly_cd as pcd_pract_splcy_cd
,CF.cpt4seq
,to_timestamp((cpt_code_date ||' '||nvl(substr(cpt4time,1,2),'00')||':'||nvl(substr(cpt4time,3,2),'00')||':00') ,'MMDDYYYY HH24":"MI":"SS') as cpt_code_ts
,row_number() over(partition by Z.company_id, Z.patient_id
Order by  CF.cpt_code_date) as rec_num
FROM  intermediate_stage_temp_eligible_encntr_data Z
  LEFT JOIN pce_qe16_slp_prd_dm..patcpt_fct CF
  on Z.company_id = CF.company_id and Z.patient_id = CF.patient_id
  LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
  ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN intermediate_stage_temp_physician_npi_spclty SPCL
  on SPCL.company_id = CF.company_id and SPCL.practitioner_code = CF.procedure_practitioner_code
  LEFT JOIN pce_qe16_slp_prd_dm..hcpcs_dim CPT_DIM 
  on CPT_DIM.hcpcs_cd = UPPER(CF.cpt_code) 
  LEFT JOIN pce_qe16_slp_prd_dm..hcpcs_ccs_dim hccs
  ON CPT_DIM.hcpcs_cd = hccs.hcpcs_cd
  LEFT JOIN pce_qe16_slp_prd_dm..hcpcs_betos_dim hbt
  ON CPT_DIM.hcpcs_cd = hbt.hcpcs_cd
 -- LEFT JOIN pce_qe16_slp_prd_dm..cdm_dim CPDIM 
 -- on CPDIM.cpt_code = CF.cpt_code and Z.company_id = CPDIM.company_id 
 --DISTRIBUTE ON (fcy_nm_hash, encntr_num_hash);
 DISTRIBUTE ON (fcy_nm, encntr_num);


--Code Change : 05/10/2019 : Added a new temp table in support of Cancer Patient Identification 
--Code Change : 08/11/2020 : Modified where clause to include the ccs_Dgns_cgy_cd in support MLH-555 Update Cancer Case codes 
--select 'processing table:  intermediate_stage_temp_dgns_ccs_dim_cancer_only' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_dgns_ccs_dim_cancer_only IF EXISTS;
CREATE TEMP TABLE intermediate_stage_temp_dgns_ccs_dim_cancer_only AS 
SELECT distinct dgns_cd, ccs_dgns_cgy_descr 
FROM pce_ae00_aco_prd_cdr..dgns_ccs_dim
--Code Change : 10/01/2020 : Updating the where clause to consider 997 as well as per MLH-591
--WHERE (ccs_dgns_cgy_cd BETWEEN 11 and 47 ) AND eff_to_Dt is NULL;
WHERE (
(CAST(ccs_dgns_cgy_cd as INT) BETWEEN 11 and 47 ) OR 
(CAST(ccs_dgns_cgy_cd as INT) = 58 AND dgns_cd like 'E85%')
 ) AND eff_to_Dt is NULL;

--Code Change : 08/11/2020 : Modified where clause to comment the existing criteria 
--WHERE lower(ccs_dgns_cgy_descr) in 
--(
--'cancer of head and neck',
--'cancer of esophagus',
--'cancer of stomach',
--'cancer of colon',
--'cancer of rectum and anus',
--'cancer of liver and intrahepatic bile duct',
--'cancer of pancreas',
--'cancer of other GI organs; peritoneum',
--'cancer of bronchus; lung',
--'cancer; other respiratory and intrathoracic',
--'cancer of bone and connective tissue',
--'Other non-epithelial cancer of skin',
--'cancer of breast',
--'cancer of uterus',
--'cancer of cervix',
--'cancer of ovary',
--'cancer of other female genital organs',
--'cancer of prostate',
--'cancer of testis',
--'cancer of other male genital organs',
--'cancer of bladder',
--'cancer of kidney and renal pelvis',
--'cancer of other urinary organs',
--'cancer of brain and nervous system',
--'cancer of thyroid',
--'cancer; other and unspecified primary'
--) 


--intermediate_stage_encntr_dgns_fct Table  creation based on Net 3 years Of patient Account Number
--select 'processing table:  intermediate_stage_encntr_dgns_fct' as table_processing;
---------------------------------------------------------------------------------------------------------------------------------------
--CODE CHANGE : MLH-591: 
----Commented the old code-----------------------------------------------------------------------------------------------------------------------------------
--DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct IF EXISTS ;  
--CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct AS 
--
--SELECT 
--	 hash8(Z.company_id) as fcy_nm_hash
--   	,hash8(Z.patient_id) as encntr_num_hash
--   	,Z.company_id as fcy_nm
--   	,z.patient_id AS encntr_num
--       	,VSET_FCY.alt_cd as fcy_num
--       	,DF.company_id
--       	,DF.patient_id
--       	,nvl(DF.icd_code, '-100') as icd_code
--       	,DF.icd_type
--       	,DF.diagnosis_code_present_on_admission_flag
--       	,DF.icd_version
--       	,DF.diagnosisseq
--	,case when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='final' and DF.diagnosisseq=1 then 'Primary'
--	   		  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='final' and DF.diagnosisseq>1 then 'Secondary'
--			  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='discharge' and DF.diagnosisseq=1 then 'Primary'
--			  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='discharge' and DF.diagnosisseq>1 then 'Secondary'
--			  when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq=1 then 'Primary'
--			  when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq>1 then 'Secondary'
--	   		else DF.diagnosistype end as diagnosistype
--	,case when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='PARAGON' then 0
--	   		  when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='3M CODING AND REIMBURSEMENT' then 0
--			  when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='POWERCHART' then 1
--			  ELSE 0 end as sourcesystemdiag_rnk
--	,DF.sourcesystemdiag
--    	,CASE WHEN CANCER.dgns_cd is NOT NULL THEN DF.icd_code ELSE '-100' END as cancer_dgns_cd
--       	,CASE WHEN CANCER.dgns_cd is NOT NULL THEN 1 ELSE NULL END as cancer_case_ind
--	,CANCER.ccs_dgns_cgy_descr as cancer_case_code_descr
--	,row_number() over(partition by Z.company_id, Z.patient_id Order by  DF.diagnosisseq) as rec_num 
--	   -----****
--	,nvl(DGNSD.ccs_dgns_cgy_cd,'-100') AS ccs_dgns_cgy_cd
--	,nvl(DGNSD.ccs_dgns_cgy_descr,'UNKNOWN') AS ccs_dgns_cgy_descr
--	,NVL(DGNSD.ccs_dgns_lvl_1_cd,'-100') AS ccs_dgns_lvl_1_cd
--	,NVL(DGNSD.ccs_dgns_lvl_1_descr,'UNKNOWN') as ccs_dgns_lvl_1_descr
--	,nvl(DGNSD.ccs_dgns_lvl_2_cd,'-100') as ccs_dgns_lvl_2_cd
--	,nvl(DGNSD.ccs_dgns_lvl_2_descr,'UNKNOWN') AS ccs_dgns_lvl_2_descr
--  FROM intermediate_stage_temp_eligible_encntr_data Z 
--  LEFT JOIN pce_qe16_slp_prd_dm..dgns_fct DF on Z.company_id = DF.company_id and Z.patient_id = DF.patient_id
--  LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNSD ON REPLACE(DF.icd_code,'.','')=REPLACE(DGNSD.dgns_cd,'.','') AND DF.icd_version = DGNSD.dgns_icd_ver 
--  LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
--  LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_temp_dgns_ccs_dim_cancer_only CANCER on CANCER.dgns_cd = replace(DF.icd_code, '.','')
--  --DISTRIBUTE ON (fcy_nm_hash, encntr_num_hash); 
--  DISTRIBUTE ON (fcy_nm, encntr_num); 
   
 DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct IF EXISTS ;  
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct AS 
with encntr_dgns_data as (
SELECT 
	 hash8(Z.company_id) as fcy_nm_hash
   	,hash8(Z.patient_id) as encntr_num_hash
   	,Z.company_id as fcy_nm
   	,z.patient_id AS encntr_num
       	,VSET_FCY.alt_cd as fcy_num
       	,DF.company_id
       	,DF.patient_id
       	,nvl(DF.icd_code, '-100') as icd_code
       	,DF.icd_type
       	,DF.diagnosis_code_present_on_admission_flag
       	,DF.icd_version
       	,DF.diagnosisseq
	,case when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='final' and DF.diagnosisseq=1 then 'Primary'
	   		  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='final' and DF.diagnosisseq>1 then 'Secondary'
			  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='discharge' and DF.diagnosisseq=1 then 'Primary'
			  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='discharge' and DF.diagnosisseq>1 then 'Secondary'
			  when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq=1 then 'Primary'
			  when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq>1 then 'Secondary'
	   		else DF.diagnosistype end as zdiagnosistype
	,case when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='PARAGON' then 0
	   		  when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='3M CODING AND REIMBURSEMENT' then 0
			  when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='POWERCHART' then 1
			  ELSE 0 end as sourcesystemdiag_rnk
	,DF.sourcesystemdiag
	--CODE CHANGE: MLH-591 : Added two new indicators irrespective of diagnosistype 
	,case when REPLACE(DF.icd_code,'.','') in ('Z5111','Z510','Z5112') THEN 1 ELSE 0 END as non_cancer_case_dgns_ind 
	,case when non_cancer_case_dgns_ind =0 AND CANCER.dgns_cd is NOT NULL THEN 1 ELSE 0 END cancer_case_dgns_ind 
	,case when zdiagnosistype ='Primary' 
	--and DF.diagnosisseq =1 
	and  non_cancer_case_dgns_ind=1 then non_cancer_case_dgns_ind else 0 end  as prim_dgns_non_cancer_case_ind
	,case when zdiagnosistype ='Secondary' 
	--and DF.diagnosisseq =2 
	and cancer_case_dgns_ind = 1 then cancer_case_dgns_ind else 0 end sec_dgns_cancer_case_ind
	,case when zdiagnosistype ='Primary' 
	--and DF.diagnosisseq =1
	and cancer_case_dgns_ind = 1 then cancer_case_dgns_ind else 0 end prim_dgns_cancer_case_ind
    ,CASE WHEN cancer_case_dgns_ind =1 THEN DF.icd_code ELSE '-100' END as cancer_dgns_cd
--       	,CASE WHEN CANCER.dgns_cd is NOT NULL THEN 1 ELSE NULL END as cancer_case_ind
	,nvl(CANCER.ccs_dgns_cgy_descr,'UNKNOWN') as cancer_case_code_descr
	,row_number() over(partition by Z.company_id, Z.patient_id Order by  DF.diagnosisseq) as rec_num 
	   -----****
	,nvl(DGNSD.ccs_dgns_cgy_cd,'-100') AS ccs_dgns_cgy_cd
	,nvl(DGNSD.ccs_dgns_cgy_descr,'UNKNOWN') AS ccs_dgns_cgy_descr
	,NVL(DGNSD.ccs_dgns_lvl_1_cd,'-100') AS ccs_dgns_lvl_1_cd
	,NVL(DGNSD.ccs_dgns_lvl_1_descr,'UNKNOWN') as ccs_dgns_lvl_1_descr
	,nvl(DGNSD.ccs_dgns_lvl_2_cd,'-100') as ccs_dgns_lvl_2_cd
	,nvl(DGNSD.ccs_dgns_lvl_2_descr,'UNKNOWN') AS ccs_dgns_lvl_2_descr
  FROM intermediate_stage_temp_eligible_encntr_data Z 
  LEFT JOIN pce_qe16_slp_prd_dm..dgns_fct DF on Z.company_id = DF.company_id and Z.patient_id = DF.patient_id
  LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNSD ON REPLACE(DF.icd_code,'.','')=REPLACE(DGNSD.dgns_cd,'.','') AND DF.icd_version = DGNSD.dgns_icd_ver 
  LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_temp_dgns_ccs_dim_cancer_only CANCER on replace(CANCER.dgns_cd,'.','') = replace(DF.icd_code, '.','')
  )
SELECT fcy_nm_hash
       , encntr_num_hash
       , fcy_nm
       , encntr_num
       , fcy_num
       , company_id
       , patient_id
       , icd_code
       , icd_type
       , diagnosis_code_present_on_admission_flag
       , icd_version
       , diagnosisseq
       , zdiagnosistype as diagnosistype
       , sourcesystemdiag_rnk
       , sourcesystemdiag
       , non_cancer_case_dgns_ind
       , cancer_case_dgns_ind
       , prim_dgns_non_cancer_case_ind
       , sec_dgns_cancer_case_ind
       , prim_dgns_cancer_case_ind
       , cancer_dgns_cd
       , cancer_case_code_descr
       , rec_num
       , ccs_dgns_cgy_cd
       , ccs_dgns_cgy_descr
       , ccs_dgns_lvl_1_cd
       , ccs_dgns_lvl_1_descr
       , ccs_dgns_lvl_2_cd
       , ccs_dgns_lvl_2_descr
  FROM encntr_dgns_data
    DISTRIBUTE ON (fcy_nm, encntr_num);   
 
 
   
-----intermediate_stage_encntr_pcd_fct Table  creation based on Net 3 years Of patient Account Number
--select 'processing table:  intermediate_stage_encntr_pcd_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct IF EXISTS ; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct AS 
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
	   ,nvl(pcdd.icd_pcd_ccs_cgy_descr,'UNKNOWN') AS icd_pcd_ccs_cgy_descr
	   ,NVL(PCDD.icd_pcd_ccs_lvl_1_cd,'-100') AS icd_pcd_ccs_lvl_1_cd
	   ,NVL(PCDD.icd_pcd_ccs_lvl_1_descr,'UNKNOWN') AS icd_pcd_ccs_lvl_1_descr
	   ,NVL(PCDD.icd_pcd_ccs_lvl_2_cd,'-100') AS icd_pcd_ccs_lvl_2_cd
	   ,NVL(PCDD.icd_pcd_ccs_lvl_2_descr,'UNKNOWN') AS icd_pcd_ccs_lvl_2_descr
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
  LEFT JOIN pce_qe16_slp_prd_dm..pcd_fct DF
  on Z.company_id = DF.company_id and Z.patient_id = DF.patient_id
  
  -----------Srujan Update Start---------------------------
  /*Start Join for PCD CCS Attributes*/
  LEFT JOIN pce_qe16_slp_prd_dm..pcd_dim pcdd on replace(df.icd_code,'.','') = replace(pcdd.icd_pcd_cd,'.','') and df.icd_version = pcdd.icd_ver
  /*End Join for PCD CCS Attributes*/
   ---Srujan Update End----------------
   
   
  LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
  ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN intermediate_stage_temp_physician_npi_spclty SURGEON
  on SURGEON.company_id = DF.company_id and SURGEON.practitioner_code = DF.surgeon_code
    LEFT JOIN intermediate_stage_temp_physician_npi_spclty ORDERING
  on ORDERING.company_id = DF.company_id and ORDERING.practitioner_code = DF.orderingphysician
  -- DISTRIBUTE ON (fcy_nm_hash, encntr_num_hash);
  DISTRIBUTE ON (fcy_nm, encntr_num);


--#################################################################--
--                Fix for Service Line                             --
--#################################################################-- 

--select 'processing table:  temp_eligible_svc_ln_anl_fct' as table_processing;
DROP TABLE temp_eligible_svc_ln_anl_fct IF EXISTS;
CREATE TEMP TABLE temp_eligible_svc_ln_anl_fct AS
(
-- CPT HCPCS ---
SELECT distinct p.company_id, p.patient_id,p.cpt_code as code ,'CPT' as criteria,
rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln , rnk.services as svc_nm, rnk.cd, rnk.cd_type,rnk.descr as cd_descr,
rnk.svc_cgy_rnk, rnk.svc_ln_rnk, rnk.sub_svc_ln_rnk, rnk.svc_rnk, tempe.inpatient_outpatient_flag
FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct p
INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on p.company_id = tempe.company_id and p.patient_id = tempe.patient_id
INNER JOIN pce_qe16_slp_prd_dm..svc_hier_dim rnk
on p.cpt_code = rnk.cd and rnk.cd_type in ('HCPCS','CPT') and lower(rnk.svc_cgy) in ('surgical','medical')  

UNION

SELECT distinct p.company_id, p.patient_id,p.cpt_code as code,'CPT' as criteria,
rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln , rnk.services as svc_nm, rnk.cd, rnk.cd_type,rnk.descr as cd_descr,
rnk.svc_cgy_rnk, rnk.svc_ln_rnk, rnk.sub_svc_ln_rnk, rnk.svc_rnk, tempe.inpatient_outpatient_flag
FROM pce_qe16_slp_prd_dm..intermediate_stage_cpt_fct p
INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on p.company_id = tempe.company_id and p.patient_id = tempe.patient_id
INNER JOIN pce_qe16_slp_prd_dm..svc_hier_dim rnk
on p.cpt_code = rnk.cd and rnk.cd_type in ('HCPCS','CPT') and lower(rnk.svc_cgy) in ('surgical','medical')  

UNION 

-- PCD ICD 10 ICD 9 --
SELECT distinct pf.company_id, pf.patient_id,pf.icd_code as code,'ICD 10/9 PCS' as criteria,
rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln , rnk.services as svc_nm, rnk.cd, rnk.cd_type,rnk.descr as cd_descr,
rnk.svc_cgy_rnk, rnk.svc_ln_rnk, rnk.sub_svc_ln_rnk, rnk.svc_rnk, tempe.inpatient_outpatient_flag
FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct pf
INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on pf.company_id = tempe.company_id and pf.patient_id = tempe.patient_id
INNER JOIN pce_qe16_slp_prd_dm..svc_hier_dim rnk
on pf.icd_code = rnk.cd and rnk.cd_type in ('ICD 10 PCS','ICD 9 PCS') and lower(rnk.svc_cgy) in ('surgical','medical') 
WHERE pf.icd_type = 'P'

UNION

--ICD DGNS --
SELECT distinct df.company_id, df.patient_id,df.icd_code as code,'ICD 10 DGNS' as criteria,
rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln , rnk.services as svc_nm, rnk.cd, rnk.cd_type,rnk.descr as cd_descr,
rnk.svc_cgy_rnk, rnk.svc_ln_rnk, rnk.sub_svc_ln_rnk, rnk.svc_rnk, tempe.inpatient_outpatient_flag
FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct df
INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on df.company_id = tempe.company_id and df.patient_id = tempe.patient_id
INNER JOIN pce_qe16_slp_prd_dm..svc_hier_dim rnk 
on df.icd_code = rnk.cd and lower(rnk.svc_cgy) in ('surgical','medical') and rnk.cd_type in ('ICD 10 DGNS')

--MS DRG -----
UNION

SELECT distinct 
patd.company_id, patd.patient_id, patd.msdrg_code as code,'MSDRG' as criteria,
rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln,rnk.services as svc_nm,rnk.cd,rnk.cd_type,rnk.descr as cd_descr,
rnk.svc_cgy_rnk,rnk.svc_ln_rnk,rnk.sub_svc_ln_rnk,rnk.svc_rnk, tempe.inpatient_outpatient_flag
FROM pce_qe16_oper_prd_zoom..cv_patdisch patd
INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on patd.company_id = tempe.company_id and patd.patient_id = tempe.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..svc_hier_dim rnk
on patd.msdrg_code = rnk.cd and rnk.cd_type in ('MS-DRG') and lower(rnk.svc_cgy) in ('surgical','medical')
where patd.inpatient_outpatient_flag = 'I ');

----********************************************
--select 'processing table:  temp_svc_ln_anl_fct' as table_processing;
DROP TABLE temp_svc_ln_anl_fct IF EXISTS;
CREATE TEMP TABLE temp_svc_ln_anl_fct AS
SELECT sv.* , patd.msdrg_code, 
cg.mclaren_major_slp_grouping,
row_number() over(partition by sv.company_id, sv.patient_id 
Order by sv.svc_ln_rnk,sv.sub_svc_ln_rnk,sv.svc_rnk) as org_rec_num,
case when ((cg.mclaren_major_slp_grouping = sv.svc_ln) and sv.criteria = 'MS-DRG') then 999
when cg.mclaren_major_slp_grouping = sv.svc_ln then 99 else org_rec_num end as temp_rec_num

FROM temp_eligible_svc_ln_anl_fct sv
INNER JOIN pce_qe16_oper_prd_zoom..cv_patdisch patd on sv.patient_id = patd.patient_id and sv.company_id = patd.company_id
LEFT JOIN pce_qe16_oper_prd_zoom..cv_drgmap cg on patd.msdrg_code = cg.ms_drg_code
ORDER BY svc_cgy_rnk, svc_ln_rnk, sub_svc_ln_rnk, svc_rnk;

---************************
--select 'processing table:  temp_rnk_svc_ln_anl_fct' as table_processing;
DROP TABLE temp_rnk_svc_ln_anl_fct IF EXISTS;
CREATE TEMP TABLE temp_rnk_svc_ln_anl_fct AS
SELECT  
        tsv.company_id 
       ,tsv.patient_id
       ,tsv.inpatient_outpatient_flag
       ,tsv.code
       ,tsv.criteria as based_on
       ,tsv.svc_cgy
       ,tsv.svc_ln
       ,tsv.mclaren_major_slp_grouping
       ,tsv.sub_svc_ln
       ,tsv.svc_nm
       ,tsv.cd
       ,tsv.cd_type
       ,tsv.cd_descr
       ,tsv.svc_cgy_rnk
       ,tsv.svc_ln_rnk
       ,tsv.sub_svc_ln_rnk
       ,tsv.svc_rnk
       ,tsv.msdrg_code
       ,row_number() over(partition by tsv.company_id, tsv.patient_id 
		Order by tsv.temp_rec_num desc, tsv.svc_ln_rnk, tsv.sub_svc_ln_rnk, tsv.svc_rnk) as ip_rec_num
	   ,tsv.org_rec_num as op_rec_num
	   ,case when tsv.inpatient_outpatient_flag = 'I' then ip_rec_num else op_rec_num end as rec_num
FROM temp_svc_ln_anl_fct tsv;

---- svc_ln_anl_fct *************************************************************************************************
--select 'processing table:  intermediate_stage_svc_ln_anl_fct' as table_processing;
DROP TABLE intermediate_stage_svc_ln_anl_fct IF EXISTS;
CREATE TABLE intermediate_stage_svc_ln_anl_fct AS
SELECT  
        te.company_id as fcy_nm
       ,te.patient_id as encntr_num
       ,sv.company_id 
       ,sv.patient_id
       ,sv.inpatient_outpatient_flag
       ,sv.code
       ,sv.based_on
       ,sv.svc_cgy
       ,sv.svc_ln
       ,sv.mclaren_major_slp_grouping
       ,sv.sub_svc_ln
       ,sv.svc_nm
       ,sv.cd
       ,sv.cd_type
       ,sv.cd_descr
       ,sv.rec_num
       ,sv.svc_cgy_rnk
       ,sv.svc_ln_rnk
       ,sv.sub_svc_ln_rnk
       ,sv.svc_rnk
       ,sv.msdrg_code
       ,te.admission_ts as adm_dt
       ,te.discharge_ts as dschrg_dt
       ,now() rcrd_insrt_dt
       
FROM intermediate_stage_temp_eligible_encntrs te
LEFT JOIN temp_rnk_svc_ln_anl_fct sv on te.company_id = sv.company_id and te.patient_id = sv.patient_id;

--select 'processing table:  intermediate_stage_temp_encntr_svc_hier' as table_processing;
DROP TABLE intermediate_stage_temp_encntr_svc_hier IF EXISTS;
CREATE TEMP TABLE intermediate_stage_temp_encntr_svc_hier AS 
select * from pce_qe16_slp_prd_dm..intermediate_stage_svc_ln_anl_fct where rec_num=1;

--###############################################################################--
--                     End of Service Line Ranking                               --
--###############################################################################--

--select 'processing table:  intermediate_stage_temp_fips_adr_dim' as table_processing;
DROP TABLE intermediate_stage_temp_fips_adr_dim IF EXISTS; 
CREATE TEMP TABLE intermediate_stage_temp_fips_adr_dim as 
(
   select 
Q.ptnt_zip_cd as fips_zip_cd,
F.fips_cnty_descr,
Q.ste_descr as ptnt_fips_ste_descr
from pce_qe16_slp_prd_dm..stnd_ptnt_zip_dim Q

LEFT JOIN pce_qe16_slp_prd_dm..fips_adr_dim F 
on F.fips_cnty_cd = Q.cnty_fips_nm and Q.ste_cd = F.fips_ste_descr
);

--select 'processing table: intermediate_stage_temp_ptnt_type_fcy_std_cd ' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_type_fcy_std_cd

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_ptnt_type_fcy_std_cd AS (
		SELECT PATTYPE.company_id
		,PATTYPE.patient_type_code
		,PATTYPE.patient_type_description
		,MAP.standard_patient_type_code
		,STD.std_encntr_type_descr FROM pce_qe16_slp_prd_dm..pattype_dim PATTYPE LEFT JOIN pce_qe16_slp_prd_dm..pattype_map_dim MAP ON MAP.patient_type_code = PATTYPE.patient_type_code
		AND MAP.company_id = PATTYPE.company_id LEFT JOIN pce_qe16_slp_prd_dm..stnd_ptnt_tp_dim STD ON STD.std_encntr_type_cd = MAP.standard_patient_type_code
		);

--select 'processing table:  intermediate_stage_temp_ptnt_adm_dgns' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_adm_dgns IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_ptnt_adm_dgns AS (
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
		FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id AND ENCNTR.patient_id = X.patient_id
		WHERE lower(diagnosistype) = 'admitting'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = '10'
		AND Z.icd_version = 'ICD10' LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 1
		);

--select 'processing table:  intermediate_stage_temp_ptnt_prim_dgns' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_prim_dgns IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_ptnt_prim_dgns AS (
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
		FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(diagnosistype) = 'primary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = 'ICD10'
		AND Z.icd_version = 'ICD10' LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 1
		);

--select 'processing table:  intermediate_stage_temp_ptnt_second_dgns' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_second_dgns IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_ptnt_second_dgns AS (
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
		FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(diagnosistype) = 'secondary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = 'ICD10'
		AND Z.icd_version = 'ICD10' LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 1
		);

SELECT count(*)
FROM intermediate_stage_temp_ptnt_second_dgns;--4,432,358

--select 'processing table: intermediate_stage_temp_ptnt_trty_dgns ' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_trty_dgns IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_ptnt_trty_dgns AS (
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
		FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct X
		WHERE lower(diagnosistype) = 'secondary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = 'ICD10'
		AND Z.icd_version = 'ICD10' LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 2
		);--5,781,165

--select 'processing table: intermediate_stage_temp_ptnt_prim_proc ' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_prim_proc

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_ptnt_prim_proc AS (
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
		FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(proceduretype) = 'primary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..pcd_dim PCDDIM ON PCDDIM.icd_pcd_cd = Z.icd_code WHERE Z.row_num = 1
		);

SELECT count(*)
FROM intermediate_stage_temp_ptnt_prim_proc;--332,451

--select 'processing table:  intermediate_stage_temp_ptnt_scdy_proc' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_scdy_proc

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_ptnt_scdy_proc AS (
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
		FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(proceduretype) = 'secondary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..pcd_dim PCDDIM ON PCDDIM.icd_pcd_cd = Z.icd_code WHERE Z.row_num = 1
		);

SELECT count(*)
FROM intermediate_stage_temp_ptnt_scdy_proc;--145,743

--select 'processing table:  intermediate_stage_temp_ptnt_trty_proc' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_trty_proc

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_ptnt_trty_proc AS (
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
		FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct X
		INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(proceduretype) = 'secondary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..pcd_dim PCDDIM ON PCDDIM.icd_pcd_cd = Z.icd_code WHERE Z.row_num = 2
		);--84961

--select 'processing table:  intermediate_stage_temp_obsrv' as table_processing;
DROP TABLE intermediate_stage_temp_obsrv

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_obsrv AS (
		SELECT pb.patient_id
		,pb.company_id
		,sum(pb.quantity) AS qty FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct pb WHERE pb.revenue_code = '0762' GROUP BY pb.patient_id
		,pb.company_id
		);--215561

--Code Change : Modified the existing logic (Rev Code) based on SPL Dimension

--select 'processing table:  intermediate_stage_temp_icu' as table_processing;
DROP TABLE intermediate_stage_temp_icu

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_icu AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS icu_days
		FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct ZOOM 
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_spl_dim SP 
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
	CREATE TEMP TABLE intermediate_stage_temp_ccu AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ccu_days
		FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct ZOOM 
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_spl_dim SP 
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --CCU 
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B ICU' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B CICU/CCU (CORONARY CARE)'))
			) GROUP BY 1,2
		);

--select 'processing table:  intermediate_stage_temp_nrs' as table_processing;
DROP TABLE intermediate_stage_temp_nrs

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_nrs AS (
		SELECT patient_id
		,company_id
		,count(DISTINCT pb.service_date) AS nrs_days FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct pb WHERE pb.revenue_code BETWEEN '0170'
			AND '0179' GROUP BY patient_id
		,company_id
		);--32134

--select 'processing table: intermediate_stage_temp_rtne ' as table_processing;
DROP TABLE intermediate_stage_temp_rtne

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_rtne AS (
		SELECT patient_id
		,company_id
		,count(DISTINCT pb.service_date) AS rtne_days FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct pb WHERE (
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
	CREATE TEMP TABLE intermediate_stage_temp_ed_case AS (
		SELECT DISTINCT patient_id
		,company_id FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct PB INNER JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_ED_CPT ON VSET_ED_CPT.cd = PB.cpt_code
		AND VSET_ED_CPT.cohrt_nm = 'ED_VISIT'
		);--2136885

--select 'processing table: intermediate_stage_temp_dschrg_inpatient_ltcsnf ' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient_ltcsnf

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_dschrg_inpatient_ltcsnf AS (
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
	CREATE TEMP TABLE intermediate_stage_temp_dschrg_inpatient_nbrn AS (
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
				)
			OR lower(admitservice) IN (
				'nbn'
				,'oin'
				,'scn'
				,'l1n'
				,'bbn'
				,'nb'
				,'newborn'
				)
			) GROUP BY 1
		,2
		);--16347

--select 'processing table: intermediate_stage_temp_dschrg_inpatient_rehab ' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient_rehab

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_dschrg_inpatient_rehab AS (
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
	CREATE TEMP TABLE intermediate_stage_temp_dschrg_inpatient_psych AS (
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
	CREATE TEMP TABLE intermediate_stage_temp_dschrg_inpatient_spclcare AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_spclcare_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I'
		AND (
			lower(Z.primary_payer_code) in ('select','selec')
			OR lower(Z.patient_type) = lower('bsch')
	
			) GROUP BY 1
		,2
		);--1440 

--CODE CHANGE : Discharge - Hospice Old Logic 
----select 'processing table: intermediate_stage_temp_dschrg_inpatient_hospice ' as table_processing;
--DROP TABLE intermediate_stage_temp_dschrg_inpatient_hospice;
--
--IF EXISTS;
--	CREATE TEMP TABLE intermediate_stage_temp_dschrg_inpatient_hospice AS (
--		SELECT DISTINCT Z.patient_id
--		,Z.company_id
--		,1 AS dschrg_hospice_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
--		AND Z.patient_id = ENCNTR.patient_id INNER JOIN pce_qe16_prd_qadv..val_set_dim VSET_HOSPICE ON VSET_HOSPICE.cohrt_nm = 'Sepsis Mortality'
--		AND Z.primary_payer_code = VSET_HOSPICE.cd WHERE Z.discharge_date IS NOT NULL
--		AND Z.inpatient_outpatient_flag = 'I'
--		AND Z.discharge_total_charges > 0 GROUP BY 1
--		,2
--		);--4577


--Code Change : Discharge - Hospice New Logic 

--select 'processing table: intermediate_stage_temp_payer_fcy_std_code ' as table_processing;
DROP TABLE intermediate_stage_temp_payer_fcy_std_code

IF EXISTS;
--	CREATE TEMP TABLE intermediate_stage_temp_payer_fcy_std_code AS (
--		SELECT PT.company_id
--		,PT.payer_code
--		,PT.payer_description
--		,PT.payer_code AS fcy_payer_code
--		,PT.payer_description AS fcy_payer_description
--		,PM.standard_payer_code AS std_payer_code
--		,QAPM.std_pyr_descr AS std_payer_descr
--		,PT.payor_group1
--		,PT.payor_group2
--		,PT.payor_group3 FROM pce_qe16_slp_prd_dm..paymap_dim PM INNER JOIN pce_qe16_slp_prd_dm..paymstr_dim PT ON PM.company_id = PT.company_id
--		AND PM.payer_code = PT.payer_code INNER JOIN pce_qe16_slp_prd_dm..stnd_fcy_pyr_dim QAPM ON QAPM.std_pyr_cd = PM.standard_payer_code
--		);--16559

	CREATE TEMP TABLE intermediate_stage_temp_payer_fcy_std_code AS (
	with qadv_data AS (
	select distinct std_pyr_cd, std_pyr_descr from  pce_qe16_slp_prd_dm..stnd_fcy_pyr_dim )
		SELECT PMSTR.company_id
		,PMSTR.payer_code
		,PMSTR.payer_description
		,PMSTR.payer_code AS fcy_payer_code
		,PMSTR.payer_description AS fcy_payer_description
		,PMAP.standard_payer_code AS std_payer_code
		,QAPYR.std_pyr_descr AS std_payer_descr
		,PMSTR.payor_group1
		,PMSTR.payor_group2
		,PMSTR.payor_group3 
	     from pce_qe16_slp_prd_dm..paymstr_dim PMSTR
	        INNER JOIN pce_qe16_slp_prd_dm..paymap_dim PMAP
	on PMSTR.payer_code = PMAP.payer_code and PMSTR.company_id = PMAP.company_id
	 INNER JOIN qadv_data QAPYR
     on QAPYR.std_pyr_cd = PMAP.standard_payer_code 
		);--16559

--select 'processing table: intermediate_stage_temp_dschrg_inpatient_hospice ' as table_processing;
DROP TABLE intermediate_stage_temp_dschrg_inpatient_hospice

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_dschrg_inpatient_hospice AS (
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
	CREATE TEMP TABLE intermediate_stage_temp_dschrg_inpatient_lipmip AS (
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
	CREATE TEMP TABLE intermediate_stage_temp_dschrg_inpatient_acute AS (
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
	CREATE TEMP TABLE intermediate_stage_temp_dschrg_inpatient AS (
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
	CREATE TEMP TABLE intermediate_stage_temp_derived_ptnt_days AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN (sum(ZOOM.quantity) =0) THEN NULL ELSE sum(ZOOM.quantity) END  AS ptnt_days 
		FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct ZOOM INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id WHERE (
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

--code change : Added logic to calculate Endoscopy Cases based on Rev Code 0750

--select 'processing table: intermediate_stage_temp_endoscopy_case ' as table_processing;
DROP TABLE intermediate_stage_temp_endoscopy_case IF EXISTS;
CREATE TEMP TABLE intermediate_stage_temp_endoscopy_case AS (
SELECT DISTINCT patient_id,company_id 
 FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct spl
WHERE spl.persp_clncl_smy_descr = 'ENDOSCOPIC PROCEDURES');

----- Code Change above as requested 04012020---			
--		SELECT DISTINCT patient_id,company_id 
--		FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct 
--	WHERE (revenue_code IN ('0750') OR raw_chargcode IN ('64000001','64000002','64000003','64000004','64000005')) GROUP BY patient_id,company_id)

--code change : Added logic to calculate Surgercy Cases based on SPL Dimension

--select 'processing table: intermediate_stage_temp_srgl_case  ' as table_processing;
DROP TABLE intermediate_stage_temp_srgl_case

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_srgl_case AS (
		SELECT DISTINCT patient_id
		,company_id FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct CF 
		INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_spl_dim SP
		on CF.charge_code = SP.cdm_cd and CF.fcy_num = SP.fcy_num
		WHERE SP.persp_clncl_smy_descr in ('SURGERY TIME', 'AMBULATORY SURGERY SERVICES') 
		GROUP BY patient_id
		,company_id
		);

--code change : Added logic to calculate  Lithotripsy  Cases based on SPL Dimension
   --select 'processing table: intermediate_stage_temp_lithotripsy_case ' as table_processing;
DROP TABLE intermediate_stage_temp_lithotripsy_case IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_lithotripsy_case AS (
		SELECT DISTINCT patient_id
		,company_id FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct CF 
		INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_spl_dim SP
		on CF.charge_code = SP.cdm_cd and CF.fcy_num = SP.fcy_num
		WHERE UPPER(SP.persp_clncl_dtl_descr) in ('PF LITHOLAPAXY COMPLICATED > 2.5 CM','LITHOTRIPSY KIDNEY','PERC NEPHROLITHOTOMY W/WO DILATION <2 CM') 
		GROUP BY patient_id
		,company_id
		);

 --code change : Added logic to calculate  CathLab Cases based on intermediate_stage_svc_ln_anl_fct
  
  	CREATE TEMP TABLE temp_cathlab_case	AS
(	SELECT DISTINCT patient_id, company_id 
	FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct cf
	WHERE cf.cpt_code in ('93451','93452','93453','93454','93455','93456','93457','93458','93459','93460','93461','93462','93501','93508','93510',
                            '93511','93514','93524','93526','93527','93528','93529','93530','93531','93532','93533','93542','93543','93544','93545',
                            '93555','93556','93561','93562','93566','93567','93568')
UNION
	SELECT DISTINCT patient_id, company_id 
	FROM pce_qe16_slp_prd_dm..intermediate_stage_cpt_fct cpf
	WHERE cpf.cpt_code in ('93451','93452','93453','93454','93455','93456','93457','93458','93459','93460','93461','93462','93501','93508','93510',
                            '93511','93514','93524','93526','93527','93528','93529','93530','93531','93532','93533','93542','93543','93544','93545',
                            '93555','93556','93561','93562','93566','93567','93568'));	
	
	--select 'processing table: intermediate_stage_temp_cathlab_case ' as table_processing;
DROP TABLE intermediate_stage_temp_cathlab_case IF EXISTS;
	
	CREATE TEMP TABLE intermediate_stage_temp_cathlab_case AS
	(
	  SELECT DISTINCT patient_id, company_id 
	  FROM temp_cathlab_case);  

--Code Change : Added logic to calculate patient_days_StepDown based on SPL Dimension
--select 'processing table: intermediate_stage_temp_derived_ptnt_days_stepdown ' as table_processing;
DROP TABLE intermediate_stage_temp_derived_ptnt_days_stepdown

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_derived_ptnt_days_stepdown AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum( ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_stepdown 
		FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct ZOOM 
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_spl_dim SP 
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
CREATE TEMP TABLE intermediate_stage_temp_derived_ptnt_days_rtne AS (
                SELECT ZOOM.patient_id
                ,ZOOM.company_id
                ,CASE WHEN(sum( ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_rtne
                FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct ZOOM
                 INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_spl_dim SP
                 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
                 WHERE (
                 --Telemetry
                   (SP.persp_clncl_smy_cd='110109')
                        ) GROUP BY 1,2
                );

--select 'processing table: intermediate_stage_temp_derived_ptnt_days_nbrn ' as table_processing;
DROP TABLE intermediate_stage_temp_derived_ptnt_days_nbrn

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_derived_ptnt_days_nbrn AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_nbrn
		FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct ZOOM 
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_spl_dim SP 
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
	CREATE TEMP TABLE  intermediate_stage_temp_derived_ptnt_days_rnb_only AS (
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
	CREATE TEMP TABLE intermediate_stage_temp_derived_ptnt_days_psych AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_psych
		FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct ZOOM 
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_spl_dim SP 
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
	CREATE TEMP TABLE intermediate_stage_temp_derived_ptnt_days_rehab AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_rehab
		FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct ZOOM 
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_spl_dim SP 
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
	CREATE TEMP TABLE intermediate_stage_temp_derived_ptnt_days_acute AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_acute
		FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct ZOOM 
--		 INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_spl_dim SP 
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
--CREATE TEMP TABLE intermediate_stage_temp_derived_ptnt_days_acute AS (
--    SELECT ZOOM.patient_id
--		,ZOOM.company_id
--		,count(DISTINCT ZOOM.service_date) AS ptnt_days_acute 
--		 FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct ZOOM 
--		INNER JOIN intermediate_stage_temp_dschrg_inpatient ON intermediate_stage_temp_dschrg_inpatient.patient_id = ZOOM.patient_id WHERE (
--			--NOT Rehab / Psych /NewBorn / Hospice/LTC/SNF
--			revenue_code NOT IN ('0170','0171','0172''0173','0174','0175','0179','0114','0124','0134','0144','0154','0204','0118','0128','0138','0148','0158',
--			'0650','0651','0652','0653','0654','0655','0656','0657','0659') 
--			) GROUP BY 1 ,2
--);



		
--New
--select 'processing table:  intermediate_stage_encntr_cnslt_pract_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_cnslt_pract_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_cnslt_pract_fct as 
with cnslt_pract_1 as 
(
select C1.company_id, C1.patient_id, 
C1.practitioner_code as cnslt_pract_1_cd,
SPCL.npi as cnslt_pract_1_npi,
SPCL.practitioner_name as cnslt_pract_1_nm,
SPCL.practitioner_spclty_description as cnslt_pract_1_spclty,
SPCL.mcare_spcly_cd as cnslt_pract_1_mcare_spcly_cd
FROM intermediate_stage_temp_eligible_encntr_data Z
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_pract_fct C1
on C1.company_id = Z.company_id and Z.patient_id = C1.patient_id
LEFT JOIN intermediate_stage_temp_physician_npi_spclty SPCL
on SPCL.company_id = C1.company_id and SPCL.practitioner_code = C1.practitioner_code
WHERE lower(C1.raw_role) = 'consulting 1'),
cnslt_pract_2 as 
(
select C1.company_id, C1.patient_id, C1.practitioner_code as cnslt_pract_2_cd,
SPCL.npi as cnslt_pract_2_npi,
SPCL.practitioner_name as cnslt_pract_2_nm,
SPCL.practitioner_spclty_description as cnslt_pract_2_spclty,
SPCL.mcare_spcly_cd as cnslt_pract_2_mcare_spcly_cd
FROM intermediate_stage_temp_eligible_encntr_data Z
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_pract_fct C1
on C1.company_id = Z.company_id and Z.patient_id = C1.patient_id
INNER JOIN pce_qe16_slp_prd_dm..phys_dim P
on P.practitioner_code = C1.practitioner_code and C1.company_id = P.company_id
LEFT JOIN intermediate_stage_temp_physician_npi_spclty SPCL
on SPCL.company_id = C1.company_id and SPCL.practitioner_code = C1.practitioner_code
WHERE lower(C1.raw_role) = 'consulting 2'),
cnslt_pract_3 as 
(
select C1.company_id, C1.patient_id, C1.practitioner_code as cnslt_pract_3_cd,
SPCL.npi as cnslt_pract_3_npi,
SPCL.practitioner_name as cnslt_pract_3_nm,
SPCL.practitioner_spclty_description as cnslt_pract_3_spclty,
SPCL.mcare_spcly_cd as cnslt_pract_3_mcare_spcly_cd
FROM intermediate_stage_temp_eligible_encntr_data Z
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_pract_fct C1
on C1.company_id = Z.company_id and Z.patient_id = C1.patient_id
INNER JOIN pce_qe16_slp_prd_dm..phys_dim P
on P.practitioner_code = C1.practitioner_code and C1.company_id = P.company_id
LEFT JOIN intermediate_stage_temp_physician_npi_spclty SPCL
on SPCL.company_id = C1.company_id and SPCL.practitioner_code = C1.practitioner_code
WHERE lower(C1.raw_role) = 'consulting 3')
select T1.company_id as fcy_nm, T1.patient_id as encntr_num,
C1.cnslt_pract_1_cd, cnslt_pract_1_nm,  C1.cnslt_pract_1_npi, C1.cnslt_pract_1_spclty, C1.cnslt_pract_1_mcare_spcly_cd, 
C2.cnslt_pract_2_cd, cnslt_pract_2_nm,  C2.cnslt_pract_2_npi, C2.cnslt_pract_2_spclty, C2.cnslt_pract_2_mcare_spcly_cd, 
C3.cnslt_pract_3_cd, cnslt_pract_3_nm,  C3.cnslt_pract_3_npi, C3.cnslt_pract_3_spclty, C3.cnslt_pract_3_mcare_spcly_cd
FROM intermediate_stage_temp_eligible_encntr_data T1
LEFT JOIN cnslt_pract_1 C1
on C1.company_id = T1.company_id and T1.patient_id = C1.patient_id
LEFT JOIN cnslt_pract_2 C2
on C2.company_id = T1.company_id and T1.patient_id = C2.patient_id
LEFT JOIN cnslt_pract_3 C3
on C3.company_id = T1.company_id and T1.patient_id = C3.patient_id;

--select 'processing table: intermediate_stage_temp_surgeon_pract ' as table_processing;
DROP TABLE intermediate_stage_temp_surgeon_pract IF exists;
CREATE TEMP TABLE intermediate_stage_temp_surgeon_pract AS 
(
  select Z.company_id, Z.patient_id, P.surgeon_code as prim_srgn_cd, 
  PHY.npi as prim_srgn_npi,
  PHY.practitioner_name as prim_srgn_nm,
  PHY.practitioner_spclty_description as prim_srgn_spclty,
  PHY.mcare_spcly_cd as prim_srgn_mcare_spcly_cd
  from intermediate_stage_temp_eligible_encntr_data Z
  LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct P 
  on P.company_id = Z.company_id and Z.patient_id = P.patient_id 
  LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_temp_physician_npi_spclty PHY
  on PHY.practitioner_code = P.surgeon_code and P.company_id = PHY.company_id
  WHERE P.proceduretype='Primary' AND surgeon_code is NOT NULL
);


---New

--select 'processing table: intermediate_stage_temp_physician_fcy_std_spclty ' as table_processing;
DROP TABLE intermediate_stage_temp_physician_fcy_std_spclty

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_physician_fcy_std_spclty AS (
		SELECT DISTINCT PT.company_id
		,PT.practitioner_code
		,PT.practitioner_name
		,PT.practitioner_specialty_code AS practitioner_spclty_code
		,PM.standard_practitioner_specialty_code AS standard_practitioner_spclty_code
		,STD.descr AS practitioner_spclty_description FROM pce_qe16_slp_prd_dm..phys_spec_map_dim PM
		INNER JOIN pce_qe16_slp_prd_dm..phys_dim PT 
		ON PM.practitioner_code = PT.practitioner_code
		AND PT.company_id = PM.company_id 
		INNER JOIN pce_qe16_slp_prd_dm..stnd_physcn_spcly_dim STD 
		ON STD.cd = PM.standard_practitioner_specialty_code
		);--154290


--select 'processing table:  intermediate_stage_temp_discharge_fcy_std_status_code' as table_processing;
DROP TABLE intermediate_stage_temp_discharge_fcy_std_status_code

IF EXISTS;
	CREATE TEMP TABLE intermediate_stage_temp_discharge_fcy_std_status_code AS (
		SELECT DISTINCT ZOOM.discharge_status
		,DISSTATUS.dschrg_sts_cd
		,DISSTATUS.dschrg_sts_descr FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ZOOM.company_id = ENCNTR.company_id
		AND ZOOM.patient_id = ENCNTR.patient_id LEFT JOIN pce_qe16_slp_prd_dm..dschrg_sts_dim DISSTATUS ON CAST(DISSTATUS.dschrg_sts_cd AS INT) = CAST(ZOOM.discharge_status AS INT)
		);--37
		
--CODE CHANGE :08/31/2020  MLH-581 
--select 'processing table: intermediate_stage_chrg_agg_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_agg_fct  IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_agg_fct
AS 
select fcy_nm as fcy_nm , encntr_num as encntr_num ,
max(X.prfssnl_chrg_ind) as prfssnl_chrg_ind,
max(X.fcy_chrg_ind) as fcy_chrg_ind,
sum(CASE WHEN prfssnl_chrg_ind =1  THEN X.total_charge
	    else NULL END ) as prfssnl_chrg_amt,
sum(CASE WHEN fcy_chrg_ind =1  THEN X.total_charge
	    else NULL END ) as fcy_chrg_amt,
sum(CASE WHEN prfssnl_chrg_ind =1  THEN X.rcc_based_direct_cst_amt
	    else 0 END ) as prfssnl_direct_cst_amt,
sum(CASE WHEN fcy_chrg_ind =1  THEN X.rcc_based_direct_cst_amt
	    else 0 END ) as fcy_direct_cst_amt,
sum(CASE WHEN prfssnl_chrg_ind =1  THEN X.rcc_based_indirect_cst_amt
	    else 0 END ) as prfssnl_indirect_cst_amt,
sum(CASE WHEN fcy_chrg_ind =1  THEN X.rcc_based_indirect_cst_amt
	    else 0 END ) as fcy_indirect_cst_amt,
sum(CASE WHEN prfssnl_chrg_ind =1  THEN X.rcc_based_total_cst_amt
	    else 0 END ) as prfssnl_total_cst_amt,
sum(CASE WHEN fcy_chrg_ind =1  THEN X.rcc_based_total_cst_amt
	    else 0 END ) as fcy_total_cst_amt
from pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct X
GROUP BY 1,2;

--CODE CHANGE :08/31/2020  MLH-581 
--select 'processing table: intermediate_stage_ptnt_fnc_txn_agg_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_ptnt_fnc_txn_agg_fct  IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_ptnt_fnc_txn_agg_fct 
AS 
select fcy_nm ,encntr_num ,
sum(CASE WHEN fcy_pymt_ind = 1  THEN amount
	    else NULL END ) as fcy_pymt_amt,
sum(CASE WHEN fcy_adj_ind =  1  THEN amount
	    else NULL END ) as fcy_adj_amt
from pce_qe16_slp_prd_dm..intermediate_stage_fnc_txn_fct X
GROUP BY 1,2;

--NET Reveneue ----------------------------------------NET Revenue Model 
--Inpatient 

----Qualifiers 
----select 'processing table: intermediate_stage_temp_eligible_encntr_data ' as table_processing;
--DROP TABLE intermediate_stage_temp_eligible_encntr_data IF EXISTS;
--	CREATE TEMP TABLE intermediate_stage_temp_eligible_encntr_data AS (
--		SELECT DISTINCT ZOOM.company_id
--		,ZOOM.patient_id , ZOOM.inpatient_outpatient_flag ,
--		ZOOM.admission_ts, ZOOM.discharge_ts FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM WHERE
----CODE change: Commented discharge_total_charges > 0 
----		ZOOM.discharge_total_charges > 0 AND 
----CODE Change: Added Discharge ts in the filter based on McLaren's request 
--		(cast(ZOOM.admission_ts AS DATE) >= DATE ('2015-10-01') OR cast(ZOOM.discharge_ts AS DATE) >= DATE ('2015-10-01'))
--		);
--SELECT count(*)
--FROM intermediate_stage_temp_eligible_encntr_data;

--select 'processing table:  intermediate_stage_temp_table_all_ip_rows' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_table_all_ip_rows IF Exists; 
CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_table_all_ip_rows  As 
with all_ip_recs as 
(
  select 
  X.company_id as fcy_nm, 
  X.inpatient_outpatient_flag as in_or_out_patient_ind, 
  X.patient_id as encntr_num, 
  date(Z.admission_ts) as adm_dt, 
  date(Z.discharge_ts) as dschrg_dt, 
  X.msdrg_code as ms_drg_cd, 
  X.patient_type as ptnt_tp_Cd,
 --CODE CHANGE: 08/31/2020 MLH-581 commenting the following 
 X.reimbursement_amount as tot_pymt_amt, 
 X.discharge_total_charges as tot_chrg_amt, 
  -- FCYPYMT.fcy_pymt_amt  as tot_pymt_amt,
  -- FCYCHRG.fcy_chrg_amt  as tot_chrg_amt,
  X.accountbalance as acct_bal_amt,
    --case when ROUND((X.accountbalance/X.discharge_total_charges * 100),2) <= 10 THEN
    case when abs(X.accountbalance)/X.discharge_total_charges * 100 <= 10 THEN
    --  case when abs(X.accountbalance)/FCYCHRG.fcy_chrg_amt * 100 <= 10 THEN
      'Y'
	  ELSE 
	   'N' END as est_acct_paid_ind,
   --  ROUND((X.accountbalance/X.discharge_total_charges),2) as acct_bal_pcnt,
   X.accountbalance/X.discharge_total_charges as acct_bal_pcnt,
   -- X.accountbalance/FCYCHRG.fcy_chrg_amt  as acct_bal_pcnt,
  Y.payor_group1 as src_prim_payor_grp1,
  1 as cnt,
  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
 CASE WHEN ( (date(now()) BETWEEN DATE(year(now())||'-10-01') AND DATE(year(now())||'-12-31')) AND
             (date(Z.discharge_ts) BETWEEN DATE(year(now())||'-10-01') AND DATE(year(now())||'-12-31'))
           ) 
    THEN 
			'FY' || year(now())-1
	ELSE 
			Z.fiscal_yr 
	END as fiscal_yr
from pce_qe16_oper_prd_zoom..cv_patdisch X 
 INNER JOIN pce_qe16_oper_prd_zoom..cv_paymstr Y
 ON Y.company_id = X.company_id and X.primary_payer_code = Y.payer_code
 INNER JOIN intermediate_stage_temp_eligible_encntr_data Z
 on Z.company_id = X.company_id and Z.patient_id = X.patient_id
 --CODE CHANGE : 08/31/2020 MLH-581 
 LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_chrg_agg_fct FCYCHRG
 on FCYCHRG.encntr_num = X.patient_id AND FCYCHRG.fcy_nm = X.company_id 
  --CODE CHANGE : 08/31/2020 MLH-581 
 LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_ptnt_fnc_txn_agg_fct FCYPYMT
 on FCYPYMT.encntr_num = X.patient_id AND FCYPYMT.fcy_nm = X.company_id 
  WHERE 
  X.inpatient_outpatient_flag = 'I' and X.discharge_total_charges > 0 
 -- AND round(FCYCHRG.fcy_chrg_amt) > 0 -- AND X.company_id !='Lansing'
  --Added Ptnt_tp_Cd Exclusions based on "Derived Net Revenue Reference Documents" 
  --Code Change: COmmented as per reqiest from Lisa on 02/06
--  and upper(X.patient_type) NOT in ('LIP','MIP','BSCH','BSCHO','8','C','F','GCLK','LLOV','MCIV','OFCE','OFFICE','OFFICE SERIES','POV','PRO','Z','ZWH')
)
SELECT * FROM all_ip_recs;  

--###################################################################
-- CASE A - 'BlueCross','Medicare','Medicaid'
--###################################################################
--Inpatient Payment Ratio only for 'BlueCross','Medicare','Medicaid' (PAID) irrespective of DRG is NULL OR NOT 
--select 'processing table:  ip_hist_pymt_ratio_case_a' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_a IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_a AS 
select  
 --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       fiscal_yr,
	   fcy_nm, 
in_or_out_patient_ind, 
X.src_prim_payor_grp1,
'Oct 2016 thru till date' as algorithm_duration,
paid_cases as paid_cases,
tot_pymt_amt as payment,
tot_chrg_amt as charges,
--ROUND(tot_pymt_amt/tot_chrg_amt,2) as pymt_ratio
tot_pymt_amt/tot_chrg_amt as pymt_ratio
FROM 
(
	select 
	  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       fiscal_yr,	
	   fcy_nm, 
	   in_or_out_patient_ind ,
	   src_prim_payor_grp1,  
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt ) as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--	   ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--       ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_ip_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')
--Code Change : Commente the next line
--AND  cast(dschrg_dt AS DATE) >= '2016-10-01'
--Code Change : UnCommented the next line
and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and  upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
Group by 1,2,3,4) X;

--Inpatient Payment Ratio only for 'BlueCross','Medicare','Medicaid' (PAID) of a DRG and Payor



--select 'processing table:  ip_hist_pymt_ratio_drg_case_a' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_a IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_a AS 
select 
--CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
X.fiscal_yr,
X.fcy_nm, 
X.src_prim_payor_grp1 ,
'Oct 2016 thru till date' as algorithm_duration,
--CODE change: Commented the next Line
--X.ms_drg_cd,
X.paid_cases as total_paid_cases,
X.sum_drg_wghts,
X.tot_chrg_amt as total_charges,
X.tot_pymt_amt as paid_amount,
X.tot_pymt_amt/X.tot_chrg_amt as pymt_ratio,
--ROUND(X.tot_pymt_amt/X.tot_chrg_amt,2) as pymt_ratio,
X.tot_pymt_amt /X.sum_drg_wghts  as drg_weighted_pmnt_per_case
FROM 
(
	select 
    --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       fiscal_yr,	
	   fcy_nm, 
	   in_or_out_patient_ind ,
	   src_prim_payor_grp1,  
--CODE change: Commented the next Line
--	   Z.ms_drg_cd,
	   sum(cnt)  as paid_cases ,
	   sum(MSDRG.drg_wght) as sum_drg_wghts,
	   sum(tot_pymt_amt) as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--	   ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--     ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_ip_rows Z
INNER JOIN intermediate_stage_temp_ms_drg_dim_hist MSDRG
on Z.ms_drg_cd = MSDRG.ms_drg_cd
WHERE Z.est_acct_paid_ind ='Y' and Z.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--CODE change: Uncommented the next Line
and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
and MSDRG.drg_wght > 0.000
and Z.ms_drg_cd NOT IN ('-100','999')
and Z.dschrg_dt BETWEEN MSDRG.vld_fm_dt AND MSDRG.vld_to_dt 

--CODE change: Commented the next Line
--AND cast(Z.dschrg_dt AS DATE) >= '2016-10-01'
--and cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--CODE change: commented the next Line
--Group by 1,2,3,4
Group by 1,2,3,4
) X;

--CASE A 
--select 'processing table: ip_net_rvu_case_a ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_a IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_a AS 
-- Paid Cases i.e Account Balance <= 10%
Select 
PAID.* ,
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt), 2) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_ip_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30') --85,855 Records
-- Unpaid Cases with a Drg (i.e Payments would be calculated based on Historical DRG Weights Ratio)
UNION 
select UNPD.*, 
ROUND(DRGWGHT.drg_wght * PAID.drg_weighted_pmnt_per_case, 2) as est_net_rev_amt
from intermediate_stage_temp_table_all_ip_rows UNPD
LEFT JOIN intermediate_stage_temp_ms_drg_dim_hist DRGWGHT
on UNPD.ms_drg_cd = DRGWGHT.ms_drg_cd
LEFT JOIN pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_a PAID
on UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  and (UNPD.ms_drg_cd IS NOT NULL  AND  UNPD.ms_drg_cd !='-100' AND UNPD.ms_drg_cd != '999')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
and dschrg_dt BETWEEN DRGWGHT.vld_fm_dt AND DRGWGHT.vld_to_dt
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30') --4578 Unpaid Cases with a Drg
UNION
-- Unpaid Cases without DRg (i.e Payments would be calcualted based on Historical Pymnt Ratio)
select UNPD.*, 
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio, 2) as est_net_rev_amt
from intermediate_stage_temp_table_all_ip_rows UNPD
LEFT JOIN pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_a PAID
on  UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  and (UNPD.ms_drg_cd IS NULL  OR UNPD.ms_drg_cd = '-100' OR UNPD.ms_drg_cd = '999')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
;
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30');  --26 Records Unpaid Cases without Drg


--###################################################################
-- CASE B - 'Commercial'
--###################################################################
--Inpatient Payment Ratio only for 'Commercial' (PAID) irrespective of DRG is NULL OR NOT 
--select 'processing table: ip_hist_pymt_ratio_case_b ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_b IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_b AS 
select  
--CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
X.fiscal_yr, 
X.fcy_nm,
in_or_out_patient_ind, 
X.src_prim_payor_grp1,
'Oct 2016  thru till date' as algorithm_duration,
paid_cases as paid_cases,
tot_pymt_amt as payment,
tot_chrg_amt as charges,
tot_pymt_amt/tot_chrg_amt as pymt_ratio
--ROUND(tot_pymt_amt/tot_chrg_amt,2) as pymt_ratio
FROM 
(
	select  
	  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       fiscal_yr, 
	   fcy_nm,
	   in_or_out_patient_ind ,
	   src_prim_payor_grp1,  
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt )  as  tot_pymt_amt,
       sum(tot_chrg_amt)   as tot_chrg_amt
-- ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
-- ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_ip_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('Other') 
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
Group by 1,2,3,4) X;

--select 'processing table: ip_hist_pymt_ratio_drg_case_b' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_b IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_b AS 
select 
--CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
X.fiscal_yr,
X.fcy_nm,
X.src_prim_payor_grp1 ,
'Oct 2016 thru till date' as algorithm_duration,
--CODE change: Commented the next Line
--X.ms_drg_cd,
X.paid_cases as total_paid_cases,
X.sum_drg_wghts,
X.tot_chrg_amt as total_charges,
X.tot_pymt_amt as paid_amount,
X.tot_pymt_amt/X.tot_chrg_amt as pymt_ratio,
--ROUND(X.tot_pymt_amt/X.tot_chrg_amt,2) as pymt_ratio,
X.tot_pymt_amt /X.sum_drg_wghts  as drg_weighted_pmnt_per_case
FROM 
(
	select  
		  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       fiscal_yr, 
	   fcy_nm,
	   in_or_out_patient_ind ,
	   src_prim_payor_grp1,  
--CODE change: Commented the next Line
--	   Z.ms_drg_cd,
	   sum(cnt)  as paid_cases ,
	   sum(MSDRG.drg_wght) as sum_drg_wghts,
	   sum(tot_pymt_amt ) as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--- ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--  ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_ip_rows Z
INNER JOIN intermediate_stage_temp_ms_drg_dim_hist MSDRG
on Z.ms_drg_cd = MSDRG.ms_drg_cd
WHERE Z.est_acct_paid_ind ='Y' and Z.src_prim_payor_grp1 in ('Other')  
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--CODE change: Uncommented the next Line
and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
and MSDRG.drg_wght > 0.000
and Z.ms_drg_cd NOT IN ('-100', '999')
and Z.dschrg_dt BETWEEN MSDRG.vld_fm_dt AND MSDRG.vld_to_dt
--CODE change: Commented the next Line
--and cast(dschrg_dt AS DATE) >= '2016-10-01'
--and cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--CODE change: Commented the next Line
--Group by 1,2,3,4
Group by 1,2,3,4
) X;


--select 'processing table: ip_net_rvu_case_b ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_b IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_b AS 
-- Paid Cases i.e Account Balance <= 10%
Select 
PAID.* ,
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt), 2 ) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_ip_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('Other')  
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30') --7,559 Records
-- Unpaid Cases with a Drg (i.e Payments would be calculated based on Historical DRG Weights Ratio)
UNION 
select UNPD.*, 
ROUND(DRGWGHT.drg_wght * PAID.drg_weighted_pmnt_per_case, 2)  as est_net_rev_amt
from intermediate_stage_temp_table_all_ip_rows UNPD
LEFT JOIN intermediate_stage_temp_ms_drg_dim_hist DRGWGHT
on UNPD.ms_drg_cd = DRGWGHT.ms_drg_cd
LEFT JOIN pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_b PAID
on UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('Other')   and (UNPD.ms_drg_cd IS NOT NULL  AND  UNPD.ms_drg_cd !='-100' AND UNPD.ms_drg_cd != '999')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
and dschrg_dt BETWEEN DRGWGHT.vld_fm_dt AND DRGWGHT.vld_to_dt
--and cast(UNPD.dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30') --4578 Unpaid Cases with a Drg
UNION
-- Unpaid Cases without Drg (i.e Payments would be calcualted based on Historical Pymnt Ratio)
SELECT UNPD.*, 
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio ,2) as est_net_rev_amt
from intermediate_stage_temp_table_all_ip_rows UNPD
LEFT JOIN prmretlp.ip_hist_pymt_ratio_case_b PAID
on  UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('Other')  and (UNPD.ms_drg_cd IS NULL  OR UNPD.ms_drg_cd = '-100' OR UNPD.ms_drg_cd = '999' )
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
;
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30');  --4 Records Unpaid Cases without Drg

--###################################################################
-- CASE C - 'Domestic'  
--###################################################################
--Inpatient Payment Ratio only for 'Domestic' (PAID)
--select 'processing table:  ip_net_rvu_case_c' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_c IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_c AS 
select ALLCASES.*, 
ROUND(ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio ,2)  as est_net_rev_amt
FROM intermediate_stage_temp_table_all_ip_rows ALLCASES
LEFT JOIN pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Domestic')  
AND RATIO.ptnt_cgy= 'Inpatient' and RATIO.payor_group_1 = 'Domestic'
-- AND  cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--AND cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30');
;

--###################################################################
-- CASE D - 'Self Pay'  
--###################################################################
--Inpatient Payment Ratio only for 'Self Pay'  (PAID)

--select 'processing table: ip_net_rvu_case_d ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_d IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_d AS 
SELECT
  ALLCASES.fcy_nm, 
  ALLCASES.in_or_out_patient_ind, 
  ALLCASES.encntr_num, 
  ALLCASES.adm_dt,  
  ALLCASES.dschrg_dt, 
  ALLCASES.ms_drg_cd, 
  ALLCASES.ptnt_tp_cd,
  ALLCASES.tot_pymt_amt, 
  ALLCASES.tot_chrg_amt, 
  ALLCASES.acct_bal_amt,
  CASE WHEN ALLCASES.acct_bal_amt = 0 THEN 'Y' ELSE 'N' END as est_acct_paid_ind,
  (ALLCASES.acct_bal_amt/ALLCASES.tot_chrg_amt) as acct_bal_pcnt,
 -- ROUND((ALLCASES.acct_bal_amt/ALLCASES.tot_chrg_amt),2) as acct_bal_pcnt,
  ALLCASES.src_prim_payor_grp1,
  1 as cnt,
  ALLCASES.fiscal_yr,
  CASE WHEN ALLCASES.acct_bal_amt = 0 THEN 
      ROUND(ALLCASES.tot_pymt_amt ,2) 
	ELSE
	  ROUND( ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio, 2) 
    END AS  est_net_rev_amt
FROM intermediate_stage_temp_table_all_ip_rows ALLCASES
LEFT JOIN pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Self Pay' ) 
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
AND RATIO.ptnt_cgy= 'Inpatient' and RATIO.payor_group_1 = 'Self Pay' ;


--select 'processing table: ip_encntr_net_rvu ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..ip_encntr_net_rvu IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_encntr_net_rvu as 
SELECT * FROM 
(select * from ip_net_rvu_case_a UNION 
select * from ip_net_rvu_case_b UNION 
select * from ip_net_rvu_case_c UNION 
select * from ip_net_rvu_case_d
) z
--WHERE cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
;
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
--AND fcy_nm != 'Lansing';

----Outpatients
--Outpatient 

--select 'processing table:  ip_encntr_net_rvu' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_table_all_op_rows IF Exists; 
CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_table_all_op_rows As 
with all_op_recs as 
(
  select 
  X.company_id as fcy_nm, 
  X.inpatient_outpatient_flag as in_or_out_patient_ind, 
  X.patient_id as encntr_num, 
  date(X.admission_ts) as adm_dt, 
  date(X.discharge_ts) as dschrg_dt, 
  X.msdrg_code as ms_drg_cd, 
  X.patient_type as ptnt_tp_Cd,
  --CODE CHANGE : 08/31/2020 MLH-581  commenting the following 
  X.reimbursement_amount as tot_pymt_amt, 
  X.discharge_total_charges as tot_chrg_amt, 
  --FCYPYMT.fcy_pymt_amt  as tot_pymt_amt,
  --FCYCHRG.fcy_chrg_amt  as tot_chrg_amt,
  X.accountbalance as acct_bal_amt,
  --CODE CHANGE : 08/31/2020 MLH-581
  case when (abs(X.accountbalance)/X.discharge_total_charges * 100) <= 10 THEN
  --case when (abs(X.accountbalance)/FCYCHRG.fcy_chrg_amt * 100) <= 10 THEN
  --case when ROUND((X.accountbalance/X.discharge_total_charges * 100),2) <= 10 THEN
      'Y'
	  ELSE 
	   'N' END as est_acct_paid_ind,
 --CODE CHANGE: 08/31/2020 MLH-581 commenting the following 
 (X.accountbalance/X.discharge_total_charges) as acct_bal_pcnt,
 -- (X.accountbalance/FCYCHRG.fcy_chrg_amt) as acct_bal_pcnt,
--ROUND((X.accountbalance/X.discharge_total_charges),2) as acct_bal_pcnt,
  Y.payor_group1 as src_prim_payor_grp1,
  1 as cnt ,
  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
 CASE WHEN ( (date(now()) BETWEEN DATE(year(now())||'-10-01') AND DATE(year(now())||'-12-31')) AND
             (date(Z.discharge_ts) BETWEEN DATE(year(now())||'-10-01') AND DATE(year(now())||'-12-31'))
           )
    THEN
                        'FY' || year(now())-1
        ELSE
                        Z.fiscal_yr
        END as fiscal_yr
from pce_qe16_oper_prd_zoom..cv_patdisch X 
 INNER JOIN pce_qe16_oper_prd_zoom..cv_paymstr Y
 ON Y.company_id = X.company_id and X.primary_payer_code = Y.payer_code
 INNER JOIN intermediate_stage_temp_eligible_encntr_data Z
 on Z.company_id = X.company_id and Z.patient_id = X.patient_id
  --CODE CHANGE : 08/31/2020 MLH-581 
 LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_chrg_agg_fct FCYCHRG
 on FCYCHRG.encntr_num = X.patient_id AND FCYCHRG.fcy_nm = X.company_id 
  --CODE CHANGE : 08/31/2020 MLH-581 
 LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_ptnt_fnc_txn_agg_fct FCYPYMT
 on FCYPYMT.encntr_num = X.patient_id AND FCYPYMT.fcy_nm = X.company_id 
  WHERE 
    X.inpatient_outpatient_flag = 'O' and X.discharge_total_charges > 0
-- AND round(FCYCHRG.fcy_chrg_amt) > 0
 --AND X.company_id !='Lansing'
  --Added Ptnt_tp_Cd Exclusions based on "Derived Net Revenue Reference Documents"
 --Code Change : Commented as per the Request from Lisa on 02/06  
 --AND   upper(X.patient_type) NOT in ('LIP','MIP','BSCH','BSCHO','8','C','F','GCLK','LLOV','MCIV','OFCE','OFFICE','OFFICE SERIES','POV','PRO','Z','ZWH')
)
SELECT * FROM all_op_recs; --6,047,368


--###################################################################
-- CASE A - 'BlueCross','Medicare','Medicaid'
--###################################################################

--Outpatient Payment Ratio only for 'BlueCross','Medicare','Medicaid' (PAID)
--select 'processing table: op_hist_pymt_ratio_case_a ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_a IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_a AS 
select 
 --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
X.fiscal_yr,
X.fcy_nm, 
X.ptnt_tp_cd ,
X.src_prim_payor_grp1,
'Oct 2016 thru last week' as algorithm_duration,
paid_cases as paid_cases,
tot_pymt_amt as payment,
tot_chrg_amt as charges,
tot_pymt_amt/tot_chrg_amt as pymt_ratio
--ROUND(tot_pymt_amt/tot_chrg_amt,2) as pymt_ratio
FROM 
(
	select  
	  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       Z.fiscal_yr, 
	   fcy_nm, 
	   in_or_out_patient_ind ,
	   ptnt_tp_cd,
	   src_prim_payor_grp1,  
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt) as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_op_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
-- and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
Group by 1,2,3,4,5 ) X;

--'BlueCross','Medicare','Medicaid' Unpaid Encounter (Est.Net Revenue Amount) Union Paid Encounter
--select 'processing table:  op_net_rvu_case_a' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_a IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_a AS 
--Unpaid Cases
select UNPD.*, 
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio ,2) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows UNPD
LEFT JOIN pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_a PAID 
on UNPD.ptnt_tp_cd  = PAID.ptnt_tp_cd and UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr  --CODE CHANGE : AUG 2019
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  
--AND cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
UNION 
--Paid Cases 
select PAID.*, 
--PAID.tot_pymt_amt +  PAID.acct_bal_amt  as est_net_rev_amt
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt) ,2 ) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
;
--;cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30'); --1,505,622

--###################################################################
-- CASE B - 'Commercial' (Use 'Other'' for now)
--###################################################################
--Outpatient Payment Ratio only for 'Commercial' (Use 'Other'' for now) (PAID)
--select 'processing table: op_hist_pymt_ratio_case_b ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_b IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_b AS 
select 
--CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
X.fiscal_yr,
X.fcy_nm, 
X.ptnt_tp_cd,
X.src_prim_payor_grp1,
'Oct 2016  thru till date' as algorithm_duration,
paid_cases as paid_cases,
tot_pymt_amt as payment,
tot_chrg_amt as charges,
tot_pymt_amt/tot_chrg_amt as pymt_ratio
--ROUND(tot_pymt_amt/tot_chrg_amt,2) as pymt_ratio
FROM 
(
	select  
     --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       Z.fiscal_yr, 
	   fcy_nm, 
	   in_or_out_patient_ind ,
	   ptnt_tp_cd,
	   src_prim_payor_grp1,  
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt)  as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_op_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('Other')  
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
Group by 1,2,3 ,4,5
--Group by 1,2
) X;

--'Commercial' Unpaid Encounter (Est.Net Revenue Amount) Union Paid Encounter
--select 'processing table: op_net_rvu_case_b ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_b IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_b AS 
select UNPD.*, 
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio,2 )  as est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows UNPD
LEFT JOIN pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_b PAID
on UNPD.ptnt_tp_cd  = PAID.ptnt_tp_cd and UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('Other')  
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) 
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
UNION 
select PAID.*, 
--PAID.tot_pymt_amt +  PAID.acct_bal_amt  as est_net_rev_amt
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt) ,2 ) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('Other')  
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
;


--###################################################################
-- CASE C - 'Domestic'  
--###################################################################
--Outpatient Payment Ratio only for 'Domestic' (PAID)
--select 'processing table: op_net_rvu_case_c ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_c IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_c AS 
select ALLCASES.*, 
ROUND(ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio ,2) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows ALLCASES
LEFT JOIN pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Domestic')  
AND RATIO.ptnt_cgy= 'Outpatient' and RATIO.payor_group_1 = 'Domestic'  
--AND cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--AND cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
;
;

--###################################################################
-- CASE D - 'Self Pay'  
--###################################################################
--Outpatient Payment Ratio only for 'Self Pay'  (PAID)
--select 'processing table:  op_net_rvu_case_d' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_d IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_d AS 
SELECT
  ALLCASES.fcy_nm, 
  ALLCASES.in_or_out_patient_ind, 
  ALLCASES.encntr_num, 
  ALLCASES.adm_dt, 
  ALLCASES.dschrg_dt, 
  ALLCASES.ms_drg_cd, 
  ALLCASES.ptnt_tp_cd,
  ALLCASES.tot_pymt_amt, 
  ALLCASES.tot_chrg_amt, 
  ALLCASES.acct_bal_amt,
  CASE WHEN ALLCASES.acct_bal_amt = 0 THEN 'Y' ELSE 'N' END as est_acct_paid_ind,
  ALLCASES.acct_bal_amt/ALLCASES.tot_chrg_amt as acct_bal_pcnt,
 --ROUND((ALLCASES.acct_bal_amt/ALLCASES.tot_chrg_amt),2) as acct_bal_pcnt,
  ALLCASES.src_prim_payor_grp1,
  1 as cnt,
  ALLCASES.fiscal_yr,
  CASE WHEN ALLCASES.acct_bal_amt = 0 THEN 
      ROUND(ALLCASES.tot_pymt_amt ,2)
	ELSE
	  ROUND( ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio ,2 )
    END AS  est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows ALLCASES
LEFT JOIN pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Self Pay' )  
AND RATIO.ptnt_cgy= 'Outpatient' and RATIO.payor_group_1 = 'Self Pay' 
--AND cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--AND cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
;

----Gross Revenue and Net Revenue (Revenue Model - Outpatient All 4 Cases/Scenario for the period October 2017 - September 2018 )

--select 'processing table: op_net_rvu_model ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..op_net_rvu_model IF EXISTS; 
CREATE  TABLE pce_qe16_slp_prd_dm..op_net_rvu_model AS 
select fcy_nm,
sum(tot_chrg_amt) as grs_rev_amt , 
ROUND(sum(est_net_rev_amt),2) as drvd_net_rev_amt 
--ROUND(sum(tot_chrg_amt),2) as grs_rev_amt , 
--ROUND(sum(est_net_rev_amt),2) as drvd_net_rev_amt 
-- to_char(sum(tot_chrg_amt), '$999G999G999G999D99') as grs_rev_amt , 
--to_char(sum(est_net_rev_amt),'$999G999G999G999D99')  as drvd_net_rev_amt
FROM 
(select * from op_net_rvu_case_a UNION 
select * from op_net_rvu_case_b UNION 
select * from op_net_rvu_case_c UNION 
select * from op_net_rvu_case_d) Z 
--WHERE cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--and Z.fcy_nm != 'Lansing'
group by fcy_nm;

--select 'processing table:  op_encntr_net_rvu' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..op_encntr_net_rvu IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_encntr_net_rvu as 
SELECT * FROM 
(select * from op_net_rvu_case_a UNION 
select * from op_net_rvu_case_b UNION 
select * from op_net_rvu_case_c UNION 
select * from op_net_rvu_case_d
) z
--WHERE cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  
;
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--AND fcy_nm != 'Lansing';

--Resultant Table

--select 'processing table: ip_dept_revenue_charges ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..ip_dept_revenue_charges IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_dept_revenue_charges AS 
select T.fcy_nm, T.encntr_num ,T.tot_pymt_amt,T.tot_chrg_amt, T.acct_bal_amt, sum(C.total_charge) as dept_or_revenue_total_charge_amt
,CASE WHEN sum(C.total_charge) > 0 THEN 'Y' ELSE 'N' END as prof_chrg_ind
FROM intermediate_stage_temp_table_all_ip_rows T
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct C
on C.company_id = T.fcy_nm and T.encntr_num = C.patient_id
WHERE (
--Department Exclusion
 C.dept in ('01.4405','01.4442','01.4444','01.4420','01.3175','01.3157','01.4412','01.4413','01.4416','01.4418','01.4419','01.4425')
 OR
--Revenue Code Exclusion
C.revenue_code in ('0960','0961','0969','0972','0977','0982','0983','0985','0987','0990')
)
group by 1,2,3, 4,5;

--select 'processing table: op_dept_revenue_charges ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..op_dept_revenue_charges IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_dept_revenue_charges AS 
select T.fcy_nm, T.encntr_num ,T.tot_pymt_amt,T.tot_chrg_amt, T.acct_bal_amt, sum(C.total_charge) as dept_or_revenue_total_charge_amt
,CASE WHEN sum(C.total_charge) > 0 THEN 'Y' ELSE 'N' END as prof_chrg_ind
FROM intermediate_stage_temp_table_all_op_rows T
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct C
on C.company_id = T.fcy_nm and T.encntr_num = C.patient_id
WHERE (
--Department Exclusion
 C.dept in ('01.4405','01.4442','01.4444','01.4420','01.3175','01.3157','01.4412','01.4413','01.4416','01.4418','01.4419','01.4425')
 OR
--Revenue Code Exclusion
C.revenue_code in ('0960','0961','0969','0972','0977','0982','0983','0985','0987','0990')
)
group by 1,2,3, 4,5;

--select 'processing table:  intermediate_stage_encntr_net_rvu_fct_x' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_net_rvu_fct_x IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_net_rvu_fct_x AS
with combined as 
(select * from pce_qe16_slp_prd_dm..op_encntr_net_rvu
UNION 
select * from pce_qe16_slp_prd_dm..ip_encntr_net_rvu),
prof_chrg_combined as 
(
 select * from pce_qe16_slp_prd_dm..ip_dept_revenue_charges
UNION 
select * from pce_qe16_slp_prd_dm..op_dept_revenue_charges
)
SELECT X.company_id as src_fcy_nm, X.patient_id as src_encntr_num,
--SELECT
-- X.fcy_nm
--,X.fcy_num 
--,X.encntr_num 
--,Y.est_acct_paid_ind
--,ROUND(Y.est_net_rev_amt, 2) as est_net_rev_amt
Y.*
,nvl(Z.prof_chrg_ind, 'N') as prof_chrg_ind
FROM intermediate_stage_temp_eligible_encntr_data X 
LEFT JOIN combined Y
on X.company_id = Y.fcy_nm and X.patient_id   = Y.encntr_num
LEFT JOIN prof_chrg_combined Z
on X.company_id = Z.fcy_nm and X.patient_id   = Z.encntr_num; 


--select 'processing table: intermediate_stage_encntr_net_rvu_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_net_rvu_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_net_rvu_fct AS
with combined as 
(select * from pce_qe16_slp_prd_dm..op_encntr_net_rvu
UNION 
select * from pce_qe16_slp_prd_dm..ip_encntr_net_rvu),
prof_chrg_combined as 
(
 select * from pce_qe16_slp_prd_dm..ip_dept_revenue_charges
UNION 
select * from pce_qe16_slp_prd_dm..op_dept_revenue_charges
)
--SELECT X.fcy_nm as src_fcy_nm, X.fcy_num as src_fcy_num, X.encntr_num as src_encntr_num,
SELECT
 X.company_id as fcy_nm
,X.patient_id as encntr_num 
,Y.est_acct_paid_ind
,ROUND(Y.est_net_rev_amt ,2) as est_net_rev_amt
--,ROUND(Y.est_net_rev_amt,2) as est_net_rev_amt
,nvl(Z.prof_chrg_ind, 'N') as prof_chrg_ind
--Y.*
FROM intermediate_stage_temp_eligible_encntr_data X
LEFT JOIN combined Y
on X.company_id = Y.fcy_nm and X.patient_id   = Y.encntr_num
LEFT JOIN prof_chrg_combined Z
on X.company_id = Z.fcy_nm and X.patient_id   = Z.encntr_num;

--Combining all the Intermediate table

--select 'processing table: intermediate_stage_hist_pymt_ratio ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_hist_pymt_ratio IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_hist_pymt_ratio as 
select 'INPATIENT  - Medicare, Medicaid, BSBS' as scenario,  * from pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_a UNION 
select 'OUTPATIENT - Medicare, Medicaid, BSBS' as scenario,  * from pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_a UNION
select 'INPATIENT  - Others' as scenario, * from pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_b  UNION 
select 'OUTPATIENT - Others, Medicaid, BSBS' as scenario,* from pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_b; 

--select 'processing table: intermediate_stage_hist_pymt_ratio_drg_wghts ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_hist_pymt_ratio_drg_wghts IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_hist_pymt_ratio_drg_wghts as 
select 'INPATIENT  - Medicare, Medicaid, BSBS' as scenario,  * from pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_a UNION
select 'INPATIENT  - Others' as scenario,  * from pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_b;

--select 'processing table: intermediate_stage_net_rvu_model ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_net_rvu_model IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_net_rvu_model as 
select 'INPATIENT  - Medicare, Medicaid, BSBS' as scenario, * from pce_qe16_slp_prd_dm..ip_net_rvu_case_a UNION 
select 'INPATIENT  - Others' as scenario, * from pce_qe16_slp_prd_dm..ip_net_rvu_case_b UNION 
select 'INPATIENT  - Domestic' as scenario, * from pce_qe16_slp_prd_dm..ip_net_rvu_case_c UNION 
select 'INPATIENT  - Self-Pay' as scenario, * from pce_qe16_slp_prd_dm..ip_net_rvu_case_d UNION
select 'OUTPATIENT  - Medicare, Medicaid, BSBS' as scenario, * from pce_qe16_slp_prd_dm..op_net_rvu_case_a UNION 
select 'OUTPATIENT  - Others' as scenario, * from pce_qe16_slp_prd_dm..op_net_rvu_case_b UNION 
select 'OUTPATIENT  - Domestic' as scenario, * from pce_qe16_slp_prd_dm..op_net_rvu_case_c UNION 
select 'OUTPATIENT  - Self-Pay' as scenario, * from pce_qe16_slp_prd_dm..op_net_rvu_case_d;

--NET Revenue----------------


--Code Change : Cancer Patient Identification 
--select 'processing table: intermediate_stage_temp_encntr_dgns_fct_with_cancer_case ' as table_processing;
--CODE CHANGE : MLH-591 (Commenting the old Code )
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_encntr_dgns_fct_with_cancer_case IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_encntr_dgns_fct_with_cancer_case AS 
--  SELECT W.fcy_nm, W.encntr_num, 1 as cancer_case_ind , W.cancer_dgns_cd,  W.cancer_case_code_descr
--  FROM 
--  (
--    SELECT X.fcy_nm_hash, X.encntr_num_hash, X.fcy_nm, X.encntr_num, X.fcy_num, 
--  X.patient_id, X.cancer_case_code_descr,X.cancer_dgns_cd, 
--row_number() over(partition by X.company_id, X.patient_id ORDER BY X.diagnosistype ) as rank_num
--  FROM intermediate_stage_encntr_dgns_fct X
--  WHERE --( (X.diagnosistype in ('Primary', 'Secondary') AND X.fcy_nm NOT IN ('Karmanos')) OR
--         -- (X.diagnosistype in ('Final Diagnosis') AND X.fcy_nm IN ('Karmanos'))) AND 
--  X.cancer_case_ind = 1 
-- -- WHERE X.diagnosistype in ('Primary', 'Secondary') AND 
-- -- X.cancer_case_ind = 1 
--  ) W 
--  WHERE W.rank_num = 1; 
  with encntr_dgns_agg as
  ( select 
    fcy_nm, encntr_num
  , max(non_cancer_case_dgns_ind) as non_cancer_case_dgns_ind
  , max(cancer_case_dgns_ind) as cancer_case_dgns_ind
  , max(prim_dgns_non_cancer_case_ind) as prim_dgns_non_cancer_case_ind 
  , max(sec_dgns_cancer_case_ind) as sec_dgns_cancer_case_ind
  , max(prim_dgns_cancer_case_ind) as prim_dgns_cancer_case_ind
  FROM intermediate_stage_encntr_dgns_fct
  GROUP BY 1,2
  ),
  encntr_cancer_dgns_cd as 
  (  
     select * from 
	 (select distinct fcy_nm  , encntr_num, cancer_dgns_cd,  cancer_case_code_descr, ccs_dgns_cgy_cd, ccs_dgns_cgy_descr, ccs_dgns_lvl_1_cd, ccs_dgns_lvl_1_descr, ccs_dgns_lvl_2_cd, ccs_dgns_lvl_2_descr, 
   row_number() over(partition by fcy_nm, encntr_num ORDER BY diagnosistype , diagnosisseq ) as rank_num
  FROM intermediate_stage_encntr_dgns_fct
  WHERE cancer_case_dgns_ind =1 and (sec_dgns_cancer_case_ind =1 OR prim_dgns_cancer_Case_ind =1) and diagnosistype IN ('Primary','Secondary')) X 
  WHERE X.rank_num =1 
  )
    select Z.*,
  CASE WHEN non_cancer_case_dgns_ind =1 AND prim_dgns_non_cancer_case_ind =1 AND sec_dgns_cancer_case_ind =0 AND prim_dgns_cancer_Case_ind =0 THEN 1 ELSE 0 END as maint_cancer_case_ind,
  CASE   WHEN cancer_case_dgns_ind =1 AND prim_dgns_non_cancer_case_ind =1 AND sec_dgns_cancer_case_ind =0 AND prim_dgns_cancer_Case_ind =0 THEN 0
   		 WHEN cancer_case_dgns_ind = 1 AND prim_dgns_non_cancer_case_ind =1 AND (sec_dgns_cancer_case_ind =1 OR prim_dgns_cancer_Case_ind =1) THEN 1 
  		 WHEN cancer_case_dgns_ind =1 AND prim_dgns_non_cancer_case_ind =0 AND (sec_dgns_cancer_case_ind =1 OR prim_dgns_cancer_Case_ind =1) THEN 1 
  ELSE 0 END as cancer_case_ind,
  cancer_dgns_cd, 
  cancer_case_code_descr, 
  ccs_dgns_cgy_cd, 
  ccs_dgns_cgy_descr, 
  ccs_dgns_lvl_1_cd, 
  ccs_dgns_lvl_1_descr, 
  ccs_dgns_lvl_2_cd, 
  ccs_dgns_lvl_2_descr
  FROM encntr_dgns_agg Z
  LEFT JOIN encntr_cancer_dgns_cd X
  on Z.fcy_nm = X.fcy_nm AND Z.encntr_num = X.encntr_num;
  
 --validation of MLH-591
   select maint_cancer_case_ind , 'maint_cancer_case_ind' as t, COUNT(*)
from intermediate_stage_temp_encntr_dgns_fct_with_cancer_case GROUP BY 1 UNION 
select cancer_case_ind,'cancer_case_ind_new' as t, COUNT(*)
from intermediate_stage_temp_encntr_dgns_fct_with_cancer_case GROUP BY 1;
  
 ---------------------------------------------------------------------------------------
 --CODE CHANGE : AUG 2019 Blood and Lab Utilization
 
--select 'processing table:  intermediate_stage_temp_blood_util_qty' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_blood_util_qty IF EXISTS; 
CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_blood_util_qty AS 
SELECT X.fcy_nm, X.encntr_num,
SUM(X.quantity) AS blood_util_qty
FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct X
WHERE X.cpt_code in ('P9011','P9012','P9016','P9017','P9019','P9021','P9033','P9034','P9035','P9037','P9040','P9044','P9052','P9059')
GROUP BY 1,2; 


------CODE CHANGE : Aug 2019 Lab Utilization 
--select 'processing table:  intermediate_stage_temp_lab_util_qty' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_lab_util_qty IF EXISTS;

CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_stage_temp_lab_util_qty AS 
SELECT X.fcy_nm, X.encntr_num,
SUM(X.quantity) AS lab_util_qty
FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct X
WHERE 
--CODE CHANGE: MAY 2020 Added total_charge <> 0.0000 
X.total_charge <> 0.0000 AND
X.department_group =  'Lab' AND X.cpt_code NOT IN (SELECT cd
  FROM pce_qe16_prd_qadv..val_set_dim where cohrt_id ='lab_utils')
GROUP BY 1,2; 

---
----CODE CHANGE : April 2020 Financial Transaction Fact JIRA # MLH-505
--select 'processing table: intermediate_stage_fnc_txn_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_fnc_txn_fct IF EXISTS; 

CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_fnc_txn_fct AS 
with fnc_txn_fct AS(
SELECT   facility  as fcy_nm
       , account  as encntr_num 
       , department
       , nvl(dept_dim.department_description, 'UNKNOWN') as department_description
       , transcode
       , receiveddate
       , postdate
       , transcodedesc
       , transtype
       , amount
       , covered
       , noncovered
       , deductible
       , coinsurance
       , subaccount
       , revenuecode
       ,nvl(crev.revenue_code_description,'UNKNOWN') AS revenue_code_description 
       , cpt4code
       , modifier1
       , modifier2
       , modifier3
       , modifier4
       , payorplancode
       , nvl(PAYER.payer_description , 'UNKNOWN') as payer_description
       , remitid
       , extracteddate
       , sourcesystem
       , invoiceid
       , CASE WHEN UPPER(transtype) in ('P','PAYMENT','PAYMENTS','RECEIPT') THEN 1 ELSE 0 END as fcy_pymt_ind
       , CASE WHEN UPPER(transtype) in ('A','ADJUSTMENTS','XFER') THEN 1 ELSE 0 END as fcy_adj_ind
  FROM pce_qe16_oper_prd_zoom.qe16zmp.cv_pattrans PFTF
  LEFT JOIN prmretlp.dept_dim 
  on  PFTF.facility = dept_dim.company_id and PFTF.department = dept_dim.department_code
  LEFT JOIN intermediate_stage_temp_payer_fcy_std_code PAYER 
  ON PAYER.company_id = PFTF.facility
	AND PAYER.fcy_payer_code = PFTF.payorplancode
LEFT JOIN  pce_qe16_oper_prd_zoom..cv_revcodemap crev
on PFTF.revenuecode = crev.revenue_code
)
SELECT FT.* FROM intermediate_stage_temp_eligible_encntr_data EA
INNER JOIN fnc_txn_fct FT
 on EA.company_id = FT.fcy_nm and EA.patient_id = FT.encntr_num;

--------------------------------------------------------------------------------------
---Adding logic for Primary HCPCS/BETOS
-------------------------------------------------------------------------------------

select 'encntr_prim_hcpcs_fct';

drop table encntr_prim_hcpcs_fct if exists;
create temp table encntr_prim_hcpcs_fct as
select 	p.patient_id as encntr_num,
	primary_cpt_hcpcs prim_hcpcs_cd,
	h.hcpcs_descr prim_hcpcs_descr,
	hc.ccs_hcpcs_cgy_cd prim_ccs_hcpcs_cgy_cd,
	hc.ccs_hcpcs_cgy_descr prim_ccs_hcpcs_cgy_descr,
	hb.betos_cd as prim_betos_cd,
	hb.betos_descr as prim_betos_descr,
	hb.betos_cgy_nm as prim_betos_cgy_nm
from intermediate_stage_temp_eligible_encntr_data p
	inner join pce_qe16_prd_ct..stage_claim_gold sg on p.patient_id=sg.patient_account_number
	left join pce_qe16_slp_prd_dm..hcpcs_dim h on sg.primary_cpt_hcpcs=h.hcpcs_cd
	left join pce_qe16_slp_prd_dm..hcpcs_ccs_dim hc on h.hcpcs_cd = hc.hcpcs_cd
	left join pce_qe16_slp_prd_dm..hcpcs_betos_dim hb on h.hcpcs_cd = hb.hcpcs_cd;


---------------------------------------------------------------------------------------
--Added COVID Test Indicator looking at both the soft coded CPTs (CPT Fact) and hard coded cpts (CPT Code in Charge Fact table)
---------------------------------------------------------------------------------------

create temp table encntr_covid_test as
select company_id, patient_id, cpt_code, covid_tst_ind from 
(
select company_id,patient_id, cpt_code, 1 as covid_tst_ind  from patcpt_fct
where cpt_code in ('87635','86328','86769','U0002','U0001','G2023','G2024')
group by company_id,patient_id, cpt_code 
union
select company_id,patient_id, cpt_code, covid_tst_ind from
(
select company_id,patient_id, cpt_code, row_number() over (partition by company_id,patient_id, cpt_code order by service_date desc) as covid_tst_ind  from cv_patbill
where cpt_code in ('87635','86328','86769','U0002','U0001','G2023','G2024')
)z where covid_tst_ind=1 
)a;

create temp table covid_patient as
SELECT company_id, patient_id,
max(case when icd_code='U07.1' then 1 end) as covid_ptnt_ind,
max(case when icd_code='Z20.828' then 1 end) as covid_ssp_ind
 FROM (
SELECT  company_id, patient_id, diagnosisseq,icd_code,
        row_number() over(partition by company_id, patient_id, icd_code Order by  diagnosisseq) as covid_ptnt_ind
FROM dgns_fct WHERE icd_code in ('U07.1','Z20.828')
        )a WHERE covid_ptnt_ind=1
group by patient_id, company_id
;



---------------------------------------------------------------------------------------

--select 'processing table:  intermediate_stage_encntr_anl_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct_temp IF EXISTS;

CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct_temp AS

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
	,DATE (to_timestamp((ZOOM.discharge_date || ' ' || nvl(substr(ZOOM.discharge_date, 1, 2), '00') || ':' || nvl(substr(ZOOM.discharge_date, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS dschrg_dt
	,ZOOM.discharge_time AS dschrg_tm
	,ZOOM.length_of_stay AS los
	--,NVL(ZOOM.msdrg_code,'-100') AS ms_drg_cd     --- CODE Changed for outpatient
	,CASE WHEN ZOOM.inpatient_outpatient_flag = 'I' THEN NVL(ZOOM.msdrg_code,'-100') else null end AS ms_drg_cd	
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
	,QADV_RACE.race_descr AS race_descr
	,VSET_MARITAL.cd_descr AS mar_status
	,ZOOM.birth_weight_in_grams AS brth_wght_in_grm
	,ZOOM.days_on_mechanical_ventilator AS day_on_mchnc_vntl
	,ZOOM.smoker_flag AS smk_flag
	,ZOOM.weight_in_lbs AS wght_in_lb
	,VSET_ETHCTY.cd_descr AS ethcty_descr
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
	,CVADMSVC.fielddescription as adm_svc_descr											   
	,ZOOM.dischargeservice AS dschrg_svc
	,CVDSCHRGSVC.fielddescription as dschrg_svc_descr													 
	,ZOOM.nursingstation AS nrg_stn
	,ZOOM.financialclass AS fnc_cls
	,CVFNCCLS.fielddescription as fnc_cls_descr										
	,ZOOM.financialclassoriginal AS fnc_cls_orig
	,ZOOM.finalbillflag AS fnl_bill_flag
	,DATE (ZOOM.finalbilldate) AS fnl_bill_dt
	,ZOOM.totaladjustments AS tot_adj_amt
	,ZOOM.accountbalance AS acct_bal_amt
	,ZOOM.expectedpayment AS expt_pymt_amt
	,DATE (to_timestamp((ZOOM.updatedate || ' ' || nvl(substr(ZOOM.updatedate, 1, 2), '00') || ':' || nvl(substr(ZOOM.updatedate, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS upd_dt
	--,ZOOM.updateid AS upd_id
	--,ZOOM.sourcesystem AS src_sys
	,ZOOM.total_charges_ind AS tot_chrg_ind
	,ZOOM.admitdate_yr_ind AS admdt_yr_ind
	,FCY_REF.bed_cnt
	,CASE 
		WHEN intermediate_stage_temp_dschrg_inpatient_nbrn.patient_id IS NULL
			THEN NULL
		ELSE intermediate_stage_temp_dschrg_inpatient_nbrn.dschrg_nbrn_ind
		END AS dschrg_nbrn_ind
	,CASE 
		WHEN intermediate_stage_temp_dschrg_inpatient_rehab.patient_id IS NULL
			THEN NULL
		ELSE intermediate_stage_temp_dschrg_inpatient_rehab.dschrg_rehab_ind
		END AS dschrg_rehab_ind
	,CASE 
		WHEN intermediate_stage_temp_dschrg_inpatient_psych.patient_id IS NULL
			THEN NULL
		ELSE intermediate_stage_temp_dschrg_inpatient_psych.dschrg_psych_ind
		END AS dschrg_psych_ind
	,CASE 
		WHEN intermediate_stage_temp_dschrg_inpatient_ltcsnf.patient_id IS NULL
			THEN NULL
		ELSE intermediate_stage_temp_dschrg_inpatient_ltcsnf.dschrg_ltcsnf_ind
		END AS dschrg_ltcsnf_ind
	,CASE 
		WHEN intermediate_stage_temp_dschrg_inpatient_hospice.patient_id IS NULL
			THEN NULL
		ELSE intermediate_stage_temp_dschrg_inpatient_hospice.dschrg_hospice_ind
		END AS dschrg_hospice_ind
	,CASE 
		WHEN intermediate_stage_temp_dschrg_inpatient_spclcare.patient_id IS NULL
			THEN NULL
		ELSE intermediate_stage_temp_dschrg_inpatient_spclcare.dschrg_spclcare_ind
		END AS dschrg_spclcare_ind
	,CASE 
		WHEN intermediate_stage_temp_dschrg_inpatient_lipmip.patient_id IS NULL
			THEN NULL
		ELSE intermediate_stage_temp_dschrg_inpatient_lipmip.dschrg_lipmip_ind
		END AS dschrg_lipmip_ind
	,CASE 
		WHEN intermediate_stage_temp_dschrg_inpatient_acute.patient_id IS NULL
			THEN NULL
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
	,nvl(PRIMPAYER.fcy_payer_description,'UNKNOWN') AS src_prim_pyr_descr
	,nvl(PRIMPAYER.std_payer_code,'-100') AS qadv_prim_pyr_cd
	,nvl(PRIMPAYER.std_payer_descr,'UNKNOWN') AS qadv_prim_pyr_descr
	,PRIMPAYER.payor_group1 as src_prim_payor_grp1
	,PRIMPAYER.payor_group2 as src_prim_payor_grp2
	,PRIMPAYER.payor_group3 as src_prim_payor_grp3
	,nvl(SECONPAYER.fcy_payer_code,'-100') AS src_scdy_pyr_cd
	,nvl(SECONPAYER.fcy_payer_description,'UNKNOWN') AS src_scdy_pyr_descr
	,nvl(SECONPAYER.std_payer_code,'-100') AS qadv_scdy_pyr_cd
	,nvl(SECONPAYER.std_payer_descr,'UNKNOWN') AS qadv_scdy_pyr_descr
	,SECONPAYER.payor_group1 as src_scdy_payor_grp1
	,SECONPAYER.payor_group2 as src_scdy_payor_grp2
	,SECONPAYER.payor_group3 as src_scdy_payor_grp3
	--Adding Tertiary Payer 
	,nvl(TRTYPAYER.fcy_payer_code,'-100') AS src_trty_pyr_cd
	,nvl(TRTYPAYER.fcy_payer_description,'UNKNOWN') AS src_trty_pyr_descr
	,nvl(TRTYPAYER.std_payer_code,'-100') AS qadv_trty_pyr_cd 
	,nvl(TRTYPAYER.std_payer_descr,'UNKNOWN') AS qadv_trty_pyr_descr
	,TRTYPAYER.payor_group1 as src_trty_payor_grp1
	,TRTYPAYER.payor_group2 as src_trty_payor_grp2
	,TRTYPAYER.payor_group3 as src_trty_payor_grp3
	--Adding Quarternary Payer
	,nvl(QTRPAYER.fcy_payer_code,'-100') AS src_qtr_pyr_cd
	,nvl(QTRPAYER.fcy_payer_description,'UNKNOWN') AS src_qtr_pyr_descr
	,nvl(QTRPAYER.std_payer_code,'-100') AS qadv_qtr_pyr_cd
	,nvl(QTRPAYER.std_payer_descr,'UNKNOWN') AS qadv_qtr_pyr_descr
	,QTRPAYER.payor_group1 as src_qtr_payor_grp1
	,QTRPAYER.payor_group2 as src_qtr_payor_grp2
	,QTRPAYER.payor_group3 as src_qtr_payor_grp3
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
        ,nvl(DGNSDIM.dgns_descr,'UNKNOWN') AS prim_dgns_descr
	
	-------Srujan Update Start-----------------
	/*Start Primary Diagnosis CCS Attributes*/
	,nvl(DGNSDIM.ccs_dgns_cgy_cd,'-100') AS prim_ccs_dgns_cgy_cd
	,nvl(DGNSDIM.ccs_dgns_cgy_descr,'UNKNOWN') AS prim_ccs_dgns_cgy_descr
	,nvl(DGNSDIM.ccs_dgns_lvl_1_cd,'-100') AS prim_ccs_dgns_lvl_1_cd
	,nvl(DGNSDIM.ccs_dgns_lvl_1_descr,'UNKNOWN') AS prim_ccs_dgns_lvl_1_descr
	,nvl(DGNSDIM.ccs_dgns_lvl_2_cd,'-100') AS prim_ccs_dgns_lvl_2_cd
	,nvl(DGNSDIM.ccs_dgns_lvl_2_descr,'UNKNOWN') AS prim_ccs_dgns_lvl_2_descr
	/*End Primary Diagnosis CCS Attributes*/
	-------Srujan Update End-----------------
	
	,'-100' AS prim_dgns_poa_flg_cd
--	,nvl(PRIMDGNS.prim_icd_code,'-100') AS prim_dgns_cd
--  ,nvl(PRIMDGNS.prim_icd_descr,'UNKNOWN') AS prim_dgns_descr
--	,nvl(PRIMDGNS.prim_diagnosis_code_present_on_admission_flag,'-100') AS prim_dgns_poa_flg_cd
	--------------------------------------------------------------------------------------------------
	,nvl(SCDYDGNS.scdy_icd_code,'-100') AS scdy_dgns_cd
	,nvl(SCDYDGNS.scdy_diagnosis_code_present_on_admission_flag,'-100') AS scdy_dgns_poa_flg_cd
	,nvl(SCDYDGNS.scdy_dgns_descr_long,'UNKNOWN') as scdy_dgns_descr_long
	,nvl(TRTYDGNS.trty_icd_code,'-100') AS trty_dgns_cd
	,nvl(TRTYDGNS.trty_diagnosis_code_present_on_admission_flag,'-100') AS trty_dgns_poa_flg_cd
	,nvl(TRTYDGNS.trty_dgns_descr_long,'UNKNOWN') as trty_dgns_descr_long
--Code Change: Zoom gets from Encntr but Integrated Mart gets from intermediate_stage_encntr_pcd_fct so commenting Integrated Version
--	,nvl(PRIMPROC.prim_proc_icd_code,'-100') AS prim_pcd_cd
--	,nvl(PRIMPROC.prim_proc_icd_pcd_descr,'UNKNOWN') as prim_pcd_descr
        ,nvl(PCDDIM.icd_pcd_cd,'-100') as prim_pcd_cd
	,nvl(PCDDIM.icd_pcd_descr,'UNKNOWN') as prim_pcd_descr
	
		-------Srujan Update Start-----------------
	/*Start Primary Procedure CCS Attributes*/
	,nvl(PCDDIM.icd_pcd_ccs_cgy_cd,'-100') as prim_pcd_ccs_cgy_cd
	,nvl(PCDDIM.icd_pcd_ccs_cgy_descr,'UNKNOWN') as prim_pcd_ccs_cgy_descr
	,nvl(PCDDIM.icd_pcd_ccs_lvl_1_cd,'-100') as prim_pcd_ccs_lvl_1_cd
	,nvl(PCDDIM.icd_pcd_ccs_lvl_1_descr,'UNKNOWN') as prim_pcd_ccs_lvl_1_descr
	,nvl(PCDDIM.icd_pcd_ccs_lvl_2_cd,'-100') as prim_pcd_ccs_lvl_2_cd
	,nvl(PCDDIM.icd_pcd_ccs_lvl_2_descr,'UNKNOWN') as prim_pcd_ccs_lvl_2_descr
	
		-------Srujan Update End-----------------
	/*End Primary Procedure CCS Attributes*/
	
	,nvl(SCDYPROC.scdy_proc_icd_code,'-100') AS scdy_pcd_cd
	,nvl(SCDYPROC.scdy_proc_icd_pcd_descr,'UNKNOWN') as scdy_pcd_descr
	,nvl(TRTYPROC.trty_proc_icd_code,'-100') AS trty_pcd_cd
	,nvl(TRTYPROC.trty_proc_icd_pcd_descr,'UNKNOWN') as trty_pcd_descr
	,nvl(PATTYPE.patient_type_code,'-100') AS ptnt_tp_cd
	,nvl(PATTYPE.patient_type_description,'UNKNOWN') AS ptnt_tp_descr
	,nvl(PATTYPE.standard_patient_type_code,'-100') AS std_ptnt_tp_cd
	,ADMITPRACTSPEC.npi AS adm_pract_npi
	,ATTENDPRACTSPEC.npi AS attnd_pract_npi
	,ATTENDPRACTSPEC.practitioner_spclty_description as attnd_pract_spclty_descr
        ,ATTENDPRACTSPEC.mcare_spcly_cd as attnd_pract_spclty_cd
	,ADMITPRACTSPEC.practitioner_spclty_description as adm_pract_spclty_descr
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
	,ACO_FIPSADR.fips_ste_descr
	,ZIPCODE.cnty_fips_nm AS ptnt_cnty_fips_cd
	,ZIPCODE.cnty_nm AS ptnt_cnty_nm
	,ACO_FIPSADR.fips_cnty_descr
	,ZIPCODE.ste_cd AS std_ste_cd
	,ZIPCODE.ste_descr AS std_ste_descr
	,ZIPCODE.rgon_descr AS std_rgon_descr
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
	,ephf.prim_hcpcs_cd
	,ephf.prim_hcpcs_descr
	,ephf.prim_ccs_hcpcs_cgy_cd
	,ephf.prim_ccs_hcpcs_cgy_descr
	,ephf.prim_betos_cd
	,ephf.prim_betos_descr
	,ephf.prim_betos_cgy_nm
	--CODE CHANGE : MLH-591: Added Maintenance Cancer Service Indicator
    ,CANCER.maint_cancer_case_ind
--	, CASE WHEN ZOOM.inpatient_outpatient_flag = 'I' AND UPPER(practitioner_spclty_description) in 
--	(
--	'CARDIOVASCULAR DISEASE (CARDIOLOGY)',
--	'INTERVENTIONAL CARDIOLOGY',
--	 'CARDIAC SURGERY',
--	 'THORACIC SURGERY',
--	 'CARDIAC ELECTROPHYSIOLOGY'
--	) THEN 1 ELSE 0 END AS  phys_specl_valid_ind 
FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_temp_eligible_encntr_data ZOOM3YRS
on ZOOM.company_id = ZOOM3YRS.company_id and ZOOM.patient_id = ZOOM3YRS.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_chrg_cost_fct CHRGRCC
on ZOOM.company_id = CHRGRCC.fcy_nm and ZOOM.patient_id = CHRGRCC.encntr_num
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_qly_anl_fct QADV ON ZOOM.patient_id = QADV.encntr_num AND QADV.ptnt_cl_cd = ZOOM.inpatient_outpatient_flag--AND ZOOM.company_id = QADV.fcy_num 
LEFT JOIN pce_qe16_oper_prd_zoom..cv_empi empi on ZOOM.company_id = empi.company_id and ZOOM.patient_id=empi.patient_id 
LEFT JOIN encntr_prim_hcpcs_fct ephf on ZOOM.patient_id = ephf.encntr_num
LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY ON VSET_FCY.cd = ZOOM.company_id
	AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
LEFT JOIN pce_qe16_slp_prd_dm..stnd_fcy_demog_dim FCY_REF ON VSET_FCY.alt_cd = FCY_REF.fcy_num
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
--LEFT JOIN pce_qe16_slp_prd_dm..stnd_ptnt_type_dim STNDPTNTTYPE ON STNDPTNTTYPE.std_encntr_type_Cd = PATTYPEMAP.standard_patient_type_code
LEFT JOIN pce_qe16_slp_prd_dm..stnd_adm_type_dim ADMTYPE ON ADMTYPE.adm_type_cd = ZOOM.admission_type_visit_type
LEFT JOIN pce_qe16_slp_prd_dm..stnd_adm_src_dim ADMSRC ON ADMSRC.adm_src_cd = ZOOM.point_of_origin_for_admission_or_visit
LEFT JOIN intermediate_stage_temp_discharge_fcy_std_status_code DISSTATUS ON DISSTATUS.discharge_status = ZOOM.discharge_status
LEFT JOIN pce_qe16_slp_prd_dm..stnd_ptnt_zip_dim ZIPCODE ON ZIPCODE.ptnt_zip_cd = substr(ZOOM.residential_zip_code, 1, 5)
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
LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_MARITAL ON VSET_MARITAL.cd = ZOOM.marital_status
	AND VSET_MARITAL.cohrt_id = 'MARITAL_STATUS'
LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_GENDER ON VSET_GENDER.cd = ZOOM.sex
	AND VSET_GENDER.cohrt_id = 'GENDER'
LEFT JOIN pce_qe16_prd_qadv..race_cd_ref QADV_RACE ON QADV_RACE.race_cd = ZOOM.race
LEFT JOIN pce_qe16_slp_prd_dm..ms_drg_dim ACO_MSDRG ON ACO_MSDRG.ms_drg_cd = ZOOM.msdrg_code
LEFT JOIN pce_qe16_slp_prd_dm..fips_adr_dim ACO_FIPSADR ON ACO_FIPSADR.fips_cnty_cd = substr(ZIPCODE.cnty_fips_ste_nm, 3, 3)
	AND ACO_FIPSADR.fips_ste_cd = substr(ZIPCODE.cnty_fips_ste_nm, 1, 2)
LEFT JOIN pce_qe16_slp_prd_dm..stnd_pnt_of_orig_ref QADV_POO ON QADV_POO.pnt_of_orig_cd = ZOOM.point_of_origin_for_admission_or_visit
LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNSDIM ON DGNSDIM.dgns_alt_cd = replace(ZOOM.primaryicd10diagnosiscode,'.','') and DGNSDIM.dgns_icd_ver ='ICD10' 
LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim ADMDGNS ON ADMDGNS.dgns_alt_cd = replace(ZOOM.admitdiagnosiscode,'.','') AND ADMDGNS.dgns_icd_ver ='ICD10'
LEFT JOIN pce_qe16_slp_prd_dm..pcd_dim PCDDIM ON PCDDIM.icd_pcd_cd = ZOOM.primaryicd10procedurecode and PCDDIM.icd_ver='ICD10'
LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_ETHCTY ON VSET_ETHCTY.cd = ZOOM.ethnicity_code
	AND VSET_ETHCTY.cohrt_id = 'ETHNICITY'
LEFT JOIN intermediate_stage_temp_encntr_svc_hier SVCRNK
on SVCRNK.company_id = ZOOM.company_id and SVCRNK.patient_id = ZOOM.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_temp_surgeon_pract SURGEON
on SURGEON.company_id = ZOOM.company_id and SURGEON.patient_id = ZOOM.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_cnslt_pract_fct CNSLT
on CNSLT.fcy_nm = ZOOM.company_id and CNSLT.encntr_num = ZOOM.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_net_rvu_fct NETREV
on NETREV.fcy_nm = ZOOM.company_id and NETREV.encntr_num = ZOOM.patient_id
LEFT JOIN ms_drg_dim MSDRGDIM
on MSDRGDIM.ms_drg_cd = CAST(LPAD(CAST(coalesce(ZOOM.msdrg_code,'000') as INTEGER), 3,0 ) as Varchar(3)) AND MSDRGDIM.ms_drg_type_cd IN ('SURG','MED','OTH') 
LEFT JOIN  pce_qe16_slp_prd_dm..intermediate_stage_temp_specl_valid_ind SPECLVALID
on SPECLVALID.fcy_nm = ZOOM.company_id and ZOOM.patient_id = SPECLVALID.encntr_num 
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_temp_encntr_dgns_fct_with_cancer_case CANCER
on CANCER.encntr_num = ZOOM.patient_id AND CANCER.fcy_nm = ZOOM.company_id
--CODE CHANGE : AUG 2019 Blood and Lab Utilization
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_temp_blood_util_qty BLOOD
on BLOOD.encntr_num = ZOOM.patient_id AND BLOOD.fcy_nm = ZOOM.company_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_temp_lab_util_qty LAB
on LAB.encntr_num = ZOOM.patient_id AND LAB.fcy_nm = ZOOM.company_id
---------
---LEFT JOIN pce_qe16_slp_prd_dm..client_drg_svc_line_grouper_new CLIENTDRG
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
lEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_chrg_agg_fct CFAGG
on  CFAGG.fcy_nm = ZOOM.company_id AND CFAGG.encntr_num = ZOOM.patient_id 
lEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_ptnt_fnc_txn_agg_fct PFAGG
on  PFAGG.fcy_nm = ZOOM.company_id AND PFAGG.encntr_num = ZOOM.patient_id 														  																		  
WHERE coalesce(ZOOM.msdrg_code ,'000') NOT IN ('V45','V70','V67','V04')
--WHERE ZOOM.discharge_total_charges > 0
--	AND cast(ZOOM.admission_ts AS DATE) BETWEEN add_months(CURRENT_DATE, - 36)
--		AND CURRENT_DATE
--DISTRIBUTE ON (fcy_nm_hash,encntr_num_hash);
DISTRIBUTE ON (fcy_nm, encntr_num);

-------------------------------------- SERVICE LINE Updates--------------------

DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct AS
SELECT ef.*,
case when ef.in_or_out_patient_ind = 'I ' then ef.e_mclaren_major_slp_grouping ELSE ef.e_svc_ln_nm END as cal_svc_ln,
case when cal_svc_ln is null then null else 
COALESCE(case  
when ef.in_or_out_patient_ind = 'I ' and (ef.e_mclaren_major_slp_grouping <> ef.e_svc_ln_nm) then 'Other'
ELSE ef.e_sub_svc_ln_nm END,'Other') end as cal_sub_svc_ln,
case when ef.in_or_out_patient_ind = 'I ' and (ef.e_mclaren_major_slp_grouping <> nvl(ef.e_svc_ln_nm,' ')) then 'Other' ELSE ef.e_svc_nm END as cal_svc_nm

FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct_temp ef
DISTRIBUTE ON (fcy_nm, encntr_num);
				
--------------------------
--July 2020: Oncology Related : High Emeto Genic and Anti-emetic cases 
--select 'processing table: intermediate_stage_high_emeto_antiemetic_cases ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_high_emeto_antiemetic_cases IF EXISTS; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_high_emeto_antiemetic_cases AS 
with chrg_fct_J9070_J9080_only
AS 
(select Z.fcy_nm as fcy_nm, Z.encntr_num as encntr_num, Z.cpt_Code, SUM(Z.quantity) as qty
, CASE when Z.cpt_code = 'J9070' THEN 1 END AS code1
, CASE when Z.cpt_code = 'J9080' THEN 1 END AS code2
FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct Z WHERE Z.cpt_Code IN ('J9070','J9080')
GROUP BY 1,2,3
)
select EF.fcy_nm,EF.fcy_num,  EF.encntr_num ,DATE(EF.dschrg_dt) as dschrg_dt,
     MAX(CASE when Z.cpt_code in ('J9050') THEN 1 
     when Z.cpt_code in ('J9060') THEN 1 
     WHEN Z.cpt_Code in('J9070')  AND X.cpt_code = 'J9070' AND X.qty >=23 THEN 1
	 when Z.cpt_code in ('J9080') AND X.cpt_code = 'J9080' AND  X.qty >=12 THEN 1 
     when Z.cpt_code in ('J9120') THEN 1 
	 when Z.cpt_code in ('J9000') AND X.cpt_code = 'J9070' and X.qty < 23 THEN 1 
     when Z.cpt_code in ('J9178') AND X.cpt_code = 'J9070' and X.qty < 23 THEN 1 
	 when Z.cpt_code in ('J9130') THEN 1 
     when Z.cpt_code in ('J9230') THEN 1 
	 when Z.cpt_code in ('J9320') THEN 1
    ELSE 0 END)  as high_emeto_cases,
	MAX(case when Z.cpt_code ='J0185' THEN 1
	         WHEN Z.cpt_code ='J8501' THEN 1
			 WHEN Z.cpt_code ='J1453' THEN 1 
			 WHEN UPPER(Z.persp_clncl_dtl_descr) = 'OLANZAPINE INJ 10MG' THEN 1 
			 WHEN UPPER(Z.persp_clncl_dtl_descr) = 'OLANZAPINE TAB 5MG' THEN 1 ELSE 0 END) as antimetic_cases,
    high_emeto_cases as highemeto_antiecases_denominator,  -- grand_Total
	case when high_emeto_cases = 1 AND antimetic_cases =1 THEN 1 ELSE 0 END as cases_with_antiemetic , --- numerator,
    case when high_emeto_cases = 1 AND  antimetic_cases =0  THEN 1 ELSE 0 END as cases_without_antiemetic --numerator_2
from pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct Z
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct EF
on EF.encntr_num = Z.encntr_num AND EF.fcy_nm = Z.fcy_nm 
LEFT JOIN chrg_fct_J9070_J9080_only X
ON X.fcy_nm = Z.fcy_nm and X.encntr_num = Z.encntr_num 
WHERE
(UPPER(Z.cpt_code ) IN ('J9050','J9060','J9070','J9080','J9120','J9000','J9178','J9130','J9230','J9320', 'J0185','J8501','J1453') 
OR UPPER(Z.persp_clncl_dtl_descr) IN ('OLANZAPINE INJ 10MG','OLANZAPINE TAB 5MG'))
GROUP By 1,2 ,3,4;

--July 2020 : Outpatient Chemotherapy logic added into SLP encntr_anl_Fct
------Chemotherapy Patients meeting inclusion criteria From Oct 1st 2016
--Encounter LEvel (Inclusion)
--select 'processing table: intermediate_stage_op_chemo_visits_all_inclusions ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_all_inclusions IF EXISTS; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_all_inclusions AS 
select fcy_nm, encntr_num , dschrg_dt,
--1 as inclusion_ind , 
max(chemo_proc_ind)  as  chemo_denom_incl_proc_ind,
max(chemo_dgns_ind) as chemo_denom_incl_dgns_ind, 
max(chemo_encntr_ind) as chemo_denom_incl_encntr_ind,
max(chemo_med_ind) as chemo_denom_incl_med_ind,
CASE 
 WHEN ( (max(chemo_proc_ind) =1 AND max(chemo_dgns_ind)   =1 ) AND ( max(chemo_encntr_ind) >=0 OR max(chemo_med_ind) >=0 ) ) THEN 1    -- (Tab 13 & Tab 14) c
 WHEN ( (max(chemo_dgns_ind) =1 AND max(chemo_med_ind)    =1 ) ) THEN 1    -- (Tab 13 & Tab 16) 
 WHEN ( (max(chemo_dgns_ind) =1 AND max(chemo_encntr_ind) =1 ) ) THEN 1    -- (Tab 13 & Tab 15) 
ELSE 0 END AS inclusion_ind
FROM 
(  
--Tab # 13 - Denominator - Cancer Diagnosis 
select distinct Z.fcy_nm, Z.encntr_num , Z.dschrg_dt ,  0 as chemo_proc_ind , 1 as chemo_dgns_ind, 0 as chemo_encntr_ind ,0 as chemo_med_ind
FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18  AND Z.prim_Dgns_cd in (
select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ('cancer_icd10') ) UNION ALL 

select distinct Z.fcy_nm, Z.encntr_num , Z.dschrg_dt ,  0 as chemo_proc_ind , 1 as chemo_dgns_ind ,0 as chemo_encntr_ind ,0 as chemo_med_ind
FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct DF
on Z.fcy_nm = DF.fcy_nm AND Z.encntr_num = DF.encntr_num 
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18  AND DF.icd_version ='ICD10' AND DF.icd_code  IN (
select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ('cancer_icd10') ) UNION ALL 

--Tab # 14 - Denominator - Chemo Procedure (CPT) 
select  distinct CF.fcy_nm as fcy_nm, CF.encntr_num as encntr_num, Z.dschrg_dt , 1 as chemo_proc_ind , 0 as chemo_dgns_ind, 0 as chemo_encntr_ind  ,0 as chemo_med_ind 
FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct CF 
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
on Z.encntr_num = CF.encntr_num AND Z.fcy_nm = CF.fcy_nm 
where Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18 AND  CF.cpt_code IN  
(select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim where cohrt_id ='chemo_proc_cpt')   UNION ALL
 
--Tab # 14 - Denominator - Chemo Procedure (ICD-10) 
select distinct Z.fcy_nm, Z.encntr_num, Z.dschrg_dt, 1 as chemo_proc_ind , 0 as chemo_dgns_ind, 0 as chemo_encntr_ind ,0 as chemo_med_ind
FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18 AND Z.prim_pcd_cd in (
select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ('chemo_proc_icd10') ) UNION ALL 

select distinct Z.fcy_nm, Z.encntr_num , Z.dschrg_dt ,  1 as chemo_proc_ind , 0 as chemo_dgns_ind, 0 as chemo_encntr_ind  ,0 as chemo_med_ind
FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct DF
on Z.fcy_nm = DF.fcy_nm AND Z.encntr_num = DF.encntr_num 
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18  AND DF.icd_version ='ICD10' AND DF.icd_code  IN (
select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ('chemo_proc_icd10') ) UNION ALL 

--Tab # 14 - Denominator - Chemo Procedure (Revenue Code) 
select  distinct CF.fcy_nm as fcy_nm, CF.encntr_num as encntr_num ,  Z.dschrg_dt, 1 as chemo_proc_ind , 0 as chemo_dgns_ind, 0 as chemo_encntr_ind   ,0 as chemo_med_ind
FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct CF 
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
on Z.encntr_num = CF.encntr_num AND Z.fcy_nm = CF.fcy_nm 
where Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18 AND  CF.revenue_code IN   
(select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim where cohrt_id = 'chemo_proc_revcd')  UNION ALL 

--Tab # 15 - Denominator - Chemo Encouneter (ICD 10 Code)   
select distinct Z.fcy_nm, Z.encntr_num , Z.dschrg_dt , 0 as chemo_proc_ind , 0 as chemo_dgns_ind, 1 as chemo_encntr_ind ,0 as chemo_med_ind
FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18  AND Z.prim_Dgns_cd in (
select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ( 'chemo_encntr_cpt') ) UNION ALL 

select distinct Z.fcy_nm, Z.encntr_num , Z.dschrg_dt , 0 as chemo_proc_ind , 0 as chemo_dgns_ind, 1 as chemo_encntr_ind  ,0 as chemo_med_ind
FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct DF
on Z.fcy_nm = DF.fcy_nm AND Z.encntr_num = DF.encntr_num 
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18  AND DF.icd_version ='ICD10' AND DF.icd_code  IN (
select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ('chemo_encntr_cpt') ) UNION ALL 

--Tab # 16 - Denominator - Chemo Mediciene (HCPCS Code)
select  distinct CF.fcy_nm as fcy_nm, CF.encntr_num as encntr_num, Z.dschrg_dt , 0 as chemo_proc_ind , 0 as chemo_dgns_ind, 0 as chemo_encntr_ind  ,1 as chemo_med_ind 
FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct CF 
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
on Z.encntr_num = CF.encntr_num AND Z.fcy_nm = CF.fcy_nm 
where Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.age_in_yr >= 18 AND  CF.cpt_code IN  
(select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim where cohrt_id ='chemo_medicine_cpt')


) X
GROUP BY 1, 2,3;

--Cohrt Exclusions --Encounter Level (Exclusion)  
--Encounter with Non-cancer  pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_noncancer
--Encounters with Lekumia pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_lekumia
--Encounters with AutoImmune  pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_autoimmune
--select 'processing table:  intermediate_stage_op_chemo_visits_exclusions_lekumia' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_lekumia IF EXISTS; 
CREATE  TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_lekumia AS 
select distinct fcy_nm, encntr_num , 1 as lekumia_ind FROM 
(
select distinct Z.fcy_nm, Z.encntr_num  FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.prim_Dgns_cd in (
select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ('lekumia_icd10') )
UNION ALL
select distinct EF.fcy_nm, EF.encntr_num  FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct EF
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct PF
on EF.fcy_nm= PF.fcy_nm and EF.encntr_num = PF.encntr_num 
WHERE EF.in_or_out_patient_ind = 'O' AND DATE(EF.dschrg_dt) >= '2016-10-01' AND PF.icd_version ='ICD10' AND PF.icd_code  IN 
(select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ('lekumia_icd10') and opr_typ_nm = 'NOT IN')) X; 

--Encounters with AutoImmune  pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_autoimmune
--select 'processing table:  intermediate_stage_op_chemo_visits_exclusions_autoimmune' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_autoimmune IF EXISTS; 
CREATE  TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_autoimmune AS 
select distinct fcy_nm, encntr_num , 1 as autoimmune_ind  FROM 
(
select distinct Z.fcy_nm, Z.encntr_num  FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' AND Z.prim_Dgns_cd in (
select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ('autoImmune_icd10') and opr_typ_nm = 'NOT IN' )
UNION ALL
select distinct EF.fcy_nm, EF.encntr_num  FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct EF
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct PF
on EF.fcy_nm= PF.fcy_nm and EF.encntr_num = PF.encntr_num 
WHERE EF.in_or_out_patient_ind = 'O' AND DATE(EF.dschrg_dt) >= '2016-10-01' AND PF.icd_version ='ICD10' AND PF.icd_code  IN 
(select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ('autoImmune_icd10') and opr_typ_nm = 'NOT IN')) X; 

--Encounters with Non-Cancer
--select 'processing table: intermediate_stage_op_chemo_visits_exclusions_noncancer ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_noncancer IF EXISTS; 
CREATE  TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_noncancer AS 
--'chemoNonCancer_icd10'
select distinct fcy_nm, encntr_num , 1 as noncancer_ind FROM 
(
select distinct Z.fcy_nm, Z.encntr_num   FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
WHERE Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' 
AND Z.prim_Dgns_cd in ( select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ('chemoNonCancer_icd10') and opr_typ_nm = 'NOT IN' )
UNION ALL
select distinct EF.fcy_nm, EF.encntr_num  FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct EF
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct PF
on EF.fcy_nm= PF.fcy_nm and EF.encntr_num = PF.encntr_num 
WHERE EF.in_or_out_patient_ind = 'O' AND DATE(EF.dschrg_dt) >= '2016-10-01' 
AND PF.icd_version ='ICD10' AND PF.icd_code IN (select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim WHERE cohrt_id IN ('chemoNonCancer_icd10') and opr_typ_nm = 'NOT IN')
UNION ALL
select  distinct CF.fcy_nm as fcy_nm, CF.encntr_num as encntr_num  FROM pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct CF 
INNER JOIN pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct Z
on Z.encntr_num = CF.encntr_num AND Z.fcy_nm = CF.fcy_nm 
where Z.in_or_out_patient_ind = 'O' AND DATE(Z.dschrg_dt) >= '2016-10-01' 
AND CF.cpt_code IN  (select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim where cohrt_id IN ('chemoNonCancer_hcpcs',  'chemoNonCancer_cpt'))
) X;
------Chemotherapy Patients meeting Exclusion criteria From Oct 1st 2016
--Encounter LEvel (Exclusion)
--select 'processing table: intermediate_stage_op_chemo_visits_all_exclusions ' as table_processing;
--DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_all_exclusions IF EXISTS; 
--CREATE  TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_all_exclusions AS 
--select * , 1 as exclusion_ind from 
--(select * from pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_lekumia UNION ALL 
--select * from pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_noncancer UNION ALL 
--select * from pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_autoimmune)  X ; 

--select 'processing table: intermediate_stage_op_chemo_visits_all_exclusions ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_all_exclusions IF EXISTS; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_all_exclusions AS 
select 
NON.fcy_nm, NON.encntr_num,
NON.noncancer_ind as chemo_denom_excl_noncancer_ind, 
LK.lekumia_ind as  chemo_denom_excl_lekumia_ind , 
AI.autoimmune_ind as  chemo_denom_excl_autoimmune_ind,
--CASE WHEN (NON.noncancer_ind=1 OR LK.lekumia_ind =1 OR  AI.autoimmune_ind=1) THEN 1 ELSE 0 END as exclusion_ind
CASE WHEN ((NON.noncancer_ind=1 AND AI.autoimmune_ind=1) OR LK.lekumia_ind =1 ) THEN 1 ELSE 0 END as exclusion_ind
FROM pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_noncancer NON 
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_lekumia LK
on LK.fcy_nm = NON.fcy_nm AND LK.encntr_num  = NON.encntr_num 
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_autoimmune AI
on NON.fcy_nm = AI.fcy_nm AND NON.encntr_num  = AI.encntr_num ;

--Logic : Combine all-inclusions and all-exclusions Encounters and baseline with Encntr_anl_Fct Table 
--select 'processing table:  intermediate_stage_op_chemo_visits_denominator' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_denominator IF EXISTS; 
CREATE  TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_denominator AS 
select distinct EF.fcy_nm, EF.encntr_num, EF.dschrg_dt, EF.medical_record_number, EF.in_or_out_patient_ind, 
CASE WHEN EF.in_or_out_patient_ind = 'O' THEN nvl(ALLIN.inclusion_ind,0) else ALLIN.inclusion_ind END as chemo_denom_incl_ind ,
CASE WHEN EF.in_or_out_patient_ind = 'O' THEN nvl(ALLEX.exclusion_ind,0) else ALLEX.exclusion_ind END as chemo_denom_excl_ind ,
CASE WHEN (EF.in_or_out_patient_ind ='O' AND (ALLIN.inclusion_ind =1 AND  ALLEX.exclusion_ind=1)) THEN 0
     WHEN (EF.in_or_out_patient_ind ='O' AND (ALLIN.inclusion_ind =1 AND  ALLEX.exclusion_ind=0)) THEN 1
	 WHEN (EF.in_or_out_patient_ind ='O' AND (ALLIN.inclusion_ind =0 AND  ALLEX.exclusion_ind=0)) THEN 0
	 WHEN (EF.in_or_out_patient_ind ='O' AND (ALLIN.inclusion_ind =0 AND  ALLEX.exclusion_ind=1)) THEN 0
	 WHEN EF.in_or_out_patient_ind ='I' THEN NULL 
     ELSE 0 END as chemo_denom_ind,
--CODE CHANGE: 09/10/2020 Modified 
ALLIN.chemo_denom_incl_proc_ind, 
ALLIN.chemo_denom_incl_dgns_ind, 
ALLIN.chemo_denom_incl_encntr_ind, 
ALLIN.chemo_denom_incl_med_ind,
ALLEX.chemo_denom_excl_noncancer_ind,
ALLEX.chemo_denom_excl_lekumia_ind,
ALLEX.chemo_denom_excl_autoimmune_ind
FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct EF
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_all_inclusions ALLIN
on ALLIN.fcy_nm = EF.fcy_nm AND ALLIN.encntr_num = EF.encntr_num
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_all_exclusions ALLEX
on EF.fcy_nm = ALLEX.fcy_nm AND EF.encntr_num = ALLEX.encntr_num;


--Logic : Identify all the IP visits based on the OP chemo visits Patient's MRN 
--select 'processing table:  intermediate_stage_chemo_ip_visits_for_op_chemo' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_chemo_ip_visits_for_op_chemo IF EXISTS; 
CREATE  TABLE pce_qe16_slp_prd_dm..intermediate_stage_chemo_ip_visits_for_op_chemo AS 
with op_mrn as 
(select  Z.medical_record_number as mrn FROM pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_denominator Z
WHERE Z.in_or_out_patient_ind = 'O' AND Z.chemo_denom_ind = 1  ),
ip_records as 
(select * from pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct EF 
INNER JOIN op_mrn 
ON op_mrn.mrn = EF.medical_record_number
WHERE EF.in_or_out_patient_ind = 'I' AND EF.prim_dgns_Cd in (select cd from pce_qe16_slp_prd_dm..chemo_val_set_dim where val_set_nm = 'Inpatient Chemo' )
)
select * FROM 
(select ip_records.fcy_nm, ip_records.encntr_num, ip_records.medical_record_number, ip_records.in_or_out_patient_ind, ip_records.dschrg_dt, 0 as chemo_denom_ind, 1 as chemo_numer_ind, ip_records.ed_case_ind
FROM ip_records
UNION 
select Z.fcy_nm, Z.encntr_num, Z.medical_record_number, Z.in_or_out_patient_ind, Z.dschrg_dt, Z.chemo_denom_ind, 0 as chemo_numer_ind, 0 as ed_case_ind
FROM pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_denominator Z 
INNER JOIN ip_records ON Z.medical_record_number = ip_records.medical_record_number
WHERE Z.in_or_out_patient_ind = 'O' aND  Z.chemo_denom_ind = 1 )  X 
ORDER BY X.medical_record_number, X.in_or_out_patient_ind desc, X.dschrg_dt; 

--Logic to tie Op visits with the Ip visits / ED visits which are occurred after on or  30 days of Op chemo visits 
--select 'processing table:  intermediate_stage_encntr_oncology_anl_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_oncology_anl_fct IF EXISTS; 
CREATE  TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_oncology_anl_fct AS 
with op_chemo_30_days_ip_or_ed_visits as 
(select fcy_nm, encntr_num, medical_record_number, in_or_out_patient_ind, dschrg_dt, chemo_denom_ind, chemo_numer_ind, ed_case_ind, 
case when nxt_encntr_dschrg_dt-dschrg_dt <= 30 and nxt_encntr_type='I' then 1 else 0 end as ip_visit_after_30_days_of_op_chemo_ind,
case when nxt_encntr_dschrg_dt-dschrg_dt <= 30 and nxt_encntr_type='I' and nxt_encntr_ed_case_ind =1 then 1 else 0 end as ed_visit_after_30_days_of_op_chemo_ind 
from
(
SELECT *,
lead(dschrg_dt,1) over (partition by medical_record_number order by dschrg_dt )as nxt_encntr_dschrg_dt,
lead(in_or_out_patient_ind,1) over (partition by medical_record_number order by dschrg_dt )as nxt_encntr_type,
lead(ed_case_ind,1) over (partition by medical_record_number order by dschrg_dt )as nxt_encntr_ed_case_ind
FROM pce_qe16_slp_prd_dm..intermediate_stage_chemo_ip_visits_for_op_chemo Z 
)a)
select X.*,
CASE WHEN X.in_or_out_patient_ind = 'O' THEN nvl(chemo_numer_ind,0) ELSE 0 END  as chemo_numer_ind,
nvl(ed_case_ind, 0) as ed_case_ind ,
CASE WHEN X.in_or_out_patient_ind = 'O' THEN nvl(ip_visit_after_30_days_of_op_chemo_ind,0) ELSE 0 END as ip_visit_after_30_days_of_op_chemo_ind,
CASE WHEN X.in_or_out_patient_ind = 'O' THEN nvl(ed_visit_after_30_days_of_op_chemo_ind,0) ELSE 0 END as ed_visit_after_30_days_of_op_chemo_ind,
nvl(Z.cases_without_antiemetic,0) as cases_without_antiemetic_ind,
nvl(Z.cases_with_antiemetic,0) as cases_with_antiemetic_ind,
nvl(Z.highemeto_antiecases_denominator,0) as highemeto_antiecases_denom_ind
FROm pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_denominator X
LEFT JOIN op_chemo_30_days_ip_or_ed_visits Y
on X.fcy_nm = Y.fcy_nm AND X.encntr_num = Y.encntr_num AND X.medical_record_number = Y.medical_record_number 
--Adding Highly Emeto Geneic and Anti-emetic Case INDICATOR details
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_stage_high_emeto_antiemetic_cases Z
on X.fcy_nm = Z.fcy_nm and X.encntr_num = Z.encntr_num ; 

--DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_all_inclusions IF EXISTS; 

--DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_lekumia IF EXISTS; 

--DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_autoimmune IF EXISTS; 

--DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_exclusions_noncancer IF EXISTS; 

--DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_all_exclusions IF EXISTS; 

--DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_op_chemo_visits_denominator IF EXISTS; 

--DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_chemo_ip_visits_for_op_chemo IF EXISTS; 

--DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_high_emeto_antiemetic_cases IF EXISTS; 
--------------------------

  create temp table tmp_lung_cancer_fct as 
( SELECT distinct ef.medical_record_number, ef.fcy_num, ef.fcy_nm, ef.encntr_num, 1 as lung_cancer_scrn_ind
  FROM pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct ef
  inner join pce_qe16_slp_prd_dm..intermediate_chrg_fct cf on ef.encntr_num = cf.patient_id
  inner join pce_qe16_slp_prd_dm..val_set_dim vd on cf.charge_code = vd.cd and cf.fcy_num=vd.val_set_nm and vd.cohrt_id = 'LUNG_SCREEN_CHRG_CD');

create temp table tmp_robo_encntr_fct as 
(SELECT distinct epf.patient_id as encntr_num
FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct epf
inner join pce_qe16_slp_prd_dm..val_set_dim vd on epf.icd_code = vd.cd and  vd.cohrt_id = 'ROBO_SURG_PCD_CD'
left join pce_qe16_prd..icd_pcd_cd_dim ipd on epf.icd_code = ipd.icd_pcd_cd

union 

SELECT distinct ef.encntr_num
  FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct ef
  inner join pce_qe16_slp_prd_dm..intermediate_stage_cpt_fct cpf on ef.encntr_num = cpf.patient_id
  inner join pce_qe16_slp_prd_dm..val_set_dim vd on cpf.cpt_code = vd.cd and vd.cohrt_id='ROBO_SURG_CPT_CD'
  
 union
  
  SELECT distinct ef.encntr_num
  FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct ef
  inner join pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct cf on ef.encntr_num = cf.patient_id
  inner join pce_qe16_slp_prd_dm..val_set_dim vd on cf.persp_clncl_smy_cd = vd.cd and vd.cohrt_id='ROBO_SURG_SPL_SMY_CD'
  
  
  union
  
  SELECT distinct ef.encntr_num
  FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct ef
  inner join pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct cf on ef.encntr_num = cf.patient_id
  inner join pce_qe16_slp_prd_dm..val_set_dim vd on cf.charge_code = vd.cd and vd.cohrt_id='ROBO_SURG_CHRG_CD');
  
  
  
  
 create temp table tmp_sub_encntr_fct as 
 (SELECT distinct pf.patient_id,
 case when vd.cd is null then vdd.cd else vd.cd end as pcd_cd_sub,
 case when pcd_cd_sub is null then pf.icd_code else pcd_cd_sub  end as pcd_cd,
 case when vd.cd_descr is null then vdd.cd_descr else vd.cd_descr end as pcd_descr_sub,
 case when pcd_descr_sub is null then pd.icd_pcd_descr else pcd_descr_sub  end as pcd_descr,
 case when pcd_descr_sub is not null then 1 else 2 end as pcd_row
 FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct pf
 left join pce_qe16_prd..icd_pcd_cd_dim pd on pf.icd_code = pd.icd_pcd_cd and PF.icd_type='P'
 left join pce_qe16_slp_prd_dm..val_set_dim vd on pf.icd_code = vd.cd and vd.cohrt_id in ('GASTROENTEROSTOMY_PCD','HYSTERECTOMY_PCD','LOBECTOMY_PCD','NEPHRECTOMY_PCD','PROSTATECTOMEY_PCD','ROBOTIC_ASSISTED_PROCEDURES')
 AND PF.icd_type='P'
 left join pce_qe16_slp_prd_dm..val_set_dim vdd on pf.icd_code = vdd.cd and vdd.cohrt_id =  'ROBOTIC_ASSISTED_PROCEDURES' and pf.icd_type='S');
 
 create temp table tmp_postop_sep as 
(  SELECT distinct ef.encntr_num, 1 as postop_sep_ind
  FROM pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct ef
  inner join pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct ed on ef.encntr_num = ed.patient_id
  inner join pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct ep on ef.encntr_num = ep.patient_id
  inner join pce_qe16_slp_prd_dm..ahrq_val_set_dim avd on replace(ed.icd_code,'.','')=avd.cd  and avd.cohrt_id='SEPTI2D' and  ed.diagnosistype not in ( 'Primary','Principal','Admission','Admission Diagnosis','Admitting')
  inner join pce_qe16_prd..ms_drg_dim mdd on ef.ms_drg_cd = mdd.ms_drg_cd
  inner join pce_qe16_slp_prd_dm..ahrq_val_set_dim mvd on mdd.ms_drg_cd= mvd.cd and mvd.cohrt_id='SURGI2R'
  inner join pce_qe16_slp_prd_dm..ahrq_val_set_dim pvd on ep.icd_code = pvd.cd and pvd.cohrt_id = 'ORPROC'
  where mdd.ms_drg_mdc_cd<>'14'  and ef.in_or_out_patient_ind =  'I ' );
  
  CREATE TEMP TABLE TMP_ORPROC AS
(SELECT distinct ep.patient_id, 1 as orproc_ind
FROM pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct ep
inner join pce_qe16_slp_prd_dm..ahrq_val_set_dim vd on ep.icd_code = vd.cd and vd.cohrt_id = 'ORPROC');

CREATE TEMP TABLE TMP_infectid AS
(

SELECT distinct ef.ENCNTR_NUM, 1 as postop_infectid_ind
FROM pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct EF
INNER JOIN pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct ED ON ef.encntr_num = ed.patient_id
INNER JOIN pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct ep ON EF.ENCNTR_NUM = EP.PATIENT_ID
inner join pce_qe16_slp_prd_dm..ahrq_val_set_dim vd on ep.icd_code = vd.cd and vd.cohrt_id = 'ORPROC'
INNER JOIN pce_qe16_slp_prd_dm..ahrq_val_set_dim VDD ON replace(ed.icd_code,'.','') = vdd.cd and vdd.cohrt_id=  'INFECID'

);
 
 --select 'processing table: encntr_msr_fct ' as table_processing;
DROP TABLE encntr_msr_fct if exists;
 create table encntr_msr_fct as
 (select q.* from
 (select distinct ef.encntr_num,
 tsef.pcd_cd,
 tsef.pcd_descr,
 row_number() over (partition by ef.encntr_num order by tsef.pcd_row asc ) as row_num,
max(case when tlcf.lung_cancer_scrn_ind is null then 0 else tlcf.lung_cancer_scrn_ind end) as lung_cancer_scrn_ind ,
 max(case when tref.encntr_num is null then 0 else 1 end) as robotic_srgy_ind,
 max(case when tpsep.encntr_num is null then 0 else 1 end) as postop_sep_ind,
  max(case when torproc.patient_id is null then 0 else 1 end) as orproc_ind,
   max(case when tinf.encntr_num is null then 0 else 1 end) as postop_infectid_ind
 
 from pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct ef
 left join tmp_sub_encntr_fct tsef on ef.encntr_num= tsef.patient_id
 left join tmp_lung_cancer_fct tlcf on ef.encntr_num = tlcf.encntr_num
 left join tmp_robo_encntr_fct tref on ef.encntr_num = tref.encntr_num
 left join tmp_postop_sep  tpsep on ef.encntr_num=tpsep.encntr_num
 left join TMP_ORPROC torproc on ef.encntr_num= torproc.patient_id
 left join tmp_infectid tinf on ef.encntr_num = tinf.encntr_num
 where ef.encntr_num is not null
group by ef.encntr_num,tsef.pcd_cd,tsef.pcd_descr,tsef.pcd_row 
 
 ) as q
 where q.row_num=1) ;
-------------------
 CREATE TEMP TABLE intermediate_stage_encntr_anl_fct_new AS 
 Select ef.*, em.lung_cancer_scrn_ind, em.robotic_srgy_ind ,em.postop_sep_ind,em.orproc_ind,em.postop_infectid_ind
 FROM pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct ef
 LEFT JOIN pce_qe16_slp_prd_dm..encntr_msr_fct em on ef.encntr_num = em.encntr_num;

 --select 'processing table: intermediate_stage_encntr_anl_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct IF EXISTS;
 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct AS SELECT * FROM intermediate_stage_encntr_anl_fct_new;
 
 -------
 
--select 'processing table:  intermediate_chrg_cost_fct_prev' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_cost_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct;

--select 'processing table: intermediate_spl_dim_prev ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_spl_dim_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_spl_dim RENAME TO pce_qe16_slp_prd_dm..intermediate_spl_dim_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_spl_dim  RENAME TO pce_qe16_slp_prd_dm..intermediate_spl_dim;

--select 'processing table:  intermediate_chrg_fct_prev' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_chrg_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_chrg_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_chrg_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_chrg_fct;

--select 'processing table: intermediate_encntr_cnslt_pract_fct_prev ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_cnslt_pract_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct;

--select 'processing table: intermediate_encntr_dgns_fct_prev ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct;

--select 'processing table: intermediate_encntr_pcd_fct_prev ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct;

--select 'processing table:  intermediate_encntr_net_rvu_fct_prev' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_net_rvu_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct;

--select 'processing table: intermediate_encntr_pract_fct_prev ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_pract_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct;

--select 'processing table:  intermediate_svc_ln_anl_fct_prev' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct_prev if EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_svc_ln_anl_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct;

--select 'processing table:  intermediate_encntr_qly_anl_fct_prev' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_qly_anl_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_qly_anl_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_qly_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_qly_anl_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_qly_anl_fct;

--select 'processing table: intermediate_hist_pymt_ratio_prev ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_prev  IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio RENAME TO pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_hist_pymt_ratio  RENAME TO pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio;

--select 'processing table: intermediate_hist_pymt_ratio_drg_wghts_prev ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts RENAME TO pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_hist_pymt_ratio_drg_wghts  RENAME TO pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts;

--select 'processing table: intermediate_net_rvu_model_prev ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_net_rvu_model_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_net_rvu_model RENAME TO pce_qe16_slp_prd_dm..intermediate_net_rvu_model_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_net_rvu_model  RENAME TO pce_qe16_slp_prd_dm..intermediate_net_rvu_model;

--select 'processing table:  intermediate_cpt_fct_prev' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_cpt_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_cpt_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_cpt_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_cpt_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_cpt_fct;

--select 'processing table:  intermediate_encntr_anl_fct_prev' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct;

--select 'processing table: intermediate_encntr_ed_anl_fct_prev' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_ed_anl_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_ed_anl_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_ed_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_ed_anl_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_ed_anl_fct;

--select 'processing table: intermediate_encntr_oncology_anl_fct_prev ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_oncology_anl_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_oncology_anl_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_oncology_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_oncology_anl_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_oncology_anl_fct;

--select 'processing table: intermediate_chrg_agg_fct_prev ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_chrg_agg_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_chrg_agg_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_chrg_agg_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_agg_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_chrg_agg_fct;


--select 'processing table: intermediate_ptnt_fnc_txn_agg_fct_prev ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..intermediate_ptnt_fnc_txn_agg_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_ptnt_fnc_txn_agg_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_ptnt_fnc_txn_agg_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_ptnt_fnc_txn_agg_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_ptnt_fnc_txn_agg_fct;


 
-----------

--select 'processing table:  prd_chrg_cost_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_chrg_cost_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_chrg_cost_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct;

--select 'processing table: prd_spl_dim ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_spl_dim IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_spl_dim AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_spl_dim;

--select 'processing table: prd_chrg_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_chrg_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_chrg_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct;

--select 'processing table: prd_encntr_cnslt_pract_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_cnslt_pract_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_cnslt_pract_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct;

--select 'processing table:prd_encntr_dgns_fct  ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_dgns_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_dgns_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct;

--select 'processing table: prd_encntr_pcd_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_pcd_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_pcd_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct;

--select 'processing table: prd_encntr_net_rvu_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_net_rvu_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_net_rvu_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct;

--select 'processing table: prd_encntr_pract_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_pract_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_pract_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct;

--select 'processing table:prd_svc_ln_anl_fct  ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_svc_ln_anl_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_svc_ln_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct;

--select 'processing table: prd_encntr_qly_anl_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_qly_anl_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_qly_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_qly_anl_fct;

--select 'processing table: prd_hist_pymt_ratio ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_hist_pymt_ratio if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_hist_pymt_ratio AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio;

--select 'processing table:prd_hist_pymt_ratio_drg_wghts ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_hist_pymt_ratio_drg_wghts if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_hist_pymt_ratio_drg_wghts AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts;

--select 'processing table: prd_net_rvu_model ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_net_rvu_model if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_net_rvu_model AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_net_rvu_model;

--select 'processing table: prd_cpt_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_cpt_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_cpt_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_cpt_fct;

--select 'processing table:prd_encntr_anl_fct  ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_anl_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct
DISTRIBUTE ON (fcy_nm,encntr_num);

--select 'processing table:prd_fnc_txn_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_fnc_txn_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_fnc_txn_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_stage_fnc_txn_fct
DISTRIBUTE ON (fcy_nm,encntr_num);


--select 'processing table:prd_encntr_ed_anl_fct  ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_ed_anl_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_ed_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_ed_anl_fct;

--select 'processing table:  prd_encntr_oncology_anl_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_oncology_anl_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_oncology_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_oncology_anl_fct;

--select 'processing table: prd_chrg_agg_fct' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_chrg_agg_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_chrg_agg_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_chrg_agg_fct;

--select 'processing table: prd_ptnt_fnc_txn_agg_fct ' as table_processing;
DROP TABLE pce_qe16_slp_prd_dm..prd_ptnt_fnc_txn_agg_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_ptnt_fnc_txn_agg_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_ptnt_fnc_txn_agg_fct;


\unset ON_ERROR_STOP
