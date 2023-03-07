\set ON_ERROR_STOP ON;
--Qualifiers 


DROP TABLE temp_fiscal_year_tbl IF EXISTS; 
CREATE Table temp_fiscal_year_tbl AS 
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
select * from fiscal_year_tbl ORDER BY FY_NUM DESC LIMIT 1; 

DROP TABLE temp_dates_tbl IF EXISTS; 

CREATE TEMP TABLE temp_dates_tbl 
AS 
select 
CASE WHEN DATE_PART('day',now()) >=15 THEN last_day(now() - INTERVAL '1 MONTH') ELSE last_day(now() - INTERVAL '2 MONTH') END as curr_year_end_dt;


DROP TABLE temp_eligible_encntr_data_inpatient IF EXISTS;
	CREATE TEMP TABLE temp_eligible_encntr_data_inpatient AS (
		SELECT DISTINCT ZOOM.company_id
		,ZOOM.patient_id , ZOOM.inpatient_outpatient_flag ,
		ZOOM.admission_ts 
		,case WHEN ZOOM.inpatient_outpatient_flag ='O' THEN COALESCE(Date(zoom.discharge_ts), Date(zoom.admission_ts)) ELSE Date(zoom.discharge_ts) END AS discharge_ts
		,ZOOM.msdrg_code 
		,CAST(NULL as NUMERIC(14,2)) as ms_drg_wght
        ,CAST(NULL as NUMERIC(14,2)) as ms_drg_geo_mean_los_num
        ,CAST(NULL as NUMERIC(14,2)) as ms_drg_arthm_mean_los_num
                ,CASE WHEN  MONTH(date_trunc('quarter',date(discharge_ts))) >= 10  THEN 'FY' || (YEAR(discharge_ts) + 1) 
					   WHEN  MONTH(date_trunc('quarter',date(discharge_ts))) < 10   THEN 'FY' || YEAR(discharge_ts) 
					   WHEN  ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND MONTH(date_trunc('quarter',date(admission_ts))) >= 10  THEN  'FY' || (YEAR(admission_ts) + 1) 
					   WHEN  ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND MONTH(date_trunc('quarter',date(admission_ts))) < 10  THEN   'FY' || YEAR(admission_ts) 
					   ELSE
					   NULL END as fiscal_yr
		,CASE WHEN Date(zoom.discharge_ts) BETWEEN  (Select Fiscal_start from temp_fiscal_year_tbl) and --now()- Day(now()) THEN 'C'
		 (select curr_year_end_dt from temp_dates_tbl) THEN 'C' 
				      WHEN Date(zoom.discharge_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '1 year' FROM temp_dates_tbl) THEN 'P'
				      WHEN Date(zoom.discharge_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '2 year' from temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '2 year' FROM temp_dates_tbl) THEN 'P-1'
					  WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) BETWEEN  (Select Fiscal_start from temp_fiscal_year_tbl) and 
                       (select curr_year_end_dt	FROM temp_dates_tbl)				  THEN 'C'
				      WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND
                                           Date(zoom.admission_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '1 year' FROM temp_dates_tbl)  THEN 'P'
				      WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND
                                           Date(zoom.admission_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '2 year' from temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '2 year' FROM temp_dates_tbl)  THEN 'P-1'                       
					   ELSE NULL END AS fiscal_yr_tp
--		, CASE     WHEN Date(zoom.discharge_ts) between '2014-10-01' and '2015-09-30' THEN 'FY2015'
--		           WHEN Date(zoom.discharge_ts) between '2015-10-01' and '2016-09-30' THEN 'FY2016'
--		           WHEN Date(zoom.discharge_ts) between '2016-10-01' and '2017-09-30' THEN 'FY2017'
--			   WHEN Date(zoom.discharge_ts) between '2017-10-01' and '2018-09-30' THEN 'FY2018'
--			   WHEN Date(zoom.discharge_ts) between '2018-10-01' and '2019-09-30' THEN 'FY2019'
--			   WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) between '2014-10-01' and '2015-09-30' THEN 'FY2015'
--			   WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) between '2015-10-01' and '2016-09-30' THEN 'FY2016'
--			   WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) between '2016-10-01' and '2017-09-30' THEN 'FY2017'
--			   WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) between '2017-10-01' and '2018-09-30' THEN 'FY2018'
--			   WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) between '2018-10-01' and '2019-09-30' THEN 'FY2019'
--			   ELSE 
--			   NULL END as fiscal_yr
		FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM 
		WHERE ZOOM.inpatient_outpatient_flag ='I' AND 
 coalesce(ZOOM.msdrg_code,'000') NOT IN ('V45','V70') AND 
 --initcap(ZOOM.company_id) <> 'Lansing' AND 
--CODE change: Commented discharge_total_charges > 0 
--		ZOOM.discharge_total_charges > 0 AND 
--CODE Change: Added Discharge ts in the filter based on McLaren's request 
--		(cast(ZOOM.admission_ts AS DATE) >= DATE ('2015-10-01') OR cast(ZOOM.discharge_ts AS DATE) >= DATE ('2015-10-01'))
( (cast(ZOOM.admission_ts AS DATE) BETWEEN DATE('2015-10-01') AND now()) AND (cast(ZOOM.discharge_ts AS DATE) BETWEEN DATE('2015-10-01') AND now())) 
		);


SELECT count(*)
FROM temp_eligible_encntr_data_inpatient;

DROP TABLE temp_eligible_encntr_data_outpatient IF EXISTS;
	CREATE TEMP TABLE temp_eligible_encntr_data_outpatient AS (
		SELECT DISTINCT ZOOM.company_id
		,ZOOM.patient_id , ZOOM.inpatient_outpatient_flag ,
		ZOOM.admission_ts
		,case WHEN ZOOM.inpatient_outpatient_flag ='O' THEN COALESCE(Date(zoom.discharge_ts), Date(zoom.admission_ts)) ELSE Date(zoom.discharge_ts) END AS discharge_ts
		,ZOOM.msdrg_code 
		,CAST(NULL as NUMERIC(14,2)) as ms_drg_wght
        ,CAST(NULL as NUMERIC(14,2)) as ms_drg_geo_mean_los_num
        ,CAST(NULL as NUMERIC(14,2)) as ms_drg_arthm_mean_los_num
                ,CASE WHEN  MONTH(date_trunc('quarter',date(discharge_ts))) >= 10  THEN 'FY' || (YEAR(discharge_ts) + 1) 
					   WHEN  MONTH(date_trunc('quarter',date(discharge_ts))) < 10   THEN 'FY' || YEAR(discharge_ts) 
					   WHEN  ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND MONTH(date_trunc('quarter',date(admission_ts))) >= 10  THEN  'FY' || (YEAR(admission_ts) + 1) 
					   WHEN  ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND MONTH(date_trunc('quarter',date(admission_ts))) < 10  THEN   'FY' || YEAR(admission_ts) 
					   ELSE
					   NULL END as fiscal_yr
		,CASE WHEN Date(zoom.discharge_ts) BETWEEN  (Select Fiscal_start from temp_fiscal_year_tbl) and (select curr_year_end_dt from temp_dates_tbl) THEN 'C' 
				      WHEN Date(zoom.discharge_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '1 year' FROM temp_dates_tbl) THEN 'P'
	                  WHEN Date(zoom.discharge_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '2 year' from temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '2 year' FROM temp_dates_tbl) THEN 'P-1'
	WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) BETWEEN  (Select Fiscal_start from temp_fiscal_year_tbl) and (select curr_year_end_dt	FROM temp_dates_tbl) THEN 'C'
				      WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND
                                           Date(zoom.admission_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '1 year' FROM temp_dates_tbl)  THEN 'P'
                      WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND
                                           Date(zoom.admission_ts) BETWEEN  (Select DATE(Fiscal_start) - Interval '2 year' from temp_fiscal_year_tbl) and (select curr_year_end_dt - Interval '2 year' FROM temp_dates_tbl)  THEN 'P-1'                                     
									 ELSE NULL END AS fiscal_yr_tp
--		, CASE     WHEN Date(zoom.discharge_ts) between '2014-10-01' and '2015-09-30' THEN 'FY2015'
--		           WHEN Date(zoom.discharge_ts) between '2015-10-01' and '2016-09-30' THEN 'FY2016'
--		           WHEN Date(zoom.discharge_ts) between '2016-10-01' and '2017-09-30' THEN 'FY2017'
--			   WHEN Date(zoom.discharge_ts) between '2017-10-01' and '2018-09-30' THEN 'FY2018'
--			   WHEN Date(zoom.discharge_ts) between '2018-10-01' and '2019-09-30' THEN 'FY2019'
--			   WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) between '2014-10-01' and '2015-09-30' THEN 'FY2015'
--			   WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) between '2015-10-01' and '2016-09-30' THEN 'FY2016'
--			   WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) between '2016-10-01' and '2017-09-30' THEN 'FY2017'
--			   WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) between '2017-10-01' and '2018-09-30' THEN 'FY2018'
--			   WHEN ZOOM.inpatient_outpatient_flag ='O' AND Date(zoom.discharge_ts) IS NULL AND Date(zoom.admission_ts) between '2018-10-01' and '2019-09-30' THEN 'FY2019'
--			   ELSE 
--			   NULL END as fiscal_yr
		FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM 
		WHERE ZOOM.inpatient_outpatient_flag ='O' AND 
 coalesce(ZOOM.msdrg_code,'000') NOT IN ('V45','V70') AND 
 --initcap(ZOOM.company_id) <> 'Lansing' AND 
--CODE change: Commented discharge_total_charges > 0 
--		ZOOM.discharge_total_charges > 0 AND 
--CODE Change: Added Discharge ts in the filter based on McLaren's request 
--		(cast(ZOOM.admission_ts AS DATE) >= DATE ('2015-10-01') OR cast(ZOOM.discharge_ts AS DATE) >= DATE ('2015-10-01'))
 (cast(ZOOM.admission_ts AS DATE) BETWEEN DATE('2015-10-01') AND now()) 
		);

SELECT count(*)
FROM temp_eligible_encntr_data_outpatient;

--CODE CHANGE : AUG 2019 (a) Ms_Drg_Dim Historical CMI Weights
DROP TABLE pce_qe16_slp_prd_dm..temp_ms_drg_dim_hist  IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..temp_ms_drg_dim_hist AS 
select ms_drg_cd, 
CAST(case_mix_idnx_num as NUMERIC(14,2)) as drg_wght,
CAST(geo_mean_los_num as NUMERIC(14,2)) as ms_drg_geo_mean_los_num,
CAST(arthm_mean_los_num as NUMERIC(14,2)) as ms_drg_arthm_mean_los_num,
drg_vrsn, vld_fm_dt, nvl(vld_to_dt, now()) as vld_to_dt
  FROM pce_ae00_aco_prd_cdr..ms_drg_dim_h 
  WHERE case_mix_idnx_num NOT IN ('UNKNOWN');
  
   
--CODE CHANGE : AUG 2019 (a) Ms_Drg_Dim Historical CMI Weights   temp_eligible_encntr_data
DROP TABLE pce_qe16_slp_prd_dm..temp_eligible_encntrs IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..temp_eligible_encntrs 
AS 
SELECT X.*
FROM temp_eligible_encntr_data_inpatient X
UNION 
SELECT Y.*
FROM temp_eligible_encntr_data_outpatient Y; 
		
SELECT count(*)
FROM temp_eligible_encntrs;

DROP TABLE pce_qe16_slp_prd_dm..temp_encntr_with_ms_drg_wghts IF EXISTS; 

CREATE TEMP TABLE pce_qe16_slp_prd_dm..temp_encntr_with_ms_drg_wghts AS 
SELECT 
X.company_id, X.patient_id,  X.inpatient_outpatient_flag,  X.admission_ts,  X.discharge_ts,  X.msdrg_code, 
nvl(DRGWGHT.drg_wght , X.ms_drg_wght) as ms_drg_wght, 
nvl(DRGWGHT.ms_drg_geo_mean_los_num, X.ms_drg_geo_mean_los_num) as ms_drg_geo_mean_los_num,
nvl(DRGWGHT.ms_drg_arthm_mean_los_num, X.ms_drg_arthm_mean_los_num) as ms_drg_arthm_mean_los_num, 
X.fiscal_yr,  X.fiscal_yr_tp
FROM pce_qe16_slp_prd_dm..temp_eligible_encntrs X 
INNER JOIN pce_qe16_slp_prd_dm..temp_ms_drg_dim_hist DRGWGHT
ON X.msdrg_code= DRGWGHT.ms_drg_cd  AND date(X.discharge_ts)  BETWEEN DRGWGHT.vld_fm_dt AND DRGWGHT.vld_to_dt;


DROP TABLE pce_qe16_slp_prd_dm..temp_encntr_without_ms_drg_wghts IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..temp_encntr_without_ms_drg_wghts AS 
with recs_with_weights AS 
(select distinct patient_id , company_id FROM pce_qe16_slp_prd_dm..temp_encntr_with_ms_drg_wghts)
select * FROM pce_qe16_slp_prd_dm..temp_eligible_encntrs X  
WHERE (patient_id || company_id) NOT IN (select (patient_id || company_id) from recs_with_weights );

DROP TABLE pce_qe16_slp_prd_dm..temp_eligible_encntr_data IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..temp_eligible_encntr_data 
AS 
SELECT X.*
FROM temp_encntr_with_ms_drg_wghts X
UNION 
SELECT Y.*
FROM temp_encntr_without_ms_drg_wghts Y; 
		
SELECT count(*)
FROM temp_eligible_encntr_data;


----------------------------------------


DROP TABLE pce_qe16_slp_prd_dm..temp_physician_npi_spclty

IF EXISTS;
	CREATE TEMP TABLE pce_qe16_slp_prd_dm..temp_physician_npi_spclty AS (
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
		,coalesce(NPIREG.hcare_pvdr_txnmy_cl_nm , NPIREG.hcare_scdy_pvdr_txnmy_cl_nm)  AS practitioner_spclty_description 
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

--drop table intermediate_svc_hier_dim if exists; 
--create table intermediate_svc_hier_dim as sElect * from intermediate_svc_hier_dim; 

--CODE Change : Adding intermediate_intermediate_spl_dim to fix the Lansing encounters with Charge Code but SPL Code is NULL issue 
DROP TABLE intermediate_spl_dim IF EXISTS;
CREATE TABLE intermediate_spl_dim as
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
DROP TABLE intermediate_encntr_qly_anl_fct IF EXISTS;

CREATE TABLE intermediate_encntr_qly_anl_fct as
select hash8(z.company_id)::bigint as src_company_id_hash,
hash8(Z.patient_id)::bigint as src_patient_id_hash,
Z.company_id as src_company_id, Z.patient_id as src_patient_id , QADV.* from temp_eligible_encntr_data Z
INNER JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
LEFT JOIN pce_qe16_slp_prd_dm..encntr_fct QADV
on Z.patient_id = QADV.encntr_num and QADV.fcy_num = VSET_FCY.alt_cd
DISTRIBUTE ON (src_company_id_hash, src_patient_id_hash);

--intermediate_chrg_fct Table creation based on Net 3 years Of patient Account Number
DROP TABLE pce_qe16_slp_prd_dm..intermediate_chrg_fct_temp IF EXISTS ; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_chrg_fct_temp AS 
(
  SELECT Z.company_id as src_company_id,
         VSET_FCY.alt_cd as fcy_num
        ,Z.patient_id as src_patient_id
       ,CH.company_id
       ,CH.patient_id
       ,DATE (to_timestamp((CH.service_date || ' ' || nvl(substr(CH.service_date, 1, 2), '00') || ':' || nvl(substr(CH.service_date, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS service_date
       ,nvl(CH.charge_code, '-100') as charge_code
       ,CH.quantity
       ,CH.total_charge
       ,CH.total_variable_cost
       ,CH.total_fixed_cost
       ,nvl(CH.cpt_code,'-100') as cpt_code
       ,nvl(CH.revenue_code,'-100') as revenue_code
       ,nvl(CH.ordering_practitioner_code,'-100') as ordering_practitioner_code
       ,CH.cpt_modifier_1
       ,CH.cpt_modifier_2
       ,CH.cpt_modifier_3
       ,CH.cpt_modifier_4
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
	   ,Z.fiscal_yr
           ,nvl(RCC.directrcc,0) as direct_cost_ratio
           ,nvl(RCC.indirectrcc,0) as indirect_cost_ratio
           ,nvl(RCC.totalrcc,0) as total_cost_ratio
	   ,Round(nvl(CH.total_charge * RCC.directrcc,  0),2) as rcc_based_direct_cst_amt
	   ,ROUND(nvl(CH.total_charge * RCC.indirectrcc, 0),2) as rcc_based_indirect_cst_amt
	   ,Round(nvl(CH.total_charge * RCC.totalrcc, 0),2) as rcc_based_total_cst_amt
--Code Change: 03/06 Added crline as per McLaren's Request
           ,nvl(RCC.crline,0) as crline
--	   ,RAWCHRGCD.charge_code_description as raw_chrg_cd_descr
  FROM temp_eligible_encntr_data Z
  LEFT JOIN pce_qe16_slp_prd_dm..cv_patbill CH 
  on Z.company_id = CH.company_id and Z.patient_id = CH.patient_id
  LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
  ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN pce_qe16_slp_prd_dm..cdm_dim CHRGCD
  on CHRGCD.company_id = CH.company_id and CHRGCD.charge_code = CH.charge_code
  LEFT JOIN pce_qe16_slp_prd_dm..manual_rcc_cost RCC
  on RCC.fy = Z.fiscal_yr and CH.charge_code = RCC.charge_code and CH.company_id = RCC.company_id
--  WHERE initcap(CH.company_id) <> 'Lansing'
--  LEFT JOIN pce_qe16_slp_prd_dm..cdm_dim RAWCHRGCD
--  on RAWCHRGCD.company_id = CH.company_id and RAWCHRGCD.charge_code = CH.raw_chargcode
)
DISTRIBUTE ON (src_company_id, src_patient_id,charge_code);

-- Table with hash values on key columns

DROP TABLE pce_qe16_slp_prd_dm..intermediate_chrg_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_chrg_fct as 
SELECT
hash8(intermediate_chrg_fct_temp.src_company_id)::bigint as src_company_id_hash
,hash8(intermediate_chrg_fct_temp.src_patient_id)::bigint as src_patient_id_hash
,hash8(intermediate_chrg_fct_temp.charge_code)::bigint as charge_code_hash
,intermediate_chrg_fct_temp.*
--,intermediate_spl_dim.cdm_cd
--,intermediate_spl_dim.cdm_strt_cdr_dk
--,intermediate_spl_dim.cdm_strt_dt
--,intermediate_spl_dim.cdm_end_cdr_dk
--,intermediate_spl_dim.cdm_end_dt
--,intermediate_spl_dim.cdm_descr
,intermediate_spl_dim.persp_clncl_dtl_pcd_cd_v10
,intermediate_spl_dim.persp_clncl_dtl_pcd_descr_v10
,intermediate_spl_dim.spl_unit_cnvr
,intermediate_spl_dim.persp_clncl_dtl_cd
,intermediate_spl_dim.persp_clncl_dtl_descr
,intermediate_spl_dim.persp_clncl_dtl_unit
,intermediate_spl_dim.persp_clncl_smy_cd
,intermediate_spl_dim.persp_clncl_smy_descr
,intermediate_spl_dim.persp_clncl_std_dept_cd_v10
,intermediate_spl_dim.persp_clncl_std_dept_descr_v10
,intermediate_spl_dim.persp_clncl_std_dept_v10_rollup_cgy_cd
,intermediate_spl_dim.persp_clncl_std_dept_v10_rollup_cgy_descr
,intermediate_spl_dim.persp_clncl_dtl_spl_modfr_cd
,intermediate_spl_dim.persp_clncl_dtl_spl_modfr_descr
,rev_cl_dim.prn_rev_cd
,rev_cl_dim.prn_rev_descr
--,rev_cl_dim.rev_cd
,rev_cl_dim.rev_descr
,rev_cl_dim.rev_cd_grp_nm
,rev_cl_dim.rev_cd_num_fmt_nm
,rev_cl_dim.rev_cd_shrt_descr
--,hcpcs_dim.hcpcs_cd
,hcpcs_dim.hcpcs_descr
,hcpcs_dim.hcpcs_descr_long
--,dept_dim.department_code
,dept_dim.department_description
,dept_dim.department_group
--CODE Change : 06/19 OR Time Calcuation 
,CASE WHEN intermediate_spl_dim.persp_clncl_smy_descr = 'SURGERY TIME' AND 
UPPER(intermediate_spl_dim.persp_clncl_dtl_descr) <> 'OR MINOR FLAT RATE' AND UPPER(intermediate_spl_dim.persp_clncl_dtl_descr) IN ('OR MINOR 1 HR','OR MAJOR 1 HR','ROBOTIC OR TIME 1 HOUR') THEN 
   ROUND(intermediate_chrg_fct_temp.quantity * intermediate_spl_dim.spl_unit_cnvr * intermediate_spl_dim.persp_clncl_dtl_unit,2) 
   ELSE 
   0  END as calculated_or_hrs
,row_number() over(partition by intermediate_chrg_fct_temp.src_company_id, intermediate_chrg_fct_temp.src_patient_id
Order by  intermediate_chrg_fct_temp.service_date) as rec_num
FROM intermediate_chrg_fct_temp
LEFT JOIN prmretlp.intermediate_spl_dim on intermediate_chrg_fct_temp.charge_code=intermediate_spl_dim.cdm_cd and intermediate_chrg_fct_temp.fcy_num=intermediate_spl_dim.fcy_num
LEFT JOIN prmretlp.rev_cl_dim on intermediate_chrg_fct_temp.revenue_code = rev_cl_dim.rev_cd 
LEFT JOIN prmretlp.hcpcs_dim  on intermediate_chrg_fct_temp.cpt_code = hcpcs_dim.hcpcs_cd
LEFT JOIN prmretlp.dept_dim on  intermediate_chrg_fct_temp.company_id = dept_dim.company_id and intermediate_chrg_fct_temp.dept = dept_dim.department_code
DISTRIBUTE ON (src_company_id_hash,src_patient_id_hash,charge_code_hash);


--Cost Model:  From the Charge Fact, do a sum total of the Indirect Cost for each encounter and add it in the Encounter Analysis Fact. Ditto Direct Cost and Total Cost Amt

DROP TABLE pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct IF EXISTS ; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct AS 
(
select 
  src_company_id
, src_patient_id 
, sum(rcc_based_direct_cst_amt)   as  agg_rcc_based_direct_cst_amt
, sum(rcc_based_indirect_cst_amt) as agg_rcc_based_indirect_cst_amt
, sum(rcc_based_total_cst_amt)      as agg_rcc_based_total_cst_amt
, sum(calculated_or_hrs) as agg_calculated_or_hrs
FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct
GROUP BY 1,2
);

--select 'Total in intermediate_chrg_cost_fct' , count(*) from  pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct;
--intermediate_encntr_pract_fct Table  creation based on Net 3 years Of patient Account Number


DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct IF EXISTS ; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct AS 
(
  SELECT 
		hash8(Z.company_id) as src_company_id_hash
	   ,hash8(Z.patient_id) as src_patient_id_hash
	   ,Z.company_id as src_company_id
	   ,Z.patient_id as src_patient_id
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
  FROM temp_eligible_encntr_data Z 
  LEFT JOIN pce_qe16_slp_prd_dm..cv_patprac CH
  on Z.company_id = CH.company_id and Z.patient_id = CH.patient_id
  LEFT JOIN temp_physician_npi_spclty SPCL
on SPCL.company_id = CH.company_id and SPCL.practitioner_code = CH.practitioner_code
)
 DISTRIBUTE ON (src_company_id_hash, src_patient_id_hash);
 
 
--Code Change :  Logic to mark specl_valid_ind for Inpatient (Medical DRG's) 
DROP TABLE pce_qe16_slp_prd_dm..temp_specl_valid_ind IF EXISTS ; 
CREATE  TEMP TABLE pce_qe16_slp_prd_dm..temp_specl_valid_ind AS 
select distinct P.src_company_id, P.src_patient_id, 1 as specl_valid_ind
FROM intermediate_encntr_pract_fct P 
INNER JOIN temp_eligible_encntr_data A 
on A.company_id = P.src_company_id and A.patient_id = P.src_patient_id and A.inpatient_outpatient_flag = 'I'
INNER JOIN temp_physician_npi_spclty S
on S.company_id = P.src_company_id and S.practitioner_code = P.practitioner_code
INNER JOIN ms_drg_dim MSDRG
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

--intermediate_cpt_fct tABLE 
DROP TABLE pce_qe16_slp_prd_dm..intermediate_cpt_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_cpt_fct AS 
SELECT 
hash8(Z.company_id) as src_company_id_hash
,hash8(Z.patient_id) as src_patient_id_hash
,Z.company_id as src_company_id
,z.patient_id AS src_patient_id
,VSET_FCY.alt_cd as fcy_num
,CF.company_id
,CF.patient_id
,CF.cpt_code
,CPT_DIM.hcpcs_descr as cpt_descr
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
FROM  temp_eligible_encntr_data Z
  LEFT JOIN pce_qe16_slp_prd_dm..patcpt_fct CF
  on Z.company_id = CF.company_id and Z.patient_id = CF.patient_id
  LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
  ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN temp_physician_npi_spclty SPCL
  on SPCL.company_id = CF.company_id and SPCL.practitioner_code = CF.procedure_practitioner_code
  LEFT JOIN pce_qe16_slp_prd_dm..hcpcs_dim CPT_DIM 
  on CPT_DIM.hcpcs_cd = CF.cpt_code 
   DISTRIBUTE ON (src_company_id_hash, src_patient_id_hash);


--Code Change : 05/10 : Added a new temp table in support of Cancer Patient Identification 

DROP TABLE pce_qe16_slp_prd_dm..temp_dgns_ccs_dim_cancer_only IF EXISTS;
CREATE TEMP TABLE temp_dgns_ccs_dim_cancer_only AS 
SELECT distinct dgns_cd, ccs_dgns_cgy_descr 
FROM pce_ae00_aco_prd_cdr..dgns_ccs_dim
WHERE lower(ccs_dgns_cgy_descr) in 
(
'cancer of head and neck',
'cancer of esophagus',
'cancer of stomach',
'cancer of colon',
'cancer of rectum and anus',
'cancer of liver and intrahepatic bile duct',
'cancer of pancreas',
'cancer of other GI organs; peritoneum',
'cancer of bronchus; lung',
'cancer; other respiratory and intrathoracic',
'cancer of bone and connective tissue',
'Other non-epithelial cancer of skin',
'cancer of breast',
'cancer of uterus',
'cancer of cervix',
'cancer of ovary',
'cancer of other female genital organs',
'cancer of prostate',
'cancer of testis',
'cancer of other male genital organs',
'cancer of bladder',
'cancer of kidney and renal pelvis',
'cancer of other urinary organs',
'cancer of brain and nervous system',
'cancer of thyroid',
'cancer; other and unspecified primary'
) AND eff_to_Dt is NULL; 


--intermediate_encntr_dgns_fct Table  creation based on Net 3 years Of patient Account Number
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct IF EXISTS ; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct AS 
SELECT 
		hash8(Z.company_id) as src_company_id_hash
	   ,hash8(Z.patient_id) as src_patient_id_hash
	   ,Z.company_id as src_company_id
	   ,z.patient_id AS src_patient_id
       ,VSET_FCY.alt_cd as fcy_num
       , DF.company_id
       , DF.patient_id
       , nvl(DF.icd_code, '-100') as icd_code
       , DF.icd_type
--       , nvl(DF.surgeon_code, '-100') as surgeon_code
--       , DF.procedure_date
       , DF.diagnosis_code_present_on_admission_flag
       , DF.icd_version
--       , DF.procedureseq
--       , DF.proceduretype
--       , nvl(DF.orderingphysician, '-100') as orderingphysician
--       , DF.procedurestarttime
--       , DF.procedureenddate
--       , DF.procedureendtime
--       , DF.updatedateproc
--       , DF.sourcesystemproc
       , DF.diagnosisseq
       , DF.diagnosistype
       , CASE WHEN CANCER.dgns_cd is NOT NULL THEN DF.icd_code ELSE '-100' END as cancer_dgns_cd
       , CASE WHEN CANCER.dgns_cd is NOT NULL THEN 1 ELSE NULL END as cancer_case_ind
	   ,CANCER.ccs_dgns_cgy_descr as cancer_case_code_descr
	   , row_number() over(partition by Z.company_id, Z.patient_id
Order by  DF.diagnosisseq) as rec_num 
	    
  FROM temp_eligible_encntr_data Z 
  LEFT JOIN pce_qe16_slp_prd_dm..dgns_fct DF
  on Z.company_id = DF.company_id and Z.patient_id = DF.patient_id
  LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
  ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN pce_qe16_slp_prd_dm..temp_dgns_ccs_dim_cancer_only CANCER
  on CANCER.dgns_cd = replace(DF.icd_code, '.','')
   DISTRIBUTE ON (src_company_id_hash, src_patient_id_hash);
   
-----intermediate_encntr_pcd_fct Table  creation based on Net 3 years Of patient Account Number
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct IF EXISTS ; 
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct AS 
SELECT 
		hash8(Z.company_id) as src_company_id_hash
	   , hash8(Z.patient_id) as src_patient_id_hash
	   , Z.company_id as src_company_id
	   , z.patient_id AS src_patient_id
       , VSET_FCY.alt_cd as fcy_num
       , DF.company_id
       , DF.patient_id
       , nvl(DF.icd_code, '-100') as icd_code
       , DF.icd_type
       , nvl(DF.surgeon_code, '-100') as surgeon_code
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
  FROM temp_eligible_encntr_data Z 
  LEFT JOIN pce_qe16_slp_prd_dm..pcd_fct DF
  on Z.company_id = DF.company_id and Z.patient_id = DF.patient_id
  LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
  ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN temp_physician_npi_spclty SURGEON
  on SURGEON.company_id = DF.company_id and SURGEON.practitioner_code = DF.surgeon_code
    LEFT JOIN temp_physician_npi_spclty ORDERING
  on ORDERING.company_id = DF.company_id and ORDERING.practitioner_code = DF.orderingphysician
   DISTRIBUTE ON (src_company_id_hash, src_patient_id_hash);
------

--intermediate_svc_ln_anl_fct Code
--based on cv_patbill / intermediate_chrg_fct 

DROP TABLE temp_svc_based_on_patbill IF exists; 
CREATE TEMP TABLE temp_svc_based_on_patbill as 
select  
hash8(EN.company_id) as src_company_id_hash, hash8(EN.patient_id) as src_patient_id_hash,
EN.company_id, EN.patient_id ,EN.inpatient_outpatient_flag, 'cpt' as based_on,
--nvl(RNK.cd,'-100'),
RNK.svc_cgy,RNK.svc_ln,RNK.sub_svc_ln , RNK.services as svc_nm, RNK.cd, RNK.cd_type,RNK.descr as cd_descr,
EN.admission_ts as adm_dt, EN.discharge_ts as dschrg_dt
from temp_eligible_encntr_data EN
LEFT JOIN intermediate_chrg_fct CH
on EN.company_id = CH.company_id and CH.patient_id = EN.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_svc_hier_dim RNK
on CH.cpt_code = RNK.cd and lower(RNK.cd_type) in ('hcpcs','cpt') and lower(RNK.svc_cgy) in ('surgical','medical')  ;

--Based on PCD_fct (pcs-secondary/ICD 9/10)
DROP TABLE temp_svc_based_on_pcdfct IF exists; 
CREATE TEMP TABLE temp_svc_based_on_pcdfct as 
select  
hash8(EN.company_id) as src_company_id_hash, hash8(EN.patient_id) as src_patient_id_hash,
EN.company_id, EN.patient_id ,EN.inpatient_outpatient_flag, 'pcs' as  based_on,
--nvl(RNK.cd,'-100'),
RNK.svc_cgy,RNK.svc_ln,RNK.sub_svc_ln , RNK.services as svc_nm, RNK.cd, RNK.cd_type,RNK.descr as cd_descr,
EN.admission_ts as adm_dt, EN.discharge_ts as dschrg_dt
from temp_eligible_encntr_data EN
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct X 
on EN.company_id = X.company_id and EN.patient_id = X.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_svc_hier_dim RNK
on lower(X.icd_code) = lower(RNK.cd) and lower(RNK.cd_type) IN ('pcs secondary','ip icd9 pcs','icd 10 pcs') and icd_type='P';

--Based on Dgns Fact 
--?????
--Based on patcpt 
DROP TABLE temp_svc_based_on_patcpt IF exists; 
CREATE TEMP TABLE temp_svc_based_on_patcpt as 
select  
hash8(EN.company_id) as src_company_id_hash, hash8(EN.patient_id) as src_patient_id_hash,
EN.company_id, EN.patient_id ,EN.inpatient_outpatient_flag, 'cpt' as based_on,
--nvl(RNK.cd,'-100'),
RNK.svc_cgy,RNK.svc_ln,RNK.sub_svc_ln , RNK.services as svc_nm, RNK.cd, RNK.cd_type,RNK.descr as cd_descr,
EN.admission_ts as adm_dt, EN.discharge_ts as dschrg_dt
from temp_eligible_encntr_data EN
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_cpt_fct X
on EN.company_id = X.company_id and EN.patient_id = X.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_svc_hier_dim RNK
on lower(RNK.cd) = lower(X.cpt_code) and lower(RNK.cd_type) = 'cpt' ;

--MS-DRG (Inpatient)
DROP TABLE temp_svc_based_on_patdisch IF exists; 
CREATE TEMP TABLE temp_svc_based_on_patdisch as 
select  
hash8(EN.company_id) as src_company_id_hash, hash8(EN.patient_id) as src_patient_id_hash,
EN.company_id, EN.patient_id ,EN.inpatient_outpatient_flag, 'msdrg' as based_on,
--nvl(RNK.cd,'-100'),
RNK.svc_cgy,RNK.svc_ln,RNK.sub_svc_ln , RNK.services as svc_nm, RNK.cd, RNK.cd_type,RNK.descr as cd_descr,
EN.admission_ts as adm_dt, EN.discharge_ts as dschrg_dt
from temp_eligible_encntr_data X
LEFT JOIN pce_qe16_oper_prd_zoom..cv_patdisch EN
on EN.company_id = X.company_id and EN.patient_id = X.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_svc_hier_dim RNK
ON EN.msdrg_code = CAST(LPAD(CAST(coalesce(RNK.cd,'000') as INTEGER), 3,0 ) as Varchar(3)) and lower(RNK.cd_type) = 'ms-drg';
--on EN.msdrg_code = RNK.cd and lower(RNK.cd_type) = 'ms-drg';

DROP TABLE intermediate_svc_ln_anl_fct IF exists;
CREATE TABLE intermediate_svc_ln_anl_fct As 
with  all_joined as (
select * from temp_svc_based_on_patbill union   -- //HCPCS / CPT 
select * from temp_svc_based_on_patdisch union  --Ms-Drg-code
select * from temp_svc_based_on_pcdfct  union   --ICD 9/10 PCS
select * from temp_svc_based_on_patcpt   --CPT 
 )
select * 
, row_number() over(partition by src_company_id_hash,src_patient_id_hash
Order by  based_on) as rec_num 
from all_joined
distribute on (src_company_id_hash,src_patient_id_hash);


DROP TABLE temp_encntr_svc_hier IF EXISTS;
CREATE TEMP TABLE temp_encntr_svc_hier AS 
With inpatient_svc as 
(
   select * from (
select Z.company_id , Z.patient_id , Z.inpatient_outpatient_flag , HIER.svc_cgy, HIER.svc_ln, HIER.sub_svc_ln, HIER.services, 
HIER.ip_lvl_1_rnk as lvl_1_rnk, 
HIER.ip_lvl_2_rnk as lvl_2_rnk,
HIER.ip_lvl_3_rnk as lvl_3_rnk,
HIER.ip_lvl_4_rnk as lvl_4_rnk,
row_number() over(partition by Z.company_id, Z.patient_id ,Z.inpatient_outpatient_flag
Order by  HIER.ip_lvl_1_rnk, HIER.ip_lvl_2_rnk,HIER.ip_lvl_3_rnk,HIER.ip_lvl_4_rnk) as rec_num
from temp_eligible_encntr_data Z
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct SVCLN
on Z.company_id = SVCLN.company_id and Z.patient_id = SVCLN.patient_id and SVCLN.svc_ln is NOT NULL
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_svc_hier_dim HIER
on HIER.cd = SVCLN.cd
WHERE Z.inpatient_outpatient_flag ='I' and upper(HIER.use_hry_ind) = 'YES'
) A
WHERE A.rec_num =1
),
outpatient_svc as 
(
select * from (
select Z.company_id , Z.patient_id , Z.inpatient_outpatient_flag , HIER.svc_cgy, HIER.svc_ln, HIER.sub_svc_ln, HIER.services, 
HIER.op_lvl_1_rnk as lvl_1_rnk, 
HIER.op_lvl_2_rnk as lvl_2_rnk,
HIER.op_lvl_3_rnk as lvl_3_rnk,
HIER.op_lvl_4_rnk as lvl_4_rnk,
row_number() over(partition by Z.company_id, Z.patient_id ,Z.inpatient_outpatient_flag
Order by  HIER.op_lvl_1_rnk, HIER.op_lvl_2_rnk,HIER.op_lvl_3_rnk,HIER.op_lvl_4_rnk) as rec_num
from temp_eligible_encntr_data Z
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct SVCLN
on Z.company_id = SVCLN.company_id and Z.patient_id = SVCLN.patient_id and SVCLN.svc_ln is NOT NULL
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_svc_hier_dim HIER
on HIER.cd = SVCLN.cd
WHERE Z.inpatient_outpatient_flag ='O'  and upper(HIER.use_hry_ind) = 'YES'
) A
WHERE A.rec_num =1)
select * from inpatient_svc UNION 
select * from outpatient_svc;

--
DROP TABLE temp_fips_adr_dim IF EXISTS; 
CREATE TEMP TABLE temp_fips_adr_dim as 
(
   select 
Q.ptnt_zip_cd as fips_zip_cd,
F.fips_cnty_descr,
Q.ste_descr as ptnt_fips_ste_descr
from pce_qe16_slp_prd_dm..stnd_ptnt_zip_dim Q

LEFT JOIN pce_qe16_slp_prd_dm..fips_adr_dim F 
on F.fips_cnty_cd = Q.cnty_fips_nm and Q.ste_cd = F.fips_ste_descr
);

DROP TABLE temp_ptnt_type_fcy_std_cd

IF EXISTS;
	CREATE TEMP TABLE temp_ptnt_type_fcy_std_cd AS (
		SELECT PATTYPE.company_id
		,PATTYPE.patient_type_code
		,PATTYPE.patient_type_description
		,MAP.standard_patient_type_code
		,STD.std_encntr_type_descr FROM pce_qe16_slp_prd_dm..pattype_dim PATTYPE LEFT JOIN pce_qe16_slp_prd_dm..pattype_map_dim MAP ON MAP.patient_type_code = PATTYPE.patient_type_code
		AND MAP.company_id = PATTYPE.company_id LEFT JOIN pce_qe16_slp_prd_dm..stnd_ptnt_tp_dim STD ON STD.std_encntr_type_cd = MAP.standard_patient_type_code
		);

DROP TABLE temp_ptnt_adm_dgns

IF EXISTS;
	CREATE TEMP TABLE temp_ptnt_adm_dgns AS (
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
		FROM pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct X
		INNER JOIN temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(diagnosistype) = 'admitting'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = '10'
		AND Z.icd_version = 'ICD10' LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 1
		);

DROP TABLE temp_ptnt_prim_dgns

IF EXISTS;
	CREATE TEMP TABLE temp_ptnt_prim_dgns AS (
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
				,X.patient_id ORDER BY diagnosisseq
				) AS row_num
		FROM pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct X
		INNER JOIN temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(diagnosistype) = 'primary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = '10'
		AND Z.icd_version = 'ICD10' LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 1
		);

DROP TABLE temp_ptnt_second_dgns

IF EXISTS;
	CREATE TEMP TABLE temp_ptnt_second_dgns AS (
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
				,X.patient_id ORDER BY diagnosisseq
				) AS row_num
		FROM pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct X
		INNER JOIN temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(diagnosistype) = 'secondary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = '10'
		AND Z.icd_version = 'ICD10' LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 1
		);

SELECT count(*)
FROM temp_ptnt_second_dgns;--4,432,358

DROP TABLE temp_ptnt_trty_dgns

IF EXISTS;
	CREATE TEMP TABLE temp_ptnt_trty_dgns AS (
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
				,X.patient_id ORDER BY diagnosisseq
				) AS row_num
		FROM pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct X
		WHERE lower(diagnosistype) = 'secondary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..dgns_dim DGNS ON DGNS.dgns_alt_cd = replace(Z.icd_code, '.', '')
		AND DGNS.dgns_icd_ver = '10'
		AND Z.icd_version = 'ICD10' LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSETPOA ON VSETPOA.cohrt_id = 'POAFLAGS'
		AND VSETPOA.cd = Z.diagnosis_code_present_on_admission_flag WHERE Z.row_num = 2
		);--5,781,165

DROP TABLE temp_ptnt_prim_proc

IF EXISTS;
	CREATE TEMP TABLE temp_ptnt_prim_proc AS (
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
		FROM pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct X
		INNER JOIN temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(proceduretype) = 'primary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..pcd_dim PCDDIM ON PCDDIM.icd_pcd_cd = Z.icd_code WHERE Z.row_num = 1
		);

SELECT count(*)
FROM temp_ptnt_prim_proc;--332,451

DROP TABLE temp_ptnt_scdy_proc

IF EXISTS;
	CREATE TEMP TABLE temp_ptnt_scdy_proc AS (
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
		FROM pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct X
		INNER JOIN temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(proceduretype) = 'secondary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..pcd_dim PCDDIM ON PCDDIM.icd_pcd_cd = Z.icd_code WHERE Z.row_num = 1
		);

SELECT count(*)
FROM temp_ptnt_scdy_proc;--145,743

DROP TABLE temp_ptnt_trty_proc

IF EXISTS;
	CREATE TEMP TABLE temp_ptnt_trty_proc AS (
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
		FROM pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct X
		INNER JOIN temp_eligible_encntr_data ENCNTR ON ENCNTR.company_id = X.company_id
			AND ENCNTR.patient_id = X.patient_id
		WHERE lower(proceduretype) = 'secondary'
		) Z LEFT JOIN pce_qe16_slp_prd_dm..pcd_dim PCDDIM ON PCDDIM.icd_pcd_cd = Z.icd_code WHERE Z.row_num = 2
		);--84961

DROP TABLE temp_obsrv

IF EXISTS;
	CREATE TEMP TABLE temp_obsrv AS (
		SELECT pb.patient_id
		,pb.company_id
		,sum(pb.quantity) AS qty FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct pb WHERE pb.revenue_code = '0762' GROUP BY pb.patient_id
		,pb.company_id
		);--215561

--Code Change : Modified the existing logic (Rev Code) based on SPL Dimension

DROP TABLE temp_icu

IF EXISTS;
	CREATE TEMP TABLE temp_icu AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS icu_days
		FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct ZOOM 
--		 INNER JOIN temp_dschrg_inpatient ON temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_spl_dim SP 
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --ICU 
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B ICU' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B ICU','R&B NURSERY INTENSIVE LEVEL III(NICU)','R&B NURSERY INTENSIVE LEVEL IV (NICU)',
		   'R&B TRAUMA ICU'))
			) GROUP BY 1,2
		);


--Code Change : Modified the existing logic (Rev Code) based on SPL Dimension
DROP TABLE temp_ccu

IF EXISTS;
	CREATE TEMP TABLE temp_ccu AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ccu_days
		FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct ZOOM 
--		 INNER JOIN temp_dschrg_inpatient ON temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_spl_dim SP 
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --CCU 
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B ICU' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B CICU/CCU (CORONARY CARE)'))
			) GROUP BY 1,2
		);

DROP TABLE temp_nrs

IF EXISTS;
	CREATE TEMP TABLE temp_nrs AS (
		SELECT patient_id
		,company_id
		,count(DISTINCT pb.service_date) AS nrs_days FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct pb WHERE pb.revenue_code BETWEEN '0170'
			AND '0179' GROUP BY patient_id
		,company_id
		);--32134

DROP TABLE temp_rtne

IF EXISTS;
	CREATE TEMP TABLE temp_rtne AS (
		SELECT patient_id
		,company_id
		,count(DISTINCT pb.service_date) AS rtne_days FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct pb WHERE (
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

DROP TABLE temp_ed_case

IF EXISTS;
	CREATE TEMP TABLE temp_ed_case AS (
		SELECT DISTINCT patient_id
		,company_id FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct PB INNER JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_ED_CPT ON VSET_ED_CPT.cd = PB.cpt_code
		AND VSET_ED_CPT.cohrt_nm = 'ED_VISIT'
		);--2136885

DROP TABLE temp_dschrg_inpatient_ltcsnf

IF EXISTS;
	CREATE TEMP TABLE temp_dschrg_inpatient_ltcsnf AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_ltcsnf_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
		AND Z.patient_id = ENCNTR.patient_id WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I'
		AND (
			lower(Z.primary_payer_code) in ('select','selec')
			OR lower(Z.patient_type) = lower('bsch')
			) GROUP BY 1
		,2
		);--1437

DROP TABLE temp_dschrg_inpatient_nbrn

IF EXISTS;
	CREATE TEMP TABLE temp_dschrg_inpatient_nbrn AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_nbrn_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
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

DROP TABLE temp_dschrg_inpatient_rehab

IF EXISTS;
	CREATE TEMP TABLE temp_dschrg_inpatient_rehab AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_rehab_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
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

DROP TABLE temp_dschrg_inpatient_psych

IF EXISTS;
	CREATE TEMP TABLE temp_dschrg_inpatient_psych AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_psych_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
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

DROP TABLE temp_dschrg_inpatient_spclcare

IF EXISTS;
	CREATE TEMP TABLE temp_dschrg_inpatient_spclcare AS (
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
--DROP TABLE temp_dschrg_inpatient_hospice
--
--IF EXISTS;
--	CREATE TEMP TABLE temp_dschrg_inpatient_hospice AS (
--		SELECT DISTINCT Z.patient_id
--		,Z.company_id
--		,1 AS dschrg_hospice_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
--		AND Z.patient_id = ENCNTR.patient_id INNER JOIN pce_qe16_prd_qadv..val_set_dim VSET_HOSPICE ON VSET_HOSPICE.cohrt_nm = 'Sepsis Mortality'
--		AND Z.primary_payer_code = VSET_HOSPICE.cd WHERE Z.discharge_date IS NOT NULL
--		AND Z.inpatient_outpatient_flag = 'I'
--		AND Z.discharge_total_charges > 0 GROUP BY 1
--		,2
--		);--4577


--Code Change : Discharge - Hospice New Logic 

DROP TABLE temp_payer_fcy_std_code

IF EXISTS;
--	CREATE TEMP TABLE temp_payer_fcy_std_code AS (
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

	CREATE TEMP TABLE temp_payer_fcy_std_code AS (
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

DROP TABLE temp_dschrg_inpatient_hospice

IF EXISTS;
	CREATE TEMP TABLE temp_dschrg_inpatient_hospice AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_hospice_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z
		INNER JOIN temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
		AND Z.patient_id = ENCNTR.patient_id
		INNER JOIN temp_payer_fcy_std_code VSET_HOSPICE
        ON VSET_HOSPICE.payer_code = Z.primary_payer_code
	    WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I' AND VSET_HOSPICE.payor_group3 = 'Hospice'
		AND Z.discharge_total_charges > 0 GROUP BY 1
		,2
		);--4577
		
		

DROP TABLE temp_dschrg_inpatient_lipmip

IF EXISTS;
	CREATE TEMP TABLE temp_dschrg_inpatient_lipmip AS (
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

DROP TABLE temp_dschrg_inpatient_acute

IF EXISTS;
	CREATE TEMP TABLE temp_dschrg_inpatient_acute AS (
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
			END AS dschrg_acute_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
		AND Z.patient_id = ENCNTR.patient_id LEFT JOIN temp_dschrg_inpatient_nbrn NB ON NB.patient_id = Z.patient_id
		AND NB.company_id = Z.company_id LEFT JOIN temp_dschrg_inpatient_lipmip LIPMIP ON LIPMIP.patient_id = Z.patient_id
		AND LIPMIP.company_id = Z.company_id LEFT JOIN temp_dschrg_inpatient_rehab REHAB ON REHAB.patient_id = Z.patient_id
		AND REHAB.company_id = Z.company_id LEFT JOIN temp_dschrg_inpatient_psych PSYCH ON PSYCH.patient_id = Z.patient_id
		AND PSYCH.company_id = Z.company_id LEFT JOIN temp_dschrg_inpatient_ltcsnf LTCSNF ON LTCSNF.patient_id = Z.patient_id
		AND LTCSNF.company_id = Z.company_id LEFT JOIN temp_dschrg_inpatient_hospice HOSPICE ON HOSPICE.patient_id = Z.patient_id
		AND HOSPICE.company_id = Z.company_id WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I'
		AND Z.discharge_total_charges > 0
		);--323191 

DROP TABLE temp_dschrg_inpatient

IF EXISTS;
	CREATE TEMP TABLE temp_dschrg_inpatient AS (
		SELECT DISTINCT Z.patient_id
		,Z.company_id
		,1 AS dschrg_ind FROM pce_qe16_oper_prd_zoom..cv_patdisch Z INNER JOIN temp_eligible_encntr_data ENCNTR ON Z.company_id = ENCNTR.company_id
		AND Z.patient_id = ENCNTR.patient_id WHERE Z.discharge_date IS NOT NULL
		AND Z.inpatient_outpatient_flag = 'I' GROUP BY 1
		,2
		);--323191

DROP TABLE temp_derived_ptnt_days

IF EXISTS;
	CREATE TEMP TABLE temp_derived_ptnt_days AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN (sum(ZOOM.quantity) =0) THEN NULL ELSE sum(ZOOM.quantity) END  AS ptnt_days 
		FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct ZOOM INNER JOIN temp_dschrg_inpatient ON temp_dschrg_inpatient.patient_id = ZOOM.patient_id WHERE (
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

DROP TABLE temp_endoscopy_case

IF EXISTS;
	CREATE TEMP TABLE temp_endoscopy_case AS (
		SELECT DISTINCT patient_id
		,company_id FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct WHERE revenue_code IN (
			'0750'
			) GROUP BY patient_id
		,company_id
		);

--code change : Added logic to calculate Surgercy Cases based on SPL Dimension

DROP TABLE temp_srgl_case

IF EXISTS;
	CREATE TEMP TABLE temp_srgl_case AS (
		SELECT DISTINCT patient_id
		,company_id FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct CF 
		INNER JOIN pce_qe16_slp_prd_dm..intermediate_spl_dim SP
		on CF.charge_code = SP.cdm_cd and CF.fcy_num = SP.fcy_num
		WHERE SP.persp_clncl_smy_descr in ('SURGERY TIME', 'AMBULATORY SURGERY SERVICES') 
		GROUP BY patient_id
		,company_id
		);

--code change : Added logic to calculate  Lithotripsy  Cases based on SPL Dimension
   DROP TABLE temp_lithotripsy_case IF EXISTS;
	CREATE TEMP TABLE temp_lithotripsy_case AS (
		SELECT DISTINCT patient_id
		,company_id FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct CF 
		INNER JOIN pce_qe16_slp_prd_dm..intermediate_spl_dim SP
		on CF.charge_code = SP.cdm_cd and CF.fcy_num = SP.fcy_num
		WHERE UPPER(SP.persp_clncl_dtl_descr) in ('PF LITHOLAPAXY COMPLICATED > 2.5 CM','LITHOTRIPSY KIDNEY','PERC NEPHROLITHOTOMY W/WO DILATION <2 CM') 
		GROUP BY patient_id
		,company_id
		);

 --code change : Added logic to calculate  CathLab Cases based on intermediate_svc_ln_anl_fct
    DROP TABLE temp_cathlab_case IF EXISTS;
	CREATE TEMP TABLE temp_cathlab_case AS
	(
	  SELECT DISTINCT patient_id, company_id 
	  FROM pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct 
	  WHERE UPPER(svc_nm) = UPPER('Right & Left Dx Cath')
	);

--Code Change : Added logic to calculate patient_days_StepDown based on SPL Dimension
DROP TABLE temp_derived_ptnt_days_stepdown

IF EXISTS;
	CREATE TEMP TABLE temp_derived_ptnt_days_stepdown AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum( ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_stepdown 
		FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct ZOOM 
--		 INNER JOIN temp_dschrg_inpatient ON temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_spl_dim SP 
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

DROP TABLE temp_derived_ptnt_days_nbrn

IF EXISTS;
	CREATE TEMP TABLE temp_derived_ptnt_days_nbrn AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_nbrn
		FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct ZOOM 
--		 INNER JOIN temp_dschrg_inpatient ON temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_spl_dim SP 
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --NewBorn
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B NURSERY' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B NURSERY','R&B NURSERY INTERMEDIATE LEVEL II'))
			) GROUP BY 1,2
		);
		


--CODE change: Modified the existing logic (Rev Code) based on SPL Dimension

DROP TABLE temp_derived_ptnt_days_psych

IF EXISTS;
	CREATE TEMP TABLE temp_derived_ptnt_days_psych AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_psych
		FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct ZOOM 
--		 INNER JOIN temp_dschrg_inpatient ON temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_spl_dim SP 
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --Psych
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B PSYCH' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B PSYCH ISOLATION','R&B PSYCH PRIVATE','R&B PSYCH SEMI PRIVATE'))
		   OR 
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B DETOX' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B DETOX SEMI PRIVATE'))
			) GROUP BY 1,2
		);
		
--CODE change: Modified the existing logic (REv Code) based on SPL Dimension
DROP TABLE temp_derived_ptnt_days_rehab

IF EXISTS;
	CREATE TEMP TABLE temp_derived_ptnt_days_rehab AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_rehab
		FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct ZOOM 
--		 INNER JOIN temp_dschrg_inpatient ON temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_spl_dim SP 
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --Rehab
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B REHAB' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B REHAB ISOLATION','R&B REHAB PRIVATE','R&B REHAB SEMI PRIVATE'))
			) GROUP BY 1,2
		);
		
--CODE change: Modified the existing logic (REv Code) based on SPL Dimension
		
DROP TABLE temp_derived_ptnt_days_acute IF EXISTS; 

--Code Change: Modified the logic to calculate ptnt_days_acute
DROP TABLE temp_derived_ptnt_days_acute

IF EXISTS;
	CREATE TEMP TABLE temp_derived_ptnt_days_acute AS (
		SELECT ZOOM.patient_id
		,ZOOM.company_id
		,CASE WHEN(sum(ZOOM.quantity) =0 ) THEN NULL ELSE sum(ZOOM.quantity) END AS ptnt_days_acute
		FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct ZOOM 
--		 INNER JOIN temp_dschrg_inpatient ON temp_dschrg_inpatient.patient_id = ZOOM.patient_id 
		 INNER JOIN pce_qe16_slp_prd_dm..intermediate_spl_dim SP 
		 ON SP.fcy_num = ZOOM.fcy_num and ZOOM.charge_code = SP.cdm_cd
		 WHERE (
		 --Acute
		   (UPPER(SP.persp_clncl_smy_descr) = 'R&B MED/SURG' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B ISOLATION PRIVATE','R&B MED/SURG DELUXE','R&B MED/SURG PRIVATE',
		   'R&B MED/SURG SEMI PRIVATE','R&B OB','R&B ONCOLOGY','R&B PEDIATRIC'))
		    OR
		    (UPPER(SP.persp_clncl_smy_descr) = 'R&B MISC' AND UPPER(SP.persp_clncl_dtl_descr) IN ('R&B MISC'))
			) GROUP BY 1,2
		);

--Old version of logic to calculate ptnt_days_acute
--CREATE TEMP TABLE temp_derived_ptnt_days_acute AS (
--    SELECT ZOOM.patient_id
--		,ZOOM.company_id
--		,count(DISTINCT ZOOM.service_date) AS ptnt_days_acute 
--		 FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct ZOOM 
--		INNER JOIN temp_dschrg_inpatient ON temp_dschrg_inpatient.patient_id = ZOOM.patient_id WHERE (
--			--NOT Rehab / Psych /NewBorn / Hospice/LTC/SNF
--			revenue_code NOT IN ('0170','0171','0172''0173','0174','0175','0179','0114','0124','0134','0144','0154','0204','0118','0128','0138','0148','0158',
--			'0650','0651','0652','0653','0654','0655','0656','0657','0659') 
--			) GROUP BY 1 ,2
--);



		
--New
DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct as 
with cnslt_pract_1 as 
(
select C1.company_id, C1.patient_id, 
C1.practitioner_code as cnslt_pract_1_cd,
SPCL.npi as cnslt_pract_1_npi,
SPCL.practitioner_name as cnslt_pract_1_nm,
SPCL.practitioner_spclty_description as cnslt_pract_1_spclty,
SPCL.mcare_spcly_cd as cnslt_pract_1_mcare_spcly_cd
FROM temp_eligible_encntr_data Z
INNER JOIN pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct C1
on C1.company_id = Z.company_id and Z.patient_id = C1.patient_id
LEFT JOIN temp_physician_npi_spclty SPCL
on SPCL.company_id = C1.company_id and SPCL.practitioner_code = C1.practitioner_code
WHERE lower(C1.raw_role) = 'consulting 1'),
cnslt_pract_2 as 
(
select C1.company_id, C1.patient_id, C1.practitioner_code as cnslt_pract_2_cd,
SPCL.npi as cnslt_pract_2_npi,
SPCL.practitioner_name as cnslt_pract_2_nm,
SPCL.practitioner_spclty_description as cnslt_pract_2_spclty,
SPCL.mcare_spcly_cd as cnslt_pract_2_mcare_spcly_cd
FROM temp_eligible_encntr_data Z
INNER JOIN pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct C1
on C1.company_id = Z.company_id and Z.patient_id = C1.patient_id
INNER JOIN pce_qe16_slp_prd_dm..phys_dim P
on P.practitioner_code = C1.practitioner_code and C1.company_id = P.company_id
LEFT JOIN temp_physician_npi_spclty SPCL
on SPCL.company_id = C1.company_id and SPCL.practitioner_code = C1.practitioner_code
WHERE lower(C1.raw_role) = 'consulting 2'),
cnslt_pract_3 as 
(
select C1.company_id, C1.patient_id, C1.practitioner_code as cnslt_pract_3_cd,
SPCL.npi as cnslt_pract_3_npi,
SPCL.practitioner_name as cnslt_pract_3_nm,
SPCL.practitioner_spclty_description as cnslt_pract_3_spclty,
SPCL.mcare_spcly_cd as cnslt_pract_3_mcare_spcly_cd
FROM temp_eligible_encntr_data Z
INNER JOIN pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct C1
on C1.company_id = Z.company_id and Z.patient_id = C1.patient_id
INNER JOIN pce_qe16_slp_prd_dm..phys_dim P
on P.practitioner_code = C1.practitioner_code and C1.company_id = P.company_id
LEFT JOIN temp_physician_npi_spclty SPCL
on SPCL.company_id = C1.company_id and SPCL.practitioner_code = C1.practitioner_code
WHERE lower(C1.raw_role) = 'consulting 3')
select T1.company_id as src_company_id, T1.patient_id as src_patient_id,
C1.cnslt_pract_1_cd, cnslt_pract_1_nm,  C1.cnslt_pract_1_npi, C1.cnslt_pract_1_spclty, C1.cnslt_pract_1_mcare_spcly_cd, 
C2.cnslt_pract_2_cd, cnslt_pract_2_nm,  C2.cnslt_pract_2_npi, C2.cnslt_pract_2_spclty, C2.cnslt_pract_2_mcare_spcly_cd, 
C3.cnslt_pract_3_cd, cnslt_pract_3_nm,  C3.cnslt_pract_3_npi, C3.cnslt_pract_3_spclty, C3.cnslt_pract_3_mcare_spcly_cd
FROM temp_eligible_encntr_data T1
LEFT JOIN cnslt_pract_1 C1
on C1.company_id = T1.company_id and T1.patient_id = C1.patient_id
LEFT JOIN cnslt_pract_2 C2
on C2.company_id = T1.company_id and T1.patient_id = C2.patient_id
LEFT JOIN cnslt_pract_3 C3
on C3.company_id = T1.company_id and T1.patient_id = C3.patient_id;

DROP TABLE temp_surgeon_pract IF exists;
CREATE TEMP TABLE temp_surgeon_pract AS 
(
  select Z.company_id, Z.patient_id, P.surgeon_code as prim_srgn_cd, 
  PHY.npi as prim_srgn_npi,
  PHY.practitioner_name as prim_srgn_nm,
  PHY.practitioner_spclty_description as prim_srgn_spclty,
  PHY.mcare_spcly_cd as prim_srgn_mcare_spcly_cd
  from temp_eligible_encntr_data Z
  LEFT JOIN pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct P 
  on P.company_id = Z.company_id and Z.patient_id = P.patient_id 
  LEFT JOIN pce_qe16_slp_prd_dm..temp_physician_npi_spclty PHY
  on PHY.practitioner_code = P.surgeon_code and P.company_id = PHY.company_id
  WHERE P.proceduretype='Primary' AND surgeon_code is NOT NULL
);


---New

DROP TABLE temp_physician_fcy_std_spclty

IF EXISTS;
	CREATE TEMP TABLE temp_physician_fcy_std_spclty AS (
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


DROP TABLE temp_discharge_fcy_std_status_code

IF EXISTS;
	CREATE TEMP TABLE temp_discharge_fcy_std_status_code AS (
		SELECT DISTINCT ZOOM.discharge_status
		,DISSTATUS.dschrg_sts_cd
		,DISSTATUS.dschrg_sts_descr FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM INNER JOIN temp_eligible_encntr_data ENCNTR ON ZOOM.company_id = ENCNTR.company_id
		AND ZOOM.patient_id = ENCNTR.patient_id LEFT JOIN pce_qe16_slp_prd_dm..dschrg_sts_dim DISSTATUS ON CAST(DISSTATUS.dschrg_sts_cd AS INT) = CAST(ZOOM.discharge_status AS INT)
		);--37

--NET Reveneue ----------------------------------------NET Revenue Model 
--Inpatient 

----Qualifiers 
--DROP TABLE temp_eligible_encntr_data IF EXISTS;
--	CREATE TEMP TABLE temp_eligible_encntr_data AS (
--		SELECT DISTINCT ZOOM.company_id
--		,ZOOM.patient_id , ZOOM.inpatient_outpatient_flag ,
--		ZOOM.admission_ts, ZOOM.discharge_ts FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM WHERE
----CODE change: Commented discharge_total_charges > 0 
----		ZOOM.discharge_total_charges > 0 AND 
----CODE Change: Added Discharge ts in the filter based on McLaren's request 
--		(cast(ZOOM.admission_ts AS DATE) >= DATE ('2015-10-01') OR cast(ZOOM.discharge_ts AS DATE) >= DATE ('2015-10-01'))
--		);
--SELECT count(*)
--FROM temp_eligible_encntr_data;

DROP TABLE pce_qe16_slp_prd_dm..temp_table_all_ip_rows IF Exists; 
CREATE TEMP TABLE pce_qe16_slp_prd_dm..temp_table_all_ip_rows  As 
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
  X.reimbursement_amount as tot_pymt_amt, 
  X.discharge_total_charges as tot_chrg_amt, 
  X.accountbalance as acct_bal_amt,
    --case when ROUND((X.accountbalance/X.discharge_total_charges * 100),2) <= 10 THEN
    case when abs(X.accountbalance)/X.discharge_total_charges * 100 <= 10 THEN
      'Y'
	  ELSE 
	   'N' END as est_acct_paid_ind,
   --  ROUND((X.accountbalance/X.discharge_total_charges),2) as acct_bal_pcnt,
    X.accountbalance/X.discharge_total_charges as acct_bal_pcnt,
  Y.payor_group1 as src_prim_payor_grp1,
  1 as cnt
from pce_qe16_oper_prd_zoom..cv_patdisch X 
 INNER JOIN pce_qe16_oper_prd_zoom..cv_paymstr Y
 ON Y.company_id = X.company_id and X.primary_payer_code = Y.payer_code
 INNER JOIN temp_eligible_encntr_data Z
 on Z.company_id = X.company_id and Z.patient_id = X.patient_id
  WHERE 
  X.inpatient_outpatient_flag = 'I' and X.discharge_total_charges > 0 -- AND X.company_id !='Lansing'
  --Added Ptnt_tp_Cd Exclusions based on "Derived Net Revenue Reference Documents" 
  --Code Change: COmmented as per reqiest from Lisa on 02/06
--  and upper(X.patient_type) NOT in ('LIP','MIP','BSCH','BSCHO','8','C','F','GCLK','LLOV','MCIV','OFCE','OFFICE','OFFICE SERIES','POV','PRO','Z','ZWH')
)
SELECT * FROM all_ip_recs;  

--###################################################################
-- CASE A - 'BlueCross','Medicare','Medicaid'
--###################################################################
--Inpatient Payment Ratio only for 'BlueCross','Medicare','Medicaid' (PAID) irrespective of DRG is NULL OR NOT 
DROP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_a IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_a AS 
select  fcy_nm, 
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
	select  fcy_nm, 
	   in_or_out_patient_ind ,
	   src_prim_payor_grp1,  
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt ) as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--	   ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--       ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM temp_table_all_ip_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')
--Code Change : Commente the next line
--AND  cast(dschrg_dt AS DATE) >= '2016-10-01'
--Code Change : UnCommented the next line
and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and  upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
Group by 1,2,3) X;

--Inpatient Payment Ratio only for 'BlueCross','Medicare','Medicaid' (PAID) of a DRG and Payor



DROP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_a IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_a AS 
select X.fcy_nm, 
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
	select  fcy_nm, 
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
FROM temp_table_all_ip_rows Z
INNER JOIN temp_ms_drg_dim_hist MSDRG
on Z.ms_drg_cd = MSDRG.ms_drg_cd
WHERE Z.est_acct_paid_ind ='Y' and Z.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--CODE change: Uncommented the next Line
and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
and MSDRG.drg_wght > 0.000
and Z.ms_drg_cd NOT IN ('-100','999')
and Z.dschrg_dt BETWEEN MSDRG.vld_fm_dt AND MSDRG.vld_to_dt 

--CODE change: Commented the next Line
--AND cast(Z.dschrg_dt AS DATE) >= '2016-10-01'
--and cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--CODE change: commented the next Line
--Group by 1,2,3,4
Group by 1,2,3
) X;

--CASE A 
DROP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_a IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_a AS 
-- Paid Cases i.e Account Balance <= 10%
Select 
PAID.* ,
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt), 2) as est_net_rev_amt
FROM temp_table_all_ip_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30') --85,855 Records
-- Unpaid Cases with a Drg (i.e Payments would be calculated based on Historical DRG Weights Ratio)
UNION 
select UNPD.*, 
ROUND(DRGWGHT.drg_wght * PAID.drg_weighted_pmnt_per_case, 2) as est_net_rev_amt
from temp_table_all_ip_rows UNPD
LEFT JOIN temp_ms_drg_dim_hist DRGWGHT
on UNPD.ms_drg_cd = DRGWGHT.ms_drg_cd
LEFT JOIN pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_a PAID
on UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm 
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  and (UNPD.ms_drg_cd IS NOT NULL  AND  UNPD.ms_drg_cd !='-100' AND UNPD.ms_drg_cd != '999')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
and dschrg_dt BETWEEN DRGWGHT.vld_fm_dt AND DRGWGHT.vld_to_dt
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30') --4578 Unpaid Cases with a Drg
UNION
-- Unpaid Cases without DRg (i.e Payments would be calcualted based on Historical Pymnt Ratio)
select UNPD.*, 
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio, 2) as est_net_rev_amt
from temp_table_all_ip_rows UNPD
LEFT JOIN pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_a PAID
on  UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm 
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  and (UNPD.ms_drg_cd IS NULL  OR UNPD.ms_drg_cd = '-100' OR UNPD.ms_drg_cd = '999')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
;
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30');  --26 Records Unpaid Cases without Drg


--###################################################################
-- CASE B - 'Commercial'
--###################################################################
--Inpatient Payment Ratio only for 'Commercial' (PAID) irrespective of DRG is NULL OR NOT 
DROP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_b IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_b AS 
select  X.fcy_nm,
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
	select  fcy_nm,
	   in_or_out_patient_ind ,
	   src_prim_payor_grp1,  
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt )  as  tot_pymt_amt,
       sum(tot_chrg_amt)   as tot_chrg_amt
-- ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
-- ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM temp_table_all_ip_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('Other') 
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
Group by 1,2,3) X;

DROP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_b IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_b AS 
select X.fcy_nm,
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
	select  fcy_nm,
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
FROM temp_table_all_ip_rows Z
INNER JOIN temp_ms_drg_dim_hist MSDRG
on Z.ms_drg_cd = MSDRG.ms_drg_cd
WHERE Z.est_acct_paid_ind ='Y' and Z.src_prim_payor_grp1 in ('Other')  
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--CODE change: Uncommented the next Line
and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
and MSDRG.drg_wght > 0.000
and Z.ms_drg_cd NOT IN ('-100', '999')
and Z.dschrg_dt BETWEEN MSDRG.vld_fm_dt AND MSDRG.vld_to_dt
--CODE change: Commented the next Line
--and cast(dschrg_dt AS DATE) >= '2016-10-01'
--and cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--CODE change: Commented the next Line
--Group by 1,2,3,4
Group by 1,2,3
) X;


DROP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_b IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_b AS 
-- Paid Cases i.e Account Balance <= 10%
Select 
PAID.* ,
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt), 2 ) as est_net_rev_amt
FROM temp_table_all_ip_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('Other')  
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30') --7,559 Records
-- Unpaid Cases with a Drg (i.e Payments would be calculated based on Historical DRG Weights Ratio)
UNION 
select UNPD.*, 
ROUND(DRGWGHT.drg_wght * PAID.drg_weighted_pmnt_per_case, 2)  as est_net_rev_amt
from temp_table_all_ip_rows UNPD
LEFT JOIN temp_ms_drg_dim_hist DRGWGHT
on UNPD.ms_drg_cd = DRGWGHT.ms_drg_cd
LEFT JOIN pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_b PAID
on UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('Other')   and (UNPD.ms_drg_cd IS NOT NULL  AND  UNPD.ms_drg_cd !='-100' AND UNPD.ms_drg_cd != '999')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
and dschrg_dt BETWEEN DRGWGHT.vld_fm_dt AND DRGWGHT.vld_to_dt
--and cast(UNPD.dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30') --4578 Unpaid Cases with a Drg
UNION
-- Unpaid Cases without Drg (i.e Payments would be calcualted based on Historical Pymnt Ratio)
SELECT UNPD.*, 
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio ,2) as est_net_rev_amt
from temp_table_all_ip_rows UNPD
LEFT JOIN prmretlp.ip_hist_pymt_ratio_case_b PAID
on  UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('Other')  and (UNPD.ms_drg_cd IS NULL  OR UNPD.ms_drg_cd = '-100' OR UNPD.ms_drg_cd = '999' )
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))
;
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30');  --4 Records Unpaid Cases without Drg

--###################################################################
-- CASE C - 'Domestic'  
--###################################################################
--Inpatient Payment Ratio only for 'Domestic' (PAID)
DROP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_c IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_net_rvu_case_c AS 
select ALLCASES.*, 
ROUND(ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio ,2)  as est_net_rev_amt
FROM temp_table_all_ip_rows ALLCASES
LEFT JOIN pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Domestic')  
AND RATIO.ptnt_cgy= 'Inpatient' and RATIO.payor_group_1 = 'Domestic'
-- AND  cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--AND cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30');
;

--###################################################################
-- CASE D - 'Self Pay'  
--###################################################################
--Inpatient Payment Ratio only for 'Self Pay'  (PAID)

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
  CASE WHEN ALLCASES.acct_bal_amt = 0 THEN 
      ROUND(ALLCASES.tot_pymt_amt ,2) 
	ELSE
	  ROUND( ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio, 2) 
    END AS  est_net_rev_amt
FROM temp_table_all_ip_rows ALLCASES
LEFT JOIN pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Self Pay' ) 
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
AND RATIO.ptnt_cgy= 'Inpatient' and RATIO.payor_group_1 = 'Self Pay' ;


DROP TABLE pce_qe16_slp_prd_dm..ip_encntr_net_rvu IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_encntr_net_rvu as 
SELECT * FROM 
(select * from ip_net_rvu_case_a UNION 
select * from ip_net_rvu_case_b UNION 
select * from ip_net_rvu_case_c UNION 
select * from ip_net_rvu_case_d
) z
--WHERE cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
;
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
--AND fcy_nm != 'Lansing';

----Outpatients
--Outpatient 

DROP TABLE pce_qe16_slp_prd_dm..temp_table_all_op_rows IF Exists; 
CREATE TEMP TABLE pce_qe16_slp_prd_dm..temp_table_all_op_rows As 
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
  X.reimbursement_amount as tot_pymt_amt, 
  X.discharge_total_charges as tot_chrg_amt, 
  X.accountbalance as acct_bal_amt,
  case when (abs(X.accountbalance)/X.discharge_total_charges * 100) <= 10 THEN
  --case when ROUND((X.accountbalance/X.discharge_total_charges * 100),2) <= 10 THEN
      'Y'
	  ELSE 
	   'N' END as est_acct_paid_ind,
  (X.accountbalance/X.discharge_total_charges) as acct_bal_pcnt,
--ROUND((X.accountbalance/X.discharge_total_charges),2) as acct_bal_pcnt,
  Y.payor_group1 as src_prim_payor_grp1,
  1 as cnt
from pce_qe16_oper_prd_zoom..cv_patdisch X 
 INNER JOIN pce_qe16_oper_prd_zoom..cv_paymstr Y
 ON Y.company_id = X.company_id and X.primary_payer_code = Y.payer_code
 INNER JOIN temp_eligible_encntr_data Z
 on Z.company_id = X.company_id and Z.patient_id = X.patient_id
  WHERE 
    X.inpatient_outpatient_flag = 'O' and X.discharge_total_charges > 0 
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
DROP TABLE pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_a IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_a AS 
select X.fcy_nm, 
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
	select  fcy_nm, 
	   in_or_out_patient_ind ,
	   ptnt_tp_cd,
	   src_prim_payor_grp1,  
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt) as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM temp_table_all_op_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
-- and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
Group by 1,2,3,4 ) X;

--'BlueCross','Medicare','Medicaid' Unpaid Encounter (Est.Net Revenue Amount) Union Paid Encounter
DROP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_a IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_a AS 
--Unpaid Cases
select UNPD.*, 
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio ,2) as est_net_rev_amt
FROM temp_table_all_op_rows UNPD
LEFT JOIN pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_a PAID 
on UNPD.ptnt_tp_cd  = PAID.ptnt_tp_cd and UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  
--AND cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
UNION 
--Paid Cases 
select PAID.*, 
--PAID.tot_pymt_amt +  PAID.acct_bal_amt  as est_net_rev_amt
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt) ,2 ) as est_net_rev_amt
FROM temp_table_all_op_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
;
--;cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30'); --1,505,622

--###################################################################
-- CASE B - 'Commercial' (Use 'Other'' for now)
--###################################################################
--Outpatient Payment Ratio only for 'Commercial' (Use 'Other'' for now) (PAID)
DROP TABLE pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_b IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_b AS 
select X.fcy_nm, 
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
	select  fcy_nm, 
	   in_or_out_patient_ind ,
	   ptnt_tp_cd,
	   src_prim_payor_grp1,  
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt)  as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM temp_table_all_op_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('Other')  
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
Group by 1,2,3 ,4 
--Group by 1,2
) X;

--'Commercial' Unpaid Encounter (Est.Net Revenue Amount) Union Paid Encounter
DROP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_b IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_b AS 
select UNPD.*, 
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio,2 )  as est_net_rev_amt
FROM temp_table_all_op_rows UNPD
LEFT JOIN pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_b PAID
on UNPD.ptnt_tp_cd  = PAID.ptnt_tp_cd and UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('Other')  
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) 
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
UNION 
select PAID.*, 
--PAID.tot_pymt_amt +  PAID.acct_bal_amt  as est_net_rev_amt
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt) ,2 ) as est_net_rev_amt
FROM temp_table_all_op_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('Other')  
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
;


--###################################################################
-- CASE C - 'Domestic'  
--###################################################################
--Outpatient Payment Ratio only for 'Domestic' (PAID)
DROP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_c IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_net_rvu_case_c AS 
select ALLCASES.*, 
ROUND(ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio ,2) as est_net_rev_amt
FROM temp_table_all_op_rows ALLCASES
LEFT JOIN pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Domestic')  
AND RATIO.ptnt_cgy= 'Outpatient' and RATIO.payor_group_1 = 'Domestic'  
--AND cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--AND cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
;
;

--###################################################################
-- CASE D - 'Self Pay'  
--###################################################################
--Outpatient Payment Ratio only for 'Self Pay'  (PAID)
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
  CASE WHEN ALLCASES.acct_bal_amt = 0 THEN 
      ROUND(ALLCASES.tot_pymt_amt ,2)
	ELSE
	  ROUND( ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio ,2 )
    END AS  est_net_rev_amt
FROM temp_table_all_op_rows ALLCASES
LEFT JOIN pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Self Pay' )  
AND RATIO.ptnt_cgy= 'Outpatient' and RATIO.payor_group_1 = 'Self Pay' 
--AND cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--AND cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
;

----Gross Revenue and Net Revenue (Revenue Model - Outpatient All 4 Cases/Scenario for the period October 2017 - September 2018 )

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
--WHERE cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--and Z.fcy_nm != 'Lansing'
group by fcy_nm;

DROP TABLE pce_qe16_slp_prd_dm..op_encntr_net_rvu IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_encntr_net_rvu as 
SELECT * FROM 
(select * from op_net_rvu_case_a UNION 
select * from op_net_rvu_case_b UNION 
select * from op_net_rvu_case_c UNION 
select * from op_net_rvu_case_d
) z
--WHERE cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from temp_fiscal_year_tbl) and (now()- Day(Now()))  
;
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--AND fcy_nm != 'Lansing';

--Resultant Table

DROP TABLE pce_qe16_slp_prd_dm..ip_dept_revenue_charges IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..ip_dept_revenue_charges AS 
select T.fcy_nm, T.encntr_num ,T.tot_pymt_amt,T.tot_chrg_amt, T.acct_bal_amt, sum(C.total_charge) as dept_or_revenue_total_charge_amt
,CASE WHEN sum(C.total_charge) > 0 THEN 'Y' ELSE 'N' END as prof_chrg_ind
FROM temp_table_all_ip_rows T
INNER JOIN pce_qe16_slp_prd_dm..intermediate_chrg_fct C
on C.company_id = T.fcy_nm and T.encntr_num = C.patient_id
WHERE (
--Department Exclusion
 C.dept in ('01.4405','01.4442','01.4444','01.4420','01.3175','01.3157','01.4412','01.4413','01.4416','01.4418','01.4419','01.4425')
 OR
--Revenue Code Exclusion
C.revenue_code in ('0960','0961','0969','0972','0977','0982','0983','0985','0987','0990')
)
group by 1,2,3, 4,5;

DROP TABLE pce_qe16_slp_prd_dm..op_dept_revenue_charges IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..op_dept_revenue_charges AS 
select T.fcy_nm, T.encntr_num ,T.tot_pymt_amt,T.tot_chrg_amt, T.acct_bal_amt, sum(C.total_charge) as dept_or_revenue_total_charge_amt
,CASE WHEN sum(C.total_charge) > 0 THEN 'Y' ELSE 'N' END as prof_chrg_ind
FROM temp_table_all_op_rows T
INNER JOIN pce_qe16_slp_prd_dm..intermediate_chrg_fct C
on C.company_id = T.fcy_nm and T.encntr_num = C.patient_id
WHERE (
--Department Exclusion
 C.dept in ('01.4405','01.4442','01.4444','01.4420','01.3175','01.3157','01.4412','01.4413','01.4416','01.4418','01.4419','01.4425')
 OR
--Revenue Code Exclusion
C.revenue_code in ('0960','0961','0969','0972','0977','0982','0983','0985','0987','0990')
)
group by 1,2,3, 4,5;

DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct_x IF EXISTS;
CREATE TEMP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct_x AS
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
FROM temp_eligible_encntr_data X 
LEFT JOIN combined Y
on X.company_id = Y.fcy_nm and X.patient_id   = Y.encntr_num
LEFT JOIN prof_chrg_combined Z
on X.company_id = Z.fcy_nm and X.patient_id   = Z.encntr_num; 


DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct AS
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
FROM temp_eligible_encntr_data X
LEFT JOIN combined Y
on X.company_id = Y.fcy_nm and X.patient_id   = Y.encntr_num
LEFT JOIN prof_chrg_combined Z
on X.company_id = Z.fcy_nm and X.patient_id   = Z.encntr_num;

--Combining all the Intermediate table

DROP TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio as 
select 'INPATIENT  - Medicare, Medicaid, BSBS' as scenario,  * from pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_a UNION 
select 'OUTPATIENT - Medicare, Medicaid, BSBS' as scenario,  * from pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_a UNION
select 'INPATIENT  - Others' as scenario, * from pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_case_b  UNION 
select 'OUTPATIENT - Others, Medicaid, BSBS' as scenario,* from pce_qe16_slp_prd_dm..op_hist_pymt_ratio_case_b; 

DROP TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts as 
select 'INPATIENT  - Medicare, Medicaid, BSBS' as scenario,  * from pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_a UNION
select 'INPATIENT  - Others' as scenario,  * from pce_qe16_slp_prd_dm..ip_hist_pymt_ratio_drg_case_b;

DROP TABLE pce_qe16_slp_prd_dm..intermediate_net_rvu_model IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_net_rvu_model as 
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
DROP TABLE pce_qe16_slp_prd_dm..temp_encntr_dgns_fct_with_cancer_case IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..temp_encntr_dgns_fct_with_cancer_case AS 
  SELECT W.src_company_id, W.src_patient_id, 1 as cancer_case_ind , W.cancer_dgns_cd,  W.cancer_case_code_descr
  FROM 
  (
    SELECT X.src_company_id_hash, X.src_patient_id_hash, X.src_company_id, X.src_patient_id, X.fcy_num, 
  X.patient_id, X.cancer_case_code_descr,X.cancer_dgns_cd, 
row_number() over(partition by X.company_id, X.patient_id ORDER BY X.diagnosistype ) as rank_num
  FROM intermediate_encntr_dgns_fct X
  WHERE ( (X.diagnosistype in ('Primary', 'Secondary') AND X.src_company_id NOT IN ('Karmanos')) OR
          (X.diagnosistype in ('Final Diagnosis') AND X.src_company_id IN ('Karmanos'))) AND 
  X.cancer_case_ind = 1 
 -- WHERE X.diagnosistype in ('Primary', 'Secondary') AND 
 -- X.cancer_case_ind = 1 
  ) W 
  WHERE W.rank_num = 1; 


DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct IF EXISTS;

	CREATE TABLE pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct AS

--WARNING! ERRORS ENCOUNTERED DURING SQL PARSING!
SELECT Distinct
	hash8(ZOOM.company_id)::bigint as src_company_id_hash
	,hash8(ZOOM.patient_id)::bigint as src_patient_id_hash
	,ZOOM.company_id AS fcy_nm
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
	,NVL(ZOOM.msdrg_code,'-100') AS ms_drg_cd
	--CODE CHANGE : AUG 2019 (a) Ms_Drg_Dim CMI Historical Weights 
	,ZOOM3YRS.ms_drg_wght
	,ZOOM3YRS.ms_drg_geo_mean_los_num
    ,ZOOM3YRS.ms_drg_arthm_mean_los_num
	,ACO_MSDRG.drg_fam_nm
	,ACO_MSDRG.case_mix_idnx_num
	,ACO_MSDRG.geo_mean_los_num
	,ACO_MSDRG.arthm_mean_los_num
	,nvl(ZOOM.apr_code,'-100') AS apr_cd
	,ZOOM.apr_severity_of_illness AS apr_svry_of_ill
	,ZOOM.apr_risk_of_mortality AS apr_rsk_of_mrtly
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
	,ZOOM.dischargeservice AS dschrg_svc
	,ZOOM.nursingstation AS nrg_stn
	,ZOOM.financialclass AS fnc_cls
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
		WHEN temp_dschrg_inpatient_nbrn.patient_id IS NULL
			THEN NULL
		ELSE temp_dschrg_inpatient_nbrn.dschrg_nbrn_ind
		END AS dschrg_nbrn_ind
	,CASE 
		WHEN temp_dschrg_inpatient_rehab.patient_id IS NULL
			THEN NULL
		ELSE temp_dschrg_inpatient_rehab.dschrg_rehab_ind
		END AS dschrg_rehab_ind
	,CASE 
		WHEN temp_dschrg_inpatient_psych.patient_id IS NULL
			THEN NULL
		ELSE temp_dschrg_inpatient_psych.dschrg_psych_ind
		END AS dschrg_psych_ind
	,CASE 
		WHEN temp_dschrg_inpatient_ltcsnf.patient_id IS NULL
			THEN NULL
		ELSE temp_dschrg_inpatient_ltcsnf.dschrg_ltcsnf_ind
		END AS dschrg_ltcsnf_ind
	,CASE 
		WHEN temp_dschrg_inpatient_hospice.patient_id IS NULL
			THEN NULL
		ELSE temp_dschrg_inpatient_hospice.dschrg_hospice_ind
		END AS dschrg_hospice_ind
	,CASE 
		WHEN temp_dschrg_inpatient_spclcare.patient_id IS NULL
			THEN NULL
		ELSE temp_dschrg_inpatient_spclcare.dschrg_spclcare_ind
		END AS dschrg_spclcare_ind
	,CASE 
		WHEN temp_dschrg_inpatient_lipmip.patient_id IS NULL
			THEN NULL
		ELSE temp_dschrg_inpatient_lipmip.dschrg_lipmip_ind
		END AS dschrg_lipmip_ind
	,CASE 
		WHEN temp_dschrg_inpatient_acute.patient_id IS NULL
			THEN NULL
		ELSE temp_dschrg_inpatient_acute.dschrg_acute_ind
		END AS dschrg_acute_ind
	,CASE 
		WHEN temp_dschrg_inpatient.patient_id IS NOT NULL
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
		WHEN temp_obsrv.qty > 0
			THEN temp_obsrv.qty
		ELSE temp_obsrv.qty
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
	, ( nvl(temp_derived_ptnt_days_acute.ptnt_days_acute ,0)+ 
	   nvl(temp_ccu.ccu_days, 0) + 
	   nvl(temp_icu.icu_days, 0) + 
	   nvl(temp_derived_ptnt_days_stepdown.ptnt_days_stepdown, 0) +
	   nvl(temp_derived_ptnt_days_nbrn.ptnt_days_nbrn, 0) + 
	   nvl(temp_derived_ptnt_days_rehab.ptnt_days_rehab, 0) +
	   nvl(temp_derived_ptnt_days_psych.ptnt_days_psych, 0)) AS ptnt_days
	,CASE 
		WHEN temp_derived_ptnt_days_psych.patient_id IS NOT NULL
			THEN temp_derived_ptnt_days_psych.ptnt_days_psych
		ELSE NULL
		END AS ptnt_days_pysch
	,CASE 
		WHEN temp_derived_ptnt_days_rehab.patient_id IS NOT NULL
			THEN temp_derived_ptnt_days_rehab.ptnt_days_rehab
		ELSE NULL
		END AS ptnt_days_rehab
	,CASE 
		WHEN temp_derived_ptnt_days_nbrn.patient_id IS NOT NULL
			THEN temp_derived_ptnt_days_nbrn.ptnt_days_nbrn
		ELSE NULL
		END AS ptnt_days_nbrn
	,CASE 
		WHEN temp_derived_ptnt_days_stepdown.patient_id IS NOT NULL
			THEN temp_derived_ptnt_days_stepdown.ptnt_days_stepdown
		ELSE NULL
		END AS ptnt_days_stepdown
	,CASE 
		WHEN temp_derived_ptnt_days_acute.patient_id IS NOT NULL
			THEN temp_derived_ptnt_days_acute.ptnt_days_acute
		ELSE NULL
		END AS ptnt_days_acute
	,CASE 
		WHEN temp_icu.patient_id IS NOT NULL
			THEN temp_icu.icu_days
		ELSE NULL
		END AS icu_days
	,CASE 
		WHEN temp_ccu.patient_id IS NOT NULL
			THEN temp_ccu.ccu_days
		ELSE NULL
		END AS ccu_days
	,CASE 
		WHEN temp_nrs.patient_id IS NOT NULL
			THEN temp_nrs.nrs_days
		ELSE NULL
		END AS nrs_days
	,CASE 
		WHEN temp_rtne.patient_id IS NOT NULL
			THEN temp_rtne.rtne_days
		ELSE NULL
		END AS rtne_days
	,CASE 
		WHEN temp_ed_case.patient_id IS NOT NULL
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
		WHEN temp_endoscopy_case.patient_id IS NULL
			THEN NULL
		ELSE 1
		END AS endoscopy_case_ind
	,CASE 
		WHEN temp_srgl_case.patient_id IS NULL
			THEN NULL
		ELSE 1
		END AS srgl_case_ind
	,CASE 
		WHEN temp_lithotripsy_case.patient_id IS NULL
			THEN NULL
		ELSE 1
		END AS lithotripsy_case_ind
	,CASE 
		WHEN temp_cathlab_case.patient_id IS NULL
			THEN NULL
		ELSE 1
		END AS cathlab_case_ind
	,nvl(ZOOM.admission_type_visit_type,'-100') AS adm_tp_cd
	,nvl(ZOOM.point_of_origin_for_admission_or_visit,'-100') AS pnt_of_orig_cd
	,nvl(ZOOM.discharge_status,'-100') AS dschrg_sts_cd
--Code Change : Zoom gets from Encntr but Integrated Mart gets from intermediate_encntr_dgns_fct so commenting Integrated Version
--  ,nvl(ADMDGNS.adm_icd_code,'-100') AS adm_dgns_cd
--	,nvl(ADMDGNS.adm_icd_descr,'UNKNOWN') AS adm_dgns_descr
--	,nvl(ADMDGNS.adm_diagnosis_code_present_on_admission_flag,'-100') AS adm_dgns_poa_flg_cd
	,nvl(ADMDGNS.dgns_cd,'-100') AS adm_dgns_cd
    ,nvl(ADMDGNS.dgns_descr,'UNKNOWN') AS adm_dgns_descr
	,'-100' AS adm_dgns_poa_flg_cd
	,nvl(DGNSDIM.dgns_cd,'-100') AS prim_dgns_cd
    ,nvl(DGNSDIM.dgns_descr,'UNKNOWN') AS prim_dgns_descr
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
--Code Change: Zoom gets from Encntr but Integrated Mart gets from intermediate_encntr_pcd_fct so commenting Integrated Version
--	,nvl(PRIMPROC.prim_proc_icd_code,'-100') AS prim_pcd_cd
--	,nvl(PRIMPROC.prim_proc_icd_pcd_descr,'UNKNOWN') as prim_pcd_descr
    ,nvl(PCDDIM.icd_pcd_cd,'-100') as prim_pcd_cd
	,nvl(PCDDIM.icd_pcd_descr,'UNKNOWN') as prim_pcd_descr
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
    ,SVCRNK.svc_cgy
	,SVCRNK.svc_ln  as svc_ln_nm
	,SVCRNK.sub_svc_ln as sub_svc_ln_nm
	,SVCRNK.services AS svc_nm
    ,SVCRNK.lvl_1_rnk
	,SVCRNK.lvl_2_rnk
	,SVCRNK.lvl_3_rnk
	,SVCRNK.lvl_4_rnk
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
	--Code Change : Physician Attributions Data Elements 
	,CASE WHEN ZOOM.inpatient_outpatient_flag = 'O' THEN 
	       nvl(ATTENDPRACTSPEC.practitioner_code,'-100') 
		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG'     THEN 
	        SURGEON.prim_srgn_cd
		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND (MSDRGDIM.ms_drg_type_cd = 'OTH' OR   MSDRGDIM.ms_drg_type_cd = 'MED')   THEN 
	        nvl(ATTENDPRACTSPEC.practitioner_code,'-100') 
		END AS attrb_physcn_cd,
        CASE WHEN ZOOM.inpatient_outpatient_flag = 'O' THEN 
	    ATTENDPRACTSPEC.practitioner_name 
		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG'     THEN 
	        SURGEON.prim_srgn_nm
		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND (MSDRGDIM.ms_drg_type_cd = 'OTH' OR   MSDRGDIM.ms_drg_type_cd = 'MED')     THEN 
	        ATTENDPRACTSPEC.practitioner_name 
		END AS attrb_physcn_nm,
	CASE WHEN ZOOM.inpatient_outpatient_flag = 'O' THEN 
	    ATTENDPRACTSPEC.npi 
		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG'     THEN 
	        SURGEON.prim_srgn_npi
		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND (MSDRGDIM.ms_drg_type_cd = 'OTH' OR   MSDRGDIM.ms_drg_type_cd = 'MED')     THEN 
	        ATTENDPRACTSPEC.npi  
		END as attrb_physn_npi,
	CASE WHEN ZOOM.inpatient_outpatient_flag = 'O' THEN 
	    ATTENDPRACTSPEC.mcare_spcly_cd 
		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG'     THEN 
	        SURGEON.prim_srgn_mcare_spcly_cd
		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND (MSDRGDIM.ms_drg_type_cd = 'OTH' OR   MSDRGDIM.ms_drg_type_cd = 'MED')     THEN 
	        ATTENDPRACTSPEC.mcare_spcly_cd 
		END AS attrb_physcn_spcl_cd,
	CASE WHEN ZOOM.inpatient_outpatient_flag = 'O' THEN 
	    ATTENDPRACTSPEC.practitioner_spclty_description 
		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND MSDRGDIM.ms_drg_type_cd = 'SURG'     THEN 
	        SURGEON.prim_srgn_spclty
		WHEN ZOOM.inpatient_outpatient_flag = 'I'  AND (MSDRGDIM.ms_drg_type_cd = 'OTH' OR   MSDRGDIM.ms_drg_type_cd = 'MED')     THEN 
	        ATTENDPRACTSPEC.practitioner_spclty_description 
		END AS attrb_physcn_spcl_cd_descr, 
		nvl(SPECLVALID.specl_valid_ind, 0 ) as specl_valid_ind 
        , nvl(CANCER.cancer_dgns_cd,'-100') as cancer_dgns_cd 
        , nvl(CANCER.cancer_case_ind , 0) as cancer_case_ind
	, nvl(CANCER.cancer_case_code_descr,'UNKNOWN') as cancer_case_code_descr
        , CLIENTDRG.mclaren_service_line as client_drg_svc_line_grp
        , CLIENTDRG.mclaren_sub_service_line as client_drg_sub_svc_line_grp
--	, CASE WHEN ZOOM.inpatient_outpatient_flag = 'I' AND UPPER(practitioner_spclty_description) in 
--	(
--	'CARDIOVASCULAR DISEASE (CARDIOLOGY)',
--	'INTERVENTIONAL CARDIOLOGY',
--	 'CARDIAC SURGERY',
--	 'THORACIC SURGERY',
--	 'CARDIAC ELECTROPHYSIOLOGY'
--	) THEN 1 ELSE 0 END AS  phys_specl_valid_ind 
FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM
INNER JOIN pce_qe16_slp_prd_dm..temp_eligible_encntr_data ZOOM3YRS
on ZOOM.company_id = ZOOM3YRS.company_id and ZOOM.patient_id = ZOOM3YRS.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct CHRGRCC
on ZOOM.company_id = CHRGRCC.src_company_id and ZOOM.patient_id = CHRGRCC.src_patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_encntr_qly_anl_fct QADV ON ZOOM.patient_id = QADV.encntr_num --AND ZOOM.company_id = QADV.fcy_num 
LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY ON VSET_FCY.cd = ZOOM.company_id
	AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
LEFT JOIN pce_qe16_slp_prd_dm..stnd_fcy_demog_dim FCY_REF ON VSET_FCY.alt_cd = FCY_REF.fcy_num
LEFT JOIN temp_dschrg_inpatient ON temp_dschrg_inpatient.patient_id = ZOOM.patient_id
	AND temp_dschrg_inpatient.company_id = ZOOM.company_id
LEFT JOIN temp_physician_npi_spclty ATTENDPRACTSPEC ON ZOOM.company_id = ATTENDPRACTSPEC.company_id
	AND ZOOM.attending_practitioner_code = ATTENDPRACTSPEC.practitioner_code
LEFT JOIN temp_physician_npi_spclty ADMITPRACTSPEC ON ZOOM.company_id = ADMITPRACTSPEC.company_id
	AND ZOOM.admitting_practitioner_code = ADMITPRACTSPEC.practitioner_code
LEFT JOIN temp_dschrg_inpatient_hospice ON temp_dschrg_inpatient_hospice.patient_id = ZOOM.patient_id
	AND temp_dschrg_inpatient_hospice.company_id = ZOOM.company_id
LEFT JOIN temp_obsrv ON temp_obsrv.patient_id = ZOOM.patient_id
	AND temp_obsrv.company_id = ZOOM.company_id
LEFT JOIN temp_icu ON temp_icu.patient_id = ZOOM.patient_id
	AND temp_icu.company_id = ZOOM.company_id
LEFT JOIN temp_ccu ON temp_ccu.patient_id = ZOOM.patient_id
	AND temp_ccu.company_id = ZOOM.company_id
LEFT JOIN temp_nrs ON temp_nrs.patient_id = ZOOM.patient_id
	AND temp_nrs.company_id = ZOOM.company_id
LEFT JOIN temp_rtne ON temp_rtne.patient_id = ZOOM.patient_id
	AND temp_rtne.company_id = ZOOM.company_id
LEFT JOIN temp_ed_case ON temp_ed_case.patient_id = ZOOM.patient_id
	AND temp_ed_case.company_id = ZOOM.company_id
LEFT JOIN temp_dschrg_inpatient_nbrn ON temp_dschrg_inpatient_nbrn.patient_id = ZOOM.patient_id
	AND temp_dschrg_inpatient_nbrn.company_id = ZOOM.company_id
LEFT JOIN temp_dschrg_inpatient_rehab ON temp_dschrg_inpatient_rehab.patient_id = ZOOM.patient_id
	AND temp_dschrg_inpatient_rehab.company_id = ZOOM.company_id
LEFT JOIN temp_dschrg_inpatient_psych ON temp_dschrg_inpatient_psych.patient_id = ZOOM.patient_id
	AND temp_dschrg_inpatient_psych.company_id = ZOOM.company_id
LEFT JOIN temp_dschrg_inpatient_ltcsnf ON temp_dschrg_inpatient_ltcsnf.patient_id = ZOOM.patient_id
	AND temp_dschrg_inpatient_ltcsnf.company_id = ZOOM.company_id
LEFT JOIN temp_dschrg_inpatient_spclcare ON temp_dschrg_inpatient_spclcare.patient_id = ZOOM.patient_id
	AND temp_dschrg_inpatient_spclcare.company_id = ZOOM.company_id
LEFT JOIN temp_dschrg_inpatient_lipmip ON temp_dschrg_inpatient_lipmip.patient_id = ZOOM.patient_id
	AND temp_dschrg_inpatient_lipmip.company_id = ZOOM.company_id
LEFT JOIN temp_dschrg_inpatient_acute ON temp_dschrg_inpatient_acute.patient_id = ZOOM.patient_id
	AND temp_dschrg_inpatient_acute.company_id = ZOOM.company_id
--Code Change: Commenting the following since ptnt_days_total would be based on SPL dimension
--LEFT JOIN temp_derived_ptnt_days ON temp_derived_ptnt_days.patient_id = ZOOM.patient_id
--AND temp_derived_ptnt_days.company_id = ZOOM.company_id
LEFT JOIN temp_srgl_case ON temp_srgl_case.patient_id = ZOOM.patient_id 
	AND temp_srgl_case.company_id = ZOOM.company_id
--Code Change : To add LITHOTRIPSY, Endoscopy and Cath Lab Case INDICATOR
LEFT JOIN temp_cathlab_case ON temp_cathlab_case.patient_id = ZOOM.patient_id 
	AND temp_cathlab_case.company_id = ZOOM.company_id
LEFT JOIN temp_lithotripsy_case ON temp_lithotripsy_case.patient_id = ZOOM.patient_id 
	AND temp_lithotripsy_case.company_id = ZOOM.company_id
LEFT JOIN temp_endoscopy_case ON temp_endoscopy_case.patient_id = ZOOM.patient_id 
	AND temp_endoscopy_case.company_id = ZOOM.company_id
LEFT JOIN temp_derived_ptnt_days_nbrn ON temp_derived_ptnt_days_nbrn.patient_id = ZOOM.patient_id
	AND temp_derived_ptnt_days_nbrn.company_id = ZOOM.company_id
--Code Change : To add Ptnt_Days_stepdown
LEFT JOIN temp_derived_ptnt_days_stepdown ON temp_derived_ptnt_days_stepdown.patient_id = ZOOM.patient_id
	AND temp_derived_ptnt_days_stepdown.company_id = ZOOM.company_id
LEFT JOIN temp_derived_ptnt_days_rehab ON temp_derived_ptnt_days_rehab.patient_id = ZOOM.patient_id
	AND temp_derived_ptnt_days_rehab.company_id = ZOOM.company_id
LEFT JOIN temp_derived_ptnt_days_acute ON temp_derived_ptnt_days_acute.patient_id = ZOOM.patient_id
	AND temp_derived_ptnt_days_acute.company_id = ZOOM.company_id
LEFT JOIN temp_derived_ptnt_days_psych ON temp_derived_ptnt_days_psych.patient_id = ZOOM.patient_id
	AND temp_derived_ptnt_days_psych.company_id = ZOOM.company_id
LEFT JOIN temp_payer_fcy_std_code PRIMPAYER ON PRIMPAYER.company_id = ZOOM.company_id
	AND PRIMPAYER.fcy_payer_code = ZOOM.primary_payer_code
LEFT JOIN temp_payer_fcy_std_code SECONPAYER ON SECONPAYER.company_id = ZOOM.company_id
	AND SECONPAYER.fcy_payer_code = ZOOM.secondary_payer_code
LEFT JOIN temp_payer_fcy_std_code TRTYPAYER ON TRTYPAYER.company_id = ZOOM.company_id
	AND TRTYPAYER.fcy_payer_code = ZOOM.tertiarypayorplan
LEFT JOIN temp_payer_fcy_std_code QTRPAYER ON QTRPAYER.company_id = ZOOM.company_id
	AND QTRPAYER.fcy_payer_code = ZOOM.quaternarypayorplan
--LEFT JOIN pce_qe16_slp_prd_dm..stnd_ptnt_type_dim STNDPTNTTYPE ON STNDPTNTTYPE.std_encntr_type_Cd = PATTYPEMAP.standard_patient_type_code
LEFT JOIN pce_qe16_slp_prd_dm..stnd_adm_type_dim ADMTYPE ON ADMTYPE.adm_type_cd = ZOOM.admission_type_visit_type
LEFT JOIN pce_qe16_slp_prd_dm..stnd_adm_src_dim ADMSRC ON ADMSRC.adm_src_cd = ZOOM.point_of_origin_for_admission_or_visit
LEFT JOIN temp_discharge_fcy_std_status_code DISSTATUS ON DISSTATUS.discharge_status = ZOOM.discharge_status
LEFT JOIN pce_qe16_slp_prd_dm..stnd_ptnt_zip_dim ZIPCODE ON ZIPCODE.ptnt_zip_cd = substr(ZOOM.residential_zip_code, 1, 5)
LEFT JOIN temp_ptnt_type_fcy_std_cd PATTYPE ON PATTYPE.patient_type_code = ZOOM.patient_type
	AND PATTYPE.company_id = ZOOM.company_id
LEFT JOIN temp_ptnt_prim_dgns PRIMDGNS ON PRIMDGNS.patient_id = ZOOM.patient_id
	AND PRIMDGNS.company_id = ZOOM.company_id
LEFT JOIN temp_ptnt_second_dgns SCDYDGNS ON SCDYDGNS.patient_id = ZOOM.patient_id
	AND SCDYDGNS.company_id = ZOOM.company_id
LEFT JOIN temp_ptnt_trty_dgns TRTYDGNS ON TRTYDGNS.patient_id = ZOOM.patient_id
	AND TRTYDGNS.company_id = ZOOM.company_id
LEFT JOIN temp_ptnt_prim_proc PRIMPROC ON PRIMPROC.patient_id = ZOOM.patient_id
	AND PRIMPROC.company_id = ZOOM.company_id
LEFT JOIN temp_ptnt_scdy_proc SCDYPROC ON SCDYPROC.patient_id = ZOOM.patient_id
	AND SCDYPROC.company_id = ZOOM.company_id
LEFT JOIN temp_ptnt_trty_proc TRTYPROC ON TRTYPROC.patient_id = ZOOM.patient_id
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
LEFT JOIN temp_encntr_svc_hier SVCRNK
on SVCRNK.company_id = ZOOM.company_id and SVCRNK.patient_id = ZOOM.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..temp_surgeon_pract SURGEON
on SURGEON.company_id = ZOOM.company_id and SURGEON.patient_id = ZOOM.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct CNSLT
on CNSLT.src_company_id = ZOOM.company_id and CNSLT.src_patient_id = ZOOM.patient_id
LEFT JOIN pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct NETREV
on NETREV.fcy_nm = ZOOM.company_id and NETREV.encntr_num = ZOOM.patient_id
LEFT JOIN ms_drg_dim MSDRGDIM
on MSDRGDIM.ms_drg_cd = CAST(LPAD(CAST(coalesce(ZOOM.msdrg_code,'000') as INTEGER), 3,0 ) as Varchar(3)) AND MSDRGDIM.ms_drg_type_cd IN ('SURG','MED','OTH') 
LEFT JOIN  pce_qe16_slp_prd_dm..temp_specl_valid_ind SPECLVALID
on SPECLVALID.src_company_id = ZOOM.company_id and ZOOM.patient_id = SPECLVALID.src_patient_id 
LEFT JOIN pce_qe16_slp_prd_dm..temp_encntr_dgns_fct_with_cancer_case CANCER
on CANCER.src_patient_id = ZOOM.patient_id AND CANCER.src_company_id = ZOOM.company_id
LEFT JOIN pce_qe16_slp_prd_dm..client_drg_svc_line_grouper CLIENTDRG
on CAST(LPAD(CAST(coalesce(ZOOM.msdrg_code,'000') as INTEGER), 3,0 ) as Varchar(3)) = CLIENTDRG.ms_drg_code
WHERE coalesce(ZOOM.msdrg_code ,'000') NOT IN ('V45','V70')
--WHERE ZOOM.discharge_total_charges > 0
--	AND cast(ZOOM.admission_ts AS DATE) BETWEEN add_months(CURRENT_DATE, - 36)
--		AND CURRENT_DATE
		DISTRIBUTE ON (
				src_company_id_hash
				,src_patient_id_hash
				);
--				

\unset ON_ERROR_STOP

