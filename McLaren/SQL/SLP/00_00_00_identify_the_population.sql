--Qualifiers
--select 'processing table:  intermediate_stage_temp_dates_tbl' as table_processing;
DROP TABLE intermediate_stage_temp_dates_tbl IF EXISTS;
CREATE TABLE intermediate_stage_temp_dates_tbl
AS
select
CASE WHEN DATE_PART('day',now()) >=15 THEN last_day(now() - INTERVAL '1 MONTH') ELSE last_day(now() - INTERVAL '2 MONTH') END as curr_year_end_dt;

--select 'processing table:  intermediate_stage_temp_fiscal_year_tbl' as table_processing;
DROP TABLE intermediate_stage_temp_fiscal_year_tbl IF EXISTS;
CREATE  Table intermediate_stage_temp_fiscal_year_tbl AS
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
	CREATE  TABLE intermediate_stage_temp_eligible_encntr_data_inpatient AS (
		SELECT DISTINCT ZOOM.company_id
		,ZOOM.patient_id , ZOOM.inpatient_outpatient_flag ,
		ZOOM.admission_ts
		,case WHEN ZOOM.inpatient_outpatient_flag ='O' THEN COALESCE(Date(zoom.discharge_ts), Date(zoom.admission_ts)) ELSE Date(zoom.discharge_ts) END AS discharge_ts
  		,case WHEN ZOOM.inpatient_outpatient_flag ='O' THEN COALESCE(year(zoom.discharge_ts), year(zoom.admission_ts)) ELSE year(zoom.discharge_ts) END AS discharge_yr
		--,ZOOM.msdrg_code
        ,coalesce (ZOOM.msdrg_code,cast(B.ms_drg_icd10 as nvarchar(10))) as msdrg_code
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
        --Added MS DRG code for caro facility from QADV - Oct 2021
        left join pce_qe16_prd_qadv.prmradmp.encntr B
        on ZOOM.patient_id=B.encntr_num and fcy_num='MI2194'
        left outer join pce_qe16_prd_qadv..ms_drg_ref as ms on b.ms_drg_icd10 = ms.ms_drg_cd and b.ms_drg_mdc_icd10 = ms.ms_drg_mdc_cd
        WHERE ZOOM.inpatient_outpatient_flag ='I' AND
 coalesce(ZOOM.msdrg_code,'000') NOT IN ('V45','V70') AND
 ((cast(ZOOM.admission_ts AS DATE) BETWEEN DATE('2015-10-01') AND now()) OR (cast(ZOOM.discharge_ts AS DATE) BETWEEN DATE('2015-10-01') AND now())));



--select 'processing table:  intermediate_stage_temp_eligible_encntr_data_outpatient' as table_processing;
DROP TABLE intermediate_stage_temp_eligible_encntr_data_outpatient IF EXISTS;
	CREATE  TABLE intermediate_stage_temp_eligible_encntr_data_outpatient AS (
		SELECT DISTINCT ZOOM.company_id
		,ZOOM.patient_id , ZOOM.inpatient_outpatient_flag ,
		ZOOM.admission_ts
		,case WHEN ZOOM.inpatient_outpatient_flag ='O' THEN COALESCE(Date(zoom.discharge_ts), Date(zoom.admission_ts)) ELSE Date(zoom.discharge_ts) END AS discharge_ts
		,case WHEN ZOOM.inpatient_outpatient_flag ='O' THEN COALESCE(year(zoom.discharge_ts), year(zoom.admission_ts)) ELSE year(zoom.discharge_ts) END AS discharge_yr
		--,ZOOM.msdrg_code
        ,coalesce (ZOOM.msdrg_code,cast(B.ms_drg_icd10 as nvarchar(10))) as msdrg_code
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
      --Added MS DRG code for caro facility from QADV - Oct 2021
        left join pce_qe16_prd_qadv.prmradmp.encntr B
        on ZOOM.patient_id=B.encntr_num and fcy_num='MI2194'
        left outer join pce_qe16_prd_qadv..ms_drg_ref as ms
        on b.ms_drg_icd10 = ms.ms_drg_cd and b.ms_drg_mdc_icd10 = ms.ms_drg_mdc_cd
		WHERE ZOOM.inpatient_outpatient_flag ='O' AND
 coalesce(ZOOM.msdrg_code,'000') NOT IN ('V45','V70') AND
 (cast(ZOOM.admission_ts AS DATE) BETWEEN DATE('2015-10-01') AND now())
		);


--CODE CHANGE : AUG 2019 (a) Ms_Drg_Dim Historical CMI Weights
--select 'processing table:  intermediate_stage_temp_ms_drg_dim_hist' as table_processing;
DROP TABLE intermediate_stage_temp_ms_drg_dim_hist  IF EXISTS;
CREATE  TABLE intermediate_stage_temp_ms_drg_dim_hist AS
select ms_drg_cd,
CAST(case_mix_idnx_num as NUMERIC(14,4)) as drg_wght,
CAST(geo_mean_los_num as NUMERIC(14,4)) as ms_drg_geo_mean_los_num,
CAST(arthm_mean_los_num as NUMERIC(14,4)) as ms_drg_arthm_mean_los_num,
drg_vrsn, vld_fm_dt, nvl(vld_to_dt, now()) as vld_to_dt
  FROM pce_ae00_aco_prd_cdr..ms_drg_dim_h
  WHERE case_mix_idnx_num NOT IN ('UNKNOWN');


--CODE CHANGE : AUG 2019 (a) Ms_Drg_Dim Historical CMI Weights   intermediate_stage_temp_eligible_encntr_data
--select 'processing table:  intermediate_stage_temp_eligible_encntrs' as table_processing;
DROP TABLE intermediate_stage_temp_eligible_encntrs IF EXISTS;
CREATE  TABLE intermediate_stage_temp_eligible_encntrs
AS
SELECT X.*
FROM intermediate_stage_temp_eligible_encntr_data_inpatient X
UNION
SELECT Y.*
FROM intermediate_stage_temp_eligible_encntr_data_outpatient Y;


--select 'processing table: intermediate_stage_temp_encntr_with_ms_drg_wghts' as table_processing;
DROP TABLE intermediate_stage_temp_encntr_with_ms_drg_wghts IF EXISTS;

CREATE TABLE intermediate_stage_temp_encntr_with_ms_drg_wghts AS
SELECT
X.company_id, X.patient_id,  X.inpatient_outpatient_flag,  X.admission_ts,  X.discharge_ts, X.discharge_yr,  X.msdrg_code,
nvl(DRGWGHT.drg_wght , X.ms_drg_wght) as ms_drg_wght,
nvl(DRGWGHT.ms_drg_geo_mean_los_num, X.ms_drg_geo_mean_los_num) as ms_drg_geo_mean_los_num,
nvl(DRGWGHT.ms_drg_arthm_mean_los_num, X.ms_drg_arthm_mean_los_num) as ms_drg_arthm_mean_los_num,
X.fiscal_yr,  X.fiscal_yr_tp
FROM  intermediate_stage_temp_eligible_encntrs X
INNER JOIN  intermediate_stage_temp_ms_drg_dim_hist DRGWGHT
ON X.msdrg_code= DRGWGHT.ms_drg_cd  AND date(X.discharge_ts)  BETWEEN DRGWGHT.vld_fm_dt AND DRGWGHT.vld_to_dt;


--select 'processing table:  intermediate_stage_temp_encntr_without_ms_drg_wghts ' as table_processing;
DROP TABLE intermediate_stage_temp_encntr_without_ms_drg_wghts IF EXISTS;
CREATE  TABLE intermediate_stage_temp_encntr_without_ms_drg_wghts AS
with recs_with_weights AS
(select distinct patient_id , company_id FROM  intermediate_stage_temp_encntr_with_ms_drg_wghts)
select * FROM  intermediate_stage_temp_eligible_encntrs X
WHERE (patient_id || company_id) NOT IN (select (patient_id || company_id) from recs_with_weights );

--select 'processing table: intermediate_stage_temp_eligible_encntr_data ' as table_processing;
DROP TABLE intermediate_stage_temp_eligible_encntr_data IF EXISTS;
CREATE  TABLE intermediate_stage_temp_eligible_encntr_data
AS
SELECT X.*
FROM intermediate_stage_temp_encntr_with_ms_drg_wghts X
UNION
SELECT Y.*
FROM intermediate_stage_temp_encntr_without_ms_drg_wghts Y;