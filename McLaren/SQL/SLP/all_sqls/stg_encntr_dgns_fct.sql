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
  		,case WHEN ZOOM.inpatient_outpatient_flag ='O' THEN COALESCE(year(zoom.discharge_ts), year(zoom.admission_ts)) ELSE year(zoom.discharge_ts) END AS discharge_yr
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
		,case WHEN ZOOM.inpatient_outpatient_flag ='O' THEN COALESCE(year(zoom.discharge_ts), year(zoom.admission_ts)) ELSE year(zoom.discharge_ts) END AS discharge_yr
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
DROP TABLE intermediate_stage_temp_ms_drg_dim_hist  IF EXISTS;
CREATE TEMP TABLE intermediate_stage_temp_ms_drg_dim_hist AS 
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
CREATE TEMP TABLE intermediate_stage_temp_eligible_encntrs 
AS 
SELECT X.*
FROM intermediate_stage_temp_eligible_encntr_data_inpatient X
UNION 
SELECT Y.*
FROM intermediate_stage_temp_eligible_encntr_data_outpatient Y; 


--select 'processing table: intermediate_stage_temp_encntr_with_ms_drg_wghts' as table_processing;
DROP TABLE intermediate_stage_temp_encntr_with_ms_drg_wghts IF EXISTS; 

CREATE TEMP TABLE intermediate_stage_temp_encntr_with_ms_drg_wghts AS 
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
CREATE TEMP TABLE intermediate_stage_temp_encntr_without_ms_drg_wghts AS 
with recs_with_weights AS 
(select distinct patient_id , company_id FROM  intermediate_stage_temp_encntr_with_ms_drg_wghts)
select * FROM  intermediate_stage_temp_eligible_encntrs X  
WHERE (patient_id || company_id) NOT IN (select (patient_id || company_id) from recs_with_weights );

--select 'processing table: intermediate_stage_temp_eligible_encntr_data ' as table_processing;
DROP TABLE intermediate_stage_temp_eligible_encntr_data IF EXISTS;
CREATE TEMP TABLE intermediate_stage_temp_eligible_encntr_data 
AS 
SELECT X.*
FROM intermediate_stage_temp_encntr_with_ms_drg_wghts X
UNION 
SELECT Y.*
FROM intermediate_stage_temp_encntr_without_ms_drg_wghts Y; 


DROP TABLE intermediate_stage_temp_dgns_ccs_dim_cancer_only IF EXISTS;

--Code Change: 01/05/2021 Commenting the existing logic 
--SELECT distinct dgns_cd, ccs_dgns_cgy_descr 
--FROM pce_ae00_aco_prd_cdr..dgns_ccs_dim
----Code Change : 10/01/2020 : Updating the where clause to consider 997 as well as per MLH-591
----WHERE (ccs_dgns_cgy_cd BETWEEN 11 and 47 ) AND eff_to_Dt is NULL;
--WHERE (
--(CAST(ccs_dgns_cgy_cd as INT) BETWEEN 11 and 47 ) OR 
--(CAST(ccs_dgns_cgy_cd as INT) = 58 AND dgns_cd like 'E85%')
 --) AND eff_to_Dt is NULL;
 --Code change : 01/05/2021 : Updating the cancer_dgns_codes as per Member's request 
CREATE TEMP TABLE intermediate_stage_temp_dgns_ccs_dim_cancer_only AS 
SELECT distinct aco.dgns_cd, aco.ccs_dgns_cgy_descr 
FROM cncr_dgns_dim cncr
INNER JOIN pce_ae00_aco_prd_cdr..dgns_ccs_dim aco
on cncr.ccs_dgns_cgy_cd = aco.ccs_dgns_cgy_cd AND aco.dgns_cd = cncr.dgns_cd AND 
aco.dgns_cd_ver = cncr.dgns_cd_ver AND aco.eff_to_dt is NULL; 

--Creation of Encounter Diagnosis Fact based on Source System Rank (3M vs PowerChart)
DROP TABLE stg_encntr_dgns_fct if exists;
create table stg_encntr_dgns_fct as
select 
	fcy_nm
	, encntr_num
	, fcy_num
	, company_id
	, patient_id
	, icd_type
	, icd_version
	, case when sourcesystemdiag<>'3M CODING AND REIMBURSEMENT' and zdiagnosistype='Admitting' then coalesce(admitdiagnosiscode,icd_code)
		when sourcesystemdiag<>'3M CODING AND REIMBURSEMENT' and zdiagnosistype='Primary' then coalesce(primaryicd10diagnosiscode,icd_code) 
		else icd_code end as icd_code
	, diagnosisseq
	, zdiagnosistype as diagnosistype
	, diagnosis_code_present_on_admission_flag
	, sourcesystemdiag_rnk
	, sourcesystemdiag 
	, rcrd_pce_cst_src_nm customersource
	, case when REPLACE(icd_code,'.','') in ('Z5111','Z510','Z5112') THEN 1 ELSE 0 END as non_cancer_case_dgns_ind
	, case when non_cancer_case_dgns_ind =0 AND CANCER.dgns_cd is NOT NULL THEN 1 ELSE 0 END cancer_case_dgns_ind
        
        ,case when zdiagnosistype ='Primary'
        --and DF.diagnosisseq =1
        and  non_cancer_case_dgns_ind=1 then non_cancer_case_dgns_ind else 0 end  as prim_dgns_non_cancer_case_ind
        ,case when zdiagnosistype ='Secondary'
        --and DF.diagnosisseq =2
        and cancer_case_dgns_ind = 1 then cancer_case_dgns_ind else 0 end sec_dgns_cancer_case_ind
        ,case when zdiagnosistype ='Primary'
        --and DF.diagnosisseq =1
        and cancer_case_dgns_ind = 1 then cancer_case_dgns_ind else 0 end prim_dgns_cancer_case_ind
	, CASE WHEN CANCER.dgns_cd is NOT NULL THEN icd_code ELSE '-100' END as cancer_dgns_cd


	, nvl(DGNSD.ccs_dgns_cgy_cd,'-100') AS ccs_dgns_cgy_cd
        , nvl(DGNSD.ccs_dgns_cgy_descr,'UNKNOWN') AS ccs_dgns_cgy_descr
        , NVL(DGNSD.ccs_dgns_lvl_1_cd,'-100') AS ccs_dgns_lvl_1_cd
        , NVL(DGNSD.ccs_dgns_lvl_1_descr,'UNKNOWN') as ccs_dgns_lvl_1_descr
        , nvl(DGNSD.ccs_dgns_lvl_2_cd,'-100') as ccs_dgns_lvl_2_cd
        , nvl(DGNSD.ccs_dgns_lvl_2_descr,'UNKNOWN') AS ccs_dgns_lvl_2_descr

 from 
(
select a.*
		,row_number() OVER (PARTITION BY a.company_id,a.patient_id,zdiagnosistype,diagnosisseq ORDER BY sourcesystemdiag_rnk) AS row_num
from
(select 
         Z.company_id as fcy_nm
     	,Z.patient_id AS encntr_num
	,VSET_FCY.alt_cd as fcy_num
	,DF.company_id
       	,DF.patient_id
	,DF.icd_type
	,nvl(DF.icd_code, '-100') as icd_code
	,PD.admitdiagnosiscode
	,PD.primaryicd10diagnosiscode
       	,DF.diagnosis_code_present_on_admission_flag
       	,DF.icd_version
       	,case when lower(DF.diagnosistype) in ('admitting','admission diagnosis','admit') then 0 else DF.diagnosisseq end as diagnosisseq
	,case when lower(DF.diagnosistype) in ('admitting','admit') then 'Admitting'
		when lower(DF.diagnosistype)='final' and DF.diagnosisseq=1 then 'Primary'
	   	when lower(DF.diagnosistype)='final' and DF.diagnosisseq>1 then 'Secondary'
		when lower(DF.diagnosistype)='discharge' and DF.diagnosisseq=0 then 'Admitting'
		when lower(DF.diagnosistype)='discharge' and DF.diagnosisseq=1 then 'Primary'
		when lower(DF.diagnosistype)='discharge' and DF.diagnosisseq>1 then 'Secondary'
		when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='admission diagnosis' then 'Admitting'
		when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq=1 then 'Primary'
		when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq>1 then 'Secondary'
	   	  ELSE initcap(DF.diagnosistype) end as zdiagnosistype
	,case when DF.rcrd_pce_cst_src_nm='INST_BILL' and upper(DF.sourcesystemdiag)='PARAGON' then 0
	   	when DF.rcrd_pce_cst_src_nm='CERNER' and upper(DF.sourcesystemdiag)='3M CODING AND REIMBURSEMENT' then 0
		when upper(DF.sourcesystemdiag)='POWERCHART' then 1
		  ELSE 0 end as sourcesystemdiag_rnk
	,DF.sourcesystemdiag
	,DF.rcrd_pce_cst_src_nm
from intermediate_stage_temp_eligible_encntr_data Z
	LEFT JOIN dgns_fct DF on Z.company_id = DF.company_id and Z.patient_id = DF.patient_id
	LEFT JOIN pce_qe16_oper_prd_zoom..cv_patdisch PD on DF.company_id=PD.company_id and DF.patient_id=PD.patient_id
	LEFT JOIN val_set_dim VSET_FCY ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
)a
where zdiagnosistype in ('Admitting','Primary','Secondary') 
)b 
LEFT JOIN dgns_dim DGNSD ON REPLACE(b.icd_code,'.','')=REPLACE(DGNSD.dgns_cd,'.','') AND b.icd_version = DGNSD.dgns_icd_ver
LEFT JOIN intermediate_stage_temp_dgns_ccs_dim_cancer_only CANCER on replace(CANCER.dgns_cd,'.','') = replace(b.icd_code, '.','')
where b.row_num=1
distribute on (fcy_nm, encntr_num)
;
--Removing duplicates wherever primary is repeated
delete from stg_encntr_dgns_fct where (fcy_nm, encntr_num, diagnosisseq) in (
select fcy_nm, encntr_num, max(diagnosisseq) diagnosisseq from stg_encntr_dgns_fct where diagnosistype='Primary'
group by fcy_nm, encntr_num having count(1)>1)
;



\unset ON_ERROR_STOP
