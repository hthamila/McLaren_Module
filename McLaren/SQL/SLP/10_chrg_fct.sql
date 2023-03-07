--CODE CHANGE: 09/30 RCC Map Derived Map - Logic
--Prepare the unique Charge code/ Rev code/ department code at Facility level based on cv_patbill
DROP TABLE  chrg_fct_cdmmstr_dim IF EXISTS;
CREATE TABLE chrg_fct_cdmmstr_dim AS
select distinct company_id, charge_code, revenue_Code, dept from pce_qe16_oper_prd_zoom..cv_patbill CH;


DROP TABLE  derived_rccmap_tbl IF EXISTS;
CREATE TABLE derived_rccmap_tbl as
SELECT
    CDMDIM.company_id,
    CDMDIM.charge_code,
    NULL AS charge_code_description,
    REVCD.revenue_code,
    REVCD.revenue_code_description,
    REVCD.client_revenue_code_group,
    REVCD.costreport_revenue_code_group,
    CDMDIM.dept,
    CRLINE.cr_line,
    CRLINE.begin_date as crline_begin_date,
    CRLINE.end_date as crline_end_date,
    CRLINE.fiscal_yr as crline_fiscal_yr,
    RCCASGN.cr_total_rcc,
    RCCASGN.cr_dir_rcc,
    RCCASGN.cr_ind_rcc,
    RCCASGN.begin_date as rccasgn_begin_date,
    RCCASGN.end_date as rccasgn_end_date,
    RCCASGN.fiscal_yr as rccasgn_fiscal_yr
FROM pce_qe16_oper_prd_zoom..cv_revcodemap_vw REVCD
         INNER JOIN chrg_fct_cdmmstr_dim CDMDIM
                    on REVCD.revenue_code = CDMDIM.revenue_code
----Cost Line Map
         INNER JOIN pce_qe16_oper_prd_zoom..cv_crlinemap_vw CRLINE
                    on CDMDIM.company_id = CRLINE.company_id AND CDMDIM.dept = CRLINE.department_code
                        AND lower(trim(CRLINE.costreport_revenue_code_group)) = lower(trim(REVCD.costreport_revenue_code_group))
    --AND CF.fiscal_yr = CRLINE.fiscal_yr
--------RCC Assign
         INNER JOIN pce_qe16_oper_prd_zoom..cv_rccassign_vw RCCASGN
                    on RCCASGN.company_id = CRLINE.company_id AND RCCASGN.company_id = CDMDIM.company_id  AND RCCASGN.cr_line = CRLINE.cr_line  AND CRLINE.fiscal_yr = RCCASGN.fiscal_yr
;

--intermediate_stage_chrg_fct Table creation based on Net 3 years Of patient Account Number
--select 'processing table:  intermediate_stage_chrg_fct_temp' as table_processing;
DROP TABLE intermediate_stage_chrg_fct_temp IF EXISTS ;
CREATE TABLE intermediate_stage_chrg_fct_temp AS
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
	,nvl(RCC.revenue_code_description,'UNKNOWN') AS revenue_code_description
	,nvl(RCC.client_revenue_code_group,'UNKNOWN') as client_revenue_code_group
	,nvl(RCC.costreport_revenue_code_group,'UNKNOWN') as costreport_revenue_code_group
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
        ,CH.cpt4full
       	,CH.subaccount
   --    	,CHRGCD.charge_code_description as chargecodedesc
        ,nvl(CH.chargecodedesc,CHRGCD.charge_code_description) as chargecodedesc
       	,CH.financialclass
       	,nvl(CH.payorplancode, '-100') as payorplancode
       	,DATE (to_timestamp((CH.updatedate || ' ' || nvl(substr(CH.updatedate, 1, 2), '00') || ':' || nvl(substr(CH.updatedate, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS')) AS updatedate
       	,CH.sourcesystem
       	,CH.raw_chargcode
	,CH.ndc
	,Z.fiscal_yr
       	,nvl(RCC.cr_dir_rcc,0) as direct_cost_ratio
       	,nvl(RCC.cr_ind_rcc,0) as indirect_cost_ratio
       	,nvl(RCC.cr_total_rcc,0) as total_cost_ratio
	,Round(nvl(CH.total_charge * RCC.cr_dir_rcc,  0),2) as rcc_based_direct_cst_amt
	,ROUND(nvl(CH.total_charge * RCC.cr_ind_rcc, 0),2) as rcc_based_indirect_cst_amt
	,Round(nvl(CH.total_charge * RCC.cr_total_rcc, 0),2) as rcc_based_total_cst_amt
--Code Change: 03/06 Added crline as per McLaren's Request
        ,nvl(RCC.cr_line,'0') as crline
	,hol.hol_ind as svc_dt_hol_ind
--      ,RAWCHRGCD.charge_code_description as raw_chrg_cd_descr
        ,CHRGCD.charge_code_description as raw_chrg_cd_descr
	,cadd.cpt_si
	,cadd.cpt_apc
	,cadd.cpt_addnd_b_rltv_wght
	,cadd.cpt_addnd_b_pymt_rt
	,cadd.cpt_addnd_b_min_unadj_copymt,
--09/15/2021: MLH-723: Added new measures based on the member's request
CH.billitemid as bill_itm_id,
CH.ndcunits as ndc_unts,
CH.ndcunitofmeasure as ndc_unt_of_msr,
CH.professional_charge_indicator as src_prfssnl_chrg_ind,
CH.diagnosis_1 as dgns_1,
CH.diagnosis_2 as dgns_2,
CH.diagnosis_3 as dgns_3,
CH.diagnosis_4 as dgns_4,
CH.diagnosis_5 as dgns_5,
CH.service_location_facility as prfmng_fcy,
CH.service_location_building as prfmg_buldng,
CH.late_charge_status as lt_chrg_sts,
CH.activity_type as actvty_tp,
CH.price_schedule as prc_schdl,
CH.tier_group as tr_grp,
CH.activity_datetime as actvty_dt_tm,
CH.gl_activity_datetime as gl_actvty_dt_tm,
CH.work_rvu as wrk_rvu,
CH.ndc_description as ndc_descr,
CH.reclass_indicator as rclss_ind
	,case when hbt.betos_cd='P8D' then 1 else null end as clnscpy_ind
    	,case when hccs.ccs_hcpcs_cgy_cd='182' then 1 else null end as mamgrphy_ind
  FROM intermediate_stage_temp_eligible_encntr_data Z
  LEFT JOIN pce_qe16_oper_prd_zoom..cv_patbill CH
  on Z.company_id = CH.company_id and Z.patient_id = CH.patient_id
  LEFT JOIN pce_qe16_slp_prd_dm..cdr_dim hol on to_date(CH.service_date,'mmddyyyy')=hol.cdr_dt
  LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
  ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN cdm_dim CHRGCD
  on CHRGCD.company_id = CH.company_id and CHRGCD.charge_code = CH.charge_code
  LEFT JOIN pce_qe16_slp_prd_dm..hcpcs_ccs_dim hccs
  on UPPER(nvl(CH.cpt_code,CHRGCD.cpt_code))=hccs.hcpcs_cd
  LEFT JOIN pce_qe16_slp_prd_dm..hcpcs_betos_dim hbt
  on UPPER(nvl(CH.cpt_code,CHRGCD.cpt_code))=hbt.hcpcs_cd
  --CODE CHANGE : 09/30 physicalized the Derived RCC Map Table to avoid the query long run
  LEFT JOIN derived_rccmap_tbl RCC
  on CH.company_id = RCC.company_id
  AND CH.charge_code = RCC.charge_code
  AND CH.dept = RCC.dept
  AND CH.revenue_code = RCC.revenue_code
  AND RCC.crline_fiscal_yr = RCC.rccasgn_fiscal_yr
AND DATE (to_timestamp((CH.service_date || ' ' || nvl(substr(CH.service_date, 1, 2), '00') || ':' || nvl(substr(CH.service_date, 3, 2), '00') || ':00'), 'MMDDYYYY HH24":"MI":"SS'))  BETWEEN DATE(RCC.rccasgn_begin_date) and DATE(RCC.rccasgn_end_date)
LEFT JOIN cpt_addndm_b_dim_h cadd on Z.discharge_yr=cadd.appl_yr and UPPER(nvl(CH.cpt_code,CHRGCD.cpt_code))=cadd.cpt_cd
)
--DISTRIBUTE ON (fcy_nm, encntr_num,charge_code);
DISTRIBUTE ON (fcy_nm, encntr_num);




----select 'processing table: intermediate_encntr_cst_fct' as table_processing;
DROP TABLE intermediate_encntr_cst_fct if exists;
CREATE TABLE intermediate_encntr_cst_fct as
select
 	 fcy_nm
     	,encntr_num
    	,client_revenue_code_group
     	,sum(quantity) qty
	,sum(total_charge) ttl_chrg_amt
	,sum(rcc_based_direct_cst_amt) drct_cst_amt
	,sum(rcc_based_indirect_cst_amt) indrct_cst_amt
	,sum(rcc_based_total_cst_amt) ttl_cst_amt
from intermediate_stage_chrg_fct_temp
	where strright(fiscal_yr,4)::int>=2015
group by 1,2,3;


-- Table with hash values on key columns

--select 'processing table:  intermediate_stage_chrg_fct' as table_processing;
DROP TABLE intermediate_stage_chrg_fct IF EXISTS;
CREATE TABLE intermediate_stage_chrg_fct as
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
LEFT JOIN intermediate_stage_spl_dim on intermediate_stage_chrg_fct_temp.charge_code=intermediate_stage_spl_dim.cdm_cd and intermediate_stage_chrg_fct_temp.fcy_num=intermediate_stage_spl_dim.fcy_num
LEFT JOIN rev_cl_dim on intermediate_stage_chrg_fct_temp.revenue_code = rev_cl_dim.rev_cd
LEFT JOIN hcpcs_dim  on intermediate_stage_chrg_fct_temp.cpt_code = hcpcs_dim.hcpcs_cd
LEFT JOIN dept_dim on  intermediate_stage_chrg_fct_temp.company_id = dept_dim.company_id and intermediate_stage_chrg_fct_temp.dept = dept_dim.department_code

--DISTRIBUTE ON (fcy_nm_hash,encntr_num_hash,charge_code_hash);
DISTRIBUTE ON (fcy_nm,encntr_num);