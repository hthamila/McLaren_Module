drop table stage_encntr_cst_anl_fct if exists; 
create table stage_encntr_cst_anl_fct as select fcy_nm, encntr_num,
 
 max(case when client_revenue_code_group='Routine Room & Board' then drct_cst_amt end ) as rtn_rm__brd_drct_cst_amt,
 max(case when client_revenue_code_group='Lab' then indrct_cst_amt end ) as lb_indrct_cst_amt,
 max(case when client_revenue_code_group='Trauma' then indrct_cst_amt end ) as trm_indrct_cst_amt,
 max(case when client_revenue_code_group='Drugs' then ttl_cst_amt end ) as drgs_ttl_cst_amt,
 max(case when client_revenue_code_group='Drugs' then ttl_chrg_amt end ) as drgs_ttl_chrg_amt,
 max(case when client_revenue_code_group='Drugs' then indrct_cst_amt end ) as drgs_indrct_cst_amt,
 max(case when client_revenue_code_group='Emergency Room' then drct_cst_amt end ) as emrgncy_rm_drct_cst_amt,
 max(case when client_revenue_code_group='Imaging' then indrct_cst_amt end ) as imgng_indrct_cst_amt,
 max(case when client_revenue_code_group='Surgery' then ttl_cst_amt end ) as srgry_ttl_cst_amt,
 max(case when client_revenue_code_group='Blood' then ttl_cst_amt end ) as bld_ttl_cst_amt,
 max(case when client_revenue_code_group='Imaging' then ttl_cst_amt end ) as imgng_ttl_cst_amt,
 max(case when client_revenue_code_group='Emergency Room' then indrct_cst_amt end ) as emrgncy_rm_indrct_cst_amt,
 max(case when client_revenue_code_group='SNF' then ttl_cst_amt end ) as snf_ttl_cst_amt,
 max(case when client_revenue_code_group='Rehab' then ttl_chrg_amt end ) as rhb_ttl_chrg_amt,
 max(case when client_revenue_code_group='Critical Care' then indrct_cst_amt end ) as crtcl_cr_indrct_cst_amt,
 max(case when client_revenue_code_group='Cardiology' then ttl_cst_amt end ) as crdlgy_ttl_cst_amt,
 max(case when client_revenue_code_group='Med-Surg Supplies' then indrct_cst_amt end ) as mdsrg_sppls_indrct_cst_amt,
 max(case when client_revenue_code_group='Clinic' then indrct_cst_amt end ) as clnc_indrct_cst_amt,
 max(case when client_revenue_code_group='Payment & Adjustment' then ttl_chrg_amt end ) as pymnt__adjstmnt_ttl_chrg_amt,
 max(case when client_revenue_code_group='SNF' then drct_cst_amt end ) as snf_drct_cst_amt,
 max(case when client_revenue_code_group='Observation or Tx Room' then ttl_cst_amt end ) as obsrvtn_r_tx_rm_ttl_cst_amt,
 max(case when client_revenue_code_group='Professional' then ttl_chrg_amt end ) as prfssnl_ttl_chrg_amt,
 max(case when client_revenue_code_group='Surgery' then drct_cst_amt end ) as srgry_drct_cst_amt,
 max(case when client_revenue_code_group='Observation or Tx Room' then indrct_cst_amt end ) as obsrvtn_r_tx_rm_indrct_cst_amt,
 max(case when client_revenue_code_group='Blood' then indrct_cst_amt end ) as bld_indrct_cst_amt,
 max(case when client_revenue_code_group='Other' then ttl_chrg_amt end ) as othr_ttl_chrg_amt,
 max(case when client_revenue_code_group='Routine Room & Board' then ttl_chrg_amt end ) as rtn_rm__brd_ttl_chrg_amt,
 max(case when client_revenue_code_group='Clinic' then ttl_chrg_amt end ) as clnc_ttl_chrg_amt,
 max(case when client_revenue_code_group='Critical Care' then ttl_cst_amt end ) as crtcl_cr_ttl_cst_amt,
 max(case when client_revenue_code_group='Lab' then ttl_cst_amt end ) as lb_ttl_cst_amt,
 max(case when client_revenue_code_group='Observation or Tx Room' then ttl_chrg_amt end ) as obsrvtn_r_tx_rm_ttl_chrg_amt,
 max(case when client_revenue_code_group='Payment & Adjustment' then drct_cst_amt end ) as pymnt__adjstmnt_drct_cst_amt,
 max(case when client_revenue_code_group='Clinic' then ttl_cst_amt end ) as clnc_ttl_cst_amt,
 max(case when client_revenue_code_group='Cardiology' then ttl_chrg_amt end ) as crdlgy_ttl_chrg_amt,
 max(case when client_revenue_code_group='Critical Care' then ttl_chrg_amt end ) as crtcl_cr_ttl_chrg_amt,
 max(case when client_revenue_code_group='Clinic' then drct_cst_amt end ) as clnc_drct_cst_amt,
 max(case when client_revenue_code_group='Med-Surg Supplies' then ttl_chrg_amt end ) as mdsrg_sppls_ttl_chrg_amt,
 max(case when client_revenue_code_group='Critical Care' then drct_cst_amt end ) as crtcl_cr_drct_cst_amt,
 max(case when client_revenue_code_group='Payment & Adjustment' then ttl_cst_amt end ) as pymnt__adjstmnt_ttl_cst_amt,
 max(case when client_revenue_code_group='Cardiology' then indrct_cst_amt end ) as crdlgy_indrct_cst_amt,
 max(case when client_revenue_code_group='Payment & Adjustment' then indrct_cst_amt end ) as pymnt__adjstmnt_indrct_cst_amt,
 max(case when client_revenue_code_group='Routine Room & Board' then ttl_cst_amt end ) as rtn_rm__brd_ttl_cst_amt,
 max(case when client_revenue_code_group='Implants' then ttl_cst_amt end ) as implnts_ttl_cst_amt,
 max(case when client_revenue_code_group='Implants' then ttl_chrg_amt end ) as implnts_ttl_chrg_amt,
 max(case when client_revenue_code_group='Rehab' then ttl_cst_amt end ) as rhb_ttl_cst_amt,
 max(case when client_revenue_code_group='Professional' then indrct_cst_amt end ) as prfssnl_indrct_cst_amt,
 max(case when client_revenue_code_group='Med-Surg Supplies' then ttl_cst_amt end ) as mdsrg_sppls_ttl_cst_amt,
 max(case when client_revenue_code_group='Drugs' then drct_cst_amt end ) as drgs_drct_cst_amt,
 max(case when client_revenue_code_group='Cardiology' then drct_cst_amt end ) as crdlgy_drct_cst_amt,
 max(case when client_revenue_code_group='Lab' then drct_cst_amt end ) as lb_drct_cst_amt,
 max(case when client_revenue_code_group='Other' then ttl_cst_amt end ) as othr_ttl_cst_amt,
 max(case when client_revenue_code_group='Rehab' then indrct_cst_amt end ) as rhb_indrct_cst_amt,
 max(case when client_revenue_code_group='Routine Room & Board' then indrct_cst_amt end ) as rtn_rm__brd_indrct_cst_amt,
 max(case when client_revenue_code_group='Surgery' then indrct_cst_amt end ) as srgry_indrct_cst_amt,
 max(case when client_revenue_code_group='Professional' then drct_cst_amt end ) as prfssnl_drct_cst_amt,
 max(case when client_revenue_code_group='Imaging' then ttl_chrg_amt end ) as imgng_ttl_chrg_amt,
 max(case when client_revenue_code_group='Trauma' then ttl_chrg_amt end ) as trm_ttl_chrg_amt,
 max(case when client_revenue_code_group='Rehab' then drct_cst_amt end ) as rhb_drct_cst_amt,
 max(case when client_revenue_code_group='Emergency Room' then ttl_chrg_amt end ) as emrgncy_rm_ttl_chrg_amt,
 max(case when client_revenue_code_group='Blood' then drct_cst_amt end ) as bld_drct_cst_amt,
 max(case when client_revenue_code_group='Trauma' then drct_cst_amt end ) as trm_drct_cst_amt,
 max(case when client_revenue_code_group='Emergency Room' then ttl_cst_amt end ) as emrgncy_rm_ttl_cst_amt,
 max(case when client_revenue_code_group='Trauma' then ttl_cst_amt end ) as trm_ttl_cst_amt,
 max(case when client_revenue_code_group='Implants' then indrct_cst_amt end ) as implnts_indrct_cst_amt,
 max(case when client_revenue_code_group='Blood' then ttl_chrg_amt end ) as bld_ttl_chrg_amt,
 max(case when client_revenue_code_group='Other' then indrct_cst_amt end ) as othr_indrct_cst_amt,
 max(case when client_revenue_code_group='Surgery' then ttl_chrg_amt end ) as srgry_ttl_chrg_amt,
 max(case when client_revenue_code_group='Other' then drct_cst_amt end ) as othr_drct_cst_amt,
 max(case when client_revenue_code_group='Observation or Tx Room' then drct_cst_amt end ) as obsrvtn_r_tx_rm_drct_cst_amt,
 max(case when client_revenue_code_group='Imaging' then drct_cst_amt end ) as imgng_drct_cst_amt,
 max(case when client_revenue_code_group='Professional' then ttl_cst_amt end ) as prfssnl_ttl_cst_amt,
 max(case when client_revenue_code_group='Implants' then drct_cst_amt end ) as implnts_drct_cst_amt,
 max(case when client_revenue_code_group='SNF' then ttl_chrg_amt end ) as snf_ttl_chrg_amt,
 max(case when client_revenue_code_group='SNF' then indrct_cst_amt end ) as snf_indrct_cst_amt,
 max(case when client_revenue_code_group='Lab' then ttl_chrg_amt end ) as lb_ttl_chrg_amt,
 max(case when client_revenue_code_group='Med-Surg Supplies' then drct_cst_amt end ) as mdsrg_sppls_drct_cst_amt,


1 as cnt from pce_qe16_slp_prd_dm..intermediate_encntr_cst_fct group by fcy_nm, encntr_num distribute on (fcy_nm, encntr_num); 

drop table stage_encntr_cst_anl_fct_prev if exists; 
alter table prd_encntr_cst_anl_fct rename to stage_encntr_cst_anl_fct_prev; 
alter table stage_encntr_cst_anl_fct rename to prd_encntr_cst_anl_fct;
