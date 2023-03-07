\set ON_ERROR_STOP ON;

DROP TABLE pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_cost_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_chrg_cost_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_chrg_cost_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_chrg_cost_fct;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_spl_dim_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_spl_dim RENAME TO pce_qe16_slp_prd_dm..intermediate_spl_dim_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_spl_dim  RENAME TO pce_qe16_slp_prd_dm..intermediate_spl_dim;

DROP TABLE pce_qe16_slp_prd_dm..prd_spl_dim IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_spl_dim AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_spl_dim;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_chrg_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_chrg_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_chrg_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_chrg_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_chrg_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_chrg_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_chrg_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_chrg_fct;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_cnslt_pract_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_cnslt_pract_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_cnslt_pract_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_cnslt_pract_fct;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_dgns_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_dgns_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_dgns_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_dgns_fct;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_pcd_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_pcd_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_pcd_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_pcd_fct;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_net_rvu_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_net_rvu_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_net_rvu_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_net_rvu_fct;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_pract_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_pract_fct IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_pract_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_pract_fct;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct_prev if EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_svc_ln_anl_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_svc_ln_anl_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_svc_ln_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_svc_ln_anl_fct;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_qly_anl_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_qly_anl_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_qly_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_qly_anl_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_qly_anl_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_qly_anl_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_qly_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_qly_anl_fct;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_prev  IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio RENAME TO pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_hist_pymt_ratio  RENAME TO pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio;

DROP TABLE pce_qe16_slp_prd_dm..prd_hist_pymt_ratio if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_hist_pymt_ratio AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts RENAME TO pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_hist_pymt_ratio_drg_wghts  RENAME TO pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts;

DROP TABLE pce_qe16_slp_prd_dm..prd_hist_pymt_ratio_drg_wghts if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_hist_pymt_ratio_drg_wghts AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_hist_pymt_ratio_drg_wghts;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_net_rvu_model_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_net_rvu_model RENAME TO pce_qe16_slp_prd_dm..intermediate_net_rvu_model_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_net_rvu_model  RENAME TO pce_qe16_slp_prd_dm..intermediate_net_rvu_model;

DROP TABLE pce_qe16_slp_prd_dm..prd_net_rvu_model if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_net_rvu_model AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_net_rvu_model;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_cpt_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_cpt_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_cpt_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_cpt_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_cpt_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_cpt_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_cpt_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_cpt_fct;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_anl_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_anl_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_anl_fct;


DROP TABLE pce_qe16_slp_prd_dm..intermediate_encntr_ed_anl_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_encntr_ed_anl_fct RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_ed_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..intermediate_stage_encntr_ed_anl_fct  RENAME TO pce_qe16_slp_prd_dm..intermediate_encntr_ed_anl_fct;

DROP TABLE pce_qe16_slp_prd_dm..prd_encntr_ed_anl_fct if EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_encntr_ed_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM pce_qe16_slp_prd_dm..intermediate_encntr_ed_anl_fct;

\unset ON_ERROR_STOP
