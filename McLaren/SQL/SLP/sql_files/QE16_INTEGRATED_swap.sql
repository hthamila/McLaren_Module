DROP TABLE pce_qe16_slp_prd_dm..chrg_cost_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..chrg_cost_fct RENAME TO pce_qe16_slp_prd_dm..chrg_cost_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_chrg_cost_fct  RENAME TO pce_qe16_slp_prd_dm..chrg_cost_fct;

DROP TABLE pce_qe16_slp_prd_dm..spl_dim_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..spl_dim RENAME TO pce_qe16_slp_prd_dm..spl_dim_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_spl_dim  RENAME TO pce_qe16_slp_prd_dm..spl_dim;

DROP TABLE pce_qe16_slp_prd_dm..chrg_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..chrg_fct RENAME TO pce_qe16_slp_prd_dm..chrg_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_chrg_fct  RENAME TO pce_qe16_slp_prd_dm..chrg_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_cnslt_pract_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_cnslt_pract_fct RENAME TO pce_qe16_slp_prd_dm..encntr_cnslt_pract_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_encntr_cnslt_pract_fct  RENAME TO pce_qe16_slp_prd_dm..encntr_cnslt_pract_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_dgns_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_dgns_fct RENAME TO pce_qe16_slp_prd_dm..encntr_dgns_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_encntr_dgns_fct  RENAME TO pce_qe16_slp_prd_dm..encntr_dgns_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_pcd_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_pcd_fct RENAME TO pce_qe16_slp_prd_dm..encntr_pcd_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_encntr_pcd_fct  RENAME TO pce_qe16_slp_prd_dm..encntr_pcd_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_net_rvu_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_net_rvu_fct RENAME TO pce_qe16_slp_prd_dm..encntr_net_rvu_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_encntr_net_rvu_fct  RENAME TO pce_qe16_slp_prd_dm..encntr_net_rvu_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_pract_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_pract_fct RENAME TO pce_qe16_slp_prd_dm..encntr_pract_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_encntr_pract_fct  RENAME TO pce_qe16_slp_prd_dm..encntr_pract_fct;

DROP TABLE pce_qe16_slp_prd_dm..svc_ln_anl_fct_prev if EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..svc_ln_anl_fct RENAME TO pce_qe16_slp_prd_dm..svc_ln_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_svc_ln_anl_fct  RENAME TO pce_qe16_slp_prd_dm..svc_ln_anl_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_qly_anl_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_qly_anl_fct RENAME TO pce_qe16_slp_prd_dm..encntr_qly_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_encntr_qly_anl_fct  RENAME TO pce_qe16_slp_prd_dm..encntr_qly_anl_fct;

DROP TABLE pce_qe16_slp_prd_dm..hist_pymt_ratio_prev  IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..hist_pymt_ratio RENAME TO pce_qe16_slp_prd_dm..hist_pymt_ratio_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_hist_pymt_ratio  RENAME TO pce_qe16_slp_prd_dm..hist_pymt_ratio;

DROP TABLE pce_qe16_slp_prd_dm..hist_pymt_ratio_drg_wghts_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..hist_pymt_ratio_drg_wghts RENAME TO pce_qe16_slp_prd_dm..hist_pymt_ratio_drg_wghts_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_hist_pymt_ratio_drg_wghts  RENAME TO pce_qe16_slp_prd_dm..hist_pymt_ratio_drg_wghts;

DROP TABLE pce_qe16_slp_prd_dm..net_rvu_model_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..net_rvu_model RENAME TO pce_qe16_slp_prd_dm..net_rvu_model_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_net_rvu_model  RENAME TO pce_qe16_slp_prd_dm..net_rvu_model;

DROP TABLE pce_qe16_slp_prd_dm..cpt_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..cpt_fct RENAME TO pce_qe16_slp_prd_dm..cpt_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_cpt_fct RENAME TO pce_qe16_slp_prd_dm..cpt_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_anl_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_anl_fct RENAME TO pce_qe16_slp_prd_dm..encntr_anl_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_encntr_anl_fct  RENAME TO pce_qe16_slp_prd_dm..encntr_anl_fct;
