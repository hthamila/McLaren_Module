DROP TABLE pce_qe16_slp_prd_dm..chrg_cost_fct_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..chrg_cost_fct RENAME TO pce_qe16_slp_prd_dm..chrg_cost_fct_old;
ALTER TABLE pce_qe16_slp_prd_dm..chrg_cost_fct_prev  RENAME TO pce_qe16_slp_prd_dm..chrg_cost_fct;

DROP TABLE pce_qe16_slp_prd_dm..chrg_fct_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..chrg_fct RENAME TO pce_qe16_slp_prd_dm..chrg_fct_old;
ALTER TABLE pce_qe16_slp_prd_dm..chrg_fct_prev  RENAME TO pce_qe16_slp_prd_dm..chrg_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_cnslt_pract_fct_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_cnslt_pract_fct RENAME TO pce_qe16_slp_prd_dm..encntr_cnslt_pract_fct_old;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_cnslt_pract_fct_prev  RENAME TO pce_qe16_slp_prd_dm..encntr_cnslt_pract_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_dgns_fct_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_dgns_fct RENAME TO pce_qe16_slp_prd_dm..encntr_dgns_fct_old;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_dgns_fct_prev  RENAME TO pce_qe16_slp_prd_dm..encntr_dgns_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_pcd_fct_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_pcd_fct RENAME TO pce_qe16_slp_prd_dm..encntr_pcd_fct_old;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_pcd_fct_prev  RENAME TO pce_qe16_slp_prd_dm..encntr_pcd_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_net_rvu_fct_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_net_rvu_fct RENAME TO pce_qe16_slp_prd_dm..encntr_net_rvu_fct_old;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_net_rvu_fct_prev  RENAME TO pce_qe16_slp_prd_dm..encntr_net_rvu_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_pract_fct_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_pract_fct RENAME TO pce_qe16_slp_prd_dm..encntr_pract_fct_old;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_pract_fct_prev  RENAME TO pce_qe16_slp_prd_dm..encntr_pract_fct;

DROP TABLE pce_qe16_slp_prd_dm..svc_ln_anl_fct_old if EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..svc_ln_anl_fct RENAME TO pce_qe16_slp_prd_dm..svc_ln_anl_fct_old;
ALTER TABLE pce_qe16_slp_prd_dm..svc_ln_anl_fct_prev  RENAME TO pce_qe16_slp_prd_dm..svc_ln_anl_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_qly_anl_fct_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_qly_anl_fct RENAME TO pce_qe16_slp_prd_dm..encntr_qly_anl_fct_old;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_qly_anl_fct_prev  RENAME TO pce_qe16_slp_prd_dm..encntr_qly_anl_fct;

DROP TABLE pce_qe16_slp_prd_dm..hist_pymt_ratio_old  IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..hist_pymt_ratio RENAME TO pce_qe16_slp_prd_dm..hist_pymt_ratio_old;
ALTER TABLE pce_qe16_slp_prd_dm..hist_pymt_ratio_prev  RENAME TO pce_qe16_slp_prd_dm..hist_pymt_ratio;

DROP TABLE pce_qe16_slp_prd_dm..hist_pymt_ratio_drg_wghts_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..hist_pymt_ratio_drg_wghts RENAME TO pce_qe16_slp_prd_dm..hist_pymt_ratio_drg_wghts_old;
ALTER TABLE pce_qe16_slp_prd_dm..hist_pymt_ratio_drg_wghts_prev  RENAME TO pce_qe16_slp_prd_dm..hist_pymt_ratio_drg_wghts;

DROP TABLE pce_qe16_slp_prd_dm..net_rvu_model_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..net_rvu_model RENAME TO pce_qe16_slp_prd_dm..net_rvu_model_old;
ALTER TABLE pce_qe16_slp_prd_dm..net_rvu_model_prev  RENAME TO pce_qe16_slp_prd_dm..net_rvu_model;

DROP TABLE pce_qe16_slp_prd_dm..cpt_fct_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..cpt_fct RENAME TO pce_qe16_slp_prd_dm..cpt_fct_old;
ALTER TABLE pce_qe16_slp_prd_dm..cpt_fct_prev RENAME TO pce_qe16_slp_prd_dm..cpt_fct;

DROP TABLE pce_qe16_slp_prd_dm..encntr_anl_fct_old IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_anl_fct RENAME TO pce_qe16_slp_prd_dm..encntr_anl_fct_old;
ALTER TABLE pce_qe16_slp_prd_dm..encntr_anl_fct_prev  RENAME TO pce_qe16_slp_prd_dm..encntr_anl_fct;
