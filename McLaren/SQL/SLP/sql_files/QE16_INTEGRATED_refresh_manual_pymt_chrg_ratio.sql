DROP TABLE pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio_prev IF EXISTS;
alter table pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio RENAME TO pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio_prev;
alter table pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio_new RENAME TO pce_qe16_slp_prd_dm..manual_pymt_chrg_ratio; 
