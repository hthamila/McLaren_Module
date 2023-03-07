DROP TABLE pce_qe16_slp_prd_dm..manual_rcc_cost_bkp IF EXISTS;
alter table pce_qe16_slp_prd_dm..manual_rcc_cost RENAME TO pce_qe16_slp_prd_dm..manual_rcc_cost_bkp;
alter table pce_qe16_slp_prd_dm..manual_rcc_cost_new RENAME TO pce_qe16_slp_prd_dm..manual_rcc_cost; 
