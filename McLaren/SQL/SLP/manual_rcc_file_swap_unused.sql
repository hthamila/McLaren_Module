drop table pce_qe16_slp_prd_dm..manual_rcc_cost_prev IF EXiSTS 
alter table pce_qe16_slp_prd_dm..manual_rcc_cost RENAME TO manual_rcc_cost_prev;
alter table pce_qe16_slp_prd_dm..manual_rcc_cost_new RENAME TO manual_rcc_cost;
