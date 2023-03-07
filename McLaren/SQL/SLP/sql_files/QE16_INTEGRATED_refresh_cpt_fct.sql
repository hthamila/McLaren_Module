DROP TABLE pce_qe16_slp_prd_dm..cpt_fct_prev IF EXISTS;
ALTER TABLE pce_qe16_slp_prd_dm..cpt_fct RENAME TO pce_qe16_slp_prd_dm..cpt_fct_prev;
ALTER TABLE pce_qe16_slp_prd_dm..stage_cpt_fct RENAME TO pce_qe16_slp_prd_dm..cpt_fct;
