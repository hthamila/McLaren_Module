
--DROP TABLE pce_qe16_slp_prd_dm..apr_drg_dim; 
--alter view pce_qe16_slp_prd_dm..apr_drg_dim_vw RENAME TO apr_drg_dim; 

--DROP TABLE pce_qe16_slp_prd_dm..ms_drg_dim; 
--alter view pce_qe16_slp_prd_dm..ms_drg_dim_vw RENAME TO ms_drg_dim; 

--DROP TABLE pce_qe16_slp_prd_dm..stnd_adm_type_dim; 
--alter view pce_qe16_slp_prd_dm..stnd_adm_type_dim_vw RENAME TO stnd_adm_type_dim; 

--DROP TABLE pce_qe16_slp_prd_dm..stnd_dschrg_sts_dim; 
alter view pce_qe16_slp_prd_dm..stnd_dschrg_sts_dim_vw RENAME TO stnd_dschrg_sts_dim;; 

DROP TABLE pce_qe16_slp_prd_dm..stnd_fcy_demog_dim; 
alter view pce_qe16_slp_prd_dm..stnd_fcy_demog_dim_vw RENAME TO stnd_fcy_demog_dim; 

--DROP TABLE pce_qe16_slp_prd_dm..stnd_ptnt_tp_dim; 
alter view pce_qe16_slp_prd_dm..stnd_ptnt_tp_dim_vw RENAME TO stnd_ptnt_tp_dim; 
