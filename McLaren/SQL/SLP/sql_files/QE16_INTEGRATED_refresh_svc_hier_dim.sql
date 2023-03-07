DROP TABLE pce_qe16_slp_prd_dm..svc_hier_dim_prev IF EXISTS; 
--CREATE TABLE pce_qe16_slp_prd_dm..svc_hier_dim_prev AS SELECT * from pce_qe16_slp_prd_dm..svc_hier_dim;
alter table pce_qe16_slp_prd_dm..svc_hier_dim RENAME TO pce_qe16_slp_prd_dm..svc_hier_dim_prev;
alter table pce_qe16_slp_prd_dm..svc_hier_dim_new RENAME TO pce_qe16_slp_prd_dm..svc_hier_dim; 
