-- --select 'processing table: intermediate_stage_encntr_anl_fct ' as table_processing;
DROP TABLE intermediate_stage_encntr_anl_fct IF EXISTS;

CREATE TABLE intermediate_stage_encntr_anl_fct AS SELECT * FROM intermediate_stage_encntr_anl_fct_new;

 -----

--select 'processing table:  intermediate_chrg_cost_fct_prev' as table_processing;
DROP TABLE intermediate_chrg_cost_fct_prev IF EXISTS;
ALTER TABLE  intermediate_chrg_cost_fct RENAME TO  intermediate_chrg_cost_fct_prev;
ALTER TABLE  intermediate_stage_chrg_cost_fct  RENAME TO  intermediate_chrg_cost_fct;

--select 'processing table: intermediate_spl_dim_prev ' as table_processing;
DROP TABLE intermediate_spl_dim_prev IF EXISTS;
ALTER TABLE  intermediate_spl_dim RENAME TO  intermediate_spl_dim_prev;
ALTER TABLE  intermediate_stage_spl_dim  RENAME TO  intermediate_spl_dim;

--select 'processing table:  intermediate_chrg_fct_prev' as table_processing;
DROP TABLE intermediate_chrg_fct_prev IF EXISTS;
ALTER TABLE  intermediate_chrg_fct RENAME TO  intermediate_chrg_fct_prev;
ALTER TABLE  intermediate_stage_chrg_fct  RENAME TO  intermediate_chrg_fct;

--select 'processing table: intermediate_encntr_cnslt_pract_fct_prev ' as table_processing;
DROP TABLE intermediate_encntr_cnslt_pract_fct_prev IF EXISTS;
ALTER TABLE  intermediate_encntr_cnslt_pract_fct RENAME TO  intermediate_encntr_cnslt_pract_fct_prev;
ALTER TABLE  intermediate_stage_encntr_cnslt_pract_fct  RENAME TO  intermediate_encntr_cnslt_pract_fct;

--select 'processing table: intermediate_encntr_dgns_fct_prev ' as table_processing;
DROP TABLE intermediate_encntr_dgns_fct_prev IF EXISTS;
ALTER TABLE  intermediate_encntr_dgns_fct RENAME TO  intermediate_encntr_dgns_fct_prev;
ALTER TABLE  intermediate_stage_encntr_dgns_fct  RENAME TO  intermediate_encntr_dgns_fct;

--select 'processing table: intermediate_encntr_pcd_fct_prev ' as table_processing;
DROP TABLE intermediate_encntr_pcd_fct_prev IF EXISTS;
ALTER TABLE  intermediate_encntr_pcd_fct RENAME TO  intermediate_encntr_pcd_fct_prev;
ALTER TABLE  intermediate_stage_encntr_pcd_fct  RENAME TO  intermediate_encntr_pcd_fct;

--select 'processing table:  intermediate_encntr_net_rvu_fct_prev' as table_processing;
DROP TABLE intermediate_encntr_net_rvu_fct_prev IF EXISTS;
ALTER TABLE  intermediate_encntr_net_rvu_fct RENAME TO  intermediate_encntr_net_rvu_fct_prev;
ALTER TABLE  intermediate_stage_encntr_net_rvu_fct  RENAME TO  intermediate_encntr_net_rvu_fct;

--select 'processing table: intermediate_encntr_pract_fct_prev ' as table_processing;
DROP TABLE intermediate_encntr_pract_fct_prev IF EXISTS;
ALTER TABLE  intermediate_encntr_pract_fct RENAME TO  intermediate_encntr_pract_fct_prev;
ALTER TABLE  intermediate_stage_encntr_pract_fct  RENAME TO  intermediate_encntr_pract_fct;

--select 'processing table:  intermediate_svc_ln_anl_fct_prev' as table_processing;
DROP TABLE intermediate_svc_ln_anl_fct_prev if EXISTS;
ALTER TABLE  intermediate_svc_ln_anl_fct RENAME TO  intermediate_svc_ln_anl_fct_prev;
ALTER TABLE  intermediate_stage_svc_ln_anl_fct  RENAME TO  intermediate_svc_ln_anl_fct;

--select 'processing table:  intermediate_encntr_qly_anl_fct_prev' as table_processing;
DROP TABLE intermediate_encntr_qly_anl_fct_prev IF EXISTS;
ALTER TABLE  intermediate_encntr_qly_anl_fct RENAME TO  intermediate_encntr_qly_anl_fct_prev;
ALTER TABLE  intermediate_stage_encntr_qly_anl_fct  RENAME TO  intermediate_encntr_qly_anl_fct;

--select 'processing table: intermediate_hist_pymt_ratio_prev ' as table_processing;
DROP TABLE intermediate_hist_pymt_ratio_prev  IF EXISTS;
ALTER TABLE  intermediate_hist_pymt_ratio RENAME TO  intermediate_hist_pymt_ratio_prev;
ALTER TABLE  intermediate_stage_hist_pymt_ratio  RENAME TO  intermediate_hist_pymt_ratio;

--select 'processing table: intermediate_hist_pymt_ratio_drg_wghts_prev ' as table_processing;
DROP TABLE intermediate_hist_pymt_ratio_drg_wghts_prev IF EXISTS;
ALTER TABLE  intermediate_hist_pymt_ratio_drg_wghts RENAME TO  intermediate_hist_pymt_ratio_drg_wghts_prev;
ALTER TABLE  intermediate_stage_hist_pymt_ratio_drg_wghts  RENAME TO  intermediate_hist_pymt_ratio_drg_wghts;

--select 'processing table: intermediate_net_rvu_model_prev ' as table_processing;
DROP TABLE intermediate_net_rvu_model_prev IF EXISTS;
ALTER TABLE  intermediate_net_rvu_model RENAME TO  intermediate_net_rvu_model_prev;
ALTER TABLE  intermediate_stage_net_rvu_model  RENAME TO  intermediate_net_rvu_model;

--select 'processing table:  intermediate_cpt_fct_prev' as table_processing;
DROP TABLE intermediate_cpt_fct_prev IF EXISTS;
ALTER TABLE  intermediate_cpt_fct RENAME TO  intermediate_cpt_fct_prev;
ALTER TABLE  intermediate_stage_cpt_fct RENAME TO  intermediate_cpt_fct;

--select 'processing table:  intermediate_encntr_anl_fct_prev' as table_processing;
DROP TABLE intermediate_encntr_anl_fct_prev IF EXISTS;
ALTER TABLE  intermediate_encntr_anl_fct RENAME TO  intermediate_encntr_anl_fct_prev;
ALTER TABLE  intermediate_stage_encntr_anl_fct  RENAME TO  intermediate_encntr_anl_fct;

--select 'processing table: intermediate_encntr_ed_anl_fct_prev' as table_processing;
DROP TABLE intermediate_encntr_ed_anl_fct_prev IF EXISTS;
ALTER TABLE  intermediate_encntr_ed_anl_fct RENAME TO  intermediate_encntr_ed_anl_fct_prev;
ALTER TABLE  intermediate_stage_encntr_ed_anl_fct  RENAME TO  intermediate_encntr_ed_anl_fct;

--select 'processing table: intermediate_encntr_oncology_anl_fct_prev ' as table_processing;
--DROP TABLE intermediate_encntr_oncology_anl_fct_prev IF EXISTS;
--ALTER TABLE  intermediate_encntr_oncology_anl_fct RENAME TO  intermediate_encntr_oncology_anl_fct_prev;
--ALTER TABLE  intermediate_stage_encntr_oncology_anl_fct  RENAME TO  intermediate_encntr_oncology_anl_fct;

--select 'processing table: intermediate_chrg_agg_fct_prev ' as table_processing;
DROP TABLE intermediate_chrg_agg_fct_prev IF EXISTS;
ALTER TABLE  intermediate_chrg_agg_fct RENAME TO  intermediate_chrg_agg_fct_prev;
ALTER TABLE  intermediate_stage_chrg_agg_fct  RENAME TO  intermediate_chrg_agg_fct;


--select 'processing table: intermediate_ptnt_fnc_txn_agg_fct_prev ' as table_processing;
DROP TABLE intermediate_ptnt_fnc_txn_agg_fct_prev IF EXISTS;
ALTER TABLE  intermediate_ptnt_fnc_txn_agg_fct RENAME TO  intermediate_ptnt_fnc_txn_agg_fct_prev;
ALTER TABLE  intermediate_stage_ptnt_fnc_txn_agg_fct  RENAME TO  intermediate_ptnt_fnc_txn_agg_fct;

DROP TABLE intermediate_encntr_prim_cpt_fct_prev IF EXISTS;
ALTER TABLE  intermediate_encntr_prim_cpt_fct RENAME TO  intermediate_encntr_prim_cpt_fct_prev;
ALTER TABLE  intermediate_stage_encntr_prim_cpt_fct  RENAME TO  intermediate_encntr_prim_cpt_fct;

-----------

--select 'processing table:  prd_chrg_cost_fct' as table_processing;
DROP TABLE prd_chrg_cost_fct IF EXISTS;
CREATE TABLE prd_chrg_cost_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_chrg_cost_fct;

--select 'processing table: prd_spl_dim ' as table_processing;
DROP TABLE prd_spl_dim IF EXISTS;
CREATE TABLE prd_spl_dim AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_spl_dim;

--select 'processing table: prd_chrg_fct ' as table_processing;
DROP TABLE prd_chrg_fct IF EXISTS;
CREATE TABLE prd_chrg_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_chrg_fct;

--select 'processing table: prd_encntr_cnslt_pract_fct ' as table_processing;
DROP TABLE prd_encntr_cnslt_pract_fct IF EXISTS;
CREATE TABLE prd_encntr_cnslt_pract_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_encntr_cnslt_pract_fct;

--select 'processing table:prd_encntr_dgns_fct  ' as table_processing;
DROP TABLE prd_encntr_dgns_fct IF EXISTS;
CREATE TABLE prd_encntr_dgns_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_encntr_dgns_fct;

--select 'processing table: prd_encntr_pcd_fct' as table_processing;
DROP TABLE prd_encntr_pcd_fct IF EXISTS;
CREATE TABLE prd_encntr_pcd_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_encntr_pcd_fct;

--select 'processing table: prd_encntr_net_rvu_fct ' as table_processing;
DROP TABLE prd_encntr_net_rvu_fct IF EXISTS;
CREATE TABLE prd_encntr_net_rvu_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_encntr_net_rvu_fct;

--select 'processing table: prd_encntr_pract_fct ' as table_processing;
DROP TABLE prd_encntr_pract_fct IF EXISTS;
CREATE TABLE prd_encntr_pract_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_encntr_pract_fct;

--select 'processing table:prd_svc_ln_anl_fct  ' as table_processing;
DROP TABLE prd_svc_ln_anl_fct if EXISTS;
CREATE TABLE prd_svc_ln_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_svc_ln_anl_fct;

--select 'processing table: prd_encntr_qly_anl_fct ' as table_processing;
DROP TABLE prd_encntr_qly_anl_fct if EXISTS;
CREATE TABLE prd_encntr_qly_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_encntr_qly_anl_fct;

--select 'processing table: prd_hist_pymt_ratio ' as table_processing;
DROP TABLE prd_hist_pymt_ratio if EXISTS;
CREATE TABLE prd_hist_pymt_ratio AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_hist_pymt_ratio;

--select 'processing table:prd_hist_pymt_ratio_drg_wghts ' as table_processing;
DROP TABLE prd_hist_pymt_ratio_drg_wghts if EXISTS;
CREATE TABLE prd_hist_pymt_ratio_drg_wghts AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_hist_pymt_ratio_drg_wghts;

--select 'processing table: prd_net_rvu_model ' as table_processing;
DROP TABLE prd_net_rvu_model if EXISTS;
CREATE TABLE prd_net_rvu_model AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_net_rvu_model;

--select 'processing table: prd_cpt_fct ' as table_processing;
DROP TABLE prd_cpt_fct if EXISTS;
CREATE TABLE prd_cpt_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_cpt_fct;

--select 'processing table:prd_encntr_anl_fct  ' as table_processing;
DROP TABLE prd_encntr_anl_fct if EXISTS;
CREATE TABLE prd_encntr_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_encntr_anl_fct
DISTRIBUTE ON (fcy_nm,encntr_num);

--select 'processing table:prd_fnc_txn_fct' as table_processing;
DROP TABLE prd_fnc_txn_fct if EXISTS;
CREATE TABLE prd_fnc_txn_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_stage_fnc_txn_fct
DISTRIBUTE ON (fcy_nm,encntr_num);


--select 'processing table:prd_encntr_ed_anl_fct  ' as table_processing;
DROP TABLE prd_encntr_ed_anl_fct if EXISTS;
CREATE TABLE prd_encntr_ed_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_encntr_ed_anl_fct;

--select 'processing table:  prd_encntr_oncology_anl_fct' as table_processing;
--DROP TABLE prd_encntr_oncology_anl_fct if EXISTS;
--CREATE TABLE prd_encntr_oncology_anl_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_encntr_oncology_anl_fct;

--select 'processing table: prd_chrg_agg_fct' as table_processing;
DROP TABLE prd_chrg_agg_fct if EXISTS;
CREATE TABLE prd_chrg_agg_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_chrg_agg_fct;

--select 'processing table: prd_ptnt_fnc_txn_agg_fct ' as table_processing;
DROP TABLE prd_ptnt_fnc_txn_agg_fct if EXISTS;
CREATE TABLE prd_ptnt_fnc_txn_agg_fct AS SELECT *,now() as rcrd_isrt_ts FROM  intermediate_ptnt_fnc_txn_agg_fct;

DROP TABLE prd_encntr_prim_cpt_fct if EXISTS;
CREATE TABLE prd_encntr_prim_cpt_fct AS SELECT *,now() as rcrd_isrt_ts FROM intermediate_encntr_prim_cpt_fct;

DROP TABLE payr_grp_dim if EXISTS;
CREATE TABLE payr_grp_dim AS SELECT *,now() as rcrd_isrt_ts FROM intermediate_stage_temp_payer_fcy_std_code;

DROP TABLE ptnt_type_dim IF EXISTS;
CREATE TABLE ptnt_type_dim AS  SELECT *,now() as rcrd_isrt_ts FROM  intermediate_stage_temp_ptnt_type_fcy_std_cd;

DROP TABLE phy_npi_spclty_dim IF EXISTS;
CREATE TABLE phy_npi_spclty_dim AS  SELECT *,now() as rcrd_isrt_ts FROM  intermediate_stage_temp_physician_npi_spclty;
