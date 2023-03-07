CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..paymap_dim AS
SELECT rcrd_load_type
       , rcrd_isrt_pcs_nm
       , rcrd_isrt_ts
       , rcrd_udt_pcs_nm
       , rcrd_udt_ts
       , rcrd_src_isrt_id
       , rcrd_src_isrt_ts
       , rcrd_src_udt_id
       , rcrd_src_udt_ts
       , rcrd_src_file_nm
       , rcrd_btch_audt_id
       , rcrd_pce_cst_nm
       , rcrd_pce_cst_src_nm
       , company_id
       , payer_code
       , CASE WHEN standard_payer_code = '#N/A' THEN NULL ELSE standard_payer_code END as standard_payer_code
  FROM pce_qe16_oper_prd_zoom.qe16zmp.cv_paymap;
