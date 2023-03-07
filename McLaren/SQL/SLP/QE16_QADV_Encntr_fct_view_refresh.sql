CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..encntr_fct AS 
SELECT fcy_num
       , dschrg_cdr_dk
--     , ptnt_cl_cd
       , encntr_num
       , dschrg_dt
       , adm_cdr_dk
       , adm_dt
--       , mdcl_rcrd_num
       , pbls_type_ind
       , apr_drg_cd
--       , apr_soi_cd
       , apr_rom_cd
--       , cms_drg_cd
--       , cms_mdc_cd
--       , ms_drg_cd
--       , ms_drg_mdc_cd
--       , ptnt_zip_cd
--       , fcy_pyr_cd
--       , fcy_scdy_pyr_cd
--       , fcy_adm_pract_cd
--       , fcy_attnd_pract_cd
--       , fcy_prim_proc_pract_cd
--       , dschrg_sts_cd
--       , pnt_of_orig_cd
--       , vbac_rt
--     , prim_diag_icd9_cd
--     , prim_proc_icd9_cd
       , otlr_cd
--       , age_val
--       , race_cd
       , gnd_cd
--       , smk_cd
       , mar_sts_cd
--       , brth_cdr_dk
--       , brth_dt
--       , brth_wt_qty
--       , ptnt_wt_qty
--       , fcy_pat_type_cd
--       , stnd_ptnt_type_cd
--       , adm_src_cd
--       , adm_type_cd
       , los_cnt
       , tot_chrg_amt
       , tot_cst_amt
       , tot_fix_cst_amt
      , tot_var_cst_amt
       , tot_pmnt_amt
       , compl_cnt
       , mrtly_cnt
       , wi_cst_amt
       , wi_var_cst_amt
       , wi_chrg_amt
       , prim_diag_icd_poa_cnt
       , prim_diag_icd_pst_adm_cnt
--       , prev_c_sect_cnt
--       , rpet_c_sect_cnt
--       , dlv_cnt
--       , c_sect_cnt
--       , nbrn_cnt
--       , abrtn_cnt
--       , prim_c_sect_cnt
--       , vgnl_dlv_cnt
--       , vbac_ind
       , csa_expc_mrtly_cnt
       , csa_expc_morbid_compl_cnt
       , csa_expc_compl_cnt
       , csa_expc_chrg_amt
       , csa_expc_los_cnt
       , csa_expc_cst_amt
       , csa_expc_svr_comp_rsk_cnt
       , csa_obs_readmit_rsk_adj_cnt
       , csa_expc_prs_readm_30dy_rsk
       , expc_mrtly_outc_case_cnt
       , expc_morbid_compl_outc_case_cnt
       , expc_compl_outc_case_cnt
       , expc_chrg_outc_case_cnt
       , expc_los_outc_case_cnt
       , expc_cst_outc_case_cnt
       , day_of_mech_vent_cnt
       , apr_expc_chrg_amt
       , apr_expc_cst_amt
       , apr_expc_fix_cst_amt
       , apr_expc_var_cst_amt
       , apr_expc_day_cnt
       , apr_expc_mrtly_cnt
       , apr_expc_compl_cnt
       , apr_expc_prev_c_section_cnt
       , apr_expc_prim_c_section_cnt
       , apr_expc_rpet_c_section_cnt
       , apr_expc_dlv_cnt
       , apr_expc_readmit_cnt
       , apr_readmit_cnt
       , drg_readmit_cnt
       , re_adm_day_cnt
       , prs_readm_30dy_rsk_out_case_cnt
       , prs_comp_out_case_cnt
       , prs_comp_rsk_out_case_cnt
       , prs_svr_comp_out_case_cnt
       , prs_svr_comp_rsk_out_case_cnt
--       , audt_sk
       , acute_readmit_days_key
       , readmit_diag_ind
       , acute_readmit_diag_ind
       , csa_cmp_cst_scl_fctr
       , csa_cmp_scl_fctr
       , csa_los_scl_fctr
       , csa_mort_scl_fctr
       , csa_readm_30dy_scl_fctr
       , csa_tot_chg_scl_fctr
       , csa_ln_readm_30dy_los_stderr
       , csa_readm_30dy_stderr
       , readmit_cnt_30dy_diag
       , readmit_den
       , readmit_dtl_ind
       , readmit_risk_adj_den
       , readmit_unpln_pln_ind
       , csa_hwr4_readm_rsk_adj_cnt
       , csa_hwr4_30d_readm_out_case_cnt
       , csa_hwr4_expc_readm
       , csa_hwr4_expc_30day_readm_scl_fctr
       , csa_hwr4_readm_unpln2pln_ind
--       , ed_visit
--       , icd10_diag_code
--       , icd10_proc_code
--       , apr_drg_icd10
--       , apr_soi_icd10
--       , apr_rom_icd10
--       , ms_drg_icd10
--       , ms_drg_mdc_icd10
--       , prev_csect_icd10_count
--       , repeat_csect_icd10_count
--       , delivery_icd10_count
--       , c_section_icd10_count
--       , newborn_icd10_count
--       , abortion_icd10_count
--       , primary_csect_icd10_count
--       , vag_delivery_icd10_count
--       , vbac_icd10_count
       , ln_los
       , csa_ln_exp_los
       , ln_total_cost
       , csa_ln_exp_comp_cost
--       , hisp_ethcty_dtl_cd
--       , hisp_ethcty_dtl_desc
--       , adm_ts
--       , dschrg_ts
--       , csa_dseses_grp
  FROM pce_qe16_prd_qadv..encntr;
