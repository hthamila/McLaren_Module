--QADV Table creation based on Net 3 years Of patient Account Number
--select 'processing table:  intermediate_stage_encntr_qly_anl_fct' as table_processing;
DROP TABLE intermediate_stage_encntr_qly_anl_fct IF EXISTS;

CREATE TABLE intermediate_stage_encntr_qly_anl_fct as
select
Z.company_id as fcy_nm
, Z.patient_id as encntr_num
       , dschrg_cdr_dk
       , ptnt_cl_cd
       , dschrg_dt
       , adm_cdr_dk
       , adm_dt
       , pbls_type_ind
       , apr_drg_cd
       , apr_rom_cd
       , otlr_cd
       , gnd_cd
       , mar_sts_cd
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
       , ln_los
       , csa_ln_exp_los
       , ln_total_cost
       , csa_ln_exp_comp_cost
       , apr_svry_of_ill
       , apr_rsk_of_mrtly
FROM intermediate_stage_temp_eligible_encntr_data Z
INNER JOIN val_set_dim VSET_FCY
ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
LEFT JOIN encntr_fct QADV
on Z.patient_id = QADV.encntr_num and QADV.fcy_num = VSET_FCY.alt_cd
--DISTRIBUTE ON (fcy_nm_hash, encntr_num_hash);
DISTRIBUTE ON (fcy_nm, encntr_num);