CREATE TABLE pce_qe16_slp_prd_dm..encntr_anl_fct_x as 
SELECT distinct fcy_nm
       , fcy_num
       , in_or_out_patient_ind
       , medical_record_number
       , encntr_num
       , adm_ts
       , adm_dt
       , dschrg_ts
       , adm_tm
       , dschrg_dt
       , dschrg_tm
       , los
       , ms_drg_cd
       , drg_fam_nm
       , case_mix_idnx_num
       , geo_mean_los_num
       , arthm_mean_los_num
       , apr_cd
       , apr_svry_of_ill
       , apr_rsk_of_mrtly
       , dschrg_tot_chrg_amt
       , dschrg_var_cst_amt
       , dschrg_fix_cst_amt
       , rmbmt_amt
       , age_in_yr
       , brth_dt
       , babys_encntr_num
       , ptnt_gnd
       , empr
       , ste_of_ptnt_orig
       , cnty_of_ptnt_orig
       , race_descr
       , mar_status
       , brth_wght_in_grm
       , day_on_mchnc_vntl
       , smk_flag
       , wght_in_lb
       , ethcty_descr
       , ed_vst_ind
       , ccn_care_setting
       , ptnt_hic_num
       , tin
       , ptnt_frst_nm
       , ptnt_mid_nm
       , ptnt_lst_nm
       , sub_fcy
       , acct_sts
       , readm_flag
       , prev_dschg_dt
       , ptnt_nm_sfx
       , adm_svc
       , dschrg_svc
       , nrg_stn
       , fnc_cls
       , fnc_cls_orig
       , fnl_bill_flag
       , fnl_bill_dt
       , tot_adj_amt
       , acct_bal_amt
       , expt_pymt_amt
       , upd_dt
       , upd_id
       , src_sys
       , tot_chrg_ind
       , admdt_yr_ind
       , bed_cnt
       , dschrg_nbrn_ind
       , dschrg_rehab_ind
       , dschrg_psych_ind
       , dschrg_ltcsnf_ind
       , dschrg_hospice_ind
       , dschrg_spclcare_ind
       , dschrg_lipmip_ind
       , dschrg_acute_ind
       , dschrg_ind
       , obsrv_hours
       , obsrv_days
       , obsrv_stay_ind
       , obsrv_psych_ind
       , ptnt_days
       , ptnt_days_pysch
       , ptnt_days_rehab
       , ptnt_days_nbrn
       , ptnt_days_stepdown
       , ptnt_days_acute
       , icu_days
       , ccu_days
       , nrs_days
       , rtne_days
       , ed_case_ind
       , src_prim_pyr_cd
       , src_prim_pyr_descr
       , qadv_prim_pyr_cd
       , qadv_prim_pyr_descr
       , src_prim_payor_grp1
       , src_prim_payor_grp2
       , src_prim_payor_grp3
       , src_scdy_pyr_cd
       , src_scdy_pyr_descr
       , qadv_scdy_pyr_cd
       , qadv_scdy_pyr_descr
       , src_scdy_payor_grp1
       , src_scdy_payor_grp2
       , src_scdy_payor_grp3
       , src_trty_pyr_cd
       , src_trty_pyr_descr
       , qadv_trty_pyr_cd
       , qadv_trty_pyr_descr
       , src_trty_payor_grp1
       , src_trty_payor_grp2
       , src_trty_payor_grp3
       , src_qtr_pyr_cd
       , src_qtr_pyr_descr
       , qadv_qtr_pyr_cd
       , qadv_qtr_pyr_descr
       , src_qtr_payor_grp1
       , src_qtr_payor_grp2
       , src_qtr_payor_grp3
       , endoscopy_case_ind
       , srgl_case_ind
       , lithotripsy_case_ind
       , cathlab_case_ind
       , adm_tp_cd
       , pnt_of_orig_cd
       , dschrg_sts_cd
       , adm_dgns_cd
       , adm_dgns_descr
       , adm_dgns_poa_flg_cd
       , prim_dgns_cd
       , prim_dgns_descr
       , prim_dgns_poa_flg_cd
       , scdy_dgns_cd
       , scdy_dgns_poa_flg_cd
       , scdy_dgns_descr_long
       , trty_dgns_cd
       , trty_dgns_poa_flg_cd
       , trty_dgns_descr_long
       , prim_pcd_cd
       , prim_pcd_descr
       , scdy_pcd_cd
       , scdy_pcd_descr
       , trty_pcd_cd
       , trty_pcd_descr
       , ptnt_tp_cd
       , ptnt_tp_descr
       , std_ptnt_tp_cd
       , adm_pract_npi
       , attnd_pract_npi
       , adm_pract_cd
       , attnd_pract_cd
       , adm_pract_nm
       , attnd_pract_nm
       , adr1
       , adr2
       , cty
       , ptnt_zip_cd
       , ptnt_mjr_cty_ste_nm
       , ptnt_mjr_cty_nm
       , ptnt_cnty_fips_ste_cd
       , fips_ste_descr
       , ptnt_cnty_fips_cd
       , ptnt_cnty_nm
       , fips_cnty_descr
       , std_ste_cd
       , std_ste_descr
       , std_rgon_descr
       , svc_cgy
       , svc_ln_nm
       , sub_svc_ln_nm
       , svc_nm
       , lvl_1_rnk
       , lvl_2_rnk
       , lvl_3_rnk
       , lvl_4_rnk
       , prim_srgn_cd
       , prim_srgn_nm
       , prim_srgn_npi
       , prim_srgn_spclty
       , prim_srgn_mcare_spcly_cd
       , cnslt_pract_1_cd
       , cnslt_pract_1_nm
       , cnslt_pract_1_npi
       , cnslt_pract_1_spclty
       , cnslt_pract_1_mcare_spcly_cd
       , cnslt_pract_2_cd
       , cnslt_pract_2_nm
       , cnslt_pract_2_npi
       , cnslt_pract_2_spclty
       , cnslt_pract_2_mcare_spcly_cd
       , cnslt_pract_3_cd
       , cnslt_pract_3_nm
       , cnslt_pract_3_npi
       , cnslt_pract_3_spclty
       , cnslt_pract_3_mcare_spcly_cd
       , est_acct_paid_ind
       , est_net_rev_amt
       , prof_chrg_ind
       , fiscal_yr
       , agg_rcc_based_direct_cst_amt
       , agg_rcc_based_indirect_cst_amt
       , agg_rcc_based_total_cst_amt
       , fiscal_yr_tp
       , attrb_physcn_cd
       , attrb_physcn_nm
       , attrb_physn_npi
       , attrb_physcn_spcl_cd
       , attrb_physcn_spcl_cd_descr
       , specl_valid_ind
  FROM pce_qe16_slp_prd_dm.prmretlp.encntr_anl_fct;