\set ON_ERROR_STOP ON;

drop table pvdr_dactiv_dim if exists;

create table pvdr_dactiv_dim as
SELECT pvdr_sk
       , ahr_offc_cred_txt
       , ahr_offc_frst_nm
       , ahr_offc_last_nm
       , ahr_offc_mid_nm
       , ahr_offc_name_pfx_txt
       , ahr_offc_name_sufx_txt
       , ahr_offc_ttl2_pos_nm
       , empr_idn_num
       , ent_type_cd
       , ent_type_descr
       , hcare_pvdr_prim_txnmy_swtc_nm
       , hcare_pvdr_txnmy_grp_nm
       , hcare_pvdr_txnmy_cd
       , hcare_pvdr_txnmy_descr
       , hcare_pvdr_txnmy_cl_nm
       , hcare_pvdr_txnmy_spclzn_nm
       , hcare_scdy_pvdr_txnmy_grp_nm
       , hcare_scdy_pvdr_txnmy_cd
       , hcare_scdy_pvdr_txnmy_descr
       , hcare_scdy_pvdr_txnmy_cl_nm
       , hcare_scdy_pvdr_txnmy_spclzn_nm
       , org_subpart_ind
       , sole_proprietor_ind
       , npi
       , pvdr_inactv_sts_dim.npi_dactv_dt
       , npi_dactv_rsn_cd
       , npi_dactv_rsn_descr
       , null npi_reactv_dt 
       , prn_org_lbn
       , prn_org_tin
       , pvdr_bsn_prct_loc_adr_cntry_nm
       , pvdr_bsn_prct_loc_adr_cty_nm
       , pvdr_bsn_prct_loc_adr_fax_num
       , pvdr_bsn_prct_loc_adr_pst_cd
       , pvdr_bsn_prct_loc_adr_ste_nm
       , pvdr_bsn_prct_loc_adr_tel_num
       , pvdr_cred_txt
       , pvdr_enumerton_dt
       , pvdr_frst_line_bsn_prct_loc_adr
       , pvdr_frst_nm
       , pvdr_gnd_cd
       , pvdr_lcn_num
       , pvdr_lcn_num_ste_cd
       , pvdr_lgl_last_nm
       , pvdr_mid_nm
       , pvdr_name_pfx_txt
       , pvdr_name_sufx_txt
       , pvdr_lgl_org_nm
       , frst_hsptl_affl_ccn_id
       , sec_hsptl_affl_ccn_id
       , third_hsptl_affl_ccn_id
       , fourth_hsptl_affl_ccn_id
       , fifth_hsptl_affl_ccn_id
       , frst_hsptl_affl_lbn_nm
       , sec_hsptl_affl_lbn_nm
       , third_hsptl_affl_lbn_nm
       , fourth_hsptl_affl_lbn_nm
       , fifth_hsptl_affl_lbn_nm
       , prim_spcly_nm
       , frst_scdy_spcly_nm
       , sec_scdy_spcly_nm
       , third_scdy_spcly_nm
       , fourth_scdy_spcly_nm
       , mdcl_sch_nm
       , graduation_yr_num
       , pvdr_sec_line_bsn_prct_loc_adr
       , rplcmt_npi
       , mcare_spcly_cd
       , mcare_spcly_descr
       , 'N' npi_actv_sts

  FROM pce_ae00_aco_uat_cdr..pvdr_dim
	join pvdr_inactv_sts_dim using (npi);

\unset ON_ERROR_STOP
