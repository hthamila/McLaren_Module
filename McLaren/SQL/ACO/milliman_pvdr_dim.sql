\set ON_ERROR_STOP ON;

drop table millman_pvdr_dim if exists;
create table millman_pvdr_dim as
select pd.pvdr_sk, 
  pd.ahr_offc_cred_txt, 
  pd.ahr_offc_frst_nm, 
  pd.ahr_offc_last_nm, 
  pd.ahr_offc_mid_nm, 
  pd.ahr_offc_name_pfx_txt, 
  pd.ahr_offc_name_sufx_txt, 
  pd.ahr_offc_ttl2_pos_nm, 
  pd.empr_idn_num, 
  case when p.prv_type='Organization' then 2
  	else 1 end as ent_type_cd, 
  CASE WHEN p.prv_type>'Organization' 
    THEN 'Individual'
  		ELSE p.prv_type
  END AS entity_type_descr,
  pd.hcare_pvdr_prim_txnmy_swtc_nm, 
  txnmy.grp_nm as hcare_pvdr_txnmy_grp_nm, 
  p.prv_taxonomy_cd as hcare_pvdr_txnmy_cd, 
  txnmy.cl|| '-' ||txnmy.spclzn as hcare_pvdr_txnmy_descr, 
  txnmy.cl as hcare_pvdr_txnmy_cl_nm, 
  txnmy.spclzn as hcare_pvdr_txnmy_spclzn_nm, 
  --secondary
  pd.hcare_scdy_pvdr_txnmy_grp_nm, 
  pd.hcare_scdy_pvdr_txnmy_cd, 
  pd.hcare_scdy_pvdr_txnmy_descr, 
  pd.hcare_scdy_pvdr_txnmy_cl_nm, 
  pd.hcare_scdy_pvdr_txnmy_spclzn_nm,
  pd.org_subpart_ind,
  pd.sole_proprietor_ind,
  pd.npi,
  pd.npi_dactv_dt, 
  pd.npi_dactv_rsn_cd, 
  pd.npi_dactv_rsn_descr, 
  pd.npi_reactv_dt,
  pd.prn_org_lbn, 
  pd.prn_org_tin, 
  pd.pvdr_bsn_prct_loc_adr_cntry_nm, 
  pd.pvdr_bsn_prct_loc_adr_cty_nm, 
  pd.pvdr_bsn_prct_loc_adr_fax_num, 
  pd.pvdr_bsn_prct_loc_adr_pst_cd, 
  pd.pvdr_bsn_prct_loc_adr_ste_nm, 
  pd.pvdr_bsn_prct_loc_adr_tel_num, 
  pd.pvdr_cred_txt, 
  pd.pvdr_enumerton_dt, 
  pd.pvdr_frst_line_bsn_prct_loc_adr, 
  pd.pvdr_frst_nm, 
  pd.pvdr_gnd_cd, 
  pd.pvdr_lcn_num, 
  pd.pvdr_lcn_num_ste_cd, 
  pd.pvdr_lgl_last_nm, 
  pd.pvdr_mid_nm, 
  pd.pvdr_name_pfx_txt, 
  pd.pvdr_name_sufx_txt, 
  p.prv_name as pvdr_lgl_org_nm, 
  pd.frst_hsptl_affl_ccn_id, 
  pd.sec_hsptl_affl_ccn_id, 
  pd.third_hsptl_affl_ccn_id, 
  pd.fourth_hsptl_affl_ccn_id, 
  pd.fifth_hsptl_affl_ccn_id, 
  pd.frst_hsptl_affl_lbn_nm, 
  pd.sec_hsptl_affl_lbn_nm, 
  pd.third_hsptl_affl_lbn_nm, 
  pd.fourth_hsptl_affl_lbn_nm, 
  pd.fifth_hsptl_affl_lbn_nm, 
  UPPER(stnd_spcly) prim_spcly_nm, 
  pd.frst_scdy_spcly_nm,
  pd.sec_scdy_spcly_nm, 
  pd.third_scdy_spcly_nm, 
  pd.fourth_scdy_spcly_nm, 
  pd.mdcl_sch_nm, 
  pd.graduation_yr_num,
  pd.pvdr_sec_line_bsn_prct_loc_adr, 
  pd.rplcmt_npi, 
  pd.mcare_spcly_cd
  
  from pce_ae00_aco_uat_cdr..pvdr_dim pd
	join pce_qe16_aco_uat_lnd..providers p on npi=prv_id
	left join pce_ae00_aco_uat_cdr..txnmy_ref txnmy on p.prv_taxonomy_cd=txnmy.txnmy_cd
where ent_type_cd is null and (pvdr_lgl_last_nm is null or
	pvdr_frst_nm is null)

union 
	
 select string_to_int(substr(RAWTOHEX(hash(p.prv_id, 0)), 17), 16) as pvdr_sk,
  NULL ahr_offc_cred_txt, 
  NULL ahr_offc_frst_nm, 
  NULL ahr_offc_last_nm, 
  NULL ahr_offc_mid_nm, 
  NULL ahr_offc_name_pfx_txt, 
  NULL ahr_offc_name_sufx_txt, 
  NULL ahr_offc_ttl2_pos_nm, 
  NULL empr_idn_num, 
  case when p.prv_type='Organization' then 2
  	else 1 end as ent_type_cd, 
  CASE WHEN p.prv_type>'Organization' 
    THEN 'Individual'
  		ELSE p.prv_type
  END AS entity_type_descr,
  NULL hcare_pvdr_prim_txnmy_swtc_nm, 
  txnmy.grp_nm as hcare_pvdr_txnmy_grp_nm, 
  p.prv_taxonomy_cd as hcare_pvdr_txnmy_cd, 
  txnmy.cl|| '-' ||txnmy.spclzn as hcare_pvdr_txnmy_descr, 
  txnmy.cl as hcare_pvdr_txnmy_cl_nm, 
  txnmy.spclzn as hcare_pvdr_txnmy_spclzn_nm, 
  --secondary
  NULL hcare_scdy_pvdr_txnmy_grp_nm, 
  NULL hcare_scdy_pvdr_txnmy_cd, 
  NULL hcare_scdy_pvdr_txnmy_descr, 
  NULL hcare_scdy_pvdr_txnmy_cl_nm, 
  NULL hcare_scdy_pvdr_txnmy_spclzn_nm,
  NULL org_subpart_ind,
  NULL sole_proprietor_ind,
  p.prv_id as npi,
  NULL npi_dactv_dt, 
  NULL npi_dactv_rsn_cd, 
  NULL npi_dactv_rsn_descr, 
  NULL npi_reactv_dt,
  NULL prn_org_lbn, 
  NULL prn_org_tin, 
  NULL pvdr_bsn_prct_loc_adr_cntry_nm, 
  NULL pvdr_bsn_prct_loc_adr_cty_nm, 
  NULL pvdr_bsn_prct_loc_adr_fax_num, 
  NULL pvdr_bsn_prct_loc_adr_pst_cd, 
  NULL pvdr_bsn_prct_loc_adr_ste_nm, 
  NULL pvdr_bsn_prct_loc_adr_tel_num, 
  NULL pvdr_cred_txt, 
  NULL pvdr_enumerton_dt, 
  NULL pvdr_frst_line_bsn_prct_loc_adr, 
  NULL pvdr_frst_nm, 
  NULL pvdr_gnd_cd, 
  NULL pvdr_lcn_num, 
  NULL pvdr_lcn_num_ste_cd, 
  NULL pvdr_lgl_last_nm, 
  NULL pvdr_mid_nm, 
  NULL pvdr_name_pfx_txt, 
  NULL pvdr_name_sufx_txt, 
  p.prv_name as pvdr_lgl_org_nm, 
  NULL frst_hsptl_affl_ccn_id, 
  NULL sec_hsptl_affl_ccn_id, 
  NULL third_hsptl_affl_ccn_id, 
  NULL fourth_hsptl_affl_ccn_id, 
  NULL fifth_hsptl_affl_ccn_id, 
  NULL frst_hsptl_affl_lbn_nm, 
  NULL sec_hsptl_affl_lbn_nm, 
  NULL third_hsptl_affl_lbn_nm, 
  NULL fourth_hsptl_affl_lbn_nm, 
  NULL fifth_hsptl_affl_lbn_nm, 
  UPPER(stnd_spcly) prim_spcly_nm, 
  NULL frst_scdy_spcly_nm,
  NULL sec_scdy_spcly_nm, 
  NULL third_scdy_spcly_nm, 
  NULL fourth_scdy_spcly_nm, 
  NULL mdcl_sch_nm, 
  NULL graduation_yr_num,
  NULL pvdr_sec_line_bsn_prct_loc_adr, 
  NULL rplcmt_npi, 
  NULL mcare_spcly_cd
  
  from pce_qe16_aco_uat_lnd..providers p 
  	left join  pce_ae00_aco_uat_cdr..pvdr_dim pd on prv_id=npi
	left join pce_ae00_aco_uat_cdr..txnmy_ref txnmy on p.prv_taxonomy_cd=txnmy.txnmy_cd
where pd.npi is null;
--delete the inactive records which do not have physician names
GROOM TABLE pce_ae00_aco_uat_cdr.pvdr_dim VERSIONS;
generate statistics on pvdr_dim;
delete from pvdr_dim where rowid in (select p.rowid from pvdr_dim p join millman_pvdr_dim using (pvdr_sk));
--Insert back records which are found on Milliman Provider File
insert into pvdr_dim select * from millman_pvdr_dim;

\unset ON_ERROR_STOP
