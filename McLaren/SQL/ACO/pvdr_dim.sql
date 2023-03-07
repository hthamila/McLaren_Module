\set ON_ERROR_STOP ON;

drop table prv_pvdr_dim if exists;
create table prv_pvdr_dim as select * from pvdr_dim;
truncate pvdr_dim;

insert into pvdr_dim
(
	 pvdr_sk
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
       , npi_dactv_dt
       , npi_dactv_rsn_cd
       , npi_dactv_rsn_descr
       , npi_reactv_dt
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
       , npi_actv_sts
)


 
with mcare_sply as 
(
select txnmy_cd, grp_nm, cl, spclzn, mcare_spcly_cd,mcare_pvdr_splr_type_descr as mcare_spcly_descr from txnmy_ref tax left join 
(select * from 
(select *, row_number() OVER (
              PARTITION BY pvdr_txnmy_cd order by mcare_spcly_cd) as row_num from mcare_pvdr_splr_to_hcare_pvdr_txnmy_ref)a
			  where a.row_num=1
)a on tax.txnmy_cd=a.pvdr_txnmy_cd 
),

phy_cmp as
(
SELECT *
   FROM (
          SELECT
            *,
            row_number()
            OVER (
              PARTITION BY npi
              ORDER BY npi, 
			      hosp_ccn_1 nulls last,
  				  hosp_ccn_2 nulls last,
  				  hosp_ccn_3 nulls last,
				  hosp_ccn_4 nulls last,
				  hosp_ccn_5 nulls last,
				  hosp_lbn_1 nulls last,
				  hosp_lbn_2 nulls last,
				  hosp_lbn_3 nulls last,
				  hosp_lbn_4 nulls last,
				  hosp_lbn_5 nulls last,
				  prim_spec nulls last,
				  secd_spec_1 nulls last,
				  secd_spec_2 nulls last,
				  secd_spec_3 nulls last,
				  secd_spec_4 nulls last,
				  med_schl_nm nulls last
			  ) AS row_num
          FROM pvdr_dim_cms_lnd where file_mn_dt=(select max(file_mn_dt) from pvdr_dim_cms_lnd)) a
   WHERE a.row_num = 1
),

npi_txnmy as
(
select string_to_int(substr(RAWTOHEX(hash(npi, 0)), 17), 16),
		npi,
  CASE WHEN (healthcare_provider_primary_taxonomy_switch_1='Y'
			or healthcare_provider_primary_taxonomy_switch_2 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_3 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_4 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_5 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_6 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_7 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_8 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_9 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_10 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_11 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_12 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_13 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_14 = 'Y'
			or healthcare_provider_primary_taxonomy_switch_15 = 'Y') then 'Y'
		ELSE
			coalesce(healthcare_provider_primary_taxonomy_switch_1, healthcare_provider_primary_taxonomy_switch_2, healthcare_provider_primary_taxonomy_switch_3, 	healthcare_provider_primary_taxonomy_switch_4,healthcare_provider_primary_taxonomy_switch_5, healthcare_provider_primary_taxonomy_switch_6, healthcare_provider_primary_taxonomy_switch_7, healthcare_provider_primary_taxonomy_switch_8, healthcare_provider_primary_taxonomy_switch_9,healthcare_provider_primary_taxonomy_switch_10,	healthcare_provider_primary_taxonomy_switch_11, healthcare_provider_primary_taxonomy_switch_12, healthcare_provider_primary_taxonomy_switch_13, healthcare_provider_primary_taxonomy_switch_14,healthcare_provider_primary_taxonomy_switch_15) 
				end as healthcare_provider_primary_taxonomy_switch,
	CASE WHEN healthcare_provider_primary_taxonomy_switch_1='Y' then healthcare_provider_taxonomy_code_1 
			when healthcare_provider_primary_taxonomy_switch_2 = 'Y' then healthcare_provider_taxonomy_code_2
			when healthcare_provider_primary_taxonomy_switch_3 = 'Y' then healthcare_provider_taxonomy_code_3
			when healthcare_provider_primary_taxonomy_switch_4 = 'Y' then healthcare_provider_taxonomy_code_4
			when healthcare_provider_primary_taxonomy_switch_5 = 'Y' then healthcare_provider_taxonomy_code_5
			when healthcare_provider_primary_taxonomy_switch_6 = 'Y' then healthcare_provider_taxonomy_code_6
			when healthcare_provider_primary_taxonomy_switch_7 = 'Y' then healthcare_provider_taxonomy_code_7
			when healthcare_provider_primary_taxonomy_switch_8 = 'Y' then healthcare_provider_taxonomy_code_8
			when healthcare_provider_primary_taxonomy_switch_9 = 'Y' then healthcare_provider_taxonomy_code_9
			when healthcare_provider_primary_taxonomy_switch_10 = 'Y' then healthcare_provider_taxonomy_code_10
			when healthcare_provider_primary_taxonomy_switch_11 = 'Y' then healthcare_provider_taxonomy_code_11
			when healthcare_provider_primary_taxonomy_switch_12 = 'Y' then healthcare_provider_taxonomy_code_12
			when healthcare_provider_primary_taxonomy_switch_13 = 'Y' then healthcare_provider_taxonomy_code_13
			when healthcare_provider_primary_taxonomy_switch_14 = 'Y' then healthcare_provider_taxonomy_code_14
			when healthcare_provider_primary_taxonomy_switch_15 = 'Y' then healthcare_provider_taxonomy_code_15
		ELSE
			NULL end as healthcare_provider_taxonomy_code,
	CASE WHEN healthcare_provider_primary_taxonomy_switch_1='Y' then healthcare_provider_taxonomy_group_1 
			when healthcare_provider_primary_taxonomy_switch_2 = 'Y' then healthcare_provider_taxonomy_group_2
			when healthcare_provider_primary_taxonomy_switch_3 = 'Y' then healthcare_provider_taxonomy_group_3
			when healthcare_provider_primary_taxonomy_switch_4 = 'Y' then healthcare_provider_taxonomy_group_4
			when healthcare_provider_primary_taxonomy_switch_5 = 'Y' then healthcare_provider_taxonomy_group_5
			when healthcare_provider_primary_taxonomy_switch_6 = 'Y' then healthcare_provider_taxonomy_group_6
			when healthcare_provider_primary_taxonomy_switch_7 = 'Y' then healthcare_provider_taxonomy_group_7
			when healthcare_provider_primary_taxonomy_switch_8 = 'Y' then healthcare_provider_taxonomy_group_8
			when healthcare_provider_primary_taxonomy_switch_9 = 'Y' then healthcare_provider_taxonomy_group_9
			when healthcare_provider_primary_taxonomy_switch_10 = 'Y' then healthcare_provider_taxonomy_group_10
			when healthcare_provider_primary_taxonomy_switch_11 = 'Y' then healthcare_provider_taxonomy_group_11
			when healthcare_provider_primary_taxonomy_switch_12 = 'Y' then healthcare_provider_taxonomy_group_12
			when healthcare_provider_primary_taxonomy_switch_13 = 'Y' then healthcare_provider_taxonomy_group_13
			when healthcare_provider_primary_taxonomy_switch_14 = 'Y' then healthcare_provider_taxonomy_group_14
			when healthcare_provider_primary_taxonomy_switch_15 = 'Y' then healthcare_provider_taxonomy_group_15
		ELSE
			NULL end as healthcare_provider_taxonomy_group,
	CASE WHEN healthcare_provider_primary_taxonomy_switch_1 in ('X','N') then healthcare_provider_taxonomy_code_1 
			when healthcare_provider_primary_taxonomy_switch_2 in ('X','N') then healthcare_provider_taxonomy_code_2
			when healthcare_provider_primary_taxonomy_switch_3 in ('X','N') then healthcare_provider_taxonomy_code_3
			when healthcare_provider_primary_taxonomy_switch_4 in ('X','N') then healthcare_provider_taxonomy_code_4
			when healthcare_provider_primary_taxonomy_switch_5 in ('X','N') then healthcare_provider_taxonomy_code_5
			when healthcare_provider_primary_taxonomy_switch_6 in ('X','N') then healthcare_provider_taxonomy_code_6
			when healthcare_provider_primary_taxonomy_switch_7 in ('X','N') then healthcare_provider_taxonomy_code_7
			when healthcare_provider_primary_taxonomy_switch_8 in ('X','N') then healthcare_provider_taxonomy_code_8
			when healthcare_provider_primary_taxonomy_switch_9 in ('X','N') then healthcare_provider_taxonomy_code_9
			when healthcare_provider_primary_taxonomy_switch_10 in ('X','N') then healthcare_provider_taxonomy_code_10
			when healthcare_provider_primary_taxonomy_switch_11 in ('X','N') then healthcare_provider_taxonomy_code_11
			when healthcare_provider_primary_taxonomy_switch_12 in ('X','N') then healthcare_provider_taxonomy_code_12
			when healthcare_provider_primary_taxonomy_switch_13 in ('X','N') then healthcare_provider_taxonomy_code_13
			when healthcare_provider_primary_taxonomy_switch_14 in ('X','N') then healthcare_provider_taxonomy_code_14
			when healthcare_provider_primary_taxonomy_switch_15 in ('X','N') then healthcare_provider_taxonomy_code_15
		ELSE
			NULL end as healthcare_secondary_provider_taxonomy_code,
	CASE WHEN healthcare_provider_primary_taxonomy_switch_1 in ('X','N') then healthcare_provider_taxonomy_group_1 
			when healthcare_provider_primary_taxonomy_switch_2 in ('X','N') then healthcare_provider_taxonomy_group_2
			when healthcare_provider_primary_taxonomy_switch_3 in ('X','N') then healthcare_provider_taxonomy_group_3
			when healthcare_provider_primary_taxonomy_switch_4 in ('X','N') then healthcare_provider_taxonomy_group_4
			when healthcare_provider_primary_taxonomy_switch_5 in ('X','N') then healthcare_provider_taxonomy_group_5
			when healthcare_provider_primary_taxonomy_switch_6 in ('X','N') then healthcare_provider_taxonomy_group_6
			when healthcare_provider_primary_taxonomy_switch_7 in ('X','N') then healthcare_provider_taxonomy_group_7
			when healthcare_provider_primary_taxonomy_switch_8 in ('X','N') then healthcare_provider_taxonomy_group_8
			when healthcare_provider_primary_taxonomy_switch_9 in ('X','N') then healthcare_provider_taxonomy_group_9
			when healthcare_provider_primary_taxonomy_switch_10 in ('X','N') then healthcare_provider_taxonomy_group_10
			when healthcare_provider_primary_taxonomy_switch_11 in ('X','N') then healthcare_provider_taxonomy_group_11
			when healthcare_provider_primary_taxonomy_switch_12 in ('X','N') then healthcare_provider_taxonomy_group_12
			when healthcare_provider_primary_taxonomy_switch_13 in ('X','N') then healthcare_provider_taxonomy_group_13
			when healthcare_provider_primary_taxonomy_switch_14 in ('X','N') then healthcare_provider_taxonomy_group_14
			when healthcare_provider_primary_taxonomy_switch_15 in ('X','N') then healthcare_provider_taxonomy_group_15
		ELSE
			NULL end as healthcare_secondary_provider_taxonomy_group 
	from pvdr_dim_nppes_lnd where file_mn_dt=(select max(file_mn_dt) from pvdr_dim_nppes_lnd)
)

SELECT
  string_to_int(substr(RAWTOHEX(hash(np.npi, 0)), 17), 16) as pvdr_sk, 
  np.authorized_official_credential_text as ahr_offc_cred_txt, 
  np.authorized_official_first_name as ahr_offc_frst_nm, 
  np.authorized_official_last_name as ahr_offc_last_nm, 
  np.authorized_official_middle_name as ahr_offc_mid_nm, 
  np.authorized_official_name_prefix_text as ahr_offc_name_pfx_txt, 
  np.authorized_official_name_suffix_text as ahr_offc_name_sufx_txt, 
  np.authorized_official_title_or_position as ahr_offc_ttl2_pos_nm, 
  np.employer_identification_number_ein as empr_idn_num, 
  np.entity_type_code as ent_type_cd, 
  CASE WHEN np.entity_type_code = 1
    THEN 'Individual'
  WHEN np.entity_type_code = 2
    THEN 'Organization' END AS ent_type_descr,
  npi.healthcare_provider_primary_taxonomy_switch as hcare_pvdr_prim_txnmy_swtc_nm, 
  txnmy.grp_nm as hcare_pvdr_txnmy_grp_nm, 
  npi.healthcare_provider_taxonomy_code as hcare_pvdr_txnmy_cd, 
  coalesce((txnmy.cl|| '-' ||txnmy.spclzn),txnmy.cl) as hcare_pvdr_txnmy_descr, 
  txnmy.cl as hcare_pvdr_txnmy_cl_nm, 
  txnmy.spclzn as hcare_pvdr_txnmy_spclzn_nm, 
  --secondary
  txnmy2.grp_nm as hcare_scdy_pvdr_txnmy_grp_nm, 
  npi.healthcare_secondary_provider_taxonomy_code as hcare_scdy_pvdr_txnmy_cd, 
  coalesce((txnmy2.cl|| '-' ||txnmy2.spclzn),txnmy2.cl) as hcare_scdy_pvdr_txnmy_descr, 
  txnmy2.cl as hcare_scdy_pvdr_txnmy_cl_nm, 
  txnmy2.spclzn as hcare_scdy_pvdr_txnmy_spclzn_nm,
    
  CASE WHEN np.is_organization_subpart = 'Y' THEN 1 WHEN np.is_organization_subpart = 'N' THEN 0 END as org_subpart_ind,
  CASE WHEN np.is_sole_proprietor = 'Y' THEN 1 WHEN np.is_sole_proprietor = 'N' THEN 0 END as sole_proprietor_ind,
  np.npi,
  np.npi_deactivation_date as npi_dactv_dt, 
  np.npi_deactivation_reason_code as npi_dactv_rsn_cd, 
  CASE WHEN np.npi_deactivation_reason_code = 'DT'
    THEN 'Death'
  WHEN np.npi_deactivation_reason_code = 'DB'
    THEN 'Disbandment'
  WHEN np.npi_deactivation_reason_code = 'FR'
    THEN 'Fraud'
  WHEN np.npi_deactivation_reason_code = 'OT'
    THEN 'Other' END        AS npi_dactv_rsn_descr, 
  np.npi_reactivation_date as npi_reactv_dt, 
  np.parent_organization_lbn as prn_org_lbn, 
  np.parent_organization_tin as prn_org_tin, 
  np.provider_business_practice_location_address_country_code_if_outside_us as pvdr_bsn_prct_loc_adr_cntry_nm, 
  np.provider_business_practice_location_address_city_name as pvdr_bsn_prct_loc_adr_cty_nm, 
  np.provider_business_practice_location_address_fax_number as pvdr_bsn_prct_loc_adr_fax_num, 
  np.provider_business_practice_location_address_postal_code as pvdr_bsn_prct_loc_adr_pst_cd, 
  np.provider_business_practice_location_address_state_name as pvdr_bsn_prct_loc_adr_ste_nm, 
  np.provider_business_practice_location_address_telephone_number as pvdr_bsn_prct_loc_adr_tel_num, 
  np.provider_credential_text as pvdr_cred_txt, 
  np.provider_enumeration_date as pvdr_enumerton_dt, 
  np.provider_first_line_business_practice_location_address as pvdr_frst_line_bsn_prct_loc_adr, 
  np.provider_first_name as pvdr_frst_nm, 
  np.provider_gender_code as pvdr_gnd_cd, 
  np.provider_license_number_1 as pvdr_lcn_num, 
  np.provider_license_number_state_code_1 as pvdr_lcn_num_ste_cd, 
  np.provider_last_name_legal_name as pvdr_lgl_last_nm, 
  np.provider_middle_name as pvdr_mid_nm, 
  np.provider_name_prefix_text as pvdr_name_pfx_txt, 
  np.provider_name_suffix_text as pvdr_name_sufx_txt, 
  np.provider_organization_name_legal_business_name as pvdr_lgl_org_nm, 
  cms.hosp_ccn_1 as frst_hsptl_affl_ccn_id, 
  cms.hosp_ccn_2 as sec_hsptl_affl_ccn_id, 
  cms.hosp_ccn_3 as third_hsptl_affl_ccn_id, 
  cms.hosp_ccn_4 as fourth_hsptl_affl_ccn_id, 
  cms.hosp_ccn_5 as fifth_hsptl_affl_ccn_id, 
  cms.hosp_lbn_1 as frst_hsptl_affl_lbn_nm, 
  cms.hosp_lbn_2 as sec_hsptl_affl_lbn_nm, 
  cms.hosp_lbn_3 as third_hsptl_affl_lbn_nm, 
  cms.hosp_lbn_4 as fourth_hsptl_affl_lbn_nm, 
  cms.hosp_lbn_5 as fifth_hsptl_affl_lbn_nm, 
  UPPER(txnmy.stnd_spcly) as prim_spcly_nm, 
  cms.secd_spec_1 as frst_scdy_spcly_nm,
  cms.secd_spec_2 as sec_scdy_spcly_nm, 
  cms.secd_spec_3 as third_scdy_spcly_nm, 
  cms.secd_spec_4 as fourth_scdy_spcly_nm, 
  cms.med_schl_nm as mdcl_sch_nm,
  cms.grad_yr as graduation_yr_num, 
  np.provider_second_line_business_practice_location_address as pvdr_sec_line_bsn_prct_loc_adr, 
  np.replacement_npi as rplcmt_npi, 
  tax.mcare_spcly_cd,
  tax.mcare_spcly_descr, 
  'Y' npi_actv_sts

from pvdr_dim_nppes_lnd np
	join npi_txnmy npi using (npi)
	left join txnmy_ref txnmy on npi.healthcare_provider_taxonomy_code=txnmy.txnmy_cd
	left join txnmy_ref txnmy2 on npi.healthcare_secondary_provider_taxonomy_code=txnmy2.txnmy_cd
	left join phy_cmp cms using (npi)
	left join mcare_sply tax on npi.healthcare_provider_taxonomy_code=tax.txnmy_cd
	where np.file_mn_dt=(select max(file_mn_dt) from pvdr_dim_nppes_lnd) and (np.npi_deactivation_date is null or date(np.npi_reactivation_date)>=date(np.npi_deactivation_date))
	
;
--loading inactive records
insert into pvdr_dim select * from pvdr_inactv_sts_dim;

\unset ON_ERROR_STOP
