\set ON_ERROR_STOP ON;

CREATE TEMP TABLE dgns0 AS
  SELECT
    cdf0.clm_id,
    cdf0.clm_line_num,
    dd0.dgns_alt_cd  AS adm_dgns_cd,
    dd0.dgns_descr   AS adm_dgns_descr,
    dd0.dgns_icd_ver AS icd_ver,
    case when dd0.dgns_alt_cd in ('U070','U071') then 1
       else 0 end as covid_adm_ind
  FROM clm_dgns_fct cdf0
    LEFT OUTER JOIN dgns_dim dd0 ON cdf0.dgns_sk = dd0.dgns_sk
  WHERE cdf0.adm_dgns_ind = 1
        AND dd0.dgns_cd IS NOT NULL;

CREATE TEMP TABLE dgns1 AS
  SELECT
    cdf.clm_id,
    cdf.clm_line_num,
    dd.dgns_alt_cd AS icd_dgns1_cd,
    dd.dgns_descr  AS icd_dgns1_descr,
    poa.poa_cd     AS poa1_cd,
    poa.poa_descr  AS poa1_descr,
    dd.dgns_icd_ver AS icd_ver,
    case when dd.dgns_alt_cd in ('U070','U071') then 1
                else 0 end as covid_prim_adm_ind
  FROM clm_dgns_fct cdf
    LEFT OUTER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
    LEFT OUTER JOIN poa_dim poa ON cdf.poa_sk = poa.poa_sk
  WHERE cdf.icd_pos_num = 1
        AND dd.dgns_cd IS NOT NULL;

CREATE TEMP TABLE dgns2 AS
  SELECT
    cdf.clm_id,
    cdf.clm_line_num,
    dd.dgns_alt_cd AS icd_dgns2_cd,
    dd.dgns_descr  AS icd_dgns2_descr,
    poa.poa_cd     AS poa2_cd,
    poa.poa_descr  AS poa2_descr
  FROM clm_dgns_fct cdf
    LEFT OUTER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
    LEFT OUTER JOIN poa_dim poa ON cdf.poa_sk = poa.poa_sk
  WHERE cdf.icd_pos_num = 2
        AND dd.dgns_cd IS NOT NULL;

CREATE TEMP TABLE dgns3 AS
  SELECT
    cdf.clm_id,
    cdf.clm_line_num,
    dd.dgns_alt_cd AS icd_dgns3_cd,
    dd.dgns_descr  AS icd_dgns3_descr,
    poa.poa_cd     AS poa3_cd,
    poa.poa_descr  AS poa3_descr
  FROM clm_dgns_fct cdf
    LEFT OUTER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
    LEFT OUTER JOIN poa_dim poa ON cdf.poa_sk = poa.poa_sk
  WHERE cdf.icd_pos_num = 3
        AND dd.dgns_cd IS NOT NULL;

CREATE TEMP TABLE dgns4 AS
  SELECT
    cdf.clm_id,
    cdf.clm_line_num,
    dd.dgns_alt_cd AS icd_dgns4_cd,
    dd.dgns_descr  AS icd_dgns4_descr,
    poa.poa_cd     AS poa4_cd,
    poa.poa_descr  AS poa4_descr
  FROM clm_dgns_fct cdf
    LEFT OUTER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
    LEFT OUTER JOIN poa_dim poa ON cdf.poa_sk = poa.poa_sk
  WHERE cdf.icd_pos_num = 4
        AND dd.dgns_cd IS NOT NULL;

CREATE TEMP TABLE pcd1 AS
  SELECT
    cpf.clm_id,
    cpf.clm_line_num,
    ipd.icd_pcd_alt_cd AS icd_pcd1_cd,
    ipd.icd_pcd_descr  AS icd_pcd1_descr,
	ipd.icd_pcd_3_dgt_cd as icd_pcd1_3_dgt_cd,
	ipd.icd_pcd_3_dgt_descr as icd_pcd1_3_dgt_descr, 
	ipd.icd_pcd_4_dgt_cd as icd_pcd1_4_dgt_cd,
	ipd.icd_pcd_4_dgt_descr as icd_pcd1_4_dgt_descr,
    ipd.icd_ver AS icd_ver
  FROM clm_pcd_fct cpf
    LEFT OUTER JOIN pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
  WHERE cpf.icd_pos_num = 1
        AND ipd.icd_pcd_cd IS NOT NULL;

CREATE TEMP TABLE pcd2 AS
  SELECT
    cpf.clm_id,
    cpf.clm_line_num,
    ipd.icd_pcd_alt_cd AS icd_pcd2_cd,
    ipd.icd_pcd_descr  AS icd_pcd2_descr
  FROM clm_pcd_fct cpf
    LEFT OUTER JOIN pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
  WHERE cpf.icd_pos_num = 2
        AND ipd.icd_pcd_cd IS NOT NULL;

CREATE TEMP TABLE pcd3 AS
  SELECT
    cpf.clm_id,
    cpf.clm_line_num,
    ipd.icd_pcd_alt_cd AS icd_pcd3_cd,
    ipd.icd_pcd_descr  AS icd_pcd3_descr
  FROM clm_pcd_fct cpf
    LEFT OUTER JOIN pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
  WHERE cpf.icd_pos_num = 3
        AND ipd.icd_pcd_cd IS NOT NULL;

CREATE TEMP TABLE pcd4 AS
  SELECT
    cpf.clm_id,
    cpf.clm_line_num,
    ipd.icd_pcd_alt_cd AS icd_pcd4_cd,
    ipd.icd_pcd_descr  AS icd_pcd4_descr
  FROM clm_pcd_fct cpf
    LEFT OUTER JOIN pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
  WHERE cpf.icd_pos_num = 4
        AND ipd.icd_pcd_cd IS NOT NULL;

CREATE TEMP TABLE hospice_adm AS
	SELECT * FROM hospice_adm_vw;

CREATE TEMP TABLE hm_hlth_cases AS
	SELECT * FROM hm_hlth_vw;

--Added logic to update SNF to ED Visit Indicator 	
CREATE TEMP TABLE snf_ed_vst_dm as
select fcy_case_id, snf_ed_vst_ind, days_fm_snf_to_ed_vst from dmart_snf_ed_visits;
		
--Added Cost Per IP Case by Facility Case ID

CREATE TEMP TABLE fcy_ip_paid_amt AS
with fcy_ip_paid as
(
select cf.fcy_case_id, sum(cf.paid_amt) as fcy_ip_paid_amt
	from clm_line_fct cf
	left join cst_modl_dim cd using (cst_modl_sk)
where cd.care_setting_sub_cgy_nm = 'Facility - Hospital Inpatient'
group by cf.fcy_case_id)

select fcy_case_id, min(clm_id) as frst_ip_clm_id, fcy_ip_paid_amt
	from clm_line_fct
		join fcy_ip_paid using (fcy_case_id)
where cst_modl_in_ptnt_clm_adm_ind in (-1,1)
	group by clm_line_fct.fcy_case_id, fcy_ip_paid_amt;


DROP TABLE clm_line_fct_ds IF EXISTS;

CREATE TABLE clm_line_fct_ds AS
  SELECT
    cf.clm_id,
    cf.clm_line_num,
    cf.clm_line_sts,
    cf.ctr_id,
    pmd.mbr_id_num,
    pmd.gnd_descr,
    pmd.brth_dt,
    pmd.dth_dt,
    pmd.frst_nm,
    pmd.last_nm,
    pmd.zip_cd                                                              AS bene_zip_cd,
    pmad.mdcl_mo_cnt,
    pmad.asgnt_wndw_strt_dt,
    pmad.asgnt_wndw_end_dt,
    pmad.elig_sts,
    pmd.cms_hcc_scor_num,
    pmd.othr_hcc_scor_num,
    coalesce(pmd.benf_flg,0) 						AS benf_flg,
    CASE WHEN cf.svc_fm_dt IS NULL
      THEN pmad.asgnt_wndw_strt_dt
    ELSE cf.svc_fm_dt END                                                   AS svc_fm_dt,
    CASE WHEN cf.svc_to_dt IS NULL
      THEN pmad.asgnt_wndw_strt_dt
    ELSE cf.svc_to_dt END                                                   AS svc_to_dt,
    cf.paid_dt,
    case when pmd.dth_dt between cf.svc_fm_dt and cf.cf.svc_to_dt then 1 else null end as clm_dth_ind,
    mdd.ms_drg_cd,
    mdd.ms_drg_bsn_line_descr,
    mdd.ms_drg_type_cd_descr,
    mdd.mclaren_major_slp_grouping					as mcl_mjr_slp_grp,
    mdd.mclaren_service_line						as mcl_svc_ln,
    mdd.mclaren_sub_service_line					as mcl_sub_svc_ln,
    rcd.rev_cd,
    hcp.hcpcs_cd,
    hip.hipps_cd,
    CASE WHEN rcd.rev_cd NOT IN ('0022', '0023', '0024')
      THEN 'HCPCS'
    ELSE 'HIPPS' END                                                        AS hcpcs_hipps_type_cd,
    cf.frst_pcd_modfr_cd,
    cf.sec_pcd_modfr_cd,
    srcpos.plc_of_svc_cd                                                    AS src_plc_of_svc_cd,
    pos.plc_of_svc_cd,
    spcly.spcly_cd,
    pvdr.npi                                                                AS bill_pvdr_npi,
    CASE WHEN pvdr.pvdr_lgl_last_nm IS NULL AND pvdr.pvdr_frst_nm IS NULL
      THEN pvdr.pvdr_lgl_org_nm
    ELSE
    pvdr.pvdr_lgl_last_nm || ', ' || pvdr.pvdr_frst_nm END                AS bill_pvdr_nm,
    pvdr.ent_type_descr,
    pvdr.pvdr_bsn_prct_loc_adr_pst_cd                                       AS bill_pvdr_pst_cd,
    upper(spcly.spcly_descr)                                                AS bill_pvdr_spcly,
	case when a5.mhpn_indicator=1 and a5.indpnd_ind = 0 then 'Employed MHPN Physician'
		 when a5.mhpn_indicator=1 and a5.indpnd_ind = 1 then 'Independent MHPN Physician'
		 when a5.mhpn_indicator=0 and a5.mpp_indicator=1 and a5.indpnd_ind = 0 then 'MPP Physician (OON)'
		 when a5.mhpn_indicator=0 and a5.mpp_indicator=1 and a5.indpnd_ind = 1 then 'Other Out of Network Physician'
		ELSE NULL END as bill_pvdr_ind_type,
	case when a5.mhpn_indicator=1 and a5.indpnd_ind = 0 then 0
		 when a5.mhpn_indicator=1 and a5.indpnd_ind = 1 then 1
		 when a5.mhpn_indicator=0 and a5.mpp_indicator=1 and a5.indpnd_ind = 0 then 0
		 when a5.mhpn_indicator=0 and a5.mpp_indicator=1 and a5.indpnd_ind = 1 then 1
		ELSE NULL END as bill_pvdr_ind,
    oprpvdr.npi                                                             AS oprg_pvdr_npi,
    oprpvdr.pvdr_lgl_last_nm || ', ' || oprpvdr.pvdr_frst_nm                AS oprg_pvdr_nm,
    oprpvdr.prim_spcly_nm                                                   AS oprg_pvdr_spcly,
    attpvdr.npi                                                             AS attnd_pvdr_npi,
    attpvdr.pvdr_lgl_last_nm || ', ' || attpvdr.pvdr_frst_nm                AS attnd_pvdr_nm,
    attpvdr.prim_spcly_nm                                                   AS attnd_pvdr_spcly,
    pcppvdr.npi                                                             AS pcp_pvdr_npi,
    pcppvdr.pvdr_lgl_last_nm || ', ' || pcppvdr.pvdr_frst_nm                AS pcp_pvdr_nm,
    pcppvdr.prim_spcly_nm                                                   AS pcp_pvdr_spcly,
    inpd.grp		                                                    AS pcp_grp_nm,
    cpc.cpc                                                                 AS pcp_cpc_pls_ind,
    COALESCE(attr,'Not Attributed')					    AS bene_attr,
    --Provider Case Attribution
    coalesce(clmfcy.attr_case_pvdr_npi,clmpvdr.attr_case_pvdr_npi) as attr_case_pvdr_npi, 
    coalesce(clmfcy.attr_case_pvdr_nm,clmpvdr.attr_case_pvdr_nm) as attr_case_pvdr_nm, 
    coalesce(clmfcy.attr_case_pvdr_spcly,clmpvdr.attr_case_pvdr_spcly) as attr_case_pvdr_spcly, 
    coalesce(clmfcy.attr_case_pvdr_type,clmpvdr.attr_case_pvdr_type) as attr_case_pvdr_type,
    ccn.cnty_nm as fcy_cnty_nm,
    btd.bill_type_cd,
    asd.adm_src_cd,
    atd.adm_type_cd,
    cf.bill_amt,
    cf.alwd_amt,
    cf.paid_amt,
    cf.co_pay_amt,
    cf.co_insr_amt,
    cf.ddcb_amt,
    cf.ptnt_pymt_amt,
    cf.day_of_svc_cnt,
    cf.svc_unit_cnt,
    dsd.dschrg_sts_cd,
    d0.icd_ver,
    d0.adm_dgns_cd,
    d1.icd_dgns1_cd,
    d2.icd_dgns2_cd,
    d1.poa1_cd,
    d2.poa2_cd,
    p1.icd_pcd1_cd,
    p1.icd_pcd1_3_dgt_cd,
    p1.icd_pcd1_3_dgt_descr,
    p1.icd_pcd1_4_dgt_cd,
    p1.icd_pcd1_4_dgt_descr,
    p2.icd_pcd2_cd,
    cf.rsk_pool_nm,
    cf.case_adm_id,
    cf.fcy_case_id,
    cmd.cst_modl_line_cd,
    cmd.cst_modl_line_descr,
    cmd.cst_modl_line_cgy_nm,
    cmd.care_svc_cgy_lbl,
    cmd.care_svc_cgy_nm,
    cmd.care_svc_sub_cgy_nm,
    cmd.care_setting_cgy_nm,
    cmd.care_setting_sub_cgy_nm,
    cf.cst_modl_utlz_type_cd,
    cf.cst_modl_utlz_cnt,
    cf.cst_modl_cst_amt,
    cf.cst_modl_in_ptnt_clm_adm_ind,
    cf.cst_modl_day_cnt,
    CASE WHEN cf.fcy_unit_cd IS NOT NULL
      THEN SUBSTR(ccn.ccn_id, 1, 2) || cf.fcy_unit_cd || SUBSTR(ccn.ccn_id, 4, 3)
    ELSE ccn.ccn_id END                                                     AS ccn_id,
    asnf.eff_fm_dt as algn_ntw_eff_fm_dt, 
    asnf.eff_to_dt as algn_ntw_eff_to_dt,
    ccn.fcy_type_descr,
    ccn.fcy_nm,
    ccn.hsptl_pvdr_type,
    CASE WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and asnf.ccn_id IS NOT NULL
      THEN asnf.rgon
    ELSE crx.ds_rgon_lng END                              AS fcy_rgon_nm,
    cf.rndrg_pvdr_tin,
    mdd.ms_drg_descr,
    mdd.ms_drg_mdc_descr,
    mdd.svc_cgy_descr,
    mdd.drg_fam_nm,
    mdd.othr_svc_cgy_descr,
    mdd.pref_snsv_cdtn_ind,
    mdd.pref_snsv_cdtn_grp_nm,
    mh.case_mix_idnx_num,
    mh.geo_mean_los_num,
    mh.arthm_mean_los_num,
    rcd.rev_cd_shrt_descr,
    rcd.prn_rev_descr,
    rcd.rev_cd_num_fmt_nm,
    rcd.rev_cd_grp_nm,
    hcp.hcpcs_descr,
    bd.betos_cd,
    bd.betos_descr,
    bd.betos_cgy_nm,
    case when bd.betos_cd in ('M5A','M5B','M5C','M5D','M6') and cst_modl_utlz_type_cd = 'Visits' THEN cf.cst_modl_utlz_cnt else null end as betos_cnslt_ind,
    hip.hipps_descr,
    asd.adm_src_descr,
    atd.adm_type_descr,
    btd.bill_type_descr,
    dsd.dschrg_sts_descr,
    pos.plc_of_svc_nm,
    d0.adm_dgns_descr,
    d1.icd_dgns1_descr,
    d2.icd_dgns2_descr,
    p1.icd_pcd1_descr,
    p2.icd_pcd2_descr,
    d1.poa1_descr,
    d2.poa2_descr,
    spcly.spcly_descr,
	--Added Logic 07/17 ED Care Measures only with Care Service Category as Emergency Room
    case when cmd.care_svc_cgy_nm='ER Visits' then nyu.ed_care_needed_not_prvntable_pct*cf.cst_modl_utlz_cnt end as ed_care_needed_not_prvntable_pct,
    case when cmd.care_svc_cgy_nm='ER Visits' then nyu.ed_care_needed_prvntable_avoidable_pct*cf.cst_modl_utlz_cnt end as ed_care_needed_prvntable_avoidable_pct,
    case when cmd.care_svc_cgy_nm='ER Visits' then nyu.treatable_emrgnt_ptnt_care_pct*cf.cst_modl_utlz_cnt end as treatable_emrgnt_ptnt_care_pct,
    case when cmd.care_svc_cgy_nm='ER Visits' then nyu.non_emrgnt_rel_pct*cf.cst_modl_utlz_cnt end as non_emrgnt_rel_pct,
    case when cmd.care_svc_cgy_nm='ER Visits' then nyu.alc_rel_pct*cf.cst_modl_utlz_cnt end as alc_rel_pct,
    case when cmd.care_svc_cgy_nm='ER Visits' then nyu.drug_rel_pct*cf.cst_modl_utlz_cnt end as drug_rel_pct,
    case when cmd.care_svc_cgy_nm='ER Visits' then nyu.injry_rel_pct*cf.cst_modl_utlz_cnt end as injry_rel_pct,
    case when cmd.care_svc_cgy_nm='ER Visits' then nyu.psychology_rel_pct*cf.cst_modl_utlz_cnt end as psychology_rel_pct,
    case when cmd.care_svc_cgy_nm='ER Visits' then nyu.unclsfd_pct*cf.cst_modl_utlz_cnt end as unclsfd_pct,
    pccs.icd_pcd_ccs_cgy_cd,
    pccs.icd_pcd_ccs_cgy_descr,
    pccs.icd_pcd_ccs_lvl_2_descr,
    dccs.ccs_dgns_cgy_cd,
    dccs.ccs_dgns_cgy_descr,
    dccs.ccs_dgns_lvl_2_descr,
    cci.chronic_cdtn_ind,
    craf.clm_readm_anl_fct_cnt                                              AS readmit_denom,
    craf.readm_clm_id,
    CASE WHEN craf.day_to_readm_cnt <= 30
      THEN 1
    ELSE 0 END                                                              AS readm_all_30_dy_ind,
    CASE WHEN craf.day_to_readm_cnt <= 90
      THEN 1
    ELSE 0 END                                                              AS readm_all_90_dy_ind,
    CASE WHEN craf.day_to_readm_cnt <= 30 AND unpln_ind = 1
      THEN 1
    ELSE 0 END                                                              AS readm_unplnd_30_dy_ind,
    CASE WHEN craf.day_to_readm_cnt <= 90 AND unpln_ind = 1
      THEN 1
    ELSE 0 END                                                              AS readm_unplnd_90_dy_ind,
    craf.day_to_readm_cnt,
    craf.unpln_ind                                                          AS readm_unpln_ind,
    craf.idnx_adm_type_cd                                                   AS readm_idnx_adm_type_cd,
    craf.cohrt_nm                                                           AS readm_cohrt_nm,
    craf.readm_paid_amt,
    readmdd.dgns_cd                                                         AS readm_dgns_cd,
    readmdd.dgns_descr                                                      AS readm_dgns_descr,
    readmipd.icd_pcd_cd                                                     AS readm_icd_pcd_cd,
    readmipd.icd_pcd_descr                                                  AS readm_icd_pcd_descr,
    sip.readm_cnt 								AS snf_ip_ind, 
    sip.days_to_readm 								AS snf_ip_readm_dys, 
    ascf.urinary_tract_adm_ind,
    ascf.htn_adm_ind,
    ascf.copd_adm_ind,
    ascf.dibts_shrt_term_cdtn_adm_ind,
    ascf.dibts_long_term_cdtn_adm_ind,
    ascf.uncntld_dibts_adm_ind,
    ascf.asthma_adm_ind,
    ascf.bacterial_pneumonia_adm_ind,
    ascf.lwr_extremity_amputation_among_dibts_ptnts_adm_ind,
    ascf.perforated_apndix_adm_ind,
    ascf.dehydrton_adm_ind,
    ascf.heart_failr_adm_ind,
    ascf.ovrl_cmpos_ind,
    ascf.acute_cmpos_ind,
    ascf.chronic_cmpos_ind,
    ascf.dibts_cmpos_ind,
    bpci.svc_fm_dt                                                          AS epsd_svc_fm_dt,
    bpci.epsd_svc_to_dt,
    bpci.epsd_nm,
    bpci.paid_amt_30_day_cnt,
    bpci.paid_amt_60_day_cnt,
    bpci.paid_amt_90_day_cnt,
    bpci.pac_paid_amt_30_day_cnt,
    bpci.pac_paid_amt_60_day_cnt,
    bpci.pac_paid_amt_90_day_cnt,
    bpci.anchr_paid_amt,
    bpci.epsd_readm_ind,
    bpci.paid_amt_snf_total_cnt,
    bpci.paid_amt_hh_total_cnt,
    bpci.paid_amt_ip_total_cnt,
    bpci.paid_amt_ltch_total_cnt,
    bpci.oprg_pvdr_spcly as bpci_oprg_pvdr_spcly,
    bpci.oprg_pvdr_nm as bpci_oprg_pvdr_nm,
    bpci.oprg_pvdr_npi as bpci_oprg_pvdr_npi,
    bpci.ntw_ind as bpci_ntw_ind,
    bpci.dschrg_sts_cd as bpci_dschrg_sts_cd,
    bpci.paid_amt_op_total_cnt,
    bpci.paid_amt_prof_total_cnt,
    hccs.ccs_hcpcs_cgy_cd,
    hccs.ccs_hcpcs_cgy_descr,
    hcp.prim_care_svc_ind,
    spcly.pcp_physcn_ind,
    spcly.aco_physcn_ind,
    spcly.aco_spcly_ind,
    CASE WHEN (ccn.ccn_id IN
               ('230227', '230207', '230193', '230141', '230167', '230041', '232020', '230080', '230105', '230216',
                '230297', 'HB1436', '233842', '233820', '233821', '233928', '238902', '902067', '237172', '237010',
                '237165', '237008', '237036', '231521', '23S167', '23S193', '23S141', '23S105', '23S216',
                '23T167', '23T141', '23T105', '235481', '235577', '235526', '235369', '23S207', '23T207', '231329')
               OR (bill_inpd.npi IS NOT NULL AND (bill_inpd.end_mn IS NULL OR bill_inpd.end_mn > cf.svc_to_dt)))
      THEN 1
    WHEN (asnf.algn = 'yes' and (cf.svc_to_dt between asnf.eff_fm_dt and coalesce(asnf.eff_to_dt,'2999-12-31')))
      THEN 2
    ELSE 0 END                                                              AS ntw_ind,
	CASE WHEN inpd.rgon='Region 3 - Genesee' then 'Region 3 - Flint'
		 WHEN inpd.rgon='Region 4 - Ingham' then 'Region 4 - Greater Lansing'
				ELSE inpd.rgon END 											AS rgon,
    inpd.indpnd_ind,
    CASE WHEN --cmd.care_svc_cgy_nm = 'Emergency Room' AND cmd.cst_modl_line_cgy_nm = 'FOP' AND cst_modl_utlz_type_cd = 'Visits'
		cmd.cst_modl_line_cd in ('P51b') --(Modified to the way we have in EU Report)
      THEN cf.cst_modl_utlz_cnt
    ELSE 0 END                                                              AS ed_vst_ind,
    ed_dschrg_ind,
    ed_fcy_paid_amt,
    CASE WHEN cmd.cst_modl_line_cgy_nm = 'FIP'
      THEN cf.cst_modl_in_ptnt_clm_adm_ind
    ELSE 0 END                                                              AS in_ptnt_adm_ind,
    case WHEN cf.cst_modl_in_ptnt_clm_adm_ind in (-1,1) then fcyip.fcy_ip_paid_amt else NULL end as fcy_ip_paid_amt,
    CASE WHEN cmd.cst_modl_line_cgy_nm = 'SNF'
      THEN cf.cst_modl_in_ptnt_clm_adm_ind
    ELSE 0 END                                                              AS snf_adm_ind,
--Added SNF Discharge based on algorithm defined on SNF Discharge Fact
--    CASE WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt > 0
--      THEN 1
--    WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt < 0
--      THEN -1
--    ELSE NULL END                                                              AS snf_dschrg_ind,
    sd.snf_dschrg_ind                                                            AS snf_dschrg_ind,
    sd.snf_fcy_paid_amt,   
--    CASE WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt > 0
--      THEN sd.snf_fcy_paid_amt
--    WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt < 0
--      THEN sd.snf_fcy_paid_amt*(-1)
--    ELSE NULL END as snf_fcy_paid_amt,  
    sd.snf_adm_cst_modl_day_cnt,
--  sd.snf_dschrg_cst_modl_day_cnt,
    CASE WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.clm_line_sts='P' then sd.snf_dschrg_cst_modl_day_cnt*1
         WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.clm_line_sts='R' then sd.snf_dschrg_cst_modl_day_cnt*(-1)
        else sd.snf_dschrg_cst_modl_day_cnt end as snf_dschrg_cst_modl_day_cnt,
 --Added logic for Negative Adjustments   
--    CASE WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt > 0
--      THEN sd.snf_dschrg_cst_modl_day_cnt*1
--    WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt < 0
--      THEN sd.snf_dschrg_cst_modl_day_cnt*(-1)
--    ELSE NULL END as snf_dschrg_cst_modl_day_cnt,
    
    CASE WHEN cmd.care_svc_cgy_nm = 'Observation' AND cmd.cst_modl_line_cgy_nm = 'FOP' AND cst_modl_utlz_type_cd = 'Visits'
      THEN cf.cst_modl_utlz_cnt
    ELSE 0 END                                                              AS obs_vst_ind,
    CASE WHEN cmd.care_svc_cgy_nm = 'Hospice' AND cst_modl_utlz_type_cd = 'Visits'
      THEN cf.cst_modl_utlz_cnt
    ELSE 0 END                                                              AS hospice_vst_ind,
    CASE WHEN care_svc_sub_cgy_nm = 'Office/Home Visits - PCP' AND cst_modl_utlz_type_cd = 'Visits'
      THEN cf.cst_modl_utlz_cnt
    ELSE 0 END                                                              AS pcp_vst_ind,
    CASE WHEN care_svc_sub_cgy_nm = 'Office/Home Visits - Specialist' AND cst_modl_utlz_type_cd = 'Visits'
      THEN cf.cst_modl_utlz_cnt
    ELSE 0 END                                                              AS spec_vst_ind,
    CASE WHEN cmd.cst_modl_line_cd in ('P33','O41l') THEN cf.cst_modl_utlz_cnt ELSE NULL END as urgnt_ind, 
    CASE WHEN hcp.anul_wlns_vst_ind=1 and cmd.care_setting_sub_cgy_nm in ('Professional - Hospital Inpatient','Professional - Facility Outpatient','Professional - Office/Other') THEN cf.cst_modl_utlz_cnt
	ELSE NULL END 							    AS anul_wlns_vst_ind,
    CASE WHEN cmd.cst_modl_line_cd='P51b' then cf.cst_modl_utlz_cnt 
	 WHEN cmd.cst_modl_line_cgy_nm = 'FIP' THEN cf.cst_modl_in_ptnt_clm_adm_ind 
        ELSE NULL END                                                       AS hsp_vst_ind,
    CASE WHEN hsp_vst_ind = 1
      THEN SUM(hsp_vst_ind)
      OVER (
        PARTITION BY pmd.mbr_id_num, date_trunc('year', cf.svc_to_dt) ) END AS yr_hsp_vst_cnt,
    pmd.care_mgn_sts_ind                                                    AS care_manage_status_ind,
    CASE WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt > 0
      THEN sed.snf_ed_vst_ind
    WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt < 0
      THEN sed.snf_ed_vst_ind*(-1)
    ELSE NULL END as snf_ed_vst_ind,
    CASE WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt > 0
      THEN sed.days_fm_snf_to_ed_vst
    WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt < 0
	THEN sed.days_fm_snf_to_ed_vst*(-1)
    ELSE NULL END as days_fm_snf_to_ed_vst,
	pdvf.days_to_fwp_vst, --Post discharge visits
	pdvf.toc_pst_dschrg_vst_hcpcs_cd,
	pdvf.toc_pst_dschrg_vst_hcpcs_descr,
	case when cf.clm_line_num = 1 and dsd.dschrg_sts_cd<>'30' then pdvf.toc_7dy_fwp_vst_ind else null end as toc_7dy_fwp_vst_ind,
	case when cf.clm_line_num = 1 and dsd.dschrg_sts_cd<>'30' then pdvf.toc_14dy_fwp_vst_ind else null end as toc_14dy_fwp_vst_ind,
        case when cf.clm_line_num = 1 and dsd.dschrg_sts_cd<>'30' then pdvf.toc_30dy_fwp_vst_ind else null end as toc_30dy_fwp_vst_ind,
	CASE WHEN cmd.care_svc_cgy_nm ='Home Health' and cf.clm_line_num='001' then hha.hha_vst_ind else null END  AS hha_case_ind,
	CASE WHEN cmd.care_svc_cgy_nm = 'Hospice' and cf.clm_line_num='001' then ha.hspc_vst_ind else null end AS hspc_case_ind,
	CASE WHEN cmd.care_svc_cgy_nm = 'Hospice' and cf.clm_line_num='001' then ha.los else null end AS hspc_los,
	pdcf.pst_ms_drg_cd,
	pdcf.pst_ms_drg_descr,
	pdcf.pst_ms_drg_mdc_descr,
	pdcf.pst_svc_cgy_descr,
	pdcf.pst_drg_fam_nm,
	pdcf.pst_ms_drg_bsn_line_descr,
	pdcf.pst_mcl_mjr_slp_grp,
	pdcf.pst_mcl_svc_ln,
	pdcf.pst_mcl_sub_svc_ln,
	pdcf.days_fm_ip,
	ips.ip_dschrg_snf_adm_ind,
	ips.pref_snf_algn_ind,
        CASE WHEN cf.clm_line_num = 1 and (d0.covid_adm_ind=1 or d1.covid_prim_adm_ind=1) then 1 ELSE NULL END as covid_adm_ind,
        CASE WHEN hcp.hcpcs_cd in ('87635','86328','86769','U0002','U0001','G2023','G2024') and cmd.care_setting_sub_cgy_nm='Facility Outpatient' then cf.cst_modl_utlz_cnt ELSE NULL END as covid_tst_ind,
        frct_ind as snf_frct_case_ind,
        frct_4dgt_descr as snf_frct_case_4dgt_icd_descr
  FROM
    pln_mbr_asgnt_dim pmad
    INNER JOIN pln_mbr_dim pmd ON pmad.pln_mbr_sk = pmd.pln_mbr_sk
    LEFT OUTER JOIN clm_line_fct cf
      ON pmad.pln_mbr_sk = cf.pln_mbr_sk AND cf.svc_to_dt BETWEEN pmad.asgnt_wndw_strt_dt AND pmad.asgnt_wndw_end_dt
    INNER JOIN qtr_asgnt_dim qad ON pmad.pln_mbr_sk = qad.pln_mbr_sk
    LEFT OUTER JOIN post_drg_care_svc_fct pdcf on cf.clm_line_fct_sk=pdcf.clm_line_fct_sk
    LEFT OUTER JOIN snf_ed_vst_dm sed on cf.fcy_case_id=sed.fcy_case_id 
    LEFT OUTER JOIN snf_dschrg_fct sd on cf.clm_id=sd.claimid and cf.clm_line_num = 1 
    LEFT OUTER JOIN hospice_adm ha on cf.pln_mbr_sk = ha.pln_mbr_sk and cf.clm_id=ha.clm_id
    LEFT OUTER JOIN hm_hlth_cases hha on cf.pln_mbr_sk = hha.pln_mbr_sk and cf.clm_id=hha.clm_id
    LEFT OUTER JOIN post_dschrg_vst_fct pdvf on cf.clm_id = pdvf.clm_id and cf.clm_line_num = 1
    LEFT OUTER JOIN fcy_ip_paid_amt fcyip on cf.fcy_case_id=fcyip.fcy_case_id and cf.clm_id=fcyip.frst_ip_clm_id
    LEFT OUTER JOIN ip_dschrg_snf_frct_adm_fct ipfr on cf.clm_id=ipfr.snf_clm_id and cf.clm_line_num = 1
    LEFT OUTER JOIN (select * from pvdr_case_attr where encntr_case_type='fcy_case_id') clmfcy on cf.fcy_case_id=clmfcy.fcy_case_id and cf.clm_id=clmfcy.encntr_num
    LEFT OUTER JOIN (select * from pvdr_case_attr where encntr_case_type='clm_id') clmpvdr on cf.clm_id=clmpvdr.encntr_num	
    LEFT OUTER JOIN ms_drg_dim mdd ON cf.ms_drg_cd = mdd.ms_drg_cd
    LEFT OUTER JOIN ms_drg_dim_h mh ON cf.ms_drg_cd = mh.ms_drg_cd and cf.svc_to_dt between mh.vld_fm_dt and COALESCE(mh.vld_to_dt,'2999-12-31')
    LEFT OUTER JOIN rev_cl_dim rcd ON cf.rev_cl_sk = rcd.rev_cl_sk
    LEFT OUTER JOIN hcpcs_dim hcp ON cf.hcpcs_sk = hcp.hcpcs_sk
    LEFT OUTER JOIN hipps_dim hip ON cf.hcpcs_sk = hip.hipps_sk
    LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
    LEFT OUTER JOIN adm_type_dim atd ON cf.adm_type_sk = atd.adm_type_sk
    LEFT OUTER JOIN dschrg_sts_dim dsd ON cf.dschrg_sts_sk = dsd.dschrg_sts_sk
    LEFT OUTER JOIN plc_of_svc_dim pos ON cf.plc_of_svc_sk = pos.plc_of_svc_sk
    LEFT OUTER JOIN plc_of_svc_dim srcpos ON cf.sbmted_plc_of_svc_sk = srcpos.plc_of_svc_sk
    LEFT OUTER JOIN dgns0 d0 ON cf.clm_id = d0.clm_id AND cf.clm_line_num = d0.clm_line_num
    LEFT OUTER JOIN dgns1 d1 ON cf.clm_id = d1.clm_id AND cf.clm_line_num = d1.clm_line_num
    LEFT OUTER JOIN dgns2 d2 ON cf.clm_id = d2.clm_id AND cf.clm_line_num = d2.clm_line_num
    LEFT OUTER JOIN dgns3 d3 ON cf.clm_id = d3.clm_id AND cf.clm_line_num = d3.clm_line_num
    LEFT OUTER JOIN dgns4 d4 ON cf.clm_id = d4.clm_id AND cf.clm_line_num = d4.clm_line_num
    LEFT OUTER JOIN pcd1 p1 ON cf.clm_id = p1.clm_id AND cf.clm_line_num = p1.clm_line_num
    LEFT OUTER JOIN pcd2 p2 ON cf.clm_id = p2.clm_id AND cf.clm_line_num = p2.clm_line_num
    LEFT OUTER JOIN pcd3 p3 ON cf.clm_id = p3.clm_id AND cf.clm_line_num = p3.clm_line_num
    LEFT OUTER JOIN pcd4 p4 ON cf.clm_id = p4.clm_id AND cf.clm_line_num = p4.clm_line_num
    LEFT OUTER JOIN aco_spcly_dim spcly ON cf.aco_spcly_sk = spcly.aco_spcly_sk
    LEFT OUTER JOIN (SELECT *
                     FROM ccn_dim
                     WHERE ccn_id NOT IN (
                       SELECT ccn_id
                       FROM ccn_dim
                       WHERE ccn_id IN (
                         SELECT ccn_id
                         FROM ccn_dim
                         GROUP BY 1
                         HAVING COUNT(*) > 1
                       ) AND fcy_type_descr != 'Dialysis Facility'
                     ) OR fcy_type_descr != 'Dialysis Facility') ccn ON ccn.ccn_sk = cf.ccn_alt_sk
    LEFT OUTER JOIN icd_pcd_ccs_dim pccs
      ON REPLACE(pccs.icd_pcd_cd, '.', '') = p1.icd_pcd1_cd AND pccs.icd_pcd_cd_ver = p1.icd_ver
    LEFT OUTER JOIN dgns_ccs_dim dccs
      ON REPLACE(dccs.dgns_cd, '.', '') = d1.icd_dgns1_cd AND dccs.dgns_cd_ver = d1.icd_ver
    LEFT OUTER JOIN dgns_ccs_chronic_cdtn_dim cci
      ON REPLACE(cci.dgns_cd, '.', '') = d1.icd_dgns1_cd AND cci.dgns_cd_ver = d1.icd_ver
    LEFT OUTER JOIN hcpcs_ccs_dim hccs ON hccs.hcpcs_ccs_sk = cf.hcpcs_sk
    LEFT OUTER JOIN mcl_pvdr_dim a5 ON a5.pvdr_sk = cf.bill_pvdr_sk
    LEFT OUTER JOIN pvdr_dim oprpvdr ON cf.oprg_pvdr_sk = oprpvdr.pvdr_sk
    LEFT OUTER JOIN pvdr_dim attpvdr ON cf.attnd_pvdr_sk = attpvdr.pvdr_sk
    LEFT OUTER JOIN pvdr_dim pvdr ON cf.bill_pvdr_sk = pvdr.pvdr_sk
    LEFT OUTER JOIN nyu_ed_algr_dim nyu
      ON nyu.icd_diagonsis_cd = d1.icd_dgns1_cd AND nyu.dgns_icd_ver = d1.icd_ver
    LEFT OUTER JOIN bene_pcp_attr pcp ON pmad.pln_mbr_sk = pcp.pln_mbr_sk
    LEFT OUTER JOIN pvdr_dim pcppvdr ON pcp.bill_pvdr_sk = pcppvdr.pvdr_sk
    LEFT OUTER JOIN bill_type_dim btd ON cf.bill_type_sk = btd.bill_type_sk
    LEFT OUTER JOIN cst_modl_dim cmd ON cf.cst_modl_sk = cmd.cst_modl_sk
    LEFT OUTER JOIN (SELECT DISTINCT grp_tin AS tin
                     FROM in_ntw_pvdr_dim) tin ON cf.rndrg_pvdr_tin = tin.tin
    LEFT OUTER JOIN mcl_pvdr_dim inpd ON inpd.pvdr_sk = pcp.bill_pvdr_sk
    LEFT OUTER JOIN clm_readm_anl_fct craf ON cf.clm_id = craf.clm_id AND cf.clm_line_num = 1
    LEFT OUTER JOIN snf_ip_readm_fct sip on cf.clm_id=sip.clm_id and cf.clm_line_num = 1
    LEFT OUTER JOIN ip_dschrg_snf_adm_fct ips on cf.clm_id=ips.clm_id and cf.clm_line_num = 1
    LEFT OUTER JOIN dgns_dim readmdd ON craf.readm_dgns_sk = readmdd.dgns_sk
    LEFT OUTER JOIN icd_pcd_dim readmipd ON craf.icd_pcd_sk = readmipd.icd_pcd_sk
    LEFT OUTER JOIN ambul_snsv_cdtn_fct ascf ON cf.clm_id = ascf.clm_id AND cf.clm_line_num = 1
    LEFT OUTER JOIN ed_cst_anl_fct edcst on cf.clm_id = edcst.clm_id
    LEFT OUTER JOIN cnty_rgon_xwalk crx ON ccn.cnty_nm = crx.cnty
    LEFT OUTER JOIN betos_dim bd ON cf.betos_sk = bd.betos_sk
    LEFT OUTER JOIN algn_snf asnf ON ccn.ccn_id = asnf.ccn_id
    LEFT OUTER JOIN in_ntw_pvdr_dim bill_inpd ON pvdr.pvdr_sk = bill_inpd.in_ntw_pvdr_sk
    LEFT OUTER JOIN bpci_epsd_pac_spcly_fct bpci ON bpci.frst_clm_id = cf.clm_id AND cf.clm_line_num = 1
    LEFT OUTER JOIN cpc_plus cpc ON cpc.npi = pcppvdr.npi
  WHERE
    qad.asgnt_yr = (SELECT val
                    FROM dt_meta
                    WHERE descr = 'asgnt_yr')
    AND qad.asgnt_qtr = (SELECT val
                         FROM dt_meta
                         WHERE descr = 'asgnt_qtr')
    AND pmad.elig_sts != 'Not Eligible'
    AND pmad.asgnt_wndw_strt_dt
    BETWEEN (SELECT val
             FROM dt_meta
             WHERE descr = 'multi_yr_strt') AND (SELECT val
                                                 FROM dt_meta
                                                 WHERE descr = 'multi_yr_end');

\unset ON_ERROR_STOP
