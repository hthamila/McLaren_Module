\set ON_ERROR_STOP ON;

drop table stg_cv_outclaims if exists;

create table stg_cv_outclaims as 
--Hospice Discharges
with oth_dschrg as
(
select claimid clm_id, member_id mbi_id, riskpool rsk_pool, prm_util_type oth_util_type, dischargestatus oth_dschrg_sts, sum(paid) oth_paid, sum(prm_util) oth_prm_util, min(prm_fromdate) svc_fm_dt, 
case when (sum(paid)<0 or claimlinestatus='R') then max(days)*-1
	else max(days) end as svc_days,
case when dischargestatus<>'30' and sum(paid)>=0 then 1
	 when dischargestatus<>'30' and (sum(paid)<0 or claimlinestatus='R') then -1
		else null end as oth_dschrg_ind
from pce_qe16_aco_prd_lnd..cv_outclaims where riskpool in ('HHA','Hospice') and prm_util_type in ('Days','Visits')
group by claimid, member_id, riskpool, prm_util_type, dischargestatus, claimlinestatus
)

SELECT 	sequencenumber
       	, o.claimid
       	, linenum
       	, contractid
       	, o.member_id
       	, o.prm_fromdate
       	, o.prm_todate
	, 
       	, paiddate
       	, drg
       	, drgversion
	, ms_drg_descr 
	, ms_drg_mdc_descr 
	, ms_drg_bsn_line_descr 
       	, ms_drg_type_cd_descr
	, mclaren_major_slp_grouping
	, mclaren_service_line
	, mclaren_sub_service_line
	, drg_fam_nm
	, pref_snsv_cdtn_grp_nm
	, pref_snsv_cdtn_ind
       	, revcode
	, prn_rev_descr
	, rev_cd_grp_nm
	, rev_cd_num_fmt_nm
	, rev_cd_shrt_descr
       	, hcpcs
       	, modifier
       	, modifier2
       	, srcpos
       	, pos
       	, plc_of_svc_nm
       	, srcspecialty
       	, specialty
       	, encounterflag
       	, providerid
	, CASE WHEN bill.pvdr_lgl_last_nm IS NULL AND bill.pvdr_frst_nm IS NULL THEN bill.pvdr_lgl_org_nm
    			ELSE
    		bill.pvdr_lgl_last_nm || ', ' || bill.pvdr_frst_nm END     	AS bill_pvdr_nm	
	, aco.spcly_descr 							AS bill_spcly_descr
	, case when a5.mhpn_indicator=1 and a5.indpnd_ind = 0 then 0
		 when a5.mhpn_indicator=1 and a5.indpnd_ind = 1 then 1
		 when a5.mhpn_indicator=0 and a5.mpp_indicator=1 and a5.indpnd_ind = 0 then 0
		 when a5.mhpn_indicator=0 and a5.mpp_indicator=1 and a5.indpnd_ind = 1 then 1
		ELSE NULL END 							AS bill_pvdr_ind
       	, medicareid
       	, bill_type_cd
       	, bill_type_descr
       	, admitsource
       	, adm_src_descr
       	, admittype
       	, adm_type_descr
       	, billed
       	, allowed
       	, paid
       	, cob
       	, copay
       	, coinsurance
       	, deductible
       	, patientpay
       	, days
       	, units
       	, o.dischargestatus
       	, dschrg_sts_descr
       	, o.claimlinestatus
       	, icdversion
       	, admitdiag
	, d0.dgns_descr         AS adm_dgns_descr
       	, icddiag1
	, d1.dgns_descr		AS prim_dgns_descr
	, icddiag2
	, d2.dgns_descr		AS scdy_dgns_descr
       	, poa1
	, p1.poa_descr		AS prim_poa_descr
	, poa2
	, p2.poa_descr		AS scdy_poa_descr
       	, icdproc1
	, icdproc2
	, pc2.icd_pcd_descr 	AS scdy_pcd_descr
       	, riskpool
       	, prm_oon_yn
       	, zip
       	, county
       	, memberstatus
       	, caseadmitid
       	, o.facilitycaseid
       	, prm_line
       	, prm_util
       	, prm_util_type
	, cmd.cst_modl_line_cd
        , cmd.cst_modl_line_descr 
        , cmd.cst_modl_line_cgy_nm
        , cmd.care_setting_cgy_nm 
        , cmd.care_setting_sub_cgy_nm 
        , cmd.care_svc_cgy_nm
        , cmd.care_svc_sub_cgy_nm 
        , cmd.care_svc_cgy_lbl
       	, o.prm_admits
       	, prm_costs
       	, prm_days
       	, prm_er_to_ip
       	, prm_prv_id_tin
       	, prm_prv_id_ccn
	, ccn.fcy_nm
	, ccn.fcy_type_descr
	, ccn.hsptl_pvdr_type
	, ccn.cnty_nm as fcy_cnty_nm
	, case when cmd.cst_modl_line_cgy_nm = 'SNF' and asnf.ccn_id IS NOT NULL
      		THEN asnf.rgon ELSE crx.ds_rgon_lng END AS fcy_rgon_nm
	, asnf.eff_fm_dt as algn_ntw_eff_fm_dt
    	, asnf.eff_to_dt as algn_ntw_eff_to_dt
       	, prm_prv_id_attending
	, atd.pvdr_lgl_last_nm || ', ' || atd.pvdr_frst_nm     		AS attnd_pvdr_nm
	, atd.prim_spcly_nm                                           	AS attnd_pvdr_spcly
       	, prm_prv_id_operating
	, opr.pvdr_lgl_last_nm || ', ' || opr.pvdr_frst_nm            	AS oprg_pvdr_nm
	, opr.prim_spcly_nm                                             AS oprg_pvdr_spcly
       	, prm_betos_code
       	, snfrm_numer_yn
       	, snfrm_denom_yn
       	, CASE WHEN mcl.ccn_ntw_ind=1 or (ntw.npi is not null and (ntw.end_mn is null or ntw.end_mn > o.prm_todate))
		THEN 1
	      WHEN (asnf.algn = 'yes' and (o.prm_todate between asnf.eff_fm_dt and coalesce(asnf.eff_to_dt,'2999-12-31')))
      		THEN 2
    		ELSE 0 END  ntw_ind
       	, slos.snf_dschrg_ind as snf_discharge 	
       	, oth.oth_dschrg_ind
       	, mh.case_mix_idnx_num
       	, oth.svc_days
       	, case when days_fm_snf_to_ed_vst=0 then snf_ed_vst_ind else null end as snf_same_day_ed_vst
       	, slos.snf_adm_cst_modl_day_cnt as snf_adm_cst_modl_days
       	, slos.snf_dschrg_cst_modl_day_cnt as snf_dschrg_cst_modl_days
       	, slos.snf_fcy_paid_amt
       	, case when hcpcs in ('87635','86328','86769','U0002','U0001','G2023','G2024') then prm_util else null end as covid_tst_ind
       	, case when admitdiag in ('U070','U071') or icddiag1 in ('U070','U071') then o.prm_admits else null end as covid_adm_ind
	, ed_dschrg_ind
    	, ed_fcy_paid_amt
	, cvh.ed_vst_30d_asc_ind
	, cvh.ed_vst_30d_prv_vst_ind
	, cvh.hsp_adm_30d_asc_ind
	, cvh.hsp_adm_30d_prv_vst_ind
	, case when v.cohrt_id='AWV' then o.prm_util else null end as prv_msr_dnmr
--	, hos.hospice_adm_ind
--	, hos.hospice_dschrg_ind
from pce_qe16_aco_prd_lnd..cv_outclaims o 
	join pce_qe16_aco_prd_lnd..cv_members using (member_id)
	left join adm_src_dim on o.admitsource=adm_src_cd
	left join adm_type_dim on o.admittype=adm_type_cd
	left join bill_type_dim on strleft(o.billtype,2)=bill_type_cd
	left join plc_of_svc_dim on o.pos=plc_of_svc_cd
	left join dschrg_sts_dim on o.dischargestatus=dschrg_sts_cd
	left join cst_modl_dim cmd on o.prm_line=cmd.cst_modl_line_cd
	left join rev_cl_dim on o.revcode=rev_cd
	left join ccn_dim ccn on o.prm_prv_id_ccn=ccn.ccn.ccn_id
	left join mcl_ccn_dim mcl on o.prm_prv_id_ccn = mcl.ccn_id
	left join algn_snf asnf ON o.prm_prv_id_ccn = asnf.ccn_id
	left join cnty_rgon_xwalk crx ON ccn.cnty_nm = crx.cnty
	left join in_ntw_pvdr_dim ntw on o.providerid = ntw.npi
	left join dgns_dim d0 on o.admitdiag=d0.dgns_alt_cd and o.icdversion=lpad(d0.dgns_icd_ver,2,0)
	left join dgns_dim d1 on o.icddiag1=d1.dgns_alt_cd and o.icdversion=lpad(d1.dgns_icd_ver,2,0)
	left join dgns_dim d2 on o.icddiag2=d2.dgns_alt_cd and o.icdversion=lpad(d2.dgns_icd_ver,2,0)
	left join poa_dim p1 on o.poa1=p1.poa_cd
	left join poa_dim p2 on o.poa2=p2.poa_cd
	left join icd_pcd_dim pc2 on o.icdproc2=pc2.icd_pcd_cd and o.icdversion=lpad(pc2.icd_ver,2,0)
	left join pvdr_dim atd on o.prm_prv_id_attending = atd.npi and atd.npi is not null
	left join pvdr_dim opr on o.prm_prv_id_operating = opr.npi and opr.npi is not null
	left join pvdr_dim bill on o.providerid = bill.npi and bill.npi is not null
	left join mcl_pvdr_dim a5 ON o.providerid = a5.npi
	left join aco_spcly_dim aco on o.specialty = aco.spcly_cd
	left join oth_dschrg oth on o.claimid = oth.clm_id and o.linenum='001'
	left join val_set_dim v ON o.hcpcs=v.cd and v.cohrt_id='AWV'
--	left join cv_hospice_adm_dschrg_fct hos on o.claimid=hos.claimid and o.linenum='001'
	left join cv_clm_hdr_msr_fct cvh on o.claimid = cvh.claimid and o.linenum='001'
	left join snf_dschrg_ed_adm_fct sed on o.claimid = sed.clm_id and o.linenum='001'
	left join snf_dschrg_ed_adm_fct sed on o.claimid = sed.clm_id and o.linenum='001'
	left join snf_dschrg_fct slos on o.claimid = slos.claimid and o.linenum='001'
	left join ed_cst_anl_fct edcst on o.claimid = edcst.fcy_case_id and o.linenum='001'
	left join ms_drg_dim_h mh ON o.drg = mh.ms_drg_cd and o.prm_todate between mh.vld_fm_dt and COALESCE(mh.vld_to_dt,'2999-12-31')
      where o.prm_todate >= (select val from dt_meta where descr='multi_yr_strt')
distribute on (member_id, svc_mnth);

drop table cv_outclaims if exists;
alter table stg_cv_outclaims rename to cv_outclaims;

\unset ON_ERROR_STOP
