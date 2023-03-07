\set ON_ERROR_STOP ON

--Diagnosis Dimension with NYU Metrics
drop table cv_dgns_dim if exists;
create table cv_dgns_dim as 
select 	d.dgns_cd, d.dgns_alt_cd, d.dgns_icd_ver, d.dgns_descr, 
		dccs.ccs_dgns_cgy_cd,
    	dccs.ccs_dgns_cgy_descr,
    	dccs.ccs_dgns_lvl_2_descr,
		cci.chronic_cdtn_ind,
		alc_rel_pct, drug_rel_pct, ed_care_needed_not_prvntable_pct, 
		ed_care_needed_prvntable_avoidable_pct, injry_rel_pct, non_emrgnt_rel_pct, psychology_rel_pct, 
		treatable_emrgnt_ptnt_care_pct, unclsfd_pct
	from dgns_dim d
		left join dgns_ccs_dim dccs on d.dgns_alt_cd=REPLACE(dccs.dgns_cd, '.', '') 
			and d.dgns_icd_ver=dccs.dgns_cd_ver
		left join dgns_ccs_chronic_cdtn_dim cci on d.dgns_alt_cd=REPLACE(cci.dgns_cd, '.', '') 
			and d.dgns_icd_ver=cci.dgns_cd_ver
	  	left join nyu_ed_algr_dim nyu on d.dgns_alt_cd=nyu.icd_diagonsis_cd
			and d.dgns_icd_ver=nyu.dgns_icd_ver
distribute on (dgns_alt_cd);

--Procedure Dimension
drop table cv_pcd_dim if exists;
create table cv_pcd_dim as 
select p.icd_pcd_cd, p.icd_pcd_descr, p.icd_ver, icd_pcd_3_dgt_cd, icd_pcd_3_dgt_descr, icd_pcd_4_dgt_cd, icd_pcd_4_dgt_descr, icd_pcd_ccs_cgy_cd, icd_pcd_ccs_cgy_descr, icd_pcd_ccs_lvl_2_descr
	from icd_pcd_dim p 
	  left join icd_pcd_ccs_dim ccs 
	  	on p.icd_pcd_cd=ccs.icd_pcd_cd
		and p.icd_ver=ccs.icd_pcd_cd_ver
distribute on (icd_pcd_cd);

--Preference Sensitive Procedure Fact
drop table prfn_snsv_pcd_fct if exists;
create table prfn_snsv_pcd_fct as
select 	claimid
		, member_id
		, prm_line
		, case when i.icd_pcd_ccs_cgy_cd='3' then sum(prm_admits) else null end as lmnctmy_ind
		, case when i.icd_pcd_ccs_cgy_cd='44' then sum(prm_admits) else null end as cabg_ind
		, case when i.icd_pcd_ccs_cgy_cd='45' then sum(prm_admits) else null end as ptca_ind
		, case when i.icd_pcd_ccs_cgy_cd='48' then sum(prm_admits) else null end as crdc_pcmkr_ind
		, case when i.icd_pcd_ccs_cgy_cd in ('51','59') then sum(prm_admits) else null end as crtd_arty_rvsln_ind
		, case when i.icd_pcd_ccs_cgy_cd in ('55','61') then sum(prm_admits) else null end as prphl_vsl_rvsln_ind
		, case when i.icd_pcd_ccs_cgy_cd='84' then sum(prm_admits) else null end as chlcytmy_ind
		, case when i.icd_pcd_ccs_cgy_cd='124' then sum(prm_admits) else null end as hystmy_ind
		, case when i.icd_pcd_ccs_cgy_cd='152' then sum(prm_admits) else null end as knee_rlpcmt_ind
		, case when i.icd_pcd_ccs_cgy_cd='153' then sum(prm_admits) else null end as hip_rplcmt_ind
		, case when i.icd_pcd_ccs_cgy_cd='154' then sum(prm_admits) else null end as arthplsty_ind
		, case when i.icd_pcd_ccs_cgy_cd='158' then sum(prm_admits) else null end as spnl_fsn_ind
		, case when i.icd_pcd_ccs_cgy_cd='244' then sum(prm_admits) else null end as brtc_srgy_ind
		, case when i.icd_pcd_ccs_cgy_cd in ('3','44','45','48','51','59','55','61','84','124','152','153','154','158','244') then sum(prm_admits) else null end as prfn_snsv_pcd_ind
	from pce_qe16_aco_prd_lnd..cv_outclaims o
left join cst_modl_dim c on o.prm_line=c.cst_modl_line_cd
left join icd_pcd_ccs_dim i on o.icdproc1=i.icd_pcd_cd
where riskpool='IP'
group by claimid, member_id, prm_line, icd_pcd_ccs_cgy_cd
distribute on (claimid, member_id);

--Historical HCC Score for Beneficiaries
drop table hist_benf_hcc_anl_fct if exists;
create table hist_benf_hcc_anl_fct as 
select claimid clm_id, member_id mbr_id_num, prm_fromdate svc_fm_dt, prm_todate svc_to_dt, dgns_cd, dgns_description dgns_descr, cms_hcc_id, cms_hcc_description cms_hcc_descr, raf_score raf_scr
from 
(
select *,
row_number() over (partition by member_id, cms_hcc_id order by prm_todate desc) as rnk
from pce_qe16_aco_prd_lnd..outclaims_diag o
join pce_qe16_aco_prd_dm..dgns_hcc_rsk_scr_dim rscr on o.icd_code=rscr.dgns_cd 
)a where a.rnk=1;

--Hospice Admit/Discharge Fact

drop table cv_hospice_adm_dschrg_fct if exists;
create table cv_hospice_adm_dschrg_fct as
select member_id, claimid, paid hospice_paid, hospice_dschrg_ind, hospice_adm_ind from ( 
select member_id, claimid, dischargestatus, claimlinestatus, paid, prm_fromdate, 
lag(dischargestatus,1) over (partition by member_id order by claimid, prm_fromdate) prior_dschrg_sts,
lead(dischargestatus,1) over (partition by member_id order by claimid, prm_fromdate) nxt_dschrg_sts,
lag(paid,1) over (partition by member_id order by claimid, prm_fromdate) prior_paid,
case when dischargestatus<>30 and claimlinestatus='P' then 1
	 when dischargestatus<>30 and claimlinestatus='R' then -1
	 	else null end as hospice_dschrg_ind,
case when dischargestatus=30 and prior_dschrg_sts is null and paid > 0  then 1
	 when dischargestatus=30 and prior_dschrg_sts is null and paid < 0  then -1
	 when dischargestatus=30 and prior_dschrg_sts<>30 and claimlinestatus='P' then 1
	 when dischargestatus=30 and prior_dschrg_sts<>30 and claimlinestatus='R' then -1
	 when dischargestatus=prior_dschrg_sts and prior_paid< 0 and nxt_dschrg_sts is null then 1 
	else null end as hospice_adm_ind
from
(select member_id, claimid, dischargestatus, claimlinestatus, sum(paid) paid, min(prm_fromdate) prm_fromdate from pce_qe16_aco_prd_lnd..cv_outclaims where riskpool='Hospice'
group by member_id, claimid, dischargestatus, claimlinestatus )a
order by member_id, claimid)b
distribute on (member_id,claimid);

\unset ON_ERROR_STOP
