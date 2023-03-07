\set ON_ERROR_STOP ON

-----------------------------------------------------------------------------------------------------------------------------------------------------
--Story# MLH-375
--ED Visit within 30-Days of ASC Indicator (should be calc. only for Ambulatory Surgery cases based on Care Service Category of Ambulatory Service)--
-----------------------------------------------------------------------------------------------------------------------------------------------------

create temp table asc_ed_vst_msr as
select *, 1 ed_vst_30d_asc_ind, ed_prm_fromdate - prm_todate as days_to_ed_from_asc from
(
select member_id, claimid, care_svc_cgy_lbl, care_svc_sub_cgy_nm, prm_fromdate, prm_todate, 
		lead(claimid) over (partition by member_id order by prm_fromdate) as ed_claimid,
		lead(care_svc_cgy_lbl) over (partition by member_id order by prm_fromdate) as ed_care_svc_cgy_lbl, 
		lead(care_svc_sub_cgy_nm) over (partition by member_id order by prm_fromdate) as ed_care_svc_sub_cgy_nm,
		lead(prm_fromdate) over (partition by member_id order by prm_fromdate) as ed_prm_fromdate,
		lead(prm_todate) over (partition by member_id order by prm_fromdate) as ed_prm_todate
from		
(
select	o.member_id
        , o.claimid
		, care_svc_cgy_lbl
		, care_svc_sub_cgy_nm
		, sum(o.prm_util) prm_util
		, min(o.prm_fromdate) prm_fromdate
		, max(o.prm_todate) prm_todate
	from pce_qe16_aco_prd_lnd..cv_outclaims o 
	INNER JOIN cst_modl_dim cd ON o.prm_line = cd.cst_modl_line_cd
	where prm_line in ('O12b','P51b')
	group by member_id, claimid, care_svc_cgy_lbl, care_svc_sub_cgy_nm
	having sum(o.prm_util)> 0
)a)b
where b.care_svc_sub_cgy_nm='Ambulatory Surgery Center' and b.ed_care_svc_sub_cgy_nm='ER Visits'
	and (ed_prm_fromdate - prm_todate) < 31;

-----------------------------------------------------------------------------------------------------------------------------------------------------
--ED Visit within 30-Days of Clinic Visit Indicator (should be calc only for Clinic Visits)
--ED Visit within 30-Days of OP Visit Indicator (should be calc. only for Outpatient Cases based on Care Setting Category of Outpatient)
-----------------------------------------------------------------------------------------------------------------------------------------------------

create temp table prv_ed_vst_msr as
select *, 1 ed_vst_30d_prv_vst_ind, ed_prm_fromdate - prm_todate as days_to_ed_from_prv_vst from
(
select member_id, claimid, care_setting_cgy_nm, care_svc_cgy_nm, prm_fromdate, prm_todate, 
		lead(claimid) over (partition by member_id order by prm_fromdate) as ed_claimid,
		lead(care_setting_cgy_nm) over (partition by member_id order by prm_fromdate) as ed_care_setting_cgy_nm, 
		lead(care_svc_cgy_nm) over (partition by member_id order by prm_fromdate) as ed_care_svc_cgy_nm,
		lead(prm_fromdate) over (partition by member_id order by prm_fromdate) as ed_prm_fromdate,
		lead(prm_todate) over (partition by member_id order by prm_fromdate) as ed_prm_todate
from		
(
select	o.member_id
        , o.claimid
		, case when v.cohrt_id='AWV' then 'AWV' else care_setting_cgy_nm end as care_setting_cgy_nm
		, care_svc_cgy_nm
		, sum(o.prm_util) prm_util
		, min(o.prm_fromdate) prm_fromdate
		, max(o.prm_todate) prm_todate
	from pce_qe16_aco_prd_lnd..cv_outclaims o 
	INNER JOIN cst_modl_dim cd ON o.prm_line = cd.cst_modl_line_cd
	LEFT JOIN val_set_dim v ON o.hcpcs=v.cd and v.cohrt_id='AWV'
	where (care_setting_cgy_nm='Outpatient (facility and professional)' and care_svc_cgy_nm='ER Visits')
		or (v.cohrt_id='AWV' and prm_util_type='Visits')
	group by member_id, claimid, care_setting_cgy_nm, care_svc_cgy_nm, v.cohrt_id
	having sum(o.prm_util)> 0
)a)b
where b.care_setting_cgy_nm='AWV' and b.ed_care_svc_cgy_nm='ER Visits'
	and (ed_prm_fromdate - prm_todate) < 31;

-----------------------------------------------------------------------------------------------------------------------------------------------------
--Hospitalization within 30-Days of Clinic Visit Indicator (should be calc only for Clinic Visits)
--Hospitalization within 30-Days of OP Visit Indicator (should be calc. only for Outpatient Cases based on Care Setting Category of Outpatient)
-----------------------------------------------------------------------------------------------------------------------------------------------------
create temp table prv_hsp_vst_msr as
select *, 1 hsp_adm_30d_prv_vst_ind, hsp_prm_fromdate - prm_todate as days_to_hsp_from_prv_vst from
(
select member_id, claimid, care_setting_cgy_nm, care_svc_cgy_nm, prm_fromdate, prm_todate, 
		lead(claimid) over (partition by member_id order by prm_fromdate) as ed_claimid,
		lead(care_setting_cgy_nm) over (partition by member_id order by prm_fromdate) as hsp_care_setting_cgy_nm, 
		lead(care_svc_cgy_nm) over (partition by member_id order by prm_fromdate) as hsp_care_svc_cgy_nm,
		lead(prm_fromdate) over (partition by member_id order by prm_fromdate) as hsp_prm_fromdate,
		lead(prm_todate) over (partition by member_id order by prm_fromdate) as hsp_prm_todate
from		
(
select	o.member_id
        , o.claimid
		, case when v.cohrt_id='AWV' then 'AWV' else care_setting_cgy_nm end as care_setting_cgy_nm
		, care_svc_cgy_nm
		, sum(prm_admits) prm_admits
		, sum(o.prm_util) prm_util
		, min(o.prm_fromdate) prm_fromdate
		, max(o.prm_todate) prm_todate
	from pce_qe16_aco_prd_lnd..cv_outclaims o 
	INNER JOIN cst_modl_dim cd ON o.prm_line = cd.cst_modl_line_cd
	LEFT JOIN val_set_dim v ON o.hcpcs=v.cd and v.cohrt_id='AWV'
	group by member_id, claimid, care_setting_cgy_nm, care_svc_cgy_nm, v.cohrt_id, prm_util_type
	having (v.cohrt_id='AWV' and prm_util_type='Visits' and sum(o.prm_util)> 0) or 
	(care_setting_cgy_nm='Hospital Inpatient (facility and professional)' and sum(prm_admits) > 0)
)a)b
where b.care_setting_cgy_nm='AWV' and b.hsp_care_setting_cgy_nm='Hospital Inpatient (facility and professional)'
	and (hsp_prm_fromdate - prm_todate) < 31;

-----------------------------------------------------------------------------------------------------------------------------------------------------
--Hospitalization within 30-Days of ASC Indicator (should be calc only for Ambulatory Surgery cases based on Care Servie Category of Ambulatory Service)
-----------------------------------------------------------------------------------------------------------------------------------------------------
create temp table asc_hsp_vst_msr as
select *, 1 hsp_adm_30d_asc_ind, hsp_prm_fromdate - prm_todate as days_to_hsp_from_asc from
(
select member_id, claimid, care_setting_cgy_nm, care_svc_sub_cgy_nm, prm_fromdate, prm_todate, 
		lead(claimid) over (partition by member_id order by prm_fromdate) as ed_claimid,
		lead(care_setting_cgy_nm) over (partition by member_id order by prm_fromdate) as hsp_care_setting_cgy_nm, 
		lead(care_svc_sub_cgy_nm) over (partition by member_id order by prm_fromdate) as hsp_care_svc_sub_cgy_nm,
		lead(prm_fromdate) over (partition by member_id order by prm_fromdate) as hsp_prm_fromdate,
		lead(prm_todate) over (partition by member_id order by prm_fromdate) as hsp_prm_todate
from		
(
select	o.member_id
        , o.claimid
		, care_setting_cgy_nm
		, care_svc_sub_cgy_nm
		, sum(prm_admits) prm_admits
		, sum(o.prm_util) prm_util
		, min(o.prm_fromdate) prm_fromdate
		, max(o.prm_todate) prm_todate
	from pce_qe16_aco_prd_lnd..cv_outclaims o 
	INNER JOIN cst_modl_dim cd ON o.prm_line = cd.cst_modl_line_cd
	group by member_id, claimid, care_setting_cgy_nm, care_svc_sub_cgy_nm
	having (care_setting_cgy_nm='Outpatient (facility and professional)' and care_svc_sub_cgy_nm='Ambulatory Surgery Center' and sum(o.prm_util)> 0) or 
	(care_setting_cgy_nm='Hospital Inpatient (facility and professional)' and sum(prm_admits) > 0)
)a)b
where b.care_setting_cgy_nm='Outpatient (facility and professional)' and b.hsp_care_setting_cgy_nm='Hospital Inpatient (facility and professional)'
	and (hsp_prm_fromdate - prm_todate) < 31;

drop table cv_clm_hdr_msr_fct if exists;
create table cv_clm_hdr_msr_fct as
select 	o.claimid
	, o.member_id
	, ed_vst_30d_asc_ind
	, days_to_ed_from_asc
	, ed_vst_30d_prv_vst_ind
	, days_to_ed_from_prv_vst
	, hsp_adm_30d_asc_ind
	, days_to_hsp_from_asc
	, hsp_adm_30d_prv_vst_ind
	, days_to_hsp_from_prv_vst
  from pce_qe16_aco_prd_lnd..cv_outclaims o 
	left join asc_ed_vst_msr asc1 on o.member_id=asc1.member_id and o.claimid=asc1.claimid
	left join prv_ed_vst_msr prv on o.member_id=prv.member_id and o.claimid=prv.claimid
	left join prv_hsp_vst_msr hprv on o.member_id=hprv.member_id and o.claimid=hprv.claimid
	left join asc_hsp_vst_msr asci on o.member_id=asci.member_id and o.claimid=asci.claimid
 	where linenum='001' and (ed_vst_30d_asc_ind=1 or ed_vst_30d_prv_vst_ind=1 or hsp_adm_30d_prv_vst_ind=1
		or hsp_adm_30d_asc_ind=1);

\unset ON_ERROR_STOP
