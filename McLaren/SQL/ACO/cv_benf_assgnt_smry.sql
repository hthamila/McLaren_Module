\set ON_ERROR_STOP ON;

---------------------------------------------------------------
--Beneficiary Summary table creation for each reporting month--
---------------------------------------------------------------
insert into cv_benf_assgnt_smry
(
	 member_id
	, frst_nm
        , last_nm
        , full_nm
        , gnd_cd
       	, dob
       	, death_date
        , ste_cd
        , zip_cd
       	, cm_status
       	, age_in_yrs
       	, elig_sts
       	, rpt_mnth
       	, rpt_prd_strt_dt
       	, rpt_prd_end_dt
       	, trnc_thrsld
       	, cms_hcc_scor_num
	, new_benf_flg
       	, bene_mnths
       	, bene_yrs
        , bene_agnd_mnths
        , bene_agdu_mnths
        , bene_esrd_mnths
        , bene_dsbl_mnths
       	, assignment_indicator
        , attr_to
        , pcp_rgon
        , pcp_grp_nm
        , pcp_pvdr_nm
        , pcp_pvdr_spcly
        , pcp_npi
        , indpnd_ind
        , pcp_cpc_pls_ind
	, hcc_cnt
	, chrnc_cdtn_cnt
	, oth_chrnc_cdtn_cnt
	, ovl_hcc_cnt
	, ovl_raf_scr
	, raf_gap_scr

)

with dt_rng as
(

select  add_months(add_months(val,-:v_cntr),-12)+1 as rpt_prd_strt_dt,
        add_months(val,-:v_cntr) as rpt_prd_end_dt,
        date_trunc('month',(add_months(val,-:v_cntr))) as rpt_mnth,
        date_trunc('month',(add_months(val,-:v_cntr))) + interval '14 days' as elig_month,
        TO_CHAR(rpt_prd_end_dt, 'YYYY MON') as mnth_yr_nm
        from
pce_qe16_aco_prd_dm..dt_meta WHERE descr = 'paid_date_mnth'

),

prior_mnth_mbr as
(
select member_id, mtw.elig_month as prior_elig_month, 0 as benf_flg 
from pce_qe16_aco_prd_lnd..cv_member_time_windows mtw
  join (select add_months(max(qtr_end_dt),-4)+interval '15 days' elig_month from pce_qe16_aco_prd_lnd..qtr_cdr)d using (elig_month)
where mtw.assignment_indicator='Y'
),

ovl_hcc_cnt as
(
select mbr_id_num, count(cms_hcc_id) ovl_hcc_cnt, sum(raf_scr) ovl_raf_scr 
	from hist_benf_hcc_anl_fct b
	inner join pce_qe16_aco_prd_lnd..cv_member_time_windows mtw on b.mbr_id_num=mtw.member_id
	inner join dt_rng dt using (elig_month)
	where svc_to_dt <=dt.rpt_prd_end_dt and mtw.assignment_indicator='Y'
	group by mbr_id_num
),

lst_hcc_cnt as
(
select mbr_id_num, count(cms_hcc_id) lst_hcc_cnt, sum(raf_scr) lst_raf_scr 
	from hist_benf_hcc_anl_fct b
	inner join pce_qe16_aco_prd_lnd..cv_member_time_windows mtw on b.mbr_id_num=mtw.member_id
	inner join dt_rng dt using (elig_month)
	where mtw.assignment_indicator='Y' and svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt 
	group by mbr_id_num
),


tot_bene as
(
select distinct member_id,
                TRIM(GET_VALUE_VARCHAR(ARRAY_SPLIT(mem_name, ','),2)) AS frst_nm,
		TRIM(GET_VALUE_VARCHAR(ARRAY_SPLIT(mem_name, ','),1)) AS last_nm,
		mem_name as full_nm,
		gender as gnd_cd, 
                pd.dob, 
                death_date, 
		mem_state AS ste_cd,
		mem_zip5 AS zip_cd,
                cm.cm_status, 
		months_between(rpt_prd_end_dt,pd.dob)/12 as age_in_yrs,
                elig_status_1 as elig_sts,
                rpt_mnth,
                rpt_prd_strt_dt,
                rpt_prd_end_dt,
                paid_amt as trnc_thrsld,
		cms_hcc_scor_num,
		coalesce(benf_flg,1) as new_benf_flg
        from pce_qe16_aco_prd_lnd..cv_member_time_windows mtw
        	 left join pce_qe16_aco_prd_lnd..cv_members pd using (member_id)
		 left join prior_mnth_mbr using (member_id)
		 left join pce_qe16_aco_prd_dm..care_managed cm using (member_id)
		      join pce_qe16_aco_prd_dm..dt_rng using (elig_month)
		 left join pce_qe16_aco_prd_lnd..hist_bene_hcc_scr h on mtw.member_id=h.mbr_id_num and dt_rng.elig_month=h.elig_month
 	         left join pce_qe16_aco_prd_lnd..xpdtr_trnc_thrsld on elig_status_1=elig_sts and dt_rng.elig_month between eff_fm_dt and eff_to_dt
		 
           	where mtw.assignment_indicator='Y'

),


mem_elig_months as
(
--select mbr_id_num, sum(bene_mnths) as bene_mnths, sum(bene_yrs) as bene_yrs, max(agnd_mnths) bene_agnd_mnths, 
--	max(agdu_mnths) bene_agdu_mnths, max(esrd_mnths) bene_esrd_mnths, max(dsbl_mnths) bene_dsbl_mnths
--	from
--	(
--	select mbr_id_num, mtw.elig_sts, sum(mdcl_mo_cnt) as bene_mnths, sum(mdcl_mo_cnt)/12 as bene_yrs,
--	 case when mtw.elig_sts='Aged Non-Dual' then sum(mdcl_mo_cnt) end as agnd_mnths,
--	 case when mtw.elig_sts='Aged Dual' then sum(mdcl_mo_cnt) end as agdu_mnths,
--	 case when mtw.elig_sts='ESRD' then sum(mdcl_mo_cnt) end as esrd_mnths,
--	 case when mtw.elig_sts='Disabled' then sum(mdcl_mo_cnt) end as dsbl_mnths
--	from pce_qe16_aco_prd_dm..pln_mbr_asgnt_dim mtw
--       	join tot_bene t on mtw.mbr_id_num=t.member_id
--          where asgnt_wndw_strt_dt >= rpt_prd_strt_dt and asgnt_wndw_end_dt<=rpt_prd_end_dt and mtw.elig_sts !='Not Eligible'
--          group by mbr_id_num,mtw.elig_sts
--	)a group by mbr_id_num

select member_id, sum(bene_mnths) as bene_mnths, sum(bene_yrs) as bene_yrs, max(agnd_mnths) bene_agnd_mnths,
        max(agdu_mnths) bene_agdu_mnths, max(esrd_mnths) bene_esrd_mnths, max(dsbl_mnths) bene_dsbl_mnths
        from
        (
        select mtw.member_id, mtw.elig_status_1, sum(memmos_medical) as bene_mnths, sum(memmos_medical)/12 as bene_yrs,
         case when mtw.elig_status_1='Aged Non-Dual' then sum(memmos_medical) end as agnd_mnths,
         case when mtw.elig_status_1='Aged Dual' then sum(memmos_medical) end as agdu_mnths,
         case when mtw.elig_status_1='ESRD' then sum(memmos_medical) end as esrd_mnths,
         case when mtw.elig_status_1='Disabled' then sum(memmos_medical) end as dsbl_mnths
        from pce_qe16_aco_prd_lnd..cv_member_time_windows mtw
                join tot_bene t on mtw.member_id=t.member_id
          where date_start >= rpt_prd_strt_dt and date_end <=rpt_prd_end_dt
          group by mtw.member_id,mtw.elig_status_1
        )a group by member_id
),

pcp_info as
(
select  m.mbr_id_num,
	mpd.rgon as pcp_rgon,
	mpd.npi as pcp_npi,
        upper(mpd.last_nm)||', '||upper(mpd.frst_nm) as pcp_pvdr_nm,
        prim_spcly_nm as pcp_pvdr_spcly,
        mpd.grp as pcp_grp_nm,
        mpd.indpnd_ind,
        m.attr as attr_to,
	cpc.cpc as pcp_cpc_pls_ind
    from pce_qe16_aco_prd_dm..hist_bene_pcp_attr m
         left join pce_qe16_aco_prd_dm..mcl_pvdr_dim mpd on m.bill_pvdr_sk=mpd.pvdr_sk
	 left join pce_qe16_aco_prd_dm..cpc_plus cpc on mpd.npi=cpc.npi
         left join pce_qe16_aco_prd_dm..pvdr_dim pd on mpd.npi=pd.npi
),

chrnc_cdtn as
(
select mbr_id_num, ccw_type, count(1) as chrnc_cdtn_cnt
from stg_ccw_fct
	join dt_rng on 1=1
where ccw_type='Chronic Condition' and
last_coded_dt between rpt_prd_strt_dt and rpt_prd_end_dt
group by mbr_id_num, ccw_type
),

oth_chrnc_cdtn as
(
select mbr_id_num, ccw_type, count(1) as oth_chrnc_cdtn_cnt
from stg_ccw_fct
	join dt_rng on 1=1
where ccw_type='Other Chronic/Potentially Disabling Condition' and
last_coded_dt between rpt_prd_strt_dt and rpt_prd_end_dt
group by mbr_id_num, ccw_type
),

benf_hcc_cnt as
(
select o.mbr_id_num, ovl_hcc_cnt, lst_hcc_cnt, ovl_raf_scr, 
	case when (ovl_raf_scr-lst_raf_scr) is null then ovl_raf_scr
		else (ovl_raf_scr-lst_raf_scr) end as raf_gap_scr
from ovl_hcc_cnt o
	left join lst_hcc_cnt c using (mbr_id_num)
)

select
	  t.member_id
	, frst_nm
	, last_nm
	, full_nm
	, gnd_cd
       	, dob
       	, death_date
	, ste_cd
	, zip_cd
       	, cm_status
       	, age_in_yrs
       	, t.elig_sts
       	, rpt_mnth
       	, rpt_prd_strt_dt
       	, rpt_prd_end_dt
       	, trnc_thrsld
       	, cms_hcc_scor_num
	, new_benf_flg
       	, me.bene_mnths
       	, me.bene_yrs
	, bene_agnd_mnths
	, bene_agdu_mnths
	, bene_esrd_mnths
	, bene_dsbl_mnths
       	,'Y' assignment_indicator
	, attr_to
	, pcp_rgon
	, pcp_grp_nm
	, pcp_pvdr_nm
	, pcp_pvdr_spcly
	, pcp_npi
	, indpnd_ind
	, pcp_cpc_pls_ind
	, coalesce(lst_hcc_cnt,0) as hcc_cnt
	, chrnc_cdtn_cnt
	, oth_chrnc_cdtn_cnt
	, ovl_hcc_cnt
	, ovl_raf_scr
	, raf_gap_scr
from tot_bene t
	join mem_elig_months me on t.member_id=me.member_id
	left join pcp_info pcp on t.member_id=pcp.mbr_id_num
	left join chrnc_cdtn cc on t.member_id=cc.mbr_id_num
	left join oth_chrnc_cdtn ot on t.member_id=ot.mbr_id_num
	left join benf_hcc_cnt bh on t.member_id=bh.mbr_id_num
;
--------------------------------------------
--Orphan Dummy Inserts for non-claims data--
--------------------------------------------
insert into cv_orphan_outclaims
(
member_id, prm_fromdate, prm_todate
)

with dt_rng as
(
select  add_months(add_months(val,-:v_cntr),-12)+1 as rpt_prd_strt_dt,
        add_months(val,-:v_cntr) as rpt_prd_end_dt,
        date_trunc('month',(add_months(val,-:v_cntr))) as rpt_mnth,
        date_trunc('month',(add_months(val,-:v_cntr))) + interval '14 days' as elig_month,
        TO_CHAR(rpt_prd_end_dt, 'YYYY MON') as mnth_yr_nm
        from
pce_qe16_aco_prd_dm..dt_meta WHERE descr = 'paid_date_mnth'
),

--mndtry_attr as
--(
--select distinct rcrd_load_type, rcrd_isrt_pcs_nm, rcrd_isrt_ts::date rcrd_isrt_ts, rcrd_src_file_nm, rcrd_btch_audt_id, rcrd_pce_cst_nm, rcrd_pce_cst_src_nm
--	from pce_qe16_aco_prd_lnd..cv_outclaims
--),

tot_bene as
(
select distinct member_id,
                rpt_mnth,
                rpt_prd_strt_dt,
                rpt_prd_end_dt
        from pce_qe16_aco_prd_lnd..cv_member_time_windows mtw
                 left join pce_qe16_aco_prd_lnd..cv_members pd using (member_id)
                 join pce_qe16_aco_prd_dm..dt_rng using (elig_month)
                where mtw.assignment_indicator='Y'
),

mbr_clm as
(
select distinct c.member_id, 'X' excl_ind
		from pce_qe16_aco_prd_lnd..cv_outclaims c 
		join pce_qe16_aco_prd_dm..dt_rng on 1=1
		where prm_todate between rpt_prd_strt_dt and rpt_prd_end_dt
)

select --rcrd_load_type, rcrd_isrt_pcs_nm, rcrd_isrt_ts, rcrd_src_file_nm, rcrd_btch_audt_id, rcrd_pce_cst_nm, rcrd_pce_cst_src_nm, 
t.member_id, t.rpt_prd_strt_dt prm_fromdate, t.rpt_prd_end_dt prm_todate from tot_bene t
--	join mndtry_attr on 1=1
	left join mbr_clm m using (member_id)
	where excl_ind is null;

\unset ON_ERROR_STOP
