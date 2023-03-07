\set ON_ERROR_STOP ON;

insert into benf_anl_fct
(
	mbr_id_num, 
	mbr_full_nm, 
	age_in_yrs, 
	brth_dt, 
	dth_dt, 
	death_ind, 
	tot_bene, 
	elig_sts, 
	elig_mo, 
	bene_mnths, 
	bene_yrs, 
	rtn_ind, 
	atr_ind, 
	mrtly_ind, 
	reactv_ind, 
	paid_amt, 
	thrsld_amt, 
	trunc_paid_amt, 
	pcp_pvdr_nm, 
	rgon, 
	pcp_pvdr_spcly, 
	pcp_grp_nm, 
	indpnd_ind, 
	cms_hcc_scor_num, 
	rpt_prd_end_dt, 
	prev_asgn_benf_ind, 
	last_svc_to_dt, 
	mo_btw_vst, 
	ntw_ind
	
) 

with dt_rng as
(
select  qtr_beg_dt,
        qtr_end_dt,
	add_months(qtr_beg_dt,-9) as rpt_prd_strt_dt,
	qtr_end_dt as rpt_prd_end_dt,
        qtr_beg_dt as mnth_strt,
        qtr_beg_dt + interval '14 days' as elig_month,
        qtr_sk,
	a.max_qtr_sk
        from
	pce_qe16_aco_prd_lnd..qtr_cdr 
	join (select max(qtr_sk) max_qtr_sk from pce_qe16_aco_prd_lnd..qtr_cdr)a on 1=1
	where qtr_sk=:v_cntr
),

--Total Beneficiaries
tot_bene as
(
select distinct member_id as mbr_id_num,
                pd.full_nm as mbr_full_nm,
		months_between(rpt_prd_end_dt,brth_dt)/12 as age_in_yrs,
		pd.brth_dt, 
		pd.dth_dt,
		case when pd.dth_dt < rpt_prd_end_dt then 1
					else null end as death_ind,
                1 tot_bene,
                elig_status_1 as elig_sts,
		rpt_prd_strt_dt,
		rpt_prd_end_dt,
		qtr_sk as curr_cntr,
		max_qtr_sk as max_cntr,
		cv_member_time_windows.elig_month as elig_mo,
		paid_amt as trnc_thrsld
        from pce_qe16_aco_prd_lnd..cv_member_time_windows
                left join pln_mbr_dim pd on member_id=mbr_id_num
                join dt_rng using (elig_month)
		left join pce_qe16_aco_prd_lnd..xpdtr_trnc_thrsld on elig_status_1=elig_sts and elig_month between eff_fm_dt and eff_to_dt
                where assignment_indicator='Y'
),

--Member Eligibility Months
mem_elig_months as
(
select member_id as mbr_id_num, sum(memmos_medical) as bene_mnths, sum(memmos_medical)/12 as bene_yrs from
        pce_qe16_aco_prd_lnd..cv_member_time_windows mtw
        join tot_bene on member_id=mbr_id_num
		join dt_rng on 1=1
        where mtw.elig_month between add_months(dt_rng.elig_month,-9) and add_months(dt_rng.elig_month,2)
        group by member_id
),

--Eligibility_Status, Total Paid Amount & Truncate Paid Amount
bene_ttl_paid_amt as
(
select cf.mbr_id_num,
           tot_bene.elig_sts,
           rpt_prd_strt_dt,
           rpt_prd_end_dt ,
           sum(cf.paid_amt) as paid_amt,
           trnc_thrsld as thrsld_amt,
	   case when sum(cf.paid_amt) > trnc_thrsld
                        then trnc_thrsld else sum(cf.paid_amt) end as trunc_paid_amt
from tot_bene
        left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
        where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
group by cf.mbr_id_num, tot_bene.elig_sts, thrsld_amt, rpt_prd_strt_dt, rpt_prd_end_dt
),


rtn_dtl as
(
select mbr_id_num, elig_mo, curr_cntr, max_cntr, rtn_month, dth_dt, 
	case when curr_cntr=max_cntr then null
		 when (curr_cntr<>max_cntr and b.mrtly_ind=1) then 1
		 else null end as mrtly_ind,
	case when curr_cntr=max_cntr then null 
		 when (curr_cntr<>max_cntr and b.rtn_ind is null and c.reactv_ind=1) then 1 
		 else null end as reactv_ind,
	case when curr_cntr=max_cntr then null
		 when (curr_cntr<>max_cntr and b.mrtly_ind=1) then null
		 when (curr_cntr<>max_cntr and b.mrtly_ind<>1 and b.rtn_ind=1 and c.reactv_ind=1) then 1
		 else rtn_ind end as rtn_ind,
	case when curr_cntr=max_cntr then null
		 when (curr_cntr<>max_cntr and b.rtn_ind is null and c.reactv_ind is null and dth_dt is null) then 1
		 else null end as atr_ind
	from tot_bene
left join

(
select distinct mtw.member_id, mtw.elig_month as rtn_month, 1 rtn_ind,
	case when m.death_date between add_months(dt_rng.mnth_strt,3) and add_months(qtr_end_dt,3) then 1 
		else null end as mrtly_ind
	from pce_qe16_aco_prd_lnd..cv_member_time_windows mtw 
	left join pce_qe16_aco_prd_lnd..cv_members m using (member_id)
	join dt_rng
on mtw.elig_month=add_months(dt_rng.elig_month,3)
where mtw.assignment_indicator='Y' 
)b
on tot_bene.mbr_id_num=b.member_id

left join

(select distinct mtw.member_id, mtw.elig_month as reactv_month, 1 reactv_ind from pce_qe16_aco_prd_lnd..cv_member_time_windows mtw join dt_rng
on mtw.elig_month=add_months(dt_rng.elig_month,6)
where assignment_indicator='Y')c
on tot_bene.mbr_id_num=c.member_id
),


--PCP Beneficiary Assignment
pcp_info as
(
select distinct m.mbr_id_num,
                mpd.rgon,
                upper(mpd.last_nm)||', '||upper(mpd.frst_nm) as pcp_pvdr_nm,
                prim_spcly_nm as pcp_pvdr_spcly,
                mpd.grp as pcp_grp_nm,
                mpd.indpnd_ind
       from hist_bene_pcp_attr m
                left join mcl_pvdr_dim mpd on m.bill_pvdr_sk=mpd.pvdr_sk
                left join pvdr_dim pd on mpd.npi=pd.npi
),

--ccn dim
ccn as
(
(SELECT *
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
                     ) OR fcy_type_descr != 'Dialysis Facility')
),

clm_rcrd as
(
select * from
(
select distinct mbr_id_num, svc_to_dt,ccn_alt_sk, bill_pvdr_sk,RANK()
           OVER (
             PARTITION BY mbr_id_num
             ORDER BY svc_to_dt DESC) AS rnk from clm_line_fct
)a
where a.rnk=1
),

--Claim line records with Network Indicator
clm as
(
select * from (
select distinct a.mbr_id_num, a.svc_to_dt, a.ntw_ind, rank() over (partition by a.mbr_id_num order by a.svc_to_dt, a.ntw_ind desc ) as rnk from
(select cf.mbr_id_num, cf.svc_to_dt,
            CASE WHEN (ccn.ccn_id IN
               ('230227', '230207', '230193', '230141', '230167', '230041', '232020', '230080', '230105', '230216',
                '230297', 'HB1436', '233842', '233820', '233821', '233928', '238902', '902067', '237172', '237010',
                '237165', '237008', '237036', '231521', '23S167', '23S193', '23S141', '23S105', '23S216',
                '23T167', '23T141', '23T105', '235481', '235577', '235526', '235369', '23S207', '23T207', '231329')
                                OR (mp.npi IS NOT NULL AND (mp.end_mn IS NULL OR mp.end_mn > svc_to_dt))
                                )
                                then 1
                                ELSE 0 END as ntw_ind
        from clm_line_fct cf
        inner join clm_rcrd using (mbr_id_num,svc_to_dt)
        left join ccn on cf.ccn_alt_sk=ccn.ccn_sk
        left join mcl_pvdr_dim mp on cf.bill_pvdr_sk=mp.pvdr_sk)a)b where b.rnk=1
),

prev_asgn_ind as
(
select a.member_id, a.elig_month, coalesce(b.prev_asgn_benf_ind,0) as prev_asgn_benf_ind from
(
select distinct member_id, elig_month from pce_qe16_aco_prd_lnd..cv_member_time_windows join dt_rng using (elig_month)
where assignment_indicator='Y')a
left join
(
select distinct member_id, 1 prev_asgn_benf_ind from pce_qe16_aco_prd_lnd..cv_member_time_windows mtw join dt_rng b on 1=1
where assignment_indicator='Y' and mtw.elig_month < b.elig_month
)b on a.member_id=b.member_id
)

select distinct mbr.mbr_id_num,
                mbr.mbr_full_nm,
		mbr.age_in_yrs,
		mbr.brth_dt,
		mbr.dth_dt,
		mbr.death_ind,
		mbr.tot_bene,
		mbr.elig_sts,
		mbr.elig_mo,
		mem.bene_mnths,
		mem.bene_yrs,
		rd.rtn_ind,
		rd.atr_ind,
		rd.mrtly_ind, 
		rd.reactv_ind, 
		tpa.paid_amt,
                tpa.thrsld_amt,
                tpa.trunc_paid_amt,
		pcp.pcp_pvdr_nm,
                pcp.rgon,
                pcp.pcp_pvdr_spcly,
                pcp.pcp_grp_nm,
                pcp.indpnd_ind,
		hcc.cms_hcc_scor_num,
		mbr.rpt_prd_end_dt,
		pab.prev_asgn_benf_ind,
		crcd.svc_to_dt as last_svc_to_dt,
		months_between(mbr.rpt_prd_end_dt,crcd.svc_to_dt) as mo_btw_vst,
		ntw_ind
				
	from tot_bene mbr
		left join mem_elig_months mem on mbr.mbr_id_num=mem.mbr_id_num
		left join bene_ttl_paid_amt tpa on mbr.mbr_id_num=tpa.mbr_id_num
		left join pcp_info pcp on mbr.mbr_id_num=pcp.mbr_id_num
		left join prev_asgn_ind pab on mbr.mbr_id_num=pab.member_id
		left join rtn_dtl rd on mbr.mbr_id_num=rd.mbr_id_num
		left join clm_rcrd crcd on mbr.mbr_id_num=crcd.mbr_id_num
		left join clm on mbr.mbr_id_num=clm.mbr_id_num and clm.svc_to_dt <= mbr.rpt_prd_end_dt
		left join pce_qe16_aco_prd_lnd..hist_bene_hcc_scr hcc on mbr.mbr_id_num=hcc.mbr_id_num and mbr.elig_mo=hcc.elig_month;

\unset ON_ERROR_STOP
