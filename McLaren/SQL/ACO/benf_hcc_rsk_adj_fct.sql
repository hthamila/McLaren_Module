\set ON_ERROR_STOP ON;

truncate benf_hcc_rsk_adj_fct;
insert into benf_hcc_rsk_adj_fct
(
	 mbr_id_num
       , mbr_full_nm
       , cms_hcc_scor_num
       , rpt_prd_strt_dt
       , rpt_prd_end_dt
       , pcp_pvdr_nm
       , pcp_pvdr_rgon
       , pcp_pvdr_spcly
       , pcp_grp_nm
       , pcp_indpnd_ind
       , bene_mnths
       , bene_yrs
       , cms_hcc_id
       , raf_scr
       , cms_hcc_descr
       , paid_amt
       , trunc_paid_amt
       , ntw_tot_paid_amt
       , oon_paid_amt
)

--Date Parameters
with dt_rng as
(
select  add_months(a.rpt_prd_strt_dt,-12) as multi_yr_strt_dt,
                a.rpt_prd_strt_dt,
        b.rpt_prd_end_dt,
        mnth_strt,
        mnth_strt + interval '14 days' as elig_month,
        TO_CHAR(rpt_prd_end_dt, 'YYYY MON') as mnth_yr_nm

        from
                (SELECT add_months(val,0) as rpt_prd_strt_dt
                        FROM dt_meta WHERE descr = 'roll_yr_strt') a join
                (SELECT  date_trunc('month',(add_months(val,0))) as mnth_strt, add_months(val,0) as rpt_prd_end_dt
                        FROM dt_meta WHERE descr = 'roll_yr_end') b on 1=1
),

--ccn dim
ccn as
(
SELECT *
        FROM ccn_dim
          WHERE ccn_id NOT IN (
              SELECT ccn_id  FROM ccn_dim
                 WHERE ccn_id IN ( SELECT ccn_id
                         FROM ccn_dim
                         GROUP BY 1
                         HAVING COUNT(*) > 1
                      		 ) AND fcy_type_descr != 'Dialysis Facility'
                    	      ) OR fcy_type_descr != 'Dialysis Facility'
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
                cv_member_time_windows.elig_month as elig_mo,
                pd.cms_hcc_scor_num,
                rpt_prd_strt_dt,
                rpt_prd_end_dt,
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
        join dt_rng on 1=1
        join tot_bene on member_id=mbr_id_num
        where mtw.elig_month between add_months(dt_rng.elig_month,-11) and dt_rng.elig_month
        group by member_id
),

--Eligibility_Status, Total Paid Amount & Truncate Paid Amount
bene_ttl_paid_amt as
(
select cf.mbr_id_num,
           tot_bene.elig_sts,
           sum(cf.paid_amt) as paid_amt,
           trnc_thrsld as thrsld_amt,
           case when sum(cf.paid_amt) > trnc_thrsld
                        then trnc_thrsld else sum(cf.paid_amt) end as trunc_paid_amt

from tot_bene
        left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
        where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
group by cf.mbr_id_num, tot_bene.elig_sts, trnc_thrsld
),

--Network Total Paid Amount
ntw_tot_paid_amt as
(
 select tot_bene.mbr_id_num, sum(cf.paid_amt) as ntw_tot_paid_amt
	 from tot_bene left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
  		left join ccn ON ccn.ccn_sk = cf.ccn_alt_sk
  		left join in_ntw_pvdr_dim bill_inpd ON cf.bill_pvdr_sk = bill_inpd.in_ntw_pvdr_sk
  	   where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
  		and (ccn.ccn_id IN
 		('230227', '230207', '230193', '230141', '230167', '230041', '232020', '230080', '230105', '230216',
   		'230297', 'HB1436', '233842', '233820', '233821', '233928', '238902', '902067', '237172', '237010',
   		'237165', '237008', '237036', '231521', '23S167', '23S193', '23S141', '23S105', '23S216',
   		'23T167', '23T141', '23T105', '235481', '235577', '235526', '235369', '23S207', '23T207', '231329')
  		 OR (bill_inpd.npi IS NOT NULL AND (bill_inpd.end_mn IS NULL OR bill_inpd.end_mn > cf.svc_to_dt)))
  	group by tot_bene.mbr_id_num
),

--Combine the data
bene_hcc_dtl as
(
select * from 
(select *, row_number() over (partition by member_id, cms_hcc_id order by prm_todate desc) as rnk from 
(select o.claimid,o.member_id,o.prm_fromdate,o.prm_todate,o.icd_code,
                rscr.cms_hcc_id,  rscr.dgns_cd, rscr.raf_score, rscr.cms_hcc_description, rscr.dgns_description, rpt_prd_end_dt
from tot_bene t
	left join pce_qe16_aco_prd_lnd..outclaims_diag o on t.mbr_id_num=o.member_id
	left join dgns_hcc_rsk_scr_dim rscr on o.icd_code=rscr.dgns_cd 
	where o.prm_todate between rpt_prd_strt_dt and rpt_prd_end_dt
and o.code_type='D' and rscr.cms_hcc_id is not null)a)b where rnk=1
)

select 	mbr.mbr_id_num, 
	mbr.mbr_full_nm, 
	mbr.cms_hcc_scor_num,
        mbr.rpt_prd_strt_dt,
        mbr.rpt_prd_end_dt,
	upper(mpd.last_nm)||', '||upper(mpd.frst_nm) as pcp_pvdr_nm,
        mpd.rgon as pcp_pvdr_rgon,
        t.prim_spcly_nm as pcp_pvdr_spcly,
        mpd.grp as pcp_grp_nm,
        mpd.indpnd_ind as pcp_indpnd_ind,
	mem.bene_mnths,
	mem.bene_yrs,
	bhd.cms_hcc_id,
	bhd.raf_score raf_scr,
	bhd.cms_hcc_description cms_hcc_descr,
        tpa.paid_amt,
	tpa.trunc_paid_amt,
	ntw.ntw_tot_paid_amt,
	(tpa.paid_amt - coalesce(ntw.ntw_tot_paid_amt,0)) as oon_paid_amt
		
	from tot_bene mbr
		inner join bene_hcc_dtl bhd on mbr.mbr_id_num=bhd.member_id
		left join mem_elig_months mem on mbr.mbr_id_num=mem.mbr_id_num
		left join bene_ttl_paid_amt tpa on mbr.mbr_id_num=tpa.mbr_id_num
		left join ntw_tot_paid_amt ntw on mbr.mbr_id_num=ntw.mbr_id_num
		left join bene_pcp_attr pcp on mbr.mbr_id_num=pcp.mbr_id_num
		left join mcl_pvdr_dim mpd on pcp.npi=mpd.npi
		left join pvdr_dim t on mpd.npi=t.npi

\unset ON_ERROR_STOP
