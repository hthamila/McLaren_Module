\set ON_ERROR_STOP ON;

insert into hist_bene_clm_aggr_fct
--insert into hist_bene_aggr_fct
(
                rpt_prd_strt_dt,
                rpt_prd_end_dt,
                mnth_yr_nm,
                mbr_id_num,
		mbr_name,
		brth_dt,
		dth_dt,
		care_mgn_sts_ind,
		attr_to,
                rgon,
                pcp_pvdr_nm,
                pcp_pvdr_spcly,
                pcp_grp_nm,
                indpnd_ind,
                cms_hcc_scor_num,
                bene_mnths,
                bene_yrs,
                age_in_yrs,
                tot_bene,
                elig_sts,
                paid_amt,
                thrsld_amt,
                trunc_paid_amt,
                ip_paid_amt,
                ip_dschrg,
                snf_paid_amt,
                snf_dschrg,
                hha_paid_amt,
                hspc_paid_amt,
                ed_paid_amt,
                ed_vst_ind,
                op_paid_amt,
                hspc_dschrg,
                hspc_los,
                ip_los,
                snf_los,
                ct_scn_ind,
                mri_ind,
                prim_care_svc_ind,
		spcly_prim_care_svc_ind,
		ntw_tot_paid_amt,
		betos_cnslt_ind,
		fllw_vst_7d_ind, 		
		fllw_vst_14d_ind, 		
		eol_7d_paid_amt ,		
		eol_14d_paid_amt, 		
		hha_case_cnt, 			
		hha_vst_cnt, 			
		avoidable_ed_ind, 		
		avg_ip_cmi, 			
		oon_ip_spend, 			
		oon_op_spend, 			
		oon_snf_spend, 		
		oon_prof_oth_spend, 	
		oon_hha_spend, 		
		oon_hspc_spend, 		
		oon_oth_spend, 		
		asthma_adm_ind, 		
		bacterial_pneu_adm_ind,
		copd_adm_ind, 			
		dehydration_adm_ind, 	
		DLTC_adm_ind, 			
		DSTC_adm_ind, 			
		ud_adm_ind, 			
		hf_adm_ind, 			
		htn_adm_ind,  			
		LEADP_ADM_IND, 		
		PAAR_ADM_IND,			
		uti_adm_ind,
		hspc_case_cnt,
		ip_dschrg_cnt,
	        ovrl_cmpos_ind,
                acute_cmpos_ind,
                chronic_cmpos_ind,
                dibts_cmpos_ind,
                snf_same_day_ed_vst_ind,
                snf_readm_ind,
                ip_readm_ind,
		algn_ntw_tot_paid_amt
			
)


--Rolling 12 month parameter information for each month
with dt_rng as
(
select  a.rpt_prd_strt_dt,
        b.rpt_prd_end_dt,
        mnth_strt,
        mnth_strt + interval '14 days' as elig_month,
        TO_CHAR(rpt_prd_end_dt, 'YYYY MON') as mnth_yr_nm
        from
                (SELECT add_months(val,-:v_cntr) as rpt_prd_strt_dt
                        FROM dt_meta WHERE descr = 'roll_yr_strt') a join
                (SELECT  date_trunc('month',(add_months(val,-:v_cntr))) as mnth_strt, add_months(val,-:v_cntr) as rpt_prd_end_dt
                        FROM dt_meta WHERE descr = 'roll_yr_end') b on 1=1
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

--Total Number of Beneficiaries Assigned & Eligibility Status (at the end of the month)
tot_bene as
(
select distinct member_id as mbr_id_num,
                pd.full_nm as mbr_name,
		pd.brth_dt,
		pd.dth_dt, 
		pd.care_mgn_sts_ind, 
                months_between(rpt_prd_end_dt,brth_dt)/12 as age_in_yrs,
                1 tot_bene,
                elig_status_1 as elig_sts,
                mnth_yr_nm,
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
--select member_id as mbr_id_num, sum(memmos_medical) as bene_mnths, sum(memmos_medical)/12 as bene_yrs from
  --      pce_qe16_aco_prd_lnd..cv_member_time_windows mtw
    --    join dt_rng on 1=1
     --   join tot_bene on member_id=mbr_id_num
     --   where mtw.elig_month between add_months(dt_rng.elig_month,-11) and dt_rng.elig_month and assignment_indicator='Y'
     --   group by member_id
select mbr_id_num, sum(mdcl_mo_cnt) as bene_mnths, sum(mdcl_mo_cnt)/12 as bene_yrs from
        pce_qe16_aco_prd_dm..pln_mbr_asgnt_dim mtw
        join tot_bene t using (mbr_id_num)
        where asgnt_wndw_strt_dt >= rpt_prd_strt_dt and asgnt_wndw_end_dt<=rpt_prd_end_dt and mtw.elig_sts !='Not Eligible'
        group by mbr_id_num
),

--CMS Score by Member
hist_bene_hcc_scr as
(
select mbr_id_num, cms_hcc_scor_num, elig_month
        from pce_qe16_aco_prd_lnd..hist_bene_hcc_scr
             join dt_rng using (elig_month)
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

--Paid Amount by Care Setting
cs_paid_amt as
(
select tot_bene.mbr_id_num,
	sum(case when cst_modl_line_cd in ('I11a','I11b','I11c','I12','I13a','I13b') then paid_amt end) as ip_paid_amt, 
	sum(case when cst_modl_line_cd in ('I11a','I11b','I11c','I12','I13a','I13b') then cst_modl_in_ptnt_clm_adm_ind end) as ip_dschrg,
	sum(case when cst_modl_line_cd in ('I31') then paid_amt end) as snf_paid_amt, 
	sum(case when cst_modl_line_cd in ('P82a') then paid_amt end) as hha_paid_amt,
	sum(case when cst_modl_line_cd in ('P82b') then paid_amt end) as hspc_paid_amt,
	sum(case when cst_modl_line_cd in ('P51b','O11','O41l') then paid_amt end) as ed_paid_amt,
	sum(case when cst_modl_line_cd in  ('P51b') then cst_modl_utlz_cnt end) as ed_vst_ind
from tot_bene
         left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
	where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
group by tot_bene.mbr_id_num
),

--Outpatient Spend
op_paid_amt as
(
select tot_bene.mbr_id_num,rpt_prd_strt_dt, rpt_prd_end_dt,sum(paid_amt) as op_paid_amt
from tot_bene
        left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
        left join cst_modl_dim cd on cf.cst_modl_line_cd=cd.cst_modl_line_cd
where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
           and care_setting_cgy_nm='Outpatient (facility and professional)'
group by (tot_bene.mbr_id_num, rpt_prd_strt_dt, rpt_prd_end_dt)
),

--Hospice Discharges
hspc_dschrg as
(
select mbr_id_num, count(hspc_dschrg) as hspc_dschrg from
(select mbr_id_num, ccn_id, svc_fm_dt,
        case when
                (svc_fm_dt = date_trunc('month',svc_fm_dt) and
                day_of_svc_cnt <> days_between(date_trunc('month',svc_fm_dt) , last_day(svc_fm_dt)+ 1))
                        or (svc_fm_dt = dth_dt) then 'Y' else 'N' end as hspc_dschrg,
                        day_of_svc_cnt
from
        (select tot_bene.mbr_id_num,
                CASE WHEN cf.fcy_unit_cd IS NOT NULL
                THEN SUBSTR(ccn.ccn_id, 1, 2) || cf.fcy_unit_cd || SUBSTR(ccn.ccn_id, 4, 3)
                        ELSE ccn.ccn_id END AS ccn_id,
                                tot_bene.dth_dt, min(svc_fm_dt) as svc_fm_dt, max(svc_to_dt) svc_to_dt, TO_CHAR(svc_to_dt, 'YYYYMM'), day_of_svc_cnt
                        from tot_bene
                                                left join pln_mbr_dim pd on tot_bene.mbr_id_num=pd.mbr_id_num
                                                left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
                                                left join ccn on ccn.ccn_sk=cf.ccn_alt_sk
                where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
                        and cst_modl_line_cd = 'P82b'
                group by tot_bene.mbr_id_num, ccn_id,fcy_unit_cd, tot_bene.dth_dt, day_of_svc_cnt, TO_CHAR(svc_to_dt, 'YYYYMM')
        )a
)b where hspc_dschrg='Y' group by mbr_id_num, hspc_dschrg
),

--Hospice Length of Stay
hspc_los as
(
select tot_bene.mbr_id_num, sum(hspc_vst_ind) as hspc_case_cnt, sum(los) as hspc_los 
from tot_bene
	left join hospice_adm_vw h on tot_bene.mbr_id_num=h.mbr_id_num
	where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
     group by tot_bene.mbr_id_num
),

--CT Scan Indicator
ct_scn_ind as
(
select tot_bene.mbr_id_num,rpt_prd_strt_dt, rpt_prd_end_dt,sum(cst_modl_utlz_cnt) as ct_scn_ind
from tot_bene
        left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
        left join cst_modl_dim cd on cf.cst_modl_line_cd=cd.cst_modl_line_cd
where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
           and rsk_pool_nm='Part B'
           and care_setting_sub_cgy_nm in ('Professional - Facility Outpatient','Professional - Office/Other')
           and cf.cst_modl_line_cd in ('P55b','P59a','P57a','P59d')
group by (tot_bene.mbr_id_num, rpt_prd_strt_dt, rpt_prd_end_dt)
),

--MRI Event Indicator
mri_ind as
(
select tot_bene.mbr_id_num,rpt_prd_strt_dt, rpt_prd_end_dt,sum(cst_modl_utlz_cnt) as mri_ind
from tot_bene
        left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
        left join cst_modl_dim cd on cf.cst_modl_line_cd=cd.cst_modl_line_cd
where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
           and rsk_pool_nm='Part B'
           and care_setting_sub_cgy_nm in ('Professional - Facility Outpatient','Professional - Office/Other')
           and cf.cst_modl_line_cd in ('P55c','P59b','P57b','P59e')
group by (tot_bene.mbr_id_num, rpt_prd_strt_dt, rpt_prd_end_dt)
),

--Inpatient Length of Stay
ip_los as
(
select tot_bene.mbr_id_num,rpt_prd_strt_dt, rpt_prd_end_dt,sum(cst_modl_day_cnt) as ip_los, sum(cst_modl_in_ptnt_clm_adm_ind) as ip_dschrg_cnt
from tot_bene
        left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
        left join cst_modl_dim cd on cf.cst_modl_line_cd=cd.cst_modl_line_cd
where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
           and care_setting_cgy_nm='Hospital Inpatient (facility and professional)'
group by (tot_bene.mbr_id_num, rpt_prd_strt_dt, rpt_prd_end_dt)
),

--SNF Discharge Count
snf_dschrg as 
(
select tot_bene.mbr_id_num,
		sum(CASE WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt > 0 then 1 
			 WHEN cmd.cst_modl_line_cgy_nm = 'SNF' and cf.clm_line_num='001' AND dsd.dschrg_sts_cd <> '30' and cf.paid_amt < 0 then -1
				 ELSE 0 END ) as snf_dschrg from
        tot_bene
        left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
		left join cst_modl_dim cmd ON cf.cst_modl_line_cd = cmd.cst_modl_line_cd
		left join dschrg_sts_dim dsd ON cf.dschrg_sts_sk = dsd.dschrg_sts_sk
		
where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
                group by (tot_bene.mbr_id_num)
),

--SNF Length of Stay
snf_los as
(
select tot_bene.mbr_id_num,rpt_prd_strt_dt, rpt_prd_end_dt,snf_dschrg,sum(cst_modl_day_cnt) as snf_los
from tot_bene
        left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
	left join snf_dschrg snf on tot_bene.mbr_id_num=snf.mbr_id_num
        left join cst_modl_dim cd on cf.cst_modl_line_cd=cd.cst_modl_line_cd
where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
           and care_setting_cgy_nm='Skilled Nursing Facility'
group by (tot_bene.mbr_id_num, rpt_prd_strt_dt, rpt_prd_end_dt,snf_dschrg)
),

--Primary Care Service Indicator
prim_care_svc_ind as
(
select tot_bene.mbr_id_num,rpt_prd_strt_dt, rpt_prd_end_dt,--sum(cst_modl_utlz_cnt) as prim_care_svc_ind
	sum(case when (hcp.prim_care_svc_ind=1 
			and (pcp_physcn_ind=1 or eu_physcn_ind=1 or hcp.hcpcs_cd='G0402')) then cst_modl_utlz_cnt else null end) as prim_care_svc_ind
from tot_bene
        inner join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
		inner join hcpcs_dim hcp ON cf.hcpcs_sk = hcp.hcpcs_sk
		inner join aco_spcly_dim spcly using (aco_spcly_sk) 
      where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
 and hcp.prim_care_svc_ind=1 and (pcp_physcn_ind=1 or eu_physcn_ind=1)
 group by (tot_bene.mbr_id_num, rpt_prd_strt_dt, rpt_prd_end_dt)
),

--Primary Care by Specialist
spcly_prim_care_svc_ind as
(
select tot_bene.mbr_id_num,rpt_prd_strt_dt, rpt_prd_end_dt,sum(cst_modl_utlz_cnt) as spcly_prim_care_svc_ind
from tot_bene
        inner join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
		inner join hcpcs_dim hcp ON cf.hcpcs_sk = hcp.hcpcs_sk
		inner join aco_spcly_dim spcly using (aco_spcly_sk) 
      where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
 and hcp.prim_care_svc_ind=1 and eu_physcn_ind=1
 group by (tot_bene.mbr_id_num, rpt_prd_strt_dt, rpt_prd_end_dt)
),

--PCP Assignment

pcp_info as
(
--select * from a_pcp_info
--union
--select * from i_pcp_info
select 	m.mbr_id_num, 
		mpd.rgon, 
		upper(mpd.last_nm)||', '||upper(mpd.frst_nm) as pcp_pvdr_nm,
		prim_spcly_nm as pcp_pvdr_spcly, 
		mpd.grp as pcp_grp_nm,
		mpd.indpnd_ind,
		m.attr as attr_to
	from hist_bene_pcp_attr m
		left join mcl_pvdr_dim mpd on m.bill_pvdr_sk=mpd.pvdr_sk
		left join pvdr_dim pd on mpd.npi=pd.npi

),

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


algn_ntw_tot_paid_amt as
(
 select tot_bene.mbr_id_num, sum(cf.paid_amt) as algn_ntw_tot_paid_amt

 from tot_bene left join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
  left join ccn ON ccn.ccn_sk = cf.ccn_alt_sk
  left join in_ntw_pvdr_dim bill_inpd ON cf.bill_pvdr_sk = bill_inpd.in_ntw_pvdr_sk
  left join algn_snf asnf ON ccn.ccn_id = asnf.ccn_id
  where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
  and (ccn.ccn_id IN
 ('230227', '230207', '230193', '230141', '230167', '230041', '232020', '230080', '230105', '230216',
   '230297', 'HB1436', '233842', '233820', '233821', '233928', '238902', '902067', '237172', '237010',
   '237165', '237008', '237036', '231521', '23S167', '23S193', '23S141', '23S105', '23S216',
   '23T167', '23T141', '23T105', '235481', '235577', '235526', '235369', '23S207', '23T207', '231329')
   OR (bill_inpd.npi IS NOT NULL AND (bill_inpd.end_mn IS NULL OR bill_inpd.end_mn > cf.svc_to_dt))
   OR (asnf.algn = 'yes' and (svc_to_dt between asnf.eff_fm_dt and coalesce(asnf.eff_to_dt,'2999-12-31')))
   )
  group by tot_bene.mbr_id_num
 ),



--Specialty Indicator by BETOS
betos_cnslt_ind as
(
select tot_bene.mbr_id_num,rpt_prd_strt_dt, rpt_prd_end_dt, sum(cf.cst_modl_utlz_cnt) as betos_cnslt_ind
from tot_bene
        inner join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
	inner join betos_dim bd using (betos_sk)
	inner join aco_spcly_dim using (aco_spcly_sk)
	inner join hcpcs_dim using (hcpcs_sk)
	where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt 
	and bd.betos_cd in ('M5A','M5B','M5C','M5D','M6') and cf.cst_modl_utlz_type_cd = 'Visits' and prim_care_svc_ind<>1
	group by (tot_bene.mbr_id_num, rpt_prd_strt_dt, rpt_prd_end_dt)
),

--Post Discharge Followup Visit Indicator
fllw_vst_ind as
(
select tot_bene.mbr_id_num, 
	sum(case when days_to_fwp_vst<=7 then 1 else 0 end) as fllw_vst_7d_ind, 
	sum(case when days_to_fwp_vst<=14 then 1 else 0 end) as fllw_vst_14d_ind
		from tot_bene
	left join pln_mbr_dim pd on tot_bene.mbr_id_num=pd.mbr_id_num
	left join post_dschrg_vst_fct pst on pd.pln_mbr_sk=pst.pln_mbr_sk
	where pst.svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
	group by tot_bene.mbr_id_num
),

--7 & 14 Day End of Life Cost 
eol_paid_amt as
(
select tot_bene.mbr_id_num, pd.dth_dt as death_dt, sum(case when days_between(pd.dth_dt, cf.svc_to_dt)<=7 then paid_amt else 0 end) as eol_7d_paid_amt, 
	sum(case when days_between(pd.dth_dt, cf.svc_to_dt)<=14 then paid_amt else 0 end) as eol_14d_paid_amt
		from tot_bene
	inner join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num
	inner join pln_mbr_dim pd on tot_bene.mbr_id_num=pd.mbr_id_num and pd.dth_dt is not null
	where pd.dth_dt between rpt_prd_strt_dt and rpt_prd_end_dt
	group by tot_bene.mbr_id_num, pd.dth_dt
),

--Average # of Home Health Agency Visits per Home Health Claim
hha_vst_ind as 
(
select mbr_id_num, sum(hha_vst_cnt) as hha_vst_cnt, sum(hha_case_ind) as hha_case_cnt from (
select tot_bene.mbr_id_num, clm_id, sum(cst_modl_utlz_cnt) as hha_vst_cnt,
	case when sum(cst_modl_utlz_cnt) > 0 then 1
		when sum(cst_modl_utlz_cnt) < 0 then -1 
		else null end as hha_case_ind
from tot_bene
	inner join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num 
where cf.cst_modl_line_cd in ('P82a') and
cf.svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
--and tot_bene.mbr_id_num='2NM2HT9CP19'
group by tot_bene.mbr_id_num,clm_id
)a group by mbr_id_num
),

--Avoidable ED Visits per NYU Algorithm
avoidable_ed_cnt as
(
select mbr_id_num, sum(avoidable_ed_ind) as avoidable_ed_ind from 
(
select distinct tot_bene.mbr_id_num, cf.clm_id, nyu.ed_care_needed_not_prvntable_pct, nyu.ed_care_needed_prvntable_avoidable_pct, nyu.treatable_emrgnt_ptnt_care_pct, nyu.non_emrgnt_rel_pct, nyu.alc_rel_pct,
	nyu.drug_rel_pct, nyu.injry_rel_pct, nyu.psychology_rel_pct, nyu.unclsfd_pct,  case when (non_emrgnt_rel_pct + treatable_emrgnt_ptnt_care_pct + ed_care_needed_prvntable_avoidable_pct) = 1 then 1 else 0 end as avoidable_ed_ind
from tot_bene
	inner join clm_line_fct cf on tot_bene.mbr_id_num=cf.mbr_id_num 
	inner join cst_modl_dim cmd ON cf.cst_modl_sk = cmd.cst_modl_sk and cmd.care_svc_cgy_nm='Emergency Room'
	inner join clm_dgns_fct cdf on cf.clm_id=cdf.clm_id and cf.clm_line_num = cdf.clm_line_num
	inner join dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
	inner join nyu_ed_algr_dim nyu ON nyu.icd_diagonsis_cd = dd.dgns_alt_cd
where cf.svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
and cdf.icd_pos_num = 1)a
group by mbr_id_num
),

--Inpatient CMI 
avg_ip_cmi as
(
select mbr_id_num, count(fcy_case_id) as cmi_encntr_cnt, sum(case_mix_idnx_num::numeric) as ip_cmi,  sum(case_mix_idnx_num::numeric)/count(fcy_case_id) as avg_ip_cmi
	from (
select distinct tot_bene.mbr_id_num, fcy_case_id, ms.ms_drg_cd, case_mix_idnx_num
	from tot_bene
		inner join clm_line_fct cf ON tot_bene.mbr_id_num=cf.mbr_id_num
		inner join cst_modl_dim cmd ON cf.cst_modl_sk = cmd.cst_modl_sk and cmd.cst_modl_line_cgy_nm = 'FIP'
		inner join ms_drg_dim_h ms ON cf.ms_drg_cd = ms.ms_drg_cd and cf.svc_to_dt between vld_fm_dt and COALESCE(vld_to_dt,'2999-12-31')
where cf.svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt)a
group by mbr_id_num
),

--OON Spend by Care Settings (Hospital IP., OP, Prof Off, SNF, HH, Hospice)
oon_spend_by_care as
(
select tot_bene.mbr_id_num,
		sum(case when cmd.care_setting_cgy_nm='Hospital Inpatient (facility and professional)'
						and (bill_inpd.npi is null or cf.svc_to_dt > coalesce(bill_inpd.end_mn,'2099-12-31'))
						then paid_amt end) as oon_ip_spend,
		sum(case when cmd.care_setting_cgy_nm='Outpatient (facility and professional)' 
						and (bill_inpd.npi is null or cf.svc_to_dt > coalesce(bill_inpd.end_mn,'2099-12-31'))
						then paid_amt end) as oon_op_spend,
		sum(case when cmd.care_setting_cgy_nm='Skilled Nursing Facility' 
						and (bill_inpd.npi is null or cf.svc_to_dt > coalesce(bill_inpd.end_mn,'2099-12-31'))
						then paid_amt end) as oon_snf_spend,
		sum(case when cmd.care_setting_cgy_nm='Professional Office/Other' 
				                and (bill_inpd.npi is null or cf.svc_to_dt > coalesce(bill_inpd.end_mn,'2099-12-31'))
					        then paid_amt end) as oon_prof_oth_spend,
		sum(case when cmd.care_setting_cgy_nm='Home Health' 
						and (bill_inpd.npi is null or cf.svc_to_dt > coalesce(bill_inpd.end_mn,'2099-12-31'))
						then paid_amt end) as oon_hha_spend,
		sum(case when cmd.care_setting_cgy_nm='Hospice' 
						and (bill_inpd.npi is null or cf.svc_to_dt > coalesce(bill_inpd.end_mn,'2099-12-31'))
						then paid_amt end) as oon_hspc_spend,
		sum(case when cmd.care_setting_cgy_nm='Other' 
						and (bill_inpd.npi is null or cf.svc_to_dt > coalesce(bill_inpd.end_mn,'2099-12-31'))
						then paid_amt end) as oon_oth_spend
	--	1 as cnt
	from tot_bene
		inner join clm_line_fct cf ON tot_bene.mbr_id_num=cf.mbr_id_num
		left join cst_modl_dim cmd ON cf.cst_modl_sk = cmd.cst_modl_sk
		left join ccn_dim c ON cf.ccn_sk=c.ccn_sk
		left join mcl_ccn_dim mcl ON c.ccn_id=mcl.ccn_id
		left join in_ntw_pvdr_dim bill_inpd ON cf.bill_pvdr_sk = bill_inpd.in_ntw_pvdr_sk
		where (c.ccn_id not in (select ccn_id from mcl_ccn_dim union select ccn_id from algn_snf) or cf.ccn_sk is null) and
		cf.svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
group by tot_bene.mbr_id_num
),

--Avoidable IP Visits per AHRQ PQI
avdbl_ip_vst as
(
select mbr_id_num, 
sum(asthma_adm_ind) asthma_adm_ind,
sum(bacterial_pneu_adm_ind) bacterial_pneu_adm_ind,
sum(COPD_adm_ind) COPD_adm_ind,
sum(dehydration_adm_ind) dehydration_adm_ind,
sum(DLTC_adm_ind) DLTC_adm_ind,
sum(DSTC_adm_ind) DSTC_adm_ind,
sum(ud_adm_ind) ud_adm_ind,
sum(hf_adm_ind) hf_adm_ind,
sum(htn_adm_ind) htn_adm_ind,
sum(LEADP_ADM_IND) LEADP_ADM_IND,
sum(PAAR_ADM_IND) PAAR_ADM_IND,
sum(uti_adm_ind) uti_adm_ind,
sum(dibts_cmpos_ind) dibts_cmpos_ind,
sum(chronic_cmpos_ind) chronic_cmpos_ind,
sum(acute_cmpos_ind) acute_cmpos_ind,
sum(ovrl_cmpos_ind) ovrl_cmpos_ind

from 
(
select tot_bene.mbr_id_num, 
case when msr_nm='asthma_adm_ind' then 1 end as asthma_adm_ind,
case when msr_nm='bacterial_pneu_adm_ind' then 1 end as bacterial_pneu_adm_ind, 
case when msr_nm='COPD_adm_ind' then 1 end as COPD_adm_ind,
case when msr_nm='dehydration_adm_ind' then 1 end as dehydration_adm_ind,
case when msr_nm='DLTC_adm_ind' then 1 end as DLTC_adm_ind,
case when msr_nm='DSTC_adm_ind' then 1 end as DSTC_adm_ind,
case when msr_nm='ud_adm_ind' then 1 end as ud_adm_ind,
case when msr_nm='hf_adm_ind' then 1 end as hf_adm_ind,
case when msr_nm='htn_adm_ind' then 1 end as htn_adm_ind,
case when msr_nm='LEADP_ADM_IND' then 1 end as LEADP_ADM_IND,
case when msr_nm='PAAR_ADM_IND' then 1 end as PAAR_ADM_IND,
case when msr_nm='uti_adm_ind' then 1 end as uti_adm_ind,
case when msr_nm in ('DLTC_adm_ind','DSTC_adm_ind','LEADP_ADM_IND','ud_adm_ind') then 1 end as dibts_cmpos_ind,
case when msr_nm in ('DLTC_adm_ind','DSTC_adm_ind','COPD_adm_ind','htn_adm_ind','hf_adm_ind','LEADP_ADM_IND','ud_adm_ind','asthma_adm_ind') then 1 end as chronic_cmpos_ind,
case when msr_nm in ('dehydration_adm_ind','bacterial_pneu_adm_ind','uti_adm_ind') then 1 end as acute_cmpos_ind,
case when msr_nm in ('DSTC_adm_ind','DLTC_adm_ind','COPD_adm_ind','htn_adm_ind','hf_adm_ind','dehydration_adm_ind','bacterial_pneu_adm_ind','uti_adm_ind','ud_adm_ind','asthma_adm_ind','LEADP_ADM_IND') then 1 end as ovrl_cmpos_ind
FROM tot_bene
inner join avdbl_ip_vst_pqi_fct cf ON tot_bene.mbr_id_num=cf.mbr_id_num
where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
 )a
group by mbr_id_num
),


snf_same_day_ed_vst_ind as
(
SELECT
      tot_bene.mbr_id_num,
	 count(fcy_case_id) as snf_same_day_ed_vst_ind

   FROM tot_bene
         left join dmart_snf_ed_visits d on tot_bene.mbr_id_num=d.mbr_id_num
		 where d.fcy_case_id is not null and days_fm_snf_to_ed_vst<=1
		 and ed_svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
		 group by 1
),

snf_readm_ind as
(
SELECT
     distinct tot_bene.mbr_id_num,
	 sum(snf.clm_readm_anl_fct_cnt) as snf_readm_ind
	 
   FROM tot_bene
   	 INNER JOIN pln_mbr_dim pd on tot_bene.mbr_id_num=pd.mbr_id_num
   	 INNER JOIN clm_readm_anl_fct snf ON pd.pln_mbr_sk=snf.pln_mbr_sk and idnx_adm_type_cd='SNF'
     	 AND snf.svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
	 group by 1

),

ip_readm_ind as
(
SELECT
     distinct tot_bene.mbr_id_num,
	      sum(ip.clm_readm_anl_fct_cnt) as ip_readm_ind
	 
   FROM tot_bene
   	 INNER JOIN pln_mbr_dim pd on tot_bene.mbr_id_num=pd.mbr_id_num
   	 INNER JOIN clm_readm_anl_fct ip ON pd.pln_mbr_sk=ip.pln_mbr_sk and idnx_adm_type_cd='IP'
     	 AND ip.svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
	 group by 1
)

select  mbr.rpt_prd_strt_dt,
                mbr.rpt_prd_end_dt,
                mbr.mnth_yr_nm,
                mbr.mbr_id_num,
		upper(mbr.mbr_name) as mbr_name,
		mbr.brth_dt, 
		mbr.dth_dt, 
		mbr.care_mgn_sts_ind, 
		pvdr.attr_to,
                pvdr.rgon,
                pvdr.pcp_pvdr_nm,
                pvdr.pcp_pvdr_spcly,
                pvdr.pcp_grp_nm,
                pvdr.indpnd_ind,
                hcr.cms_hcc_scor_num,
                mem.bene_mnths,
                mem.bene_yrs,
                mbr.age_in_yrs,
            	mbr.tot_bene,
                mbr.elig_sts,
                tpa.paid_amt,
                tpa.thrsld_amt,
                tpa.trunc_paid_amt,
                cs.ip_paid_amt,
                cs.ip_dschrg,
                cs.snf_paid_amt,
                slos.snf_dschrg,
                cs.hha_paid_amt,
                cs.hspc_paid_amt,
                cs.ed_paid_amt,
                cs.ed_vst_ind,
                oa.op_paid_amt,
                hspd.hspc_dschrg,
                hlos.hspc_los,
                ilos.ip_los,
                slos.snf_los,
                ct.ct_scn_ind,
                mri.mri_ind,
                pcp.prim_care_svc_ind,
		spcly.spcly_prim_care_svc_ind,
		nt.ntw_tot_paid_amt,
		betos.betos_cnslt_ind,
		fllw.fllw_vst_7d_ind,
		fllw.fllw_vst_14d_ind,
		eol.eol_7d_paid_amt,
		eol.eol_14d_paid_amt,
		hhav.hha_case_cnt,
		hhav.hha_vst_cnt,
		avd.avoidable_ed_ind,
		cmi.avg_ip_cmi,
		oonspd.oon_ip_spend,
		oonspd.oon_op_spend,
		oonspd.oon_snf_spend,
		oonspd.oon_prof_oth_spend,
		oonspd.oon_hha_spend,
		oonspd.oon_hspc_spend,
		oonspd.oon_oth_spend,
		avdbl.asthma_adm_ind,
		avdbl.bacterial_pneu_adm_ind,
		avdbl.copd_adm_ind,
		avdbl.dehydration_adm_ind,
		avdbl.DLTC_adm_ind,
		avdbl.DSTC_adm_ind,
		avdbl.ud_adm_ind,
		avdbl.hf_adm_ind,
		avdbl.htn_adm_ind,
		avdbl.LEADP_ADM_IND,
		avdbl.PAAR_ADM_IND,
		avdbl.uti_adm_ind,
		hlos.hspc_case_cnt,
		ilos.ip_dschrg_cnt,
		avdbl.ovrl_cmpos_ind,
		avdbl.acute_cmpos_ind,
		avdbl.chronic_cmpos_ind,
		avdbl.dibts_cmpos_ind, 
		ssnf.snf_same_day_ed_vst_ind, 
		sreadm.snf_readm_ind, 
		ireadm.ip_readm_ind,
		algn.algn_ntw_tot_paid_amt
		
from tot_bene mbr
        left join mem_elig_months mem on mbr.mbr_id_num=mem.mbr_id_num
        left join hist_bene_hcc_scr hcr on mbr.mbr_id_num=hcr.mbr_id_num
	left join bene_ttl_paid_amt tpa on mbr.mbr_id_num=tpa.mbr_id_num
        left join cs_paid_amt cs on mbr.mbr_id_num=cs.mbr_id_num
        left join op_paid_amt oa on mbr.mbr_id_num=oa.mbr_id_num
        left join hspc_dschrg hspd on mbr.mbr_id_num=hspd.mbr_id_num
        left join hspc_los hlos on mbr.mbr_id_num=hlos.mbr_id_num
        left join ct_scn_ind ct on mbr.mbr_id_num=ct.mbr_id_num
        left join mri_ind mri on mbr.mbr_id_num=mri.mbr_id_num
        left join ip_los ilos on mbr.mbr_id_num=ilos.mbr_id_num
        left join snf_los slos on mbr.mbr_id_num=slos.mbr_id_num
        left join prim_care_svc_ind pcp on mbr.mbr_id_num=pcp.mbr_id_num
	left join spcly_prim_care_svc_ind spcly on mbr.mbr_id_num=spcly.mbr_id_num
	left join pcp_info pvdr on mbr.mbr_id_num=pvdr.mbr_id_num
	left join ntw_tot_paid_amt nt on mbr.mbr_id_num=nt.mbr_id_num
	left join betos_cnslt_ind betos on mbr.mbr_id_num=betos.mbr_id_num
	left join fllw_vst_ind fllw on mbr.mbr_id_num=fllw.mbr_id_num
	left join eol_paid_amt eol on mbr.mbr_id_num=eol.mbr_id_num
	left join hha_vst_ind hhav on mbr.mbr_id_num=hhav.mbr_id_num
	left join avoidable_ed_cnt avd on mbr.mbr_id_num=avd.mbr_id_num
	left join avg_ip_cmi cmi on mbr.mbr_id_num=cmi.mbr_id_num
	left join oon_spend_by_care oonspd on mbr.mbr_id_num=oonspd.mbr_id_num
	left join avdbl_ip_vst avdbl on mbr.mbr_id_num=avdbl.mbr_id_num
	left join snf_same_day_ed_vst_ind ssnf on mbr.mbr_id_num=ssnf.mbr_id_num
	left join snf_readm_ind sreadm on mbr.mbr_id_num=sreadm.mbr_id_num
	left join ip_readm_ind ireadm on mbr.mbr_id_num=ireadm.mbr_id_num
	left join algn_ntw_tot_paid_amt algn on mbr.mbr_id_num=algn.mbr_id_num
;

\unset ON_ERROR_STOP
