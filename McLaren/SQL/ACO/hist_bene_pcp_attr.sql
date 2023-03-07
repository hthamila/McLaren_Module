\set ON_ERROR_STOP ON;

drop table temp_hist_bene_pcp_services if exists;

CREATE TABLE temp_hist_bene_pcp_services as
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


tot_bene as
(
select distinct member_id as mbr_id_num,
                pd.full_nm as mbr_name,
                months_between(:v_rpt_prd_end_dt,brth_dt)/12 as age_in_yrs,
                1 tot_bene,
                elig_status_1 as elig_sts,
                :v_rpt_prd_strt_dt rpt_prd_strt_dt,
                :v_rpt_prd_end_dt rpt_prd_end_dt
        from pce_qe16_aco_prd_lnd..cv_member_time_windows
             left join pln_mbr_dim pd on member_id=mbr_id_num
        where elig_month=:v_elig_month and assignment_indicator='Y'

)

SELECT   a.pln_mbr_sk, 
         a.mbr_id_num, 
         a.bill_pvdr_sk, 
         a.pcs_cnt, 
         a.paid_amt, 
         a.last_date, 
         a.strt_mn, 
         a.end_mn, 
         a.npi, 
         a.actv_sts, 
         a.mhpn_indicator, 
         a.mpp_indicator, 
	 a.pcp_physcn_ind,
         Rank() OVER (partition BY a.pln_mbr_sk ORDER BY a.pcp_physcn_ind DESC, a.mhpn_indicator DESC, a.pcs_cnt DESC, a.last_date DESC, a.paid_amt DESC ) AS rnk
FROM     ( 
SELECT    pln_mbr_sk, 
          mbr_id_num, 
          bill_pvdr_sk, 
          strt_mn, 
          end_mn, 
          npi, 
          actv_sts, 
          mhpn_indicator, 
          mpp_indicator,
	  pcp_physcn_ind,
          sum(cst_modl_utlz_cnt) AS pcs_cnt, 
          sum(paid_amt)          AS paid_amt, 
          max(svc_to_dt)         AS last_date 
	  from (
		select 	cf.pln_mbr_sk, 
           	cf.mbr_id_num, 
           	cf.bill_pvdr_sk,
		case when (cf.rsk_pool_nm='Part B' and int4(pd.plc_of_svc_cd) = 31 and hd.hcpcs_cd in ('99304','99305','99306','99307','99308','99309','99310','99315','99316')) then 1
		   		else 0 end as excl_plc_svc_ind,
		hd.hcpcs_cd,
		cf.rsk_pool_nm,
		pd.plc_of_svc_cd,
		cf.cst_modl_utlz_cnt, 
           	cf.paid_amt,
           	cf.svc_to_dt,
		inp.strt_mn, 
		inp.end_mn, 
		inp.npi, 
		inp.actv_sts, 
		inp.mhpn_indicator, 
		inp.mpp_indicator,
		asd.pcp_physcn_ind
		  	from clm_line_fct cf
		  	join tot_bene dt using (mbr_id_num)
		  	join mcl_pvdr_dim inp on cf.bill_pvdr_sk=inp.pvdr_sk
		  	left join plc_of_svc_dim pd using (plc_of_svc_sk)
		  	left join hcpcs_dim hd using (hcpcs_sk)
		  	left join rev_cl_dim rcd using (rev_cl_sk)
		  	left join aco_spcly_dim asd using (aco_spcly_sk)

		where cf.svc_to_dt between dt.rpt_prd_strt_dt and dt.rpt_prd_end_dt
			and (hd.prim_care_svc_ind = 1 or rcd.rev_cd in ('0521','0522','0524','0525'))
			and (asd.pcp_physcn_ind = 1 or asd.aco_spcly_ind = 1))x
			--where x.excl_plc_svc_ind <> 1 
	group by  pln_mbr_sk, 
	          mbr_id_num, 
	          bill_pvdr_sk, 
	          strt_mn, 
	          end_mn, 
	          npi, 
	          actv_sts, 
	          mhpn_indicator, 
	          mpp_indicator,
		  pcp_physcn_ind)a
;
\unset ON_ERROR_STOP
