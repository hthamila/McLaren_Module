insert into temp_hist_bene_smry_fct
with bene_mnths as 
(
select mbr_id_num, sum(mdcl_mo_cnt) mdcl_mo_cnt from
(select distinct mbr_id_num, asgnt_wndw_strt_dt, asgnt_wndw_end_dt, mdcl_mo_cnt from prmretlt.clm_line_fct_ds join (
select a.rpt_prd_strt_dt, b.rpt_prd_end_dt from 
(SELECT add_months(val,-:v_cntr) as rpt_prd_strt_dt  FROM dt_meta 
					WHERE descr = 'roll_yr_strt') a join
(SELECT add_months(val,-:v_cntr) as rpt_prd_end_dt FROM dt_meta 
					WHERE descr = 'roll_yr_end') b on 1=1) z on 1=1
where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
)a group by  mbr_id_num
)

select mbr_id_num, brth_dt, cms_hcc_scor_num, elig_sts, rgon, pcp_pvdr_npi, pcp_pvdr_nm, pcp_grp_nm,pcp_pvdr_spcly, sum(paid_amt) as paid_amt, rpt_prd_strt_dt, rpt_prd_end_dt, rsk_pool_nm, cst_modl_line_cd, cst_modl_utlz_type_cd, care_setting_sub_cgy_nm, 
fcy_case_id, hsptl_pvdr_type, sum(cst_modl_utlz_cnt) as cst_modl_utlz_cnt,
sum(prim_care_svc_ind) as prim_care_svc_ind, bene_mnths.mdcl_mo_cnt

from clm_line_fct_ds cf join (
select a.rpt_prd_strt_dt, b.rpt_prd_end_dt from 
(SELECT add_months(val,-:v_cntr) as rpt_prd_strt_dt  FROM dt_meta 
					WHERE descr = 'roll_yr_strt') a join
(SELECT add_months(val,-:v_cntr) as rpt_prd_end_dt FROM dt_meta 
					WHERE descr = 'roll_yr_end') b on 1=1
) z on 1=1
join bene_mnths using (mbr_id_num)
where svc_to_dt between rpt_prd_strt_dt and rpt_prd_end_dt
group by mbr_id_num, brth_dt, cms_hcc_scor_num, elig_sts, rgon, pcp_pvdr_npi, pcp_pvdr_nm, pcp_grp_nm,pcp_pvdr_spcly, rpt_prd_strt_dt, rpt_prd_end_dt, rsk_pool_nm, cst_modl_line_cd, cst_modl_utlz_type_cd, care_setting_sub_cgy_nm, fcy_case_id, hsptl_pvdr_type, bene_mnths.mdcl_mo_cnt
order by sum(paid_amt) desc
;
