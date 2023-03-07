\set ON_ERROR_STOP ON;

--Drop old tables---
drop table hist_cms_aggr_fct_prev if exists;
create table hist_cms_aggr_fct_prev as select * from hist_cms_aggr_fct;

delete from hist_cms_aggr_fct where mnth_yr_nm in ('National Assignable FFS','All MSSP ACOs* Benchmark');

insert into hist_cms_aggr_fct
(
rpt_prd_strt_dt,
rpt_prd_end_dt,
mnth_yr_nm,
rgon,
bene_yrs,
tot_bene,
elig_sts,
trunc_paid_amt,
ip_paid_amt,
ip_dschrg,
snf_paid_amt,
ed_vst_ind,
ct_scn_ind,
mri_ind,
prim_care_svc_ind,
spcly_prim_care_svc_ind
)

with dt_params as
(
select add_months(max(qtr_end_dt),-12)+1 as rpt_prd_strt_dt,
        max(qtr_end_dt)||' 23:00:00' rpt_prd_end_dt,
        'Q'||extract(quarter from max(qtr_end_dt))||' '||extract(year from max(qtr_end_dt)) mnth_yr_nm  from pce_qe16_aco_prd_lnd..qtr_cdr
)

------------------
--ACO - Specific--
------------------
select
rpt_prd_strt_dt, rpt_prd_end_dt, mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=105 then aco_spcfc end) bene_yrs,
max(case when msr_key=102 then aco_spcfc end) tot_bene,
'ESRD' elig_sts,
max(case when msr_key=111 then aco_spcfc end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then aco_spcfc end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then aco_spcfc end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then aco_spcfc end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then aco_spcfc end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then aco_spcfc end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then aco_spcfc end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then aco_spcfc end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then aco_spcfc end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
        join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt, mnth_yr_nm

union

select
rpt_prd_strt_dt, rpt_prd_end_dt, mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=106 then aco_spcfc end) bene_yrs,
max(case when msr_key=102 then aco_spcfc end) tot_bene,
'Disabled' elig_sts,
max(case when msr_key=112 then aco_spcfc end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then aco_spcfc end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then aco_spcfc end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then aco_spcfc end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then aco_spcfc end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then aco_spcfc end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then aco_spcfc end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then aco_spcfc end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then aco_spcfc end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
        join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt, mnth_yr_nm

union

select
rpt_prd_strt_dt, rpt_prd_end_dt, mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=107 then aco_spcfc end) bene_yrs,
max(case when msr_key=102 then aco_spcfc end) tot_bene,
'Aged Dual' elig_sts,
max(case when msr_key=113 then aco_spcfc end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then aco_spcfc end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then aco_spcfc end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then aco_spcfc end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then aco_spcfc end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then aco_spcfc end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then aco_spcfc end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then aco_spcfc end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then aco_spcfc end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
 join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt, mnth_yr_nm

union

select
rpt_prd_strt_dt, rpt_prd_end_dt, mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=108 then aco_spcfc end) bene_yrs,
max(case when msr_key=102 then aco_spcfc end) tot_bene,
'Aged Non-Dual' elig_sts,
max(case when msr_key=114 then aco_spcfc end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then aco_spcfc end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then aco_spcfc end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then aco_spcfc end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then aco_spcfc end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then aco_spcfc end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then aco_spcfc end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then aco_spcfc end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then aco_spcfc end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
 join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt, mnth_yr_nm

union
------------------
--   All MSSP   --
------------------

select
rpt_prd_strt_dt, rpt_prd_end_dt, 'All MSSP ACOs* Benchmark' mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=105 then all_mssp end) bene_yrs,
max(case when msr_key=102 then all_mssp end) tot_bene,
'ESRD' elig_sts,
max(case when msr_key=111 then all_mssp end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then all_mssp end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then all_mssp end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then all_mssp end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then all_mssp end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then all_mssp end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then all_mssp end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then all_mssp end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then all_mssp end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
 join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt

union
select
rpt_prd_strt_dt, rpt_prd_end_dt, 'All MSSP ACOs* Benchmark' mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=106 then all_mssp end) bene_yrs,
max(case when msr_key=102 then all_mssp end) tot_bene,
'Disabled' elig_sts,
max(case when msr_key=112 then all_mssp end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then all_mssp end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then all_mssp end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then all_mssp end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then all_mssp end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then all_mssp end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then all_mssp end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then all_mssp end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then all_mssp end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
 join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt

union
select
rpt_prd_strt_dt, rpt_prd_end_dt, 'All MSSP ACOs* Benchmark' mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=107 then all_mssp end) bene_yrs,
max(case when msr_key=102 then all_mssp end) tot_bene,
'Aged Dual' elig_sts,
max(case when msr_key=113 then all_mssp end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then all_mssp end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then all_mssp end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then all_mssp end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then all_mssp end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then all_mssp end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then all_mssp end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then all_mssp end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then all_mssp end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
 join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt

union
select
rpt_prd_strt_dt, rpt_prd_end_dt, 'All MSSP ACOs* Benchmark' mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=108 then all_mssp end) bene_yrs,
max(case when msr_key=102 then all_mssp end) tot_bene,
'Aged Non-Dual' elig_sts,
max(case when msr_key=114 then all_mssp end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then all_mssp end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then all_mssp end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then all_mssp end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then all_mssp end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then all_mssp end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then all_mssp end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then all_mssp end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then all_mssp end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
 join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt

union
-------------------
--National Assign--
-------------------
select
rpt_prd_strt_dt, rpt_prd_end_dt, 'National Assignable FFS' mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=105 then ntnl_assgn_ffs end) bene_yrs,
max(case when msr_key=102 then ntnl_assgn_ffs end) tot_bene,
'ESRD' elig_sts,
max(case when msr_key=111 then ntnl_assgn_ffs end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then ntnl_assgn_ffs end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then ntnl_assgn_ffs end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then ntnl_assgn_ffs end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then ntnl_assgn_ffs end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then ntnl_assgn_ffs end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then ntnl_assgn_ffs end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then ntnl_assgn_ffs end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then ntnl_assgn_ffs end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
 join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt

union
select
rpt_prd_strt_dt, rpt_prd_end_dt, 'National Assignable FFS' mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=106 then ntnl_assgn_ffs end) bene_yrs,
max(case when msr_key=102 then ntnl_assgn_ffs end) tot_bene,
'Disabled' elig_sts,
max(case when msr_key=112 then ntnl_assgn_ffs end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then ntnl_assgn_ffs end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then ntnl_assgn_ffs end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then ntnl_assgn_ffs end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then ntnl_assgn_ffs end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then ntnl_assgn_ffs end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then ntnl_assgn_ffs end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then ntnl_assgn_ffs end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then ntnl_assgn_ffs end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
 join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt

union
select
rpt_prd_strt_dt, rpt_prd_end_dt, 'National Assignable FFS' mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=107 then ntnl_assgn_ffs end) bene_yrs,
max(case when msr_key=102 then ntnl_assgn_ffs end) tot_bene,
'Aged Dual' elig_sts,
max(case when msr_key=113 then ntnl_assgn_ffs end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then ntnl_assgn_ffs end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then ntnl_assgn_ffs end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then ntnl_assgn_ffs end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then ntnl_assgn_ffs end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then ntnl_assgn_ffs end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then ntnl_assgn_ffs end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then ntnl_assgn_ffs end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then ntnl_assgn_ffs end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
 join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt

union
select rpt_prd_strt_dt, rpt_prd_end_dt, 'National Assignable FFS' mnth_yr_nm, 'CMS E&U' rgon,
max(case when msr_key=108 then ntnl_assgn_ffs end) bene_yrs,
max(case when msr_key=102 then ntnl_assgn_ffs end) tot_bene,
'Aged Non-Dual' elig_sts,
max(case when msr_key=114 then ntnl_assgn_ffs end)*bene_yrs trunc_paid_amt,
max(case when msr_key=122 then ntnl_assgn_ffs end)*bene_yrs ip_paid_amt,
(max(case when msr_key=148 then ntnl_assgn_ffs end)*bene_yrs)/1000 ip_dschrg,
max(case when msr_key=127 then ntnl_assgn_ffs end)*bene_yrs snf_paid_amt,
(max(case when msr_key=153 then ntnl_assgn_ffs end)*bene_yrs)/1000 ed_vst_ind,
(max(case when msr_key=155 then ntnl_assgn_ffs end)*bene_yrs)/1000 ct_scn_ind,
(max(case when msr_key=156 then ntnl_assgn_ffs end)*bene_yrs)/1000 mri_ind,
(max(case when msr_key=157 then ntnl_assgn_ffs end)*bene_yrs)/1000 prim_care_svc_ind,
(max(case when msr_key=159 then ntnl_assgn_ffs end)*bene_yrs)/1000 spcly_prim_care_svc_ind
from cms_eu_rpt
 join dt_params on 1=1
group by rpt_prd_strt_dt, rpt_prd_end_dt

\unset ON_ERROR_STOP

