--\set ON_ERROR_STOP ON;
---- temp for observation hours-----------
DROP TABLE temp_obv IF EXISTS;
;create temp table  temp_obv as 
(
select 
bq.patient_id
,bq.company_id
,sum(bq.quantity) as qty
from 
pce_qe16_oper_prd_zoom..cv_patbill  bq 
where
bq.revenue_code = '0762' and bq.dept not in ('31010' ,'31110','23015','3666','3675','3678','3679','3681','0378',
'0687','8378','8398','01.3110','01.3115','31310')
group by 
bq.patient_id
,bq.company_id
);
 
-----------------------Temp Service Line Name------------------------------------------------------------------------------------------------------------
DROP TABLE tmp_svc_ln_fct IF EXISTS;
;create temp table  tmp_svc_ln_fct as 
(SELECT distinct pe.encntr_num, pe.e_svc_ln_nm
  FROM pce_qe16_slp_prd_dm..prd_encntr_anl_fct pe);
----------------------temp pat discharge------------------------------------------------------------------------------------------------------------------
--select count(*) FROM tmp_pdsc ; -- 22,639,906

select company_id, count(*) FROM pce_qe16_oper_prd_zoom..cv_patdisch
where updateid ='Incarcerated' and inpatient_outpatient_flag = 'I'
and to_date(discharge_date,'mmddyyyy') between '2020-04-01' AND '2021-03-31'
GROUP BY 1; 

select company_id, (to_date(discharge_date,'mmddyyyy')), count(*) FROM pce_qe16_oper_prd_zoom..cv_patdisch
where updateid ='Incarcerated' and inpatient_outpatient_flag = 'I'
and to_date(discharge_date,'mmddyyyy') between '2020-04-01' AND '2021-03-31'
GROUP BY 1,2; 

--select company_id, dschrg_svc_excl, count(*) FROM tmp_pdsc
--where 
--date(zm_dschrg_dt) between '2020-04-01' AND '2021-03-31'
--GROUP BY 1,2; 

DROP TABLE tmp_pdsc IF EXISTS;
;create temp table  tmp_pdsc as
(

select distinct p.patient_id
,p.company_id
,p.primary_payer_code 
,case when p.patient_first_name is null then '' else p.patient_first_name end ||' '||case when p.patient_middle_name is null then '' else p.patient_middle_name end||' '||case when p.patient_last_name is null then '' else p.patient_last_name end  as  ptnt_nm
,case when p.company_id = 'Bay' then 'MI2191' 
when p.company_id = 'Central' then 'MI2061' 
when p.company_id = 'Flint' then  'MI2302' 
when p.company_id ='Karmanos' then '634342' 
when p.company_id ='Lansing' then 'MI5020' 
when p.company_id = 'Lapeer' then 'MI2001'
when p.company_id = 'Macomb' then 'MI2048' 
when p.company_id ='Northern' then '637619' 
when p.company_id ='Oakland' then 'MI2055' 
when p.company_id ='Port Huron' then '600816'
else null end as fcy_num
,p.updateid
,to_date(p.discharge_date,'mmddyyyy') as zm_dschrg_dt
,to_date(p.admissionarrival_date,'mmddyyyy') as zm_adm_dt
,p.dischargeservice
,case when (
--p.company_id in ('Oakland','Port Huron') and 
p.dischargeservice in ('BEH','GERI','REHAB','PSYCH'))then 0 else 1 end as dschrg_svc_excl
--CODE CHANGE: 10/12/21: Added source_system to handle Incarcerated Encounters 
,p.sourcesystem
from 
pce_qe16_oper_prd_zoom..cv_patdisch p

);

select company_id,sum(dschrg_svc_excl) FROM tmp_pdsc
group by 1;

--current  2635739 (lansing)
--current  2086184  (oakland)

-------------------------------------------------------------Lab Utilization--------------------------------------------------------------------------------------------------------------------------------
 DROP TABLE temp_lab IF EXISTS;
;create temp table  temp_lab as 
( select P.company_id, P.patient_id ,to_date(postdate,'mmddyyyy') as post_date,  SUM(
case when d.department_group = 'Lab' and p.inpatient_outpatient_flag = 'I'  AND X.total_charge <> 0 
	then X.quantity else null end) as sumqty
FROM pce_qe16_oper_prd_zoom..cv_patdisch p 
left join pce_qe16_oper_prd_zoom..cv_patbill X 
on  p.patient_id = X.patient_id and p.company_id = X.company_id
inner join pce_qe16_oper_prd_zoom..cv_dept d on X.company_id = d.company_id and X.dept = d.department_code
WHERE 
CPT_code NOT IN (select cd from pce_qe16_prd_qadv..val_set_dim where cohrt_id ='lab_utils')
AND year(to_date(postdate,'mmddyyyy')) >= 2017
GROUP BY 1,2,3
);

DROP TABLE tmp_lab_utlz IF EXISTS;
;create temp table   tmp_lab_utlz as 
(select 
p.company_id
,case p.company_id when 'Bay' then 'MI2191' 
when 'Central' then 'MI2061'  
when 'Flint' then  'MI2302' 
when 'Karmanos' then '634342' 
when 'Lansing' then 'MI5020' 
when 'Lapeer' then 'MI2001'
when 'Macomb' then 'MI2048' 
when 'Northern' then '637619' 
when 'Oakland' then 'MI2055' 
when 'Port Huron' then '600816' 
else null end as fcy_num
,p.patient_id
,to_date(b.postdate,'mmddyyyy') as post_dt
,to_date(p.discharge_date,'mmddyyyy') as dschrg_dt
,case when  (p.primary_payer_code in ('SELECT','SELEC') or p.patient_type in ('BSCH','BSCHO')) then 0
when (m.payor_group3 = 'Hospice') then 0
when ((p.dischargeservice  in ('NBN','NB','OIN','SCN','L1N','BBN','NURS')) or p.patient_type = 'NB') then 0
else 1 end as inptnt_ind
,CAST(avg(case when Z.sumqty > 0 THEN Z.sumqty else 0 end) AS INTEGER) as lab_volume
,sum(case when (s.pcd_dept_descr_v10 = 'ROOM AND BOARD' and b.chargecodedesc not like '%ADJ%' and inptnt_ind =1) then b.quantity else null end) as patient_days
,sum(case when (d.department_group = 'Lab' and p.inpatient_outpatient_flag = 'I' and  b.total_charge <> 0 and 
b.cpt_code in ('P9011','P9012','P9016','P9017','P9019','P9021','P9033','P9034','P9035','P9037','P9040','P9044','P9052','P9059')) then b.quantity else null end) as blood_utlzn
from 
pce_qe16_oper_prd_zoom..cv_patdisch p 
left outer join pce_qe16_oper_prd_zoom..cv_patbill b on p.patient_id = b.patient_id and p.company_id = b.company_id
inner join pce_qe16_oper_prd_zoom..cv_dept d on b.company_id = d.company_id and b.dept = d.department_code
left outer join pce_qe16_prd_qadv..fcy_chrg_cd_ref_spl s on (case b.company_id when 'Bay' then 'MI2191' when 'Central' then 'MI2061'  when 'Flint' then  'MI2302' when 'Karmanos' then '634342' when 'Lansing' then 'MI5020' when 'Lapeer' then 'MI2001'
when 'Macomb' then 'MI2048' when 'Northern' then '637619' when 'Oakland' then 'MI2055' when 'Port Huron' then '600816' else null end) = s.fcy_num
and b.charge_code = s.cdm_cd
inner join pce_qe16_oper_prd_zoom..cv_paymstr m on p.primary_payer_code = m.payer_code and p.company_id = m.company_id
left join temp_lab Z on Z.company_id = p.company_id and Z.patient_id = p.patient_id and to_date(b.postdate,'mmddyyyy') = date(Z.post_date)
where year(post_dt) >= 2017
group by  1,2,3,4,5,6

);
---------------------------------------------------readmission excl------------------------------------------------------------------------------------------------------
drop table temp_readm_excl if exists;
;create temp table  temp_readm_excl as 
(SELECT DISTINCT p.patient_id,pm.payor_group3,to_date(p.discharge_date ,'mmddyyyy') as discharge_date, (lead(pm.payor_group3) over (partition by p.medical_record_number order by to_date(p.discharge_date ,'mmddyyyy')asc, to_date(admissionarrival_date,'mmddyyyy')asc )) as next_payor_group3,
(lead(p.dischargeservice) over (partition by p.medical_record_number order by to_date(p.discharge_date ,'mmddyyyy') asc, to_date(admissionarrival_date,'mmddyyyy')asc )) as next_discharge_service
  FROM pce_qe16_oper_prd_zoom.qe16zmp.cv_patdisch p
  left join pce_qe16_oper_prd_zoom..cv_paymstr pm on p.primary_payer_code = pm.payer_code and p.company_id = pm.company_id
  where p.inpatient_outpatient_flag='I'
  );

------------------------------------------------------QA Attributes------------------------------------------------------------------------------------------------------------------------------------------
 
drop table dgns_dim_slp if exists; 
create temp table dgns_dim_slp as select * from pce_qe16_slp_prd_dm..dgns_dim ;

drop table  pcd_dim_slp if exists; 
create temp table pcd_dim_slp as select * from pce_qe16_slp_prd_dm..pcd_dim ;

drop table tmp_qadv if exists;
DROP TABLE tmp_qadv IF EXISTS;
;create temp table  tmp_qadv as
(select
 ef.encntr_num
,ef.fcy_num
,fd.fcy_nm
,ef.mdcl_rcrd_num
,ef.adm_dt
,ef.dschrg_dt
,ef.fcy_pyr_cd
,ef.age_val
,ef.ptnt_cl_cd
,ef.stnd_ptnt_type_cd
,fd.bed_cnt
,ef.ms_drg_icd10
,ef.ms_drg_mdc_icd10
,ef.adm_src_cd
,ef.nbrn_cnt
,ef.otlr_cd
,ms.ms_drg_descr as ms_drg_descr
,ms.ms_drg_bsn_line_descr
,ad.adm_src_descr as adm_src_descr
,at.adm_type_descr as adm_type_descr
,ds.dschrg_sts_descr as dschrg_sts_descr
,ef.icd10_diag_code as prim_diag_code
--,dr.icd_diag_descr as prim_diag_descr
,dr.dgns_descr as prim_diag_descr
,ef.icd10_proc_code
--,procd.icd_proc_descr
, procd.icd_pcd_descr as icd_proc_descr
,pr.fcy_pract_nm as attnd_pract_nm
,pr.stnd_pract_spcly_cd as attnd_pract_spcly_cd
,pr.stnd_pract_spcly_descr as attnd_pract_spcly_descr
,por.pnt_of_orig_descr
,ef.apr_soi_cd as apr_soi
,ef.apr_soi_icd10 as apr_soi_icd10
,ef.apr_rom_icd10 as apr_rom
,nvl(op.cd,'incl_cd') as outcomes_pyr_cd
,nvl(sp.cd,'incl_cd') as sepsis_pyr_cd
,nvl(mp.cd,'incl_cd') as medicare_pyr_cd
,nvl(rp.cd,'incl_cd') as redam_pyr_cd
--,case when ivd.encntr_num is not null then 1 else 0 end as 
,case when cast(month(ef.dschrg_dt) as varchar(4)) not in ('10','11','12') then '0'||cast(month(ef.dschrg_dt) as varchar(4)) else cast(month(ef.dschrg_dt) as varchar(4)) end as month_dschrg
,cast(year(ef.dschrg_dt) as varchar(4))||'-'||month_dschrg||'-'||'01'||' '||'00:00:00' as dschrg_dt_flt 
,max(case when (ef.icd10_diag_code in ('I21.4','I21.29','I21.11','I21.02','I21.01','I21.21','I21.19','I21.09','I21.3') and ef.age_val >=65 and ef.dschrg_sts_cd not in ('2','7')) then 1 else 0 end ) as AMI_ind
,max(ef.mrtly_cnt) as mrtly_cnt
,max(ef.csa_expc_mrtly_cnt) as csa_expc_mrtly_cnt
,max(ef.csa_mort_scl_fctr) as csa_mort_scl_fctr
,max(ef.expc_mrtly_outc_case_cnt) as expc_mrtly_outc_case_cnt
,max(ef.re_adm_day_cnt) as re_adm_day_cnt
,max(case when ((upper(tr.next_payor_group3) = upper('Hospice')) or (tr.next_discharge_service in ('BEH','GERI','REHAB','PSYCH'))) then 0 else ef.csa_hwr4_30d_readm_out_case_cnt end) AS prs_readm_30day_rsk_out_case_cnt
,max(ef.csa_hwr4_expc_readm) AS csa_expc_prs_readm_30day_rsk
,max(ef.readmit_cnt_30dy_diag) as readmit_cnt_30dy_diag
,max(ef.readmit_unpln_pln_ind) as readmit_unpln_pln_ind
,max(case when ((upper(tr.next_payor_group3) = upper('Hospice') )or (tr.next_discharge_service in ('BEH','GERI','REHAB','PSYCH'))) then 0 else ef.csa_hwr4_readm_rsk_adj_cnt end) AS csa_obs_readm_rsk_adj_cnt
,max(ef.acute_readmit_days_key) as acute_readmit_days_key
,max(ef.csa_hwr4_expc_30day_readm_scl_fctr) AS csa_readm_30day_scl_fctr
,max(ef.prs_comp_rsk_out_case_cnt) AS prs_cmplc_rsk_out_case_cnt
,max(ef.expc_los_outc_case_cnt) as expc_los_outc_case_cnt
,max(ef.csa_expc_los_cnt) as csa_expc_los_cnt
,max(ef.ln_los) as ln_los
,max(ef.csa_los_scl_fctr) as csa_los_scl_fctr
,max(ef.csa_cmp_scl_fctr) as csa_cmp_scl_fctr
,max(ef.csa_expc_compl_cnt) as csa_expc_compl_cnt
,max(ef.prs_comp_out_case_cnt) as prs_comp_out_case_cnt
,max(ef.prs_comp_rsk_out_case_cnt) as prs_comp_rsk_out_case_cnt
,max(ef.apr_expc_readmit_cnt) as apr_expc_readmit_cnt
,max(ef.apr_readmit_cnt) as apr_readmit_cnt
,max(ef.apr_expc_day_cnt) as apr_expc_day_cnt
,max(ef.apr_expc_mrtly_cnt) as apr_expc_mrtly_cnt
,max(ef.apr_expc_compl_cnt) as apr_expc_compl_cnt
,max(ef.los_cnt) as los_cnt
,max(ef.compl_cnt) as compl_cnt
,max(CASE 
			WHEN hac.hac_cd = 'CMS-8'
				THEN hac.hac_pst_adm_ind
			ELSE 0
			END) AS cms8_hac_adm_ind
,max(CASE 
			WHEN hac.hac_cd = 'CMS-7'
				THEN hac.hac_pst_adm_ind
			ELSE 0
			END) AS cms7_hac_adm_ind
,max(CASE 
			WHEN hac.hac_cd = 'CMS-6'
				THEN hac.hac_pst_adm_ind
			ELSE 0
			END) AS cms6_hac_adm_ind
,max(CASE 
			WHEN hac.hac_cd = 'CMS-5'
				THEN hac.hac_pst_adm_ind
			ELSE 0
			END) AS cms5_hac_adm_ind
,max(pf.decubitus_ulcer_obs_num)   AS PSI03_pat_msr_obs_num
,max(pf.decubitus_ulcer_obs_den)   AS PSI03_pat_msr_obs_den
,max(pf.decubitus_ulcer_exp_num)   AS PSI03_pat_msr_exp_num
,max(pf.decubitus_ulcer_exp_den)  AS PSI03_pat_msr_exp_den
,max(pf.decubitus_ulcer_exp_var)  AS PSI03_pat_msr_exp_var

,max(pf.latro_pneumothorax_obs_num) AS PSI06_pat_msr_obs_num
,max(pf.latro_pneumothorax_obs_den) AS PSI06_pat_msr_obs_den
,max(pf.latro_pneumothorax_exp_num) AS PSI06_pat_msr_exp_num
,max(pf.latro_pneumothorax_exp_den) AS PSI06_pat_msr_exp_den
,max(pf.latro_pneumothorax_exp_var) AS PSI06_pat_msr_exp_var
	
,max(pf.postop_hip_frac_obs_num) AS PSI08_pat_msr_obs_num
,max(pf.postop_hip_frac_obs_den) AS PSI08_pat_msr_obs_den
,max(pf.postop_hip_frac_exp_num) AS PSI08_pat_msr_exp_num
,max(pf.postop_hip_frac_exp_den) AS PSI08_pat_msr_exp_den
,max(pf.postop_hip_frac_exp_var) AS PSI08_pat_msr_exp_var

,max(pf.postop_hem_obs_num) AS PSI09_pat_msr_obs_num
,max(pf.postop_hem_obs_den) AS PSI09_pat_msr_obs_den
,max(pf.postop_hem_obs_num) AS PSI09_pat_msr_exp_num
,max(pf.postop_hem_obs_den) AS PSI09_pat_msr_exp_den
,max(pf.postop_hem_obs_num) AS PSI09_pat_msr_exp_var
	
, max(pf.postop_derangemnts_obs_num) AS PSI10_pat_msr_obs_num
, max(pf.postop_derangemnts_obs_num) AS PSI10_pat_msr_obs_num_a
, max(pf.postop_derangemnts_obs_den) AS PSI10_pat_msr_obs_den
, max(pf.postop_derangemnts_exp_num) AS PSI10_pat_msr_exp_num
, max(pf.postop_derangemnts_exp_den) AS PSI10_pat_msr_exp_den
, max(pf.postop_derangemnts_exp_var) AS PSI10_pat_msr_exp_var
	
,max(pf.postop_resp_failure_obs_num) AS PSI11_pat_msr_obs_num
,max(pf.postop_resp_failure_obs_den) AS PSI11_pat_msr_obs_den
,max(pf.postop_resp_failure_exp_num) AS PSI11_pat_msr_exp_num
,max(pf.postop_resp_failure_exp_den) AS PSI11_pat_msr_exp_den
,max(pf.postop_resp_failure_exp_var) AS PSI11_pat_msr_exp_var

,max(pf.postop_pul_emb_obs_num) AS PSI12_pat_msr_obs_num
,max(pf.postop_pul_emb_obs_den) AS PSI12_pat_msr_obs_den
,max(pf.postop_pul_emb_exp_num) AS PSI12_pat_msr_exp_num
,max(pf.postop_pul_emb_exp_den) AS PSI12_pat_msr_exp_den
,max(pf.postop_pul_emb_exp_var) AS PSI12_pat_msr_exp_var

,max(pf.postop_sepsis_obs_num) AS PSI13_pat_msr_obs_num
,max(pf.postop_sepsis_obs_den) AS PSI13_pat_msr_obs_den
,max(pf.postop_sepsis_exp_num) AS PSI13_pat_msr_exp_num
,max(pf.postop_sepsis_exp_den) AS PSI13_pat_msr_exp_den
,max(pf.postop_sepsis_exp_var)  AS PSI13_pat_msr_exp_var

,max(pf.postop_wound_dehis_obs_num) AS PSI14_pat_msr_obs_num
,max(pf.postop_wound_dehis_obs_den) AS  PSI14_pat_msr_obs_den
,max(pf.postop_wound_dehis_exp_num) AS PSI14_pat_msr_exp_num
,max(pf.postop_wound_dehis_exp_den) AS PSI14_pat_msr_exp_den
,max(pf.postop_wound_dehis_exp_var) AS PSI14_pat_msr_exp_var

,max(pf.tech_diff_proc_obs_num) AS PSI15_pat_msr_obs_num
,max(pf.tech_diff_proc_obs_den) AS PSI15_pat_msr_obs_den
,max(pf.tech_diff_proc_exp_num) AS PSI15_pat_msr_exp_num
,max(pf.tech_diff_proc_exp_den) AS PSI15_pat_msr_exp_den
,max(pf.tech_diff_proc_exp_var) AS PSI15_pat_msr_exp_var

,max(case when (ef.dschrg_dt between m.eff_fm_dt and m.eff_to_dt) then m.gm_los else 0 end) as gmlos
,max(case when (ef.dschrg_dt between m.eff_fm_dt and m.eff_to_dt) then (ef.los_cnt - m.gm_los) else 0 end) as los_dys_abv_gmlos
,max(case when ( (ef.dschrg_dt between m.eff_fm_dt and m.eff_to_dt) and ef.los_cnt > m.gm_los) then (ef.los_cnt - m.gm_los) else 0 end) as dys_abv_gmlos

,max(case when ccs.cohrt_id = 'ccs_compl' then 1 else 0 end) as obsrv_compl
,max( case when (i.icd_cd = '0W8NXZZ' and ivd.encntr_num is not null)then 1 else 0  end ) as episiotomy_count
,max(case when ivd.encntr_num is not null then 1 else 0 end) as vag_delv_cnt
,max(case when ((ef.icd10_diag_code in ('J43.8','J43.9','J43.1','J42','J43.2','J44.0','J43.0','J44.9','J44.1','J41.8')) or 
(ef.icd10_diag_code in ('J96.00','J96.01','J96.02','J96.20','J96.21','J96.22','J96.90','J96.91','J96.92','R09.2') and (ic.icd_cd in ('J44.0','J44.1') and  ic.icd_cl_cd ='S')))and ef.age_val >=65 and ef.dschrg_sts_cd not in ('2','7')then 1 else 0 end) as copd_ind
,max(case when ef.icd10_diag_code in ('I50.43','I50.40','I50.23','I50.42','I50.21','I50.20','I13.0','I11.0','I50.30','I13.2','I50.32','I50.31','I50.9','I50.41','I50.33','I50.22','I50.1')
and i.icd_cd not in ('02YA0Z0','02YA0Z1','02HA0RS','02HA3QZ','02HA4QZ','02HA4RS','02HA0QZ','02HA4RZ','02HA3RZ','02HA0RZ','02HA3RS','02YA0Z2')and ef.age_val >=65 and ef.dschrg_sts_cd not in ('2','7')then 1 else 0 end) as hf_ind
,max(case when ic.icd_cd  in ('R65.20','R65.21') then 1 else 0 end ) as sepsis_icd_ind	
	
from pce_qe16_prd_qadv..encntr ef
inner join pce_qe16_prd_qadv..fcy_demog_ref fd on ef.fcy_num = fd.fcy_num
left join pce_qe16_prd_qadv..hsptl_acq_cdtn hac ON ef.encntr_num = hac.encntr_num and ef.fcy_num = hac.fcy_num
left join pce_qe16_prd_qadv..ptnt_saft_ind_obs_exp pf ON ef.encntr_num = pf.encntr_num and ef.fcy_num = pf.fcy_num and  pf.psi_iqi_version = '2019' and pf.method_type = 'STD'
left outer join pce_qe16_prd_qadv..ptnt_icd_proc_cd_asgnt i on ef.encntr_num = i.encntr_num and ef.fcy_num = i.fcy_num and i.icd_cd = '0W8NXZZ'
left join pce_qe16_prd_qadv..ptnt_icd_diag_cd_asgnt  ic on ef.encntr_num = ic.encntr_num and ef.fcy_num = ic.fcy_num
left join pce_qe16_prd_qadv..val_set_dim ccs on (replace(ic.icd_cd,'.','')) = ccs.cd and ic.icd_diag_poa_cd in ('N','U')  and ic.icd_cl_cd ='S'
left outer join pce_qe16_prd_qadv..stnd_adm_src_ref as ad on ef.adm_src_cd = ad.adm_src_cd
left outer join pce_qe16_prd_qadv..stnd_adm_type_ref as at on ef.adm_type_cd = at.adm_type_cd
left outer join pce_qe16_prd_qadv..dschrg_sts_ref as ds on ef.dschrg_sts_cd = ds.dschrg_sts_cd
--CODE CHANGE: 10/14/2021 : Ms_Drg_ref have the duplicates so replacing with ms_drg_dim
--left outer join pce_qe16_prd_qadv..ms_drg_ref as ms on ef.ms_drg_icd10 = ms.ms_drg_cd and ef.ms_drg_mdc_icd10 = ms.ms_drg_mdc_cd
left outer join pce_qe16_prd_qadv..ms_drg_dim as ms on ef.ms_drg_icd10 = ms.ms_drg_cd and ef.ms_drg_mdc_icd10 = ms.ms_drg_mdc_cd
left outer join pce_qe16_prd_qadv..pract_ref as pr on ef.fcy_attnd_pract_cd = pr.fcy_pract_cd and ef.fcy_num = pr.fcy_num
--left outer join pce_qe16_prd_qadv..icd_diag_cd_ref dr on ef.icd10_diag_code = dr.icd_diag_cd 
--left outer join pce_qe16_prd_qadv..icd_proc_cd_ref procd on ef.icd10_proc_code = procd.icd_proc_cd
left outer join dgns_dim_slp dr on ef.icd10_diag_code = dr.dgns_cd AND dr.dgns_icd_ver =  'ICD10'
left outer join pcd_dim_slp procd on ef.icd10_proc_code = procd.icd_pcd_cd AND procd.icd_ver = 'ICD10'
left join pce_qe16_prd_qadv..val_set_dim op on ef.fcy_pyr_cd = op.cd and op.cohrt_id = 'outcomes'
left join pce_qe16_prd_qadv..val_set_dim sp on ef.fcy_pyr_cd = sp.cd and sp.cohrt_id = 'sepsis'
left join pce_qe16_prd_qadv..val_set_dim rp on ef.fcy_pyr_cd = rp.cd and rp.cohrt_id = 'overall_readm'
left join pce_qe16_prd_qadv..val_set_dim mp on ef.fcy_pyr_cd = mp.cd and mp.cohrt_id = 'condn_readm'
left outer join pce_qe16_prd_qadv..ms_drg_dim_hist m on ef.ms_drg_icd10 = m.ms_drg_cd 
left outer join pce_qe16_prd_qadv..ptnt_icd_diag_cd_asgnt ivd on ef.encntr_num = ivd.encntr_num and ef.fcy_num = ivd.fcy_num and ivd.icd_cd <> 'O66.0' and ef.ms_drg_icd10 in (767,768,774,775)
left outer join temp_readm_excl tr on ef.encntr_num = tr.patient_id
left outer join pce_qe16_prd_qadv..pnt_of_orig_ref por on ef.pnt_of_orig_cd = por.pnt_of_orig_cd

where year(ef.dschrg_dt)>2017 
group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37);
------------------required fields--------------------
drop table stg_encntr_qs_anl_fct_vw if exists;
create table stg_encntr_qs_anl_fct_vw as 
(SELECT distinct 
	 pds.patient_id
	,pds.ptnt_nm
	,tluz.company_id
	,tluz.inptnt_ind as ip_ind
    ,tqa.encntr_num
	,tqa.mdcl_rcrd_num
	,pds.fcy_num
	,pds.zm_dschrg_dt
    ,pds.zm_adm_dt
	,tqa.dschrg_dt
	,tqa.adm_dt
	,pds.dischargeservice
	,tqa.fcy_pyr_cd
	,tqa.age_val
	,tqa.ptnt_cl_cd
	,tqa.stnd_ptnt_type_cd
	,tqa.fcy_nm
	,tqa.bed_cnt
	,tqa.ms_drg_icd10
	,tqa.ms_drg_mdc_icd10
	,tqa.adm_src_cd
	,tqa.nbrn_cnt
	,tqa.otlr_cd
	---drill doown columns------
	,tqa.ms_drg_descr as ms_drg_descr
	,tqa.ms_drg_bsn_line_descr
	,tqa.adm_src_descr as adm_src_descr
	,tqa.adm_type_descr as adm_type_descr
	,tqa.dschrg_sts_descr as dschrg_sts_descr
	,tqa.prim_diag_code as prim_diag_code
	,tqa.prim_diag_descr as prim_diag_descr
	,tqa.attnd_pract_nm as attnd_pract_nm
	----------------soi----------------------
	,tqa.apr_soi 
	,tqa.apr_soi_icd10 
	,tqa.apr_rom
    -------------------Payer exclusion Indicators---------------
	,tqa.outcomes_pyr_cd
    ,tqa.sepsis_pyr_cd
	,tqa.medicare_pyr_cd
	,tqa.redam_pyr_cd
	,tqa.attnd_pract_spcly_cd
    ,tqa.attnd_pract_spcly_descr
    ,tqa.pnt_of_orig_descr
	,pm.payor_group3
	,tqa.icd10_proc_code
    ,tqa.icd_proc_descr
	,tslf.e_svc_ln_nm
	  -------------------Observation Hours as per Rev Code----------------------
	,tb.qty as obsv_hours
	----------------------------vaginal delivery count ------------
	,tqa.vag_delv_cnt
	 --------------------lansing exclusion-----------------------
	 , le.exclusion_ind as prsnr_excl_ind
	 ,case when ((tqa.dschrg_dt >= '06/01/2018' and pm.payor_group3 in ('Medicare', 'RAC') and tqa.fcy_num = 'MI5020') or (pm.payor_group3 in ('Medicare', 'RAC') and  tqa.fcy_num <> 'MI5020') ) then 1 else 0  end AS mdcr_pyr_incl 
	 ,case when ((tqa.dschrg_dt >= '06/01/2018' and pm.payor_group3 = 'Hospice' and tqa.fcy_num = 'MI5020') or (pm.payor_group3 = 'Hospice' and  tqa.fcy_num <> 'MI5020') )then 0  else 1 end as hspc_pyr_excl
	 ,case when (tqa.dschrg_dt >= '06/01/2018' and pds.company_id = 'Lansing' and pds.dischargeservice in ('Behavioral Medicine','Rehabilitation'))then 0 else 1 end as lnsg_dschrg_svc_excl
	--, case when (tqa.dschrg_dt >= '10/01/2018' and pds.updateid = 'Incarcerated' and pds.company_id ='Lansing') then 0 else 1 end as crnr_lnsg_prsnr_excl_ind
--	, case when (pds.updateid = 'Incarcerated') then 0 else 1 end as crnr_lnsg_prsnr_excl_ind
	, case when (pds.updateid = 'Incarcerated' OR pds.sourcesystem='Incarcerated') then 0 else 1 end as crnr_lnsg_prsnr_excl_ind
		------------------------exclusion indicators------------
	,case when ((tqa.ms_drg_mdc_icd10  in ('837','838','839','846','847','848','849')) or (hspc_pyr_excl= 0)) then 0 else 1 end as mrtly_excl_ind
	,case when  ((tqa.ms_drg_mdc_icd10  in ('837','838','839','846','847','848','849')) or (hspc_pyr_excl = 0)) then 0 else 1 end as readm_excl_ind
	--,case when  (lnsg_hspc_pyr = 0) then 0 else 1 end as outcome_pyr_ind
    --,case when (sepsis_pyr_cd <> 'incl_cd' or lnsg_hspc_pyr = 0) then 0 else 1 end as sepsis_pyr_ind
	,pds.dschrg_svc_excl
	, case when (pds.dschrg_svc_excl = 0 or crnr_lnsg_prsnr_excl_ind = 0 or prsnr_excl_ind <> 1 or lnsg_dschrg_svc_excl =0) then 0 else 1 end as dsc_prsnr_excl_ind
	  ---------------drilldown date filter-------------
	 ,case when cast(month(tqa.dschrg_dt) as varchar(4)) not in ('10','11','12') then '0'||cast(month(tqa.dschrg_dt) as varchar(4)) else cast(month(tqa.dschrg_dt) as varchar(4)) end as month_dschrg
      ,cast(year(tqa.dschrg_dt) as varchar(4))||'-'||month_dschrg||'-'||'01'||' '||'00:00:00' as dschrg_dt_flt 
	 ---------------Focus Population Indicator----------------------------
	,tqa.HF_ind
	,tqa.COPD_ind
	,tqa.sepsis_icd_ind
	--CODE CHANGE : MLH-579 COVID COnfirmed cases indicator
	,CASE WHEN pcovid.covid_adm_ind = 1 THEN 0 ELSE 1 END as non_covid_case_ind 
        --FY22 CSA Complication Index changes starts
        ,CASE WHEN pcovid.dschrg_nbrn_ind = 1 THEN 1 ELSE 0 END as dschrg_nbrn_ind
        ,CASE WHEN pcovid.dschrg_rehab_ind = 1 THEN 1 ELSE 0 END as dschrg_rehab_ind
        ,CASE WHEN pcovid.dschrg_psych_ind = 1 THEN 1 ELSE 0 END as dschrg_psych_ind
        ,CASE WHEN pcovid.dschrg_ltcsnf_ind = 1 THEN 1 ELSE 0 END as dschrg_ltcsnf_ind
        ,CASE WHEN pcovid.dschrg_hospice_ind = 1 THEN 1 ELSE 0 END as dschrg_hospice_ind
        ,CASE WHEN pcovid.dschrg_spclcare_ind = 1 THEN 1 ELSE 0 END as dschrg_spclcare_ind
        ,CASE WHEN pcovid.dschrg_lipmip_ind = 1 THEN 1 ELSE 0 END as dschrg_lipmip_ind
        ,CASE WHEN pcovid.dschrg_acute_ind = 1 THEN 1 ELSE 0 END as dschrg_acute_ind
        ,CASE WHEN pcovid.dschrg_ind = 1 THEN 1 ELSE 0 END as dschrg_ind
        ,CASE WHEN (pcovid.dschrg_hospice_ind = 1 or
                    pcovid.dschrg_psych_ind = 1  or
                    pcovid.dschrg_rehab_ind = 1 or
                    pcovid.dschrg_ltcsnf_ind = 1 or
                    pcovid.covid_adm_ind = 1 )
        THEN 1 ELSE 0 END as csa_complication_excl_ind
        --FY22 CSA Complication Index changes ends
        --CODE CHANGE : 10/19/2021: Added fin dimensions from PRD_ENCNTR_ANL_FCT
	,pcovid.fin
	,pcovid.fin_str
	,pcovid.fin_w_rcur_seq
  ,max(tqa.ami_ind) as AMI_ind

	------------------Quality Outcomes-------------------------
	,max(tqa.mrtly_cnt) as mrtly_cnt
	,max(tqa.csa_expc_mrtly_cnt) as csa_expc_mrtly_cnt
	,max(tqa.csa_mort_scl_fctr) as csa_mort_scl_fctr
	,max(tqa.expc_mrtly_outc_case_cnt) as expc_mrtly_outc_case_cnt
	,max(tqa.re_adm_day_cnt) as re_adm_day_cnt
	,max(tqa.prs_readm_30day_rsk_out_case_cnt) AS prs_readm_30day_rsk_out_case_cnt
	,max(tqa.csa_expc_prs_readm_30day_rsk) AS csa_expc_prs_readm_30day_rsk
	,max(tqa.readmit_cnt_30dy_diag) as readmit_cnt_30dy_diag
	,max(tqa.readmit_unpln_pln_ind) as readmit_unpln_pln_ind
	,max(tqa.csa_obs_readm_rsk_adj_cnt) as csa_obs_readm_rsk_adj_cnt
	,max(tqa.acute_readmit_days_key) as acute_readmit_days_key
	,max(tqa.csa_readm_30day_scl_fctr) AS csa_readm_30day_scl_fctr
	,max(tqa.prs_comp_rsk_out_case_cnt) AS prs_cmplc_rsk_out_case_cnt
	,max(tqa.expc_los_outc_case_cnt) as expc_los_outc_case_cnt
	,max(tqa.csa_expc_los_cnt) as csa_expc_los_cnt
	,max(tqa.ln_los) as ln_los
	,max(tqa.csa_los_scl_fctr) as csa_los_scl_fctr
	,max(tqa.csa_cmp_scl_fctr) as csa_cmp_scl_fctr
	,max(tqa.csa_expc_compl_cnt) as csa_expc_compl_cnt
	,max(tqa.prs_comp_out_case_cnt) as prs_comp_out_case_cnt
	,max(tqa.prs_comp_rsk_out_case_cnt) as prs_comp_rsk_out_case_cnt
	,max(tqa.apr_expc_readmit_cnt) as apr_expc_readmit_cnt
	,max(tqa.apr_readmit_cnt) as apr_readmit_cnt
	,max(tqa.apr_expc_day_cnt) as apr_expc_day_cnt
	,max(tqa.apr_expc_mrtly_cnt) as apr_expc_mrtly_cnt
	,max(tqa.apr_expc_compl_cnt) as apr_expc_compl_cnt
	,max(tqa.los_cnt) as los_cnt
	,max(tqa.compl_cnt) as compl_cnt
		---------------------------------------------------HAC--------------------------------------------
	,max(tqa.cms8_hac_adm_ind) AS cms8_hac_adm_ind
	,max(tqa.cms7_hac_adm_ind) AS cms7_hac_adm_ind
	,max(tqa.cms6_hac_adm_ind) AS cms6_hac_adm_ind
	,max(tqa.cms5_hac_adm_ind) AS cms5_hac_adm_ind
	----------------------------------------------------PSI 03 Pressure Ulcer----------------------------------------------------
	,max(tqa.PSI03_pat_msr_obs_num)   AS PSI03_pat_msr_obs_num
    ,max(tqa.PSI03_pat_msr_obs_den)   AS PSI03_pat_msr_obs_den
    ,max(tqa.PSI03_pat_msr_exp_num)   AS PSI03_pat_msr_exp_num
    ,max(tqa.PSI03_pat_msr_exp_den)  AS PSI03_pat_msr_exp_den
    ,max(tqa.PSI03_pat_msr_exp_var)  AS PSI03_pat_msr_exp_var
	----------------------------------------------------PSI 06 Iatrogenic Pneumothorax----------------------------------------
	,max(tqa.PSI06_pat_msr_obs_num) AS PSI06_pat_msr_obs_num
	,max(tqa.PSI06_pat_msr_obs_den) AS PSI06_pat_msr_obs_den
	,max(tqa.PSI06_pat_msr_exp_num) AS PSI06_pat_msr_exp_num
	,max(tqa.PSI06_pat_msr_exp_den) AS PSI06_pat_msr_exp_den
	,max(tqa.PSI06_pat_msr_exp_var) AS PSI06_pat_msr_exp_var
	
	--------------------------------------PSI 08 In Hospital Fall with Hip Fracture-------------------------------------------
	,max(tqa.PSI08_pat_msr_obs_num) AS PSI08_pat_msr_obs_num
	,max(tqa.PSI08_pat_msr_obs_den) AS PSI08_pat_msr_obs_den
	,max(tqa.PSI08_pat_msr_exp_num) AS PSI08_pat_msr_exp_num
	,max(tqa.PSI08_pat_msr_exp_den) AS PSI08_pat_msr_exp_den
	,max(tqa.PSI08_pat_msr_exp_var) AS PSI08_pat_msr_exp_var
	
	-----------------------------------PSI 09 Perioperative Hemorrhage or Hematoma----------------------------------------
	,max(tqa.PSI09_pat_msr_obs_num) AS PSI09_pat_msr_obs_num
	,max(tqa.PSI10_pat_msr_obs_num_a) as PSI10_pat_msr_obs_num_a
	,max(tqa.PSI09_pat_msr_obs_den) AS PSI09_pat_msr_obs_den
	,max(tqa.PSI09_pat_msr_exp_num) AS PSI09_pat_msr_exp_num
	,max(tqa.PSI09_pat_msr_exp_den) AS PSI09_pat_msr_exp_den
	,max(tqa.PSI09_pat_msr_exp_var) AS PSI09_pat_msr_exp_var
	
	-------------------------------PSI 10 Postoperative Acute Kidney Injury Requiring Dialysis-------------------------------
	, max(tqa.PSI10_pat_msr_obs_num) AS PSI10_pat_msr_obs_num
    , max(tqa.PSI10_pat_msr_obs_den) AS PSI10_pat_msr_obs_den
	, max(tqa.PSI10_pat_msr_exp_den) AS PSI10_pat_msr_exp_den
	, max(tqa.PSI10_pat_msr_exp_var) AS PSI10_pat_msr_exp_var
	---------------------------------------------PSI 11 Postop Respiratory Failure----------------------------------------------
	,max(tqa.PSI11_pat_msr_obs_num) AS PSI11_pat_msr_obs_num
	,max(tqa.PSI11_pat_msr_obs_den) AS PSI11_pat_msr_obs_den
	,max(tqa.PSI11_pat_msr_exp_num) AS PSI11_pat_msr_exp_num
	,max(tqa.PSI11_pat_msr_exp_den) AS PSI11_pat_msr_exp_den
	,max(tqa.PSI11_pat_msr_exp_var) AS PSI11_pat_msr_exp_var

	---------------------------------------------PSI 12 Perioperative PE or DVT------------------------------------------------
	,max(tqa.PSI12_pat_msr_obs_num) AS PSI12_pat_msr_obs_num
    ,max(tqa.PSI12_pat_msr_obs_den) AS PSI12_pat_msr_obs_den
	,max(tqa.PSI12_pat_msr_exp_num) AS PSI12_pat_msr_exp_num
    ,max(tqa.PSI12_pat_msr_exp_den) AS PSI12_pat_msr_exp_den
	,max(tqa.PSI12_pat_msr_exp_var) AS PSI12_pat_msr_exp_var

	---------------------------------------------PSI 13 Postop Sepsis-------------------------------------------------------------
	,max(tqa.PSI13_pat_msr_obs_num) AS PSI13_pat_msr_obs_num
	,max(tqa.PSI13_pat_msr_obs_den) AS PSI13_pat_msr_obs_den
	,max(tqa.PSI13_pat_msr_exp_num) AS PSI13_pat_msr_exp_num
	,max(tqa.PSI13_pat_msr_exp_den) AS PSI13_pat_msr_exp_den
	,max(tqa.PSI13_pat_msr_exp_var)  AS PSI13_pat_msr_exp_var
	---------------------------------------------PSI 14 Postop Wound Dehiscence-----------------------------------------------
	,max(tqa.PSI14_pat_msr_obs_num) AS PSI14_pat_msr_obs_num
	,max(tqa.PSI14_pat_msr_obs_den) AS  PSI14_pat_msr_obs_den
	,max(tqa.PSI14_pat_msr_exp_num) AS PSI14_pat_msr_exp_num
	,max(tqa.PSI14_pat_msr_exp_den) AS PSI14_pat_msr_exp_den
	,max(tqa.PSI14_pat_msr_exp_var) AS PSI14_pat_msr_exp_var
	---------------------------------PSI 15  Accidental Puncture or Laceration-----------------------------------------------
	,max(tqa.PSI15_pat_msr_obs_num) AS PSI15_pat_msr_obs_num
	,max(tqa.PSI15_pat_msr_obs_den) AS PSI15_pat_msr_obs_den
	,max(tqa.PSI15_pat_msr_exp_num) AS PSI15_pat_msr_exp_num
	,max(tqa.PSI15_pat_msr_exp_den) AS PSI15_pat_msr_exp_den
	,max(tqa.PSI15_pat_msr_exp_var)  AS PSI15_pat_msr_exp_var
		--------------Geo mean LOS-----------------

	  , max(tqa.dys_abv_gmlos) as dys_abv_gmlos
	  ,max(tqa.los_dys_abv_gmlos) as los_dys_abv_gmlos
	  , max(tqa.gmlos) as gmlos
        ---- Complications using CCS---------------------
	,max(tqa.obsrv_compl) as obsrv_compl
	----------------------CPT Ed Indicator-----------------
	,max(case when pb.cpt_code in ('99281','99282','99283','99284','99285','99291') then 1 else 0 end) as ed_cpt_indicator 
	 
	-------------------epsiotiomy count----------------------------------
   ,max(tqa.episiotomy_count) as episiotomy_count
   ,max(tluz.patient_days) as ptnt_days
   ,max(tluz.blood_utlzn) as blood_utlzn
   ,max(tluz.lab_volume ) as lab_volume
   ,max(tluz.post_dt) as post_dt
   ,max(tluz.dschrg_dt) as dschrg_dt_lab
   ,max(case when pds.fcy_num='634342' then 1 else 0 end) as fcy_excl_ind
	
	from tmp_qadv tqa
	left join tmp_pdsc pds on pds.patient_id = tqa.encntr_num and pds.fcy_num = tqa.fcy_num
	left join pce_qe16_oper_prd_zoom..cv_patbill pb on pds.patient_id = pb.patient_id and pds.company_id = pb.company_id and pb.cpt_code in ('99281','99282','99283','99284','99285','99291')
    left join temp_obv tb on pds.patient_id = tb.patient_id and pds.company_id = tb.company_id
	left join pce_qe16_prd..lansing_prisoner_encounters le on pds.patient_id = le.encntr_num
	left join pce_qe16_oper_prd_zoom..cv_paymstr pm on pds.primary_payer_code = pm.payer_code and pds.company_id = pm.company_id
	left join tmp_lab_utlz tluz on pds.patient_id = tluz.patient_id and pds.fcy_num = tluz.fcy_num
	left join pce_qe16_slp_prd_dm..prd_svc_ln_anl_fct psvc on pds.patient_id = psvc.patient_id and pds.company_id=psvc.company_id
	left join tmp_svc_ln_fct tslf on tqa.encntr_num = tslf.encntr_num
	--CODE CHANGE: MLH-579 COVID Confirmed Cases Exclusion
	left join pce_qe16_slp_prd_dm..prd_encntr_anl_fct pcovid on pds.patient_id = pcovid.encntr_num and pds.company_id=pcovid.fcy_nm	
	WHERE  
	
	 year(pds.zm_dschrg_dt) >= 2017

     group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75
);


/*

***************Complication Index***************

*/


drop table stg_pqsd_cmplc_idnx_fct if exists;
create table stg_pqsd_cmplc_idnx_fct as 

select distinct 
scmp.fcy_nm,
scmp.fcy_num,
scmp.first_of_month,
scmp.end_of_month,
scmp.period,
replace(scmp.msr1,'%','') as cmplc_obsr_rt,
replace(scmp.msr2,'%','') as cmplc_expc_rt,
scmp.obsv_cases as obsr_cases,
scmp.outcome_cases as outc_cases,
scmp.msr_nm

from
(
SELECT name AS fcy_nm, 
CASE WHEN FCY_NM='McLaren Bay Region' THEN 'MI2191'
WHEN FCY_NM= 'McLaren Central Michigan' THEN 'MI2061'
when fcy_nm='McLaren Flint' then 'MI2302'
when fcy_nm='McLaren Greater Lansing' then 'MI5020'
when fcy_nm='McLaren Karmanos' then '634342'
when fcy_nm='McLaren Lapeer Region' then 'MI2001'
when fcy_nm='McLaren Macomb' then 'MI2048'
when fcy_nm='McLaren Northern Michigan' then '637619'
when fcy_nm='McLaren Oakland' then 'MI2055'
when fcy_nm='McLaren Port Huron' then '600816'
end as fcy_num,
cast(SUBSTRING(PERIOD,1,instr(period, '-')-1) as date) AS  first_of_month, 
cast(SUBSTRING(PERIOD,instr(period, '-')+1,LENGTH(PERIOD)) as date) AS end_of_month,
period,
CAST(CAST((CAST(replace(column__complications_of_care___result_, '%','') as Numeric(10,2))/100) as Numeric(10,4)) as VARCHAR(10)) as msr1,
CAST(CAST((CAST(replace(column__complications_of_care___comparison_, '%','') as Numeric(10,2))/100) as Numeric(10,4)) as VARCHAR(10)) as msr2,
column__complications_of_care___occurrences_ as obsv_cases, 
column__complications_of_care___cases_ as outcome_cases, 
 'Compl_R12M' as msr_nm
  FROM pce_qe16_misc_prd_lnd..mclaren_complications_12rollmonth_20200108_00000000

  union 
  
SELECT name AS fcy_nm, 
CASE WHEN FCY_NM='McLaren Bay Region' THEN 'MI2191'
WHEN FCY_NM= 'McLaren Central Michigan' THEN 'MI2061'
when fcy_nm='McLaren Flint' then 'MI2302'
when fcy_nm='McLaren Greater Lansing' then 'MI5020'
when fcy_nm='McLaren Karmanos' then '634342'
when fcy_nm='McLaren Lapeer Region' then 'MI2001'
when fcy_nm='McLaren Macomb' then 'MI2048'
when fcy_nm='McLaren Northern Michigan' then '637619'
when fcy_nm='McLaren Oakland' then 'MI2055'
when fcy_nm='McLaren Port Huron' then '600816'
end as fcy_num,
cast(date_trunc('month',PERIOD)as date) AS  first_of_month, 
 cast(last_day(PERIOD)as date) as end_of_month,
cast(period as varchar(20)) as period ,
CAST(CAST((CAST(replace(column__complications_of_care___result_, '%','') as Numeric(10,2))/100) as Numeric(10,4)) as VARCHAR(10)) as msr1,
CAST(CAST((CAST(replace(column__complications_of_care___comparison_, '%','') as Numeric(10,2))/100) as Numeric(10,4)) as VARCHAR(10)) as msr2,
--column__complications_of_care___result_ as msr1,
--column__complications_of_care___comparison_ as msr2,
column__complications_of_care___occurrences_ as obsv_cases, 
column__complications_of_care___cases_ as outcome_cases, 
'Compl-Monthly' as msr_nm
  FROM pce_qe16_misc_prd_lnd..mclaren_complications_1month_20200108_00000000
  
  UNION
  
  SELECT name AS fcy_nm, 
CASE WHEN FCY_NM='McLaren Bay Region' THEN 'MI2191'
WHEN FCY_NM= 'McLaren Central Michigan' THEN 'MI2061'
when fcy_nm='McLaren Flint' then 'MI2302'
when fcy_nm='McLaren Greater Lansing' then 'MI5020'
when fcy_nm='McLaren Karmanos' then '634342'
when fcy_nm='McLaren Lapeer Region' then 'MI2001'
when fcy_nm='McLaren Macomb' then 'MI2048'
when fcy_nm='McLaren Northern Michigan' then '637619'
when fcy_nm='McLaren Oakland' then 'MI2055'
when fcy_nm='McLaren Port Huron' then '600816'
end as fcy_num,
cast(SUBSTRING(PERIOD,1,instr(period, '-')-1) as date) AS  first_of_month, 
cast(SUBSTRING(PERIOD,instr(period, '-')+1,LENGTH(PERIOD)) as date) AS end_of_month,
period,
cast(column__complications_of_care___result_ as varchar(10)) as msr1,
cast(column__complications_of_care___comparison_ as varchar(10) )as msr2,
column__complications_of_care___occurrences_ as obsv_cases, 
column__complications_of_care___cases_ as outcome_cases, 
 'Compl_R12M' as msr_nm
  FROM pce_qe16_prd..mclaren_complications_12rollmonth_20200207_00000000
  
  union
  
    SELECT name AS fcy_nm, 
CASE WHEN FCY_NM='McLaren Bay Region' THEN 'MI2191'
WHEN FCY_NM= 'McLaren Central Michigan' THEN 'MI2061'
when fcy_nm='McLaren Flint' then 'MI2302'
when fcy_nm='McLaren Greater Lansing' then 'MI5020'
when fcy_nm='McLaren Karmanos' then '634342'
when fcy_nm='McLaren Lapeer Region' then 'MI2001'
when fcy_nm='McLaren Macomb' then 'MI2048'
when fcy_nm='McLaren Northern Michigan' then '637619'
when fcy_nm='McLaren Oakland' then 'MI2055'
when fcy_nm='McLaren Port Huron' then '600816'
end as fcy_num,
cast(date_trunc('month',cast(PERIOD as date))as date) AS  first_of_month, 
 cast(last_day(cast(PERIOD as date))as date) as end_of_month,
cast(period as varchar(20)) as period ,
cast(column__complications_of_care___result_ as varchar(10)) as msr1,
cast(column__complications_of_care___comparison_ as varchar(10)) as msr2,
column__complications_of_care___occurrences_ as obsv_cases, 
column__complications_of_care___cases_ as outcome_cases, 
'Compl-Monthly' as msr_nm
  FROM pce_qe16_prd..mclaren_complications_1month_20200201
  
  union 
  
   select 
fcy_nm, 
fcy_num,
first_of_month,
end_of_month,
period,
msr1,
msr2,
obsv_cases,
outcome_cases,
msr_nm
from 
(
  SELECT nm AS fcy_nm, 
CASE WHEN FCY_NM='McLaren Bay Region' THEN 'MI2191'
WHEN FCY_NM= 'McLaren Central Michigan' THEN 'MI2061'
when fcy_nm='McLaren Flint' then 'MI2302'
when fcy_nm='McLaren Greater Lansing' then 'MI5020'
when fcy_nm='McLaren Karmanos' then '634342'
when fcy_nm='McLaren Lapeer Region' then 'MI2001'
when fcy_nm='McLaren Macomb' then 'MI2048'
when fcy_nm='McLaren Northern Michigan' then '637619'
when fcy_nm='McLaren Oakland' then 'MI2055'
when fcy_nm='McLaren Port Huron' then '600816'
end as fcy_num,
cast(SUBSTRING(prd,1,instr(prd, '-')-1) as date) AS  first_of_month, 
cast(SUBSTRING(prd,instr(prd, '-')+1,LENGTH(prd)) as date) AS end_of_month,
cast(prd as varchar(20)) as period ,
cast(cast(cmplc_of_care_rslt as Numeric(10,4)) as varchar(10)) as msr1,
cast(CAST(cmplc_of_care_cmpr as Numeric(10,4)) as varchar(10)) as msr2,
cmplc_of_care_occr as obsv_cases, 
cmplc_of_care_case_cnt as outcome_cases, 
'Compl_R12M' as msr_nm,
row_number() over(partition by fcy_nm, prd ORDER BY rcrd_isrt_ts desc , rcrd_btch_Audt_id desc ) as rank_num
  FROM pce_qe16_misc_prd_lnd..cmplc_roll_12mo_smy_fct
  ) Z
  WHERE Z.rank_num = 1 
  union
 select 
fcy_nm, 
fcy_num,
first_of_month,
end_of_month,
period,
msr1,
msr2,
obsv_cases,
outcome_cases,
msr_nm
from 
(SELECT 
nm AS fcy_nm, 
CASE WHEN FCY_NM='McLaren Bay Region' THEN 'MI2191'
WHEN FCY_NM= 'McLaren Central Michigan' THEN 'MI2061'
when fcy_nm='McLaren Flint' then 'MI2302'
when fcy_nm='McLaren Greater Lansing' then 'MI5020'
when fcy_nm='McLaren Karmanos' then '634342'
when fcy_nm='McLaren Lapeer Region' then 'MI2001'
when fcy_nm='McLaren Macomb' then 'MI2048'
when fcy_nm='McLaren Northern Michigan' then '637619'
when fcy_nm='McLaren Oakland' then 'MI2055'
when fcy_nm='McLaren Port Huron' then '600816'
end as fcy_num,
cast(prd as date) AS  first_of_month, 
cast(prd as date) AS end_of_month,
cast(prd as varchar(20)) as period ,
cast(CAST(cmplc_of_care_rslt as Numeric(10,4)) as varchar(10)) as msr1,
cast(CAST(cmplc_of_care_cmpr as Numeric(10,4)) as varchar(10)) as msr2,
cmplc_of_care_occr as obsv_cases, 
cmplc_of_care_case_cnt as outcome_cases, 
'Compl_R12M' as msr_nm,
row_number() over(partition by fcy_nm, prd ORDER BY rcrd_isrt_ts desc , rcrd_btch_Audt_id desc ) as rank_num
  FROM pce_qe16_misc_prd_lnd..cmplc_1mo_smy_fct
  ) Z
 WHERE rank_num = 1 
) as scmp;
/*

***************Harm Events***************

*/
DROP TABLE tmp_cdr_dim IF EXISTS;
;create temp table  tmp_cdr_dim as
--CHANGE : Feb 2021 Done the required changes as per the ACO DB cdr_dim changes
(select distinct cd.frst_day_of_mo as mo_and_yr_abbr,
min(cd.cdr_dt) as cdr_dt,
1 as join_key
from  pce_ae00_aco_prd_cdr..cdr_dim cd
where cd.cdr_dt between add_months((select max(eq.dschrg_dt)from pce_qe16_slp_prd_dm..encntr_fct eq ),-36) and (select max(eq.dschrg_dt)from pce_qe16_slp_prd_dm..encntr_fct eq )
group by 1
);

------------------Pat Discharge---------------------------------------------
DROP TABLE tmp_pat IF EXISTS;
;create temp table  tmp_pat as
(select distinct p.patient_id
,p.company_id
,p.primary_payer_code 
,case when p.company_id = 'Bay' then 'MI2191' 
when p.company_id = 'Central' then 'MI2061' 
when p.company_id = 'Flint' then  'MI2302' 
when p.company_id ='Karmanos' then '634342' 
when p.company_id ='Lansing' then 'MI5020' 
when p.company_id = 'Lapeer' then 'MI2001'
when p.company_id = 'Macomb' then 'MI2048' 
when p.company_id ='Northern' then '637619' 
when p.company_id ='Oakland' then 'MI2055' 
when p.company_id ='Port Huron' then '600816'
else null end as fcy_num
,p.updateid
,to_date(p.discharge_date,'mmddyyyy') as zm_dschrg_dt
,to_date(p.admissionarrival_date,'mmddyyyy') as zm_adm_dt
,p.dischargeservice
,case when  p.dischargeservice in ('BEH','GERI','REHAB','PSYCH') then 0 else 1 end as dschrg_svc_excl
from 
pce_qe16_oper_prd_zoom..cv_patdisch p
inner join pce_qe16_oper_prd_zoom..cv_patbill pb on p.patient_id = pb.patient_id and p.company_id = pb.company_id and pb.cpt_code in ('99281','99282','99283','99284','99285','99291')
);


DROP TABLE tmp_nhsn_dummy IF EXISTS;
;create temp table  tmp_nhsn_dummy as
(select  distinct nf.fcy_nm, 
nf.fcy_num,
0 as cdiff,
0 as cauti_events,
0 as clabsi_events,
0 as mrsa,
0 as ssi_colo,
0 as ssi_hyst,
1 as join_key
from 

pce_qe16_misc_prd_lnd..nhsn_msr_fct nf 
);


DROP TABLE tmp_nhsn IF EXISTS;
;create temp table  tmp_nhsn as
(select distinct tnd.fcy_nm,
tnd.fcy_num,
tcd.mo_and_yr_abbr,
tcd.cdr_dt,
tnd.cdiff,
tnd.cauti_events,
tnd.clabsi_events,
tnd.mrsa,
tnd.ssi_colo,
tnd.ssi_hyst

from tmp_nhsn_dummy tnd
inner join tmp_cdr_dim tcd on tnd.join_key = tcd.join_key);


DROP TABLE tmp_nhsn_msr_fct_base IF EXISTS;
;create temp table  tmp_nhsn_msr_fct_base as 
(SELECT DISTINCT nf.fcy_nm, 
nf.fcy_num, 
--CHANGE : Feb 2021 Done the required changes as per the ACO DB cdr_dim changes
cd.frst_day_of_mo as mo_and_yr_abbr ,
date_trunc('month',cd.cdr_dt) as cdr_dt,
SUM(CASE when nf.c_diff  is not null then nf.c_diff else 0 end) as cdiff,
SUM(CASE when nf.cauti_events  is not null then nf.cauti_events  else 0 end) as cauti_events, 
SUM(CASE when nf.clabsi_events is not null then nf.clabsi_events else 0 end) as clabsi_events, 
SUM(CASE when nf.mrsa is not null then nf.mrsa else 0 end) as mrsa,
SUM(CASE when nf.ssi_colo is not null then nf.ssi_colo else 0 end) as ssi_colo, 
SUM(CASE when nf.ssi_hyst is not null then nf.ssi_hyst else 0 end) as ssi_hyst

FROM pce_qe16_misc_prd_lnd.prmradmp.nhsn_msr_fct nf 
inner JOIN  pce_ae00_aco_prd_cdr..cdr_dim  cd ON nf.event_dt = cd.cdr_dt
WHERE NF.fcy_nm NOT IN ('McLaren (excl))','McLaren')
group by 1,2,3,4

);

DROP TABLE tmp_nhsn_msr_fct_main IF EXISTS;
;create temp table  tmp_nhsn_msr_fct_main as
(select tnf.fcy_nm,
tnf.fcy_num,
tnf.cdr_dt,
'NHSN' as event_type,
sum(tnf.cdiff+tnf.cauti_events+tnf.clabsi_events+tnf.mrsa+tnf.ssi_colo+tnf.ssi_hyst) as event_cnt
from tmp_nhsn_msr_fct_base tnf
group by 1,2,3);

select * from tmp_nhsn_msr_fct_main where fcy_nm = 'McLaren Flint' and date(cdr_dt) between '2019-11-01' and '2020-10-01';

DROP TABLE tmp_hac_dummy IF EXISTS;
;create temp table  tmp_hac_dummy as 
(select distinct
fd.fcy_nm,
fd.fcy_num,
0 as cms5_hac_adm_ind,
0 as cms6_hac_adm_ind,
0 as cms7_hac_adm_ind,
1 as join_key
from pce_qe16_prd..fcy_prfl_dim fd);


DROP TABLE tmp_hac_base_a IF EXISTS;
;create temp table  tmp_hac_base_a as
(select distinct
thd.fcy_nm,
thd.fcy_num,
tcd.mo_and_yr_abbr,
tcd.cdr_dt as event_dt,
thd.cms5_hac_adm_ind,
thd.cms6_hac_adm_ind,
thd.cms7_hac_adm_ind
from tmp_hac_dummy thd
inner join tmp_cdr_dim tcd on thd.join_key = tcd.join_key);

--alter table stg_encntr_qs_anl_fct_vw_new rename to stg_encntr_qs_anl_fct_vw;

DROP TABLE tmp_hac_base_b IF EXISTS;
;create temp table  tmp_hac_base_b as 
(select 
eqv.fcy_nm,
eqv.fcy_num,
cd.frst_day_of_mo AS mo_and_yr_abbr,
date_trunc('month',cd.cdr_dt) as event_dt,
sum(eqv.cms5_hac_adm_ind) as cms5_hac_adm_ind,
sum(eqv.cms6_hac_adm_ind) as cms6_hac_adm_ind,
sum(eqv.cms7_hac_adm_ind) as cms7_hac_adm_ind

from stg_encntr_qs_anl_fct_vw eqv
inner join  pce_ae00_aco_prd_cdr..cdr_dim cd on eqv.dschrg_dt = cd.cdr_dt
group by 1,2,3,4
);

DROP TABLE tmp_zero_events_hac_base IF EXISTS;
;create temp table  tmp_zero_events_hac_base as
(select q.fcy_nm,
q.fcy_num,
q.mo_and_yr_abbr,
q.event_dt,
sum(q.cms5_hac_adm_ind) as cms5_hac_adm_ind,
sum(q.cms6_hac_adm_ind) as cms6_hac_adm_ind,
sum(q.cms7_hac_adm_ind) as cms7_hac_adm_ind
from
(select * from tmp_hac_base_a
union
select * from tmp_hac_base_b)as q
group by q.fcy_nm,
q.fcy_num,
q.mo_and_yr_abbr,
q.event_dt);

drop table tmp_hac_harm_fct if exists;
create temp table  tmp_hac_harm_fct as 
(select tzh.fcy_nm,
tzh.fcy_num,
tzh.event_dt,
'HAC' as event_type,
sum(tzh.cms5_hac_adm_ind+tzh.cms6_hac_adm_ind+tzh.cms7_hac_adm_ind) as event_cnt
from
tmp_zero_events_hac_base tzh
group by 1,2,3);

DROP TABLE tmp_psi_dummy IF EXISTS;
;create temp table  tmp_psi_dummy as

(select 
fdr.fcy_nm
,fdr.fcy_num
,0  AS PSI03_pat_msr_obs_rate
,0 AS PSI06_pat_msr_obs_rate
,0 AS PSI08_pat_msr_obs_rate
,0 AS PSI09_pat_msr_obs_rate
,0 AS PSI10_pat_msr_obs_rate
,0 AS PSI11_pat_msr_obs_rate
,0 AS PSI12_pat_msr_obs_rate
,0 AS PSI13_pat_msr_obs_rate
,0 AS PSI14_pat_msr_obs_rate
,0 AS PSI15_pat_msr_obs_rate
,1 as join_key
from pce_qe16_prd..fcy_prfl_dim fdr);

DROP TABLE tmp_psi_base_a IF EXISTS;
;create temp table  tmp_psi_base_a as
(select tpd.fcy_nm,
tpd.fcy_num,
tcd.mo_and_yr_abbr,
tcd.cdr_dt as event_dt,
tpd.PSI03_pat_msr_obs_rate,
tpd.PSI06_pat_msr_obs_rate,
tpd.PSI08_pat_msr_obs_rate,
tpd.PSI09_pat_msr_obs_rate,
tpd.PSI10_pat_msr_obs_rate,
tpd.PSI11_pat_msr_obs_rate,
tpd.PSI12_pat_msr_obs_rate,
tpd.PSI13_pat_msr_obs_rate,
tpd.PSI14_pat_msr_obs_rate,
tpd.PSI15_pat_msr_obs_rate
from tmp_psi_dummy tpd
inner join tmp_cdr_dim tcd on tpd.join_key = tcd.join_key);

DROP TABLE tmp_psi_base_b IF EXISTS;
;create temp table  tmp_psi_base_b as

(select 
pf.fcy_nm,
pf.fcy_num,
--CHANGE : Feb 2021 Done the required changes as per the ACO DB cdr_dim changes
cd.frst_day_of_mo as mo_and_yr_abbr,
date_trunc('month',pf.dschrg_dt) as event_dt,
sum(pf.psi03_pat_msr_obs_num/nullif(pf.psi03_pat_msr_obs_den,0)) as PSI03_pat_msr_obs_rate,
sum(pf.psi06_pat_msr_obs_num/nullif(pf.psi06_pat_msr_obs_den,0))as PSI06_pat_msr_obs_rate,
sum(pf.psi08_pat_msr_obs_num/nullif(pf.psi08_pat_msr_obs_den,0)) as PSI08_pat_msr_obs_rate, 
sum(pf.psi09_pat_msr_obs_num/nullif(pf.psi09_pat_msr_obs_den,0)) as PSI09_pat_msr_obs_rate,
sum(pf.psi10_pat_msr_obs_num/nullif(pf.psi10_pat_msr_obs_den,0)) as PSI10_pat_msr_obs_rate,
--sum(pf.psi11_pat_msr_obs_num/nullif(case when tp.patient_id is not null then null else pf.psi11_pat_msr_obs_den end,0)) as PSI11_pat_msr_obs_rate,
sum(pf.psi11_pat_msr_obs_num/nullif(pf.psi11_pat_msr_obs_den,0)) as PSI11_pat_msr_obs_rate,
sum(pf.psi12_pat_msr_obs_num/nullif(pf.psi12_pat_msr_obs_den,0)) as PSI12_pat_msr_obs_rate,
sum(pf.psi13_pat_msr_obs_num/nullif(pf.psi13_pat_msr_obs_den,0)) as PSI13_pat_msr_obs_rate,
--sum(pf.psi13_pat_msr_obs_num/nullif(case when tp.patient_id is not null then null else pf.psi13_pat_msr_obs_den end,0)) as PSI13_pat_msr_obs_rate,
sum(pf.psi14_pat_msr_obs_num/nullif(pf.psi14_pat_msr_obs_den,0)) as PSI14_pat_msr_obs_rate,
sum(pf.psi15_pat_msr_obs_num/nullif(pf.psi15_pat_msr_obs_den,0)) as PSI15_pat_msr_obs_rate
from stg_encntr_qs_anl_fct_vw pf
inner join  pce_ae00_aco_prd_cdr..cdr_dim cd on pf.dschrg_dt = cd.cdr_dt
left join tmp_pat tp on tp.patient_id = pf.encntr_num and tp.fcy_num=pf.fcy_num

group by 1,2,3,4);

DROP TABLE tmp_zero_event_psi_base IF EXISTS;
;create temp table  tmp_zero_event_psi_base as 
(select q.fcy_nm,
q.fcy_num,
q.mo_and_yr_abbr,
q.event_dt,
sum(q.PSI03_pat_msr_obs_rate)   AS PSI03_pat_msr_obs_rate,
sum(q.PSI06_pat_msr_obs_rate) AS PSI06_pat_msr_obs_rate,
sum(q.PSI08_pat_msr_obs_rate) AS PSI08_pat_msr_obs_rate,
sum(q.PSI09_pat_msr_obs_rate) AS PSI09_pat_msr_obs_rate,
sum(q.PSI10_pat_msr_obs_rate) AS PSI10_pat_msr_obs_rate,
sum(q.PSI11_pat_msr_obs_rate) AS PSI11_pat_msr_obs_rate,
sum(q.PSI12_pat_msr_obs_rate) AS PSI12_pat_msr_obs_rate,
sum(q.PSI13_pat_msr_obs_rate) AS PSI13_pat_msr_obs_rate,
sum(q.PSI14_pat_msr_obs_rate) AS PSI14_pat_msr_obs_rate,
sum(q.PSI15_pat_msr_obs_rate) AS PSI15_pat_msr_obs_rate
from 

(select * from tmp_psi_base_a
union 
select * from tmp_psi_base_b)as q
group by q.fcy_nm,
q.fcy_num,
q.mo_and_yr_abbr,
q.event_dt

);

drop table tmp_psi_harm_fct if exists;
;create temp table  tmp_psi_harm_fct as 
(select tpsi.fcy_nm,
tpsi.fcy_num,
tpsi.event_dt,
'PSI' as event_type,
sum(PSI03_pat_msr_obs_rate+PSI06_pat_msr_obs_rate+PSI08_pat_msr_obs_rate+PSI09_pat_msr_obs_rate+PSI10_pat_msr_obs_rate+PSI11_pat_msr_obs_rate+PSI12_pat_msr_obs_rate
+PSI13_pat_msr_obs_rate+PSI14_pat_msr_obs_rate+PSI15_pat_msr_obs_rate) as event_cnt
from
tmp_zero_event_psi_base tpsi
group by 1,2,3);

DROP TABLE temp_harm_events_fct_a IF EXISTS;
;create temp table  temp_harm_events_fct_a as
(SELECT * from tmp_nhsn_msr_fct_main

union

select * from tmp_hac_harm_fct

UNION

select * from tmp_psi_harm_fct);
drop table stg_harm_events_fct if exists;
create table stg_harm_events_fct as 
(select  thf.fcy_nm,
thf.fcy_num,
thf.cdr_dt as event_dt,
1 as join_key,
sum(thf.event_cnt) as events_cnt

from temp_harm_events_fct_a thf
group by 1,2,3);


DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt, 1 as join_key from stg_encntr_qs_anl_fct_vw ef);

----

drop table stg_TMP_HARM_EVENTS_FCT_new if exists;
create table stg_TMP_HARM_EVENTS_FCT_new AS 
(SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where hf.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and last_day(add_months((select max_dt from tmp_max_dschrg_dt),-2))
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
----and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2
);

----


drop table stg_TMP_HARM_EVENTS_FCT if exists;
create table stg_TMP_HARM_EVENTS_FCT AS 
(SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where hf.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and last_day(add_months((select max_dt from tmp_max_dschrg_dt),-2))
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
--and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

);

/*

***************Zero Events***************

*/
DROP TABLE tmp_cdr_dim IF EXISTS;
;create temp table  tmp_cdr_dim as
--CHANGE : Feb 2021 Done the required changes as per the ACO DB cdr_dim changes
(select distinct cd.frst_day_of_mo as mo_and_yr_abbr,
min(cd.cdr_dt) as cdr_dt,
1 as join_key
from  pce_ae00_aco_prd_cdr..cdr_dim cd
where cd.cdr_dt between add_months((select max(eq.dschrg_dt)from pce_qe16_slp_prd_dm..encntr_fct eq ),-36) and (select max(eq.dschrg_dt)from pce_qe16_slp_prd_dm..encntr_fct eq )
group by 1
);

------------------Pat Discharge---------------------------------------------
DROP TABLE tmp_pat IF EXISTS;
;create temp table  tmp_pat as
(select distinct p.patient_id
,p.company_id
,p.primary_payer_code 
,case when p.company_id = 'Bay' then 'MI2191' 
when p.company_id = 'Central' then 'MI2061' 
when p.company_id = 'Flint' then  'MI2302' 
when p.company_id ='Karmanos' then '634342' 
when p.company_id ='Lansing' then 'MI5020' 
when p.company_id = 'Lapeer' then 'MI2001'
when p.company_id = 'Macomb' then 'MI2048' 
when p.company_id ='Northern' then '637619' 
when p.company_id ='Oakland' then 'MI2055' 
when p.company_id ='Port Huron' then '600816'
else null end as fcy_num
,p.updateid
,to_date(p.discharge_date,'mmddyyyy') as zm_dschrg_dt
,to_date(p.admissionarrival_date,'mmddyyyy') as zm_adm_dt
,p.dischargeservice
,case when  p.dischargeservice in ('BEH','GERI','REHAB','PSYCH') then 0 else 1 end as dschrg_svc_excl
from 
pce_qe16_oper_prd_zoom..cv_patdisch p
inner join pce_qe16_oper_prd_zoom..cv_patbill pb on p.patient_id = pb.patient_id and p.company_id = pb.company_id and pb.cpt_code in ('99281','99282','99283','99284','99285','99291')
);




DROP TABLE tmp_nhsn_dummy IF EXISTS;
;create temp table  tmp_nhsn_dummy as
(select  distinct nf.fcy_nm, 
nf.fcy_num,
0 as cdiff,
0 as cauti_events,
0 as clabsi_events,
0 as mrsa,
0 as ssi_colo,
0 as ssi_hyst,
1 as join_key
from 

pce_qe16_misc_prd_lnd..nhsn_msr_fct nf 
);


DROP TABLE tmp_nhsn IF EXISTS;
;create temp table  tmp_nhsn as
(select distinct tnd.fcy_nm,
tnd.fcy_num,
tcd.mo_and_yr_abbr,
tcd.cdr_dt,
tnd.cdiff,
tnd.cauti_events,
tnd.clabsi_events,
tnd.mrsa,
tnd.ssi_colo,
tnd.ssi_hyst

from tmp_nhsn_dummy tnd
inner join tmp_cdr_dim tcd on tnd.join_key = tcd.join_key);


DROP TABLE tmp_nhsn_msr_fct_base IF EXISTS;
;create temp table  tmp_nhsn_msr_fct_base as 
(SELECT DISTINCT nf.fcy_nm, 
nf.fcy_num, 
--CHANGE : Feb 2021 Done the required changes as per the ACO DB cdr_dim changes
cd.frst_day_of_mo as mo_and_yr_abbr ,
date_trunc('month',cd.cdr_dt) as cdr_dt,
SUM(CASE when nf.c_diff  is not null then nf.c_diff else 0 end) as cdiff,
SUM(CASE when nf.cauti_events  is not null then nf.cauti_events  else 0 end) as cauti_events, 
SUM(CASE when nf.clabsi_events is not null then nf.clabsi_events else 0 end) as clabsi_events, 
SUM(CASE when nf.mrsa is not null then nf.mrsa else 0 end) as mrsa,
SUM(CASE when nf.ssi_colo is not null then nf.ssi_colo else 0 end) as ssi_colo, 
SUM(CASE when nf.ssi_hyst is not null then nf.ssi_hyst else 0 end) as ssi_hyst

FROM pce_qe16_misc_prd_lnd.prmradmp.nhsn_msr_fct nf 
inner JOIN  pce_ae00_aco_prd_cdr..cdr_dim cd ON nf.event_dt = cd.cdr_dt
WHERE NF.fCY_nm NOT IN ('McLaren (excl))','McLaren')
group by 1,2,3,4);

DROP TABLE tmp_nhsn_zero_event_fct_base IF EXISTS;
;create temp table  tmp_nhsn_zero_event_fct_base as 
(select q.fcy_nm,
q.fcy_num,
q.mo_and_yr_abbr, 
q.cdr_dt,
max(q.cdiff) as cdiff, 
max(q.cauti_events) as cauti_events, 
max(q.clabsi_events) as clabsi_events, 
max(q.mrsa) as mrsa, 
max(q.ssi_colo) as ssi_colo, 
max(q.ssi_hyst) as ssi_hyst
from 
(select * from tmp_nhsn
union 
select * from tmp_nhsn_msr_fct_base)as q
group by q.fcy_nm,
q.fcy_num,
q.mo_and_yr_abbr,
q.cdr_dt);

drop table stg_nhsn_zero_event_fct if exists;
create table stg_nhsn_zero_event_fct as 
(select distinct tnzb.fcy_nm,
tnzb.fcy_num,
tnzb.mo_and_yr_abbr,
tnzb.cdr_dt as event_dt,
case when tnzb.cdiff = 0 then 1 else 0 end as cdiff_zero_event_ind,
case when tnzb.cauti_events = 0 then 1 else 0 end as cauti_zero_event_ind,
case when tnzb.clabsi_events = 0 then 1 else 0 end as clabsi_zero_event_ind,
case when tnzb.mrsa = 0 then 1 else 0 end as mrsa_zero_event_ind,
case when tnzb.ssi_colo = 0 then 1 else 0 end as ssi_colo_zero_event_ind,
case when tnzb.ssi_hyst = 0 then 1 else 0 end as ssi_hyst_zero_event_ind
from

tmp_nhsn_zero_event_fct_base tnzb);

DROP TABLE tmp_hac_dummy IF EXISTS;
;create temp table  tmp_hac_dummy as 
(select distinct
fd.fcy_nm,
fd.fcy_num,
0 as cms5_hac_adm_ind,
0 as cms6_hac_adm_ind,
0 as cms7_hac_adm_ind,
1 as join_key
from pce_qe16_prd..fcy_prfl_dim fd);


DROP TABLE tmp_hac_base_a IF EXISTS;
;create temp table  tmp_hac_base_a as
(select distinct
thd.fcy_nm,
thd.fcy_num,
tcd.mo_and_yr_abbr,
tcd.cdr_dt as event_dt,
thd.cms5_hac_adm_ind,
thd.cms6_hac_adm_ind,
thd.cms7_hac_adm_ind
from tmp_hac_dummy thd
inner join tmp_cdr_dim tcd on thd.join_key = tcd.join_key);

DROP TABLE tmp_hac_base_b IF EXISTS;
;create temp table  tmp_hac_base_b as 
(select 
eqv.fcy_nm,
eqv.fcy_num,
--CHANGE : Feb 2021 Done the required changes as per the ACO DB cdr_dim changes
cd.frst_day_of_mo as mo_and_yr_abbr,
date_trunc('month',cd.cdr_dt) as event_dt,
sum(eqv.cms5_hac_adm_ind) as cms5_hac_adm_ind,
sum(eqv.cms6_hac_adm_ind) as cms6_hac_adm_ind,
sum(eqv.cms7_hac_adm_ind) as cms7_hac_adm_ind

from stg_encntr_qs_anl_fct_vw eqv
inner join  pce_ae00_aco_prd_cdr..cdr_dim cd on eqv.dschrg_dt = cd.cdr_dt
group by 1,2,3,4
);

DROP TABLE tmp_zero_events_hac_base IF EXISTS;
;create temp table  tmp_zero_events_hac_base as
(select q.fcy_nm,
q.fcy_num,
q.mo_and_yr_abbr,
q.event_dt,
sum(q.cms5_hac_adm_ind) as cms5_hac_adm_ind,
sum(q.cms6_hac_adm_ind) as cms6_hac_adm_ind,
sum(q.cms7_hac_adm_ind) as cms7_hac_adm_ind
from
(select * from tmp_hac_base_a
union
select * from tmp_hac_base_b)as q
group by q.fcy_nm,
q.fcy_num,
q.mo_and_yr_abbr,
q.event_dt);

drop table stg_hac_zero_event_fct if exists;
create table stg_hac_zero_event_fct as 
(select distinct tznhb.fcy_nm,
tznhb.fcy_num,
tznhb.mo_and_yr_abbr,
tznhb.event_dt,
case when tznhb.cms5_hac_adm_ind = 0 then 1 else 0 end as cms5_hac_adm_ind,
case when tznhb.cms6_hac_adm_ind = 0 then 1 else 0 end as cms6_hac_adm_ind,
case when tznhb.cms7_hac_adm_ind = 0 then 1 else 0 end as cms7_hac_adm_ind
from tmp_zero_events_hac_base tznhb);

DROP TABLE tmp_psi_dummy IF EXISTS;
;create temp table  tmp_psi_dummy as

(select 
fdr.fcy_nm
,fdr.fcy_num
,0  AS PSI03_pat_msr_obs_rate
,0 AS PSI06_pat_msr_obs_rate
,0 AS PSI08_pat_msr_obs_rate
,0 AS PSI09_pat_msr_obs_rate
,0 AS PSI10_pat_msr_obs_rate
,0 AS PSI11_pat_msr_obs_rate
,0 AS PSI12_pat_msr_obs_rate
,0 AS PSI13_pat_msr_obs_rate
,0 AS PSI14_pat_msr_obs_rate
,0 AS PSI15_pat_msr_obs_rate
,1 as join_key
from pce_qe16_prd..fcy_prfl_dim fdr);

DROP TABLE tmp_psi_base_a IF EXISTS;
;create temp table  tmp_psi_base_a as
(select tpd.fcy_nm,
tpd.fcy_num,
tcd.mo_and_yr_abbr,
tcd.cdr_dt as event_dt,
tpd.PSI03_pat_msr_obs_rate,
tpd.PSI06_pat_msr_obs_rate,
tpd.PSI08_pat_msr_obs_rate,
tpd.PSI09_pat_msr_obs_rate,
tpd.PSI10_pat_msr_obs_rate,
tpd.PSI11_pat_msr_obs_rate,
tpd.PSI12_pat_msr_obs_rate,
tpd.PSI13_pat_msr_obs_rate,
tpd.PSI14_pat_msr_obs_rate,
tpd.PSI15_pat_msr_obs_rate
from tmp_psi_dummy tpd
inner join tmp_cdr_dim tcd on tpd.join_key = tcd.join_key);


DROP TABLE tmp_psi_base_b IF EXISTS;
;create temp table  tmp_psi_base_b as

(select 
pf.fcy_nm,
pf.fcy_num,
--CHANGE : Feb 2021 Done the required changes as per the ACO DB cdr_dim changes
cd.frst_day_of_mo as mo_and_yr_abbr,
date_trunc('month',pf.dschrg_dt) as event_dt,
sum(pf.psi03_pat_msr_obs_num/nullif(pf.psi03_pat_msr_obs_den,0)) as PSI03_pat_msr_obs_rate,
sum(pf.psi06_pat_msr_obs_num/nullif(pf.psi06_pat_msr_obs_den,0))as PSI06_pat_msr_obs_rate,
sum(pf.psi08_pat_msr_obs_num/nullif(pf.psi08_pat_msr_obs_den,0)) as PSI08_pat_msr_obs_rate, 
sum(pf.psi09_pat_msr_obs_num/nullif(pf.psi09_pat_msr_obs_den,0)) as PSI09_pat_msr_obs_rate,
sum(pf.psi10_pat_msr_obs_num/nullif(pf.psi10_pat_msr_obs_den,0)) as PSI10_pat_msr_obs_rate,
sum(pf.psi11_pat_msr_obs_num/nullif(case when tp.patient_id is not null then null else pf.psi11_pat_msr_obs_den end,0)) as PSI11_pat_msr_obs_rate,
sum(pf.psi12_pat_msr_obs_num/nullif(pf.psi12_pat_msr_obs_den,0)) as PSI12_pat_msr_obs_rate,
sum(pf.psi13_pat_msr_obs_num/nullif(case when tp.patient_id is not null then null else pf.psi13_pat_msr_obs_den end,0)) as PSI13_pat_msr_obs_rate,
sum(pf.psi14_pat_msr_obs_num/nullif(pf.psi14_pat_msr_obs_den,0)) as PSI14_pat_msr_obs_rate,
sum(pf.psi15_pat_msr_obs_num/nullif(pf.psi15_pat_msr_obs_den,0)) as PSI15_pat_msr_obs_rate

from stg_encntr_qs_anl_fct_vw pf
inner join  pce_ae00_aco_prd_cdr..cdr_dim cd on pf.dschrg_dt = cd.cdr_dt
left join tmp_pat tp on tp.patient_id = pf.encntr_num and tp.fcy_num=pf.fcy_num

group by 1,2,3,4
);

DROP TABLE tmp_zero_event_psi_base IF EXISTS;
;create temp table  tmp_zero_event_psi_base as 
(select q.fcy_nm,
q.fcy_num,
q.mo_and_yr_abbr,
q.event_dt,
sum(q.PSI03_pat_msr_obs_rate)   AS PSI03_pat_msr_obs_rate,
sum(q.PSI06_pat_msr_obs_rate) AS PSI06_pat_msr_obs_rate,
sum(q.PSI08_pat_msr_obs_rate) AS PSI08_pat_msr_obs_rate,
sum(q.PSI09_pat_msr_obs_rate) AS PSI09_pat_msr_obs_rate,
sum(q.PSI10_pat_msr_obs_rate) AS PSI10_pat_msr_obs_rate,
sum(q.PSI11_pat_msr_obs_rate) AS PSI11_pat_msr_obs_rate,
sum(q.PSI12_pat_msr_obs_rate) AS PSI12_pat_msr_obs_rate,
sum(q.PSI13_pat_msr_obs_rate) AS PSI13_pat_msr_obs_rate,
sum(q.PSI14_pat_msr_obs_rate) AS PSI14_pat_msr_obs_rate,
sum(q.PSI15_pat_msr_obs_rate) AS PSI15_pat_msr_obs_rate
from 

(select * from tmp_psi_base_a
union 
select * from tmp_psi_base_b)as q
group by q.fcy_nm,
q.fcy_num,
q.mo_and_yr_abbr,
q.event_dt

);

drop table stg_psi_zero_event_fct if exists;
create table stg_psi_zero_event_fct as 
(select distinct 
tzp.fcy_nm,
tzp.fcy_num,
tzp.mo_and_yr_abbr,
tzp.event_dt,
case when tzp.PSI03_pat_msr_obs_rate = 0 then 1 else 0 end as PSI03_pat_msr_obs_rate,
case when tzp.PSI06_pat_msr_obs_rate = 0 then 1 else 0 end as PSI06_pat_msr_obs_rate,
case when tzp.PSI08_pat_msr_obs_rate = 0 then 1 else 0 end as PSI08_pat_msr_obs_rate,
case when tzp.PSI09_pat_msr_obs_rate = 0 then 1 else 0 end as PSI09_pat_msr_obs_rate,
case when tzp.PSI10_pat_msr_obs_rate = 0 then 1 else 0 end as PSI10_pat_msr_obs_rate,
case when tzp.PSI11_pat_msr_obs_rate = 0 then 1 else 0 end as PSI11_pat_msr_obs_rate,
case when tzp.PSI12_pat_msr_obs_rate = 0 then 1 else 0 end as PSI12_pat_msr_obs_rate,
case when tzp.PSI13_pat_msr_obs_rate = 0 then 1 else 0 end as PSI13_pat_msr_obs_rate,
case when tzp.PSI14_pat_msr_obs_rate = 0 then 1 else 0 end as PSI14_pat_msr_obs_rate,
case when tzp.PSI15_pat_msr_obs_rate = 0 then 1 else 0 end as PSI15_pat_msr_obs_rate
from tmp_zero_event_psi_base tzp
);

drop table stg_zero_event_fct if exists;
create table stg_zero_event_fct as

(SELECT fcy_nm, fcy_num, mo_and_yr_abbr, event_dt, psi03_pat_msr_obs_rate+psi06_pat_msr_obs_rate+psi08_pat_msr_obs_rate+psi09_pat_msr_obs_rate+psi10_pat_msr_obs_rate+psi11_pat_msr_obs_rate+
psi12_pat_msr_obs_rate+psi13_pat_msr_obs_rate+psi14_pat_msr_obs_rate+psi15_pat_msr_obs_rate as msr_val,
'PSI Zero Events' as msr_nm
FROM stg_psi_zero_event_fct

union 

SELECT distinct fcy_nm, 
fcy_num, 
mo_and_yr_abbr, 
event_dt, 
cdiff_zero_event_ind+cauti_zero_event_ind+clabsi_zero_event_ind+mrsa_zero_event_ind+ssi_colo_zero_event_ind+ssi_hyst_zero_event_ind as msr_val,
'NHSN Zero Events' as msr_nm
FROM  stg_nhsn_zero_event_fct

union

SELECT distinct fcy_nm, 
fcy_num, 
mo_and_yr_abbr, 
event_dt,
cms5_hac_adm_ind+cms6_hac_adm_ind+cms7_hac_adm_ind as msr_val,
'HAC Zero Events' as msr_nm
from  stg_hac_zero_event_fct 
);

--Zero Events Fact for Clinical Outcome Score
DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt, 1 as join_key from stg_encntr_qs_anl_fct_vw ef);

drop table stg_tmp_zero_events_fct if exists;
create table stg_TMP_ZERO_EVENTS_FCT AS 
(SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2



UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM  stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
--and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2
);
---
select * from stg_tmp_zero_events_fct where rpt_dt = '2020-06-01 00:00:00' and fcy_nm = 'McLaren Greater Lansing';
---
/*

Quality Advisor Measure needed for Calculating Clinical Outcome Score

*/

----Mortality Index for Clinical Outcome Score

drop table tmp_mort if exists;
;create temp table  tmp_mort as 
(SELECT ef.encntr_num,
ef.fcy_nm,
ef.fcy_num,
ef.stnd_ptnt_type_cd,
ef.dschrg_dt,
CASE WHEN EF.mrtly_excl_ind=1 THEN ef.expc_mrtly_outc_case_cnt END AS mrtly_outc_case_w_excl, 
case when ef.adm_src_cd<>'4' then ef.mrtly_cnt else 0 end as mrtly_cnt_w_excl,
ef.mrtly_excl_ind,
ef.hspc_pyr_excl,
ef.apr_expc_mrtly_cnt,
ef.non_covid_case_ind,
1 as join_key,
( case when mrtly_outc_case_w_excl=0 then null 
 when mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end) as mort_obs_cnt,
  (case when mrtly_outc_case_w_excl=0 then null
 when mrtly_excl_ind=1 and hspc_pyr_excl =1 then apr_expc_mrtly_cnt
 else null end
 ) as mort_expc_cnt
FROM stg_encntr_qs_anl_fct_vw ef
where ef.stnd_ptnt_type_cd = '08' 
AND ef.non_covid_case_ind = 1 AND ef.dsc_prsnr_excl_ind =1 
)
;

DROP TABLE stg_tmp_mrtly_ind_r12m_fct IF EXISTS;
CREATE TABLE stg_tmp_mrtly_ind_r12m_fct as 
with grouped_data as 
(
  select fcy_nm, fcy_num,
  date_trunc('month',dschrg_dt) as report_month,
  count(distinct encntr_num) as total_encntrs,
(sum( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_agg_cases,
(sum( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_agg_cases
from tmp_mort tm where fcy_nm like  '%Bay%' and date_trunc('month',dschrg_dt) between '2019-06-01' and '2020-05-01' 
group by 1,2,3
order by 1 ASC,2 asc,3 desc
)
select fcy_nm,
fcy_num,
date(report_month) as rpt_dt,
sum(mort_obs_agg_cases) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as mort_obs_cases,
sum(mort_expc_agg_cases) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as mort_expc_cases
from grouped_data
order by 1 ASC,2 asc,3 desc;


drop table stg_tmp_mrtly_ind  IF exists; 
 create table stg_tmp_mrtly_ind as 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',tmd.max_dt)) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 UNION
 
  select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2

 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 ;
 
 select * from  stg_tmp_mrtly_ind;

-----Rest Of the SQLS
--CODE CHANGE: 08/24/2020 MLH-579 Excluding COVID _cases 
drop table stg_tmp_mrtly_ind_wo_covid if exists;
 create table stg_tmp_mrtly_ind_wo_covid as 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',tmd.max_dt)) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
-- and fcy_nm <>'McLaren Greater Lansing' 
and non_covid_case_ind = 1 
 group by 1,2
 
 UNION
 
  select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
 --and fcy_nm <>'McLaren Greater Lansing'  
 and non_covid_case_ind = 1 
 group by 1,2

 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
-- and fcy_nm <>'McLaren Greater Lansing' 
and non_covid_case_ind = 1 
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
 --and fcy_nm <>'McLaren Greater Lansing'  
 and non_covid_case_ind = 1 
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
 --and fcy_nm <>'McLaren Greater Lansing'
 and non_covid_case_ind = 1 
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
 --and fcy_nm <>'McLaren Greater Lansing' 
 and non_covid_case_ind = 1 
 group by 1,2
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
 --and fcy_nm <>'McLaren Greater Lansing'  
 and non_covid_case_ind = 1 
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
-- and fcy_nm <>'McLaren Greater Lansing'    
and non_covid_case_ind = 1 
 group by 1,2
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
-- and fcy_nm <>'McLaren Greater Lansing'     
and non_covid_case_ind = 1 
 group by 1,2
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
-- and fcy_nm <>'McLaren Greater Lansing'
and non_covid_case_ind = 1 
 group by 1,2
 
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
 --and fcy_nm <>'McLaren Greater Lansing'     
 and non_covid_case_ind = 1 
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
 --and fcy_nm <>'McLaren Greater Lansing'  
 and non_covid_case_ind = 1 
 group by 1,2
 
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.mrtly_excl_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key=tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
 --and fcy_nm <>'McLaren Greater Lansing'  
 and non_covid_case_ind = 1 
 group by 1,2
 ;
 
 drop table tmp_mort if exists;
;create temp table  tmp_mort as 
(
SELECT ef.encntr_num,
ef.fcy_nm,
ef.fcy_num,
ef.stnd_ptnt_type_cd,
ef.dschrg_dt,
CASE WHEN EF.mrtly_excl_ind=1 THEN ef.expc_mrtly_outc_case_cnt END AS mrtly_outc_case_w_excl, 
case when ef.adm_src_cd<>'4' then ef.mrtly_cnt else 0 end as mrtly_cnt_w_excl,
ef.mrtly_excl_ind,
ef.hspc_pyr_excl,
ef.apr_expc_mrtly_cnt,
ef.sepsis_icd_ind,
ef.non_covid_case_ind,
1 as join_key

FROM stg_encntr_qs_anl_fct_vw ef
where ef.stnd_ptnt_type_cd = '08'  and  ef.ms_drg_icd10 not in ('837','838','846','847','848') and ef.dsc_prsnr_excl_ind=1

);
DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt, 1 as join_key from stg_encntr_qs_anl_fct_vw ef);
 
 drop table stg_tmp_sep_mrtly_ind if exists;
 create table stg_tmp_sep_mrtly_ind as
 (select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',tmd.max_dt)) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 UNION
 
  select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
-- and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 union
 
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
  union
 
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 UNION

 
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
-- and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
  UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
-- and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
  UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
  UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
-- and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
  
  UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
   UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
  
   UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2

 );
 
 ----->>CODE CHANGE : Excluding COVID Cases
 
 drop table stg_tmp_sep_mrtly_ind_wo_covid if exists;
 create table stg_tmp_sep_mrtly_ind_wo_covid as
 (select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',tmd.max_dt)) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
 --and fcy_nm <>'McLaren Greater Lansing' 
 AND non_covid_case_ind = 1
 group by 1,2
 
 UNION
 
  select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
 AND  non_covid_case_ind = 1
 group by 1,2
 
 
 union
 
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
 AND  non_covid_case_ind = 1
 group by 1,2
 
 
  union
 
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
 --and fcy_nm <>'McLaren Greater Lansing'  
 AND non_covid_case_ind = 1
 group by 1,2
 
 UNION
 
 
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
 --and fcy_nm <>'McLaren Greater Lansing'  
 AND non_covid_case_ind = 1
 group by 1,2
 
 
  UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
 --and fcy_nm <>'McLaren Greater Lansing'  
 AND non_covid_case_ind = 1
 group by 1,2
 
 
 UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
 AND  non_covid_case_ind = 1
 group by 1,2
 
 UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
 --and fcy_nm <>'McLaren Greater Lansing'  
 AND non_covid_case_ind = 1
 group by 1,2
 
  UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
 --and fcy_nm <>'McLaren Greater Lansing'  
 AND non_covid_case_ind = 1
 group by 1,2
 
 
  UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
-- and fcy_nm <>'McLaren Greater Lansing'  
AND non_covid_case_ind = 1
 group by 1,2
 
 
  
  UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
 AND  non_covid_case_ind = 1
 group by 1,2
 
   UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
 AND  non_covid_case_ind = 1
 group by 1,2
 
  
   UNION
 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null 
 when tm.hspc_pyr_excl=1 and tm.sepsis_icd_ind=1  then mrtly_cnt_w_excl
 else null
 end)) as mort_obs_rt,
 
(AVG( case when tm.mrtly_outc_case_w_excl=0 then null
 when tm.sepsis_icd_ind=1 and tm.hspc_pyr_excl =1 then tm.apr_expc_mrtly_cnt
 else null end
 )) as mort_expc_rt,
 mort_obs_rt/mort_expc_rt as mort_oe_rt

 
 from tmp_mort tm
 left join tmp_max_dschrg_dt tmd on tm.join_key = tmd.join_key
 where tm.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
-- and fcy_nm <>'McLaren Greater Lansing'  
AND non_covid_case_ind = 1
 group by 1,2

 );
 
 drop table tmp_readm if exists;
;create temp table  tmp_readm as 
(
SELECT distinct ef.encntr_num,
ef.fcy_nm,
ef.fcy_num,
ef.stnd_ptnt_type_cd,
ef.dschrg_dt,
ef.non_covid_case_ind,
max(EF.readm_excl_ind) as readm_excl_ind,
max(ef.prs_readm_30day_rsk_out_case_cnt) as readm_outc_case,  --prs_readm_30day_rsk_out_case_cnt
max(case when EF.readm_excl_ind=1 then ef.prs_readm_30day_rsk_out_case_cnt else null end) as readm_outc_case_w_excl,
max(ef.acute_readmit_days_key) as acute_readmit_days_key,
max(EF.csa_obs_readm_rsk_adj_cnt) as csa_obs_readm_rsk_adj_cnt,
max(ef.csa_expc_prs_readm_30day_rsk) as csa_expc_prs_readm_30day_rsk,  -- csa_expc_prs_readm_30day_rsk
1 as join_key
FROM stg_encntr_qs_anl_fct_vw ef
where ef.stnd_ptnt_type_cd = '08' AND EF.readm_excl_ind =1  AND EF.dsc_prsnr_excl_ind = 1 AND EF.non_covid_case_ind= 1
group by 1,2,3,4,5,6
);


-->
 select tr.fcy_nm,
 date_trunc('month',dschrg_dt),
 sum(tr.readm_outc_case) as readm_out_ct,
 sum(tr.csa_expc_prs_readm_30day_rsk) as readm_exp_ct,
 (sum(case when (readm_outc_case=0) then null WHEN  tr.readm_excl_ind=1 THEN  csa_expc_prs_readm_30day_rsk end)) as csa_readm_expc_ct,
(SUM( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_ct,
 
(SUM( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt
 from tmp_readm tr WHERE 
 tr.readm_excl_ind=1 AND 
 --date(dschrg_dt) between '2019-08-01' and '2020-07-31' AND fcy_nm  ='McLaren Central Michigan' --'McLaren Bay Region'  --'McLaren Port Huron Hospital'
 date(dschrg_dt) between '2020-05-01' and '2021-04-30'  AND fcy_nm  ='McLaren Bay Region' --'McLaren Bay Region' 
 --AND fcy_nm  ='McLaren Oakland' --'McLaren Bay Region'  --'McLaren Port Huron Hospital'
  GROUP BY 1,2;
  
  --select * from tmp_readm LIMIT 10; 
  --select * from  stg_tmp_readm_ind;
  
--  

drop table tmp_max_dschrg_dt if exists;
;create temp table  tmp_max_dschrg_dt as 
 (select max(ef.dschrg_dt) as max_dt,1 as join_key from stg_encntr_qs_anl_fct_vw ef);
 
 
 drop table stg_tmp_readm_ind if exists;
 create table stg_tmp_readm_ind as 
  (select tr.fcy_nm,
 tr.fcy_num,
 max(date_trunc('month',tmd.max_dt)) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
 ----and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
  UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
  UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
  UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
  UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
 --and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2

 );
 
 ---->--CODE CHANGE: 08/24/2020 MLH-579 Excluding COVID _cases 
 
 drop table stg_tmp_readm_ind_wo_covid if exists;
 create table stg_tmp_readm_ind_wo_covid as 
  (select tr.fcy_nm,
 tr.fcy_num,
 max(date_trunc('month',tmd.max_dt)) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
 ----and fcy_nm <>'McLaren Greater Lansing' 
 AND non_covid_case_ind = 1
 group by 1,2
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
 --and fcy_nm <>'McLaren Greater Lansing' 
 AND non_covid_case_ind = 1
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
 AND  non_covid_case_ind = 1
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
 --and fcy_nm <>'McLaren Greater Lansing'  
 AND non_covid_case_ind = 1
 group by 1,2
 
 
  UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
 --and fcy_nm <>'McLaren Greater Lansing' 
 AND non_covid_case_ind = 1
 group by 1,2
 
 
  UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
 --and fcy_nm <>'McLaren Greater Lansing'     
 AND non_covid_case_ind = 1
 group by 1,2
 
  UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
-- and fcy_nm <>'McLaren Greater Lansing'    
AND non_covid_case_ind = 1
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
 --and fcy_nm <>'McLaren Greater Lansing'   
 AND non_covid_case_ind = 1
 group by 1,2
 
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
 --and fcy_nm <>'McLaren Greater Lansing'   
 AND non_covid_case_ind = 1
 group by 1,2
 
  UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
 --and fcy_nm <>'McLaren Greater Lansing'    
 AND non_covid_case_ind = 1
 group by 1,2
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
 --and fcy_nm <>'McLaren Greater Lansing'   
 AND non_covid_case_ind = 1
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
 --and fcy_nm <>'McLaren Greater Lansing' 
 AND non_covid_case_ind = 1
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
(AVG( case when tr.readm_outc_case=0 then null 
 when tr.readm_excl_ind=1 and tr.acute_readmit_days_key>=0 and tr.acute_readmit_days_key <31 then tr.csa_obs_readm_rsk_adj_cnt
 else 0
 end)) as readm_obs_rt,
 
(AVG( case when tr.readm_outc_case=0 then null
 when tr.readm_excl_ind=1  then tr.csa_expc_prs_readm_30day_rsk
 else null end
 )) as readm_expc_rt,
 readm_obs_rt/readm_expc_rt as readm_oe_rt

 
 from tmp_readm tr
 left join tmp_max_dschrg_dt tmd on tr.join_key=tmd.join_key
 where tr.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and ADD_MONTHS((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
 --and fcy_nm <>'McLaren Greater Lansing'
 AND non_covid_case_ind = 1
 group by 1,2

 );

 
 --->
-- select 'New ' as ttype, * from stg_tmp_readm_ind_new where rpt_dt = '2020-06-01 00:00:00' and fcy_nm = 'McLaren Port Huron Hospital' UNION
--select 'Old ' as ttype, * from tmp_readm_ind where rpt_dt = '2020-06-01 00:00:00' and fcy_nm = 'McLaren Port Huron Hospital';

 /*
 Lab Utilization for Clinical Outcomes
 */ 
 --CODE CHANGE : 08/02/2021 commenting the stg_lab_utlz_fct since its not being used
-- drop table stg_lab_utlz_fct IF EXISTS; 
-- create  table stg_lab_utlz_fct as 
--(SELECT 
--EF.encntr_num,
--ef.fcy_nm, 
--ef.fcy_num, 
--ef.dschrg_dt,
--CF.department_group,
--CF.persp_clncl_dtl_descr,
--ef.dschrg_svc,
--ef.src_prim_pyr_cd,
--EF.ptnt_tp_cd,
--ef.src_prim_payor_grp3,
--max(CASE WHEN ( (UPPER(EF.ptnt_tp_cd) <> 'BSCH' OR UPPER(EF.ptnt_tp_cd) <> 'BSCHO' OR upper(ef.src_prim_pyr_cd)<>'SELECT' OR upper(ef.src_prim_pyr_cd)<>'SELEC')
--
--OR (upper(ef.src_prim_payor_grp3) <> ('HOSPICE'))
--
--OR (upper(ef.dschrg_svc)<>'NB' OR upper(ef.dschrg_svc)<>'NBN' OR upper(ef.dschrg_svc)<>'OIN' OR upper(ef.dschrg_svc)<>'SCN' OR upper(ef.dschrg_svc)<>'L1N'
--OR upper(ef.dschrg_svc)<>'BBN' OR upper(ef.dschrg_svc)<>'NURS'))
--then 1 else 0 end) as qty_incl_ind,
--
--max(case when CF.persp_clncl_dtl_descr in ('R&B CICU/CCU (CORONARY CARE)',
--'R&B ICU',
--'R&B ISOLATION PRIVATE',
--'R&B MED/SURG DELUXE',
--'R&B MED/SURG PRIVATE',
--'R&B MED/SURG SEMI PRIVATE',
--'R&B NURSERY',
--'R&B NURSERY INTENSIVE LEVEL III(NICU)',
--'R&B NURSERY INTERMEDIATE LEVEL II',
--'R&B OB',
--'R&B ONCOLOGY',
--'R&B PEDIATRIC',
--'R&B PSYCH ISOLATION',
--'R&B PSYCH PRIVATE',
--'R&B PSYCH SEMI PRIVATE',
--'R&B REHAB ISOLATION',
--'R&B REHAB PRIVATE',
--'R&B REHAB SEMI PRIVATE',
--'R&B STEP DOWN SEMI PRIVATE (PCU)',
--'R&B TCU DELUXE','R&B TCU PRIVATE',
--'R&B TCU SEMI PRIVATE',
--'R&B TELEMETRY PRIVATE',
--'R&B TELEMETRY SEMI PRIVATE',
--'R&B TRAUMA ICU') then 1 else 0 end) as randb_incl_ind,
--
--max(case when cf.cpt_code in ('36415',
--'36430',
--'38212',
--'38221',
--'81371',
--'81372',
--'81373',
--'81374',
--'81376',
--'81379',
--'81380',
--'81381',
--'81382',
--'81479',
--'82947',
--'82948',
--'82962',
--'86160',
--'86161',
--'86162',
--'86812',
--'86829',
--'86832',
--'86833',
--'86850',
--'86860',
--'86870',
--'86880',
--'86885',
--'86886',
--'86900',
--'86901',
--'86902',
--'86904',
--'86905',
--'86906',
--'86920',
--'86921',
--'86922',
--'86923',
--'86927',
--'86945',
--'86965',
--'86970',
--'86971',
--'86978',
--'87207',
--'87220',
--'88104',
--'88108',
--'88112',
--'88142',
--'88160',
--'88161',
--'88172',
--'88173',
--'88175',
--'88177',
--'88184',
--'88185',
--'88188',
--'88189',
--'88300',
--'88302',
--'88304',
--'88305',
--'88307',
--'88309',
--'88311',
--'88312',
--'88313',
--'88314',
--'88329',
--'88331',
--'88332',
--'88333',
--'88334',
--'88341',
--'88342',
--'88344',
--'88348',
--'88360',
--'88361',
--'93005',
--'G0123',
--'G0145',
--'G0364',
--'G0416',
--'J2790',
--'J2791',
--'P9011',
--'P9012',
--'P9016',
--'P9017',
--'P9019',
--'P9021',
--'P9031',
--'P9033',
--'P9034',
--'P9035',
--'P9037',
--'P9039',
--'P9040',
--'P9044',
--'P9052',
--'P9059',
--'P9604'
--) then 1 else 0 end) as cpt_excl_ind,
--sum(CF.quantity) as quantity, 
--sum(cf.total_charge) as total_charge,
--1 as join_key
--
--FROM PCE_QE16_SLP_PRD_DM..prd_encntr_anl_fct EF
--INNER JOIN PCE_QE16_SLP_PRD_DM..prd_chrg_fct CF ON EF.encntr_num = CF.encntr_num AND EF.fcy_nm = CF.fcy_nm
--WHERE EF.in_or_out_patient_ind = 'I' 
--group by 1,2,3,4,5,6,7,8,9,10
--);

--select * from prmretlp.tmp_lab_utlz_fct where date(rpt_Dt)= '2021-01-01';

--CODE CHANGE : Populated based on SLP 
DROP TABLE tmp_lab_utlz IF EXISTS; 
create temp table  tmp_lab_utlz as 
select 
EF.fcy_nm,
EF.fcy_num,
EF.dschrg_dt,
--CODE CHANGE: EXCLUDING covid cases
CASE WHEN EF.covid_adm_ind = 1 THEN 0 ELSE 1 END as non_covid_case_ind,
1 as join_key,
SUM(EF.lab_util_qty) as qty,
SUM(EF.ptnt_days_room_n_board_fin_cases) as ptnt_days
from PCE_QE16_SLP_PRD_DM..prd_encntr_anl_fct EF 
INNER JOIN pce_qe16_slp_prd_dm..payr_grp_dim PD
on EF.fcy_nm = PD.company_id and EF.src_prim_pyr_cd = PD.payer_code
WHERE
( nvl(UPPER(EF.ptnt_tp_cd),'UNKNOWN') NOT IN ('BSCH','BSCHO') 
AND nvl(upper(ef.src_prim_pyr_cd),'UNKNOWN') not in ('SELECT','SELEC') 
--AND nvl(upper(ef.src_prim_payor_grp3),'UNKNOWN') not in ('HOSPICE')
AND nvl(upper(PD.payor_group3),'UNKNOWN') not in ('HOSPICE')
AND nvl(upper(ef.dschrg_svc),'UNKNOWN') not in ('NB','NBN','OIN','SCN','L1N','BBN','NURS'))
AND EF.in_or_out_patient_ind ='I'
GROUP BY 1,2,3,4;

-------------New SQL

DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt,1 as join_key from stg_encntr_qs_anl_fct_vw ef);

drop table stg_tmp_lab_utlz_fct if exists;
create table stg_tmp_lab_utlz_fct as 

(select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2

UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2
 
 
 UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2
 
 
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2
 
 
   UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2
 
    UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2
 
     UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2
 
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2
 
 
 UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2
 
 UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2
 
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2

  
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2
 
 UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
 --and tlu.fcy_nm <>'Lansing'
 group by 1,2
 );
 
 ----CODE CHANGE: 08/24/2020 MLH-579 Excluding COVID _cases 
 drop table stg_tmp_lab_utlz_fct_wo_covid if exists;
create  table stg_tmp_lab_utlz_fct_wo_covid as 

(select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
 and tlu.non_covid_case_ind=1
 group by 1,2

UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
 and tlu.non_covid_case_ind=1
 group by 1,2
 
 
 UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
 and tlu.non_covid_case_ind=1
 group by 1,2
 
 
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
 and tlu.non_covid_case_ind=1
 group by 1,2
 
 
   UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
 and tlu.non_covid_case_ind=1
 group by 1,2
 
    UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
 and tlu.non_covid_case_ind=1
 group by 1,2
 
     UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
 and tlu.non_covid_case_ind=1
 group by 1,2
 
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
 and tlu.non_covid_case_ind=1
 group by 1,2
 
 
 UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
 and tlu.non_covid_case_ind=1
 group by 1,2
 
 UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
 and tlu.non_covid_case_ind=1
 group by 1,2
 
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
 and tlu.non_covid_case_ind=1
 group by 1,2
 
 
  
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
 and tlu.non_covid_case_ind=1
 group by 1,2
 
 UNION

select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
 and tlu.non_covid_case_ind=1
 group by 1,2
 );
----

/*
Sepsis Bundle Compliance for Clinical Outcome Score

*/
DROP TABLE tmp_sep_compl IF EXISTS;
;create temp table  tmp_sep_compl as
(SELECT csf.fcy_nm,  
csf.fcy_num,
csf.dschrg_dt,
1 as join_key,
count(distinct(case when csf.sep1_cgy = 'E' then csf.patient_id else null end)) as numr,
count(distinct(case when csf.sep1_cgy = 'D' or csf.sep1_cgy = 'E' then csf.patient_id else null end)) as dnmr

FROM pce_qe16_misc_prd_lnd.prmretlp.cm_sep1_fct csf
group by 1,2,3);

DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt, 1 as join_key from stg_encntr_qs_anl_fct_vw ef);

drop table stg_tmp_sep_compl_fct if exists;
create  table stg_tmp_sep_compl_fct as 
(select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and last_day(add_months((select max_dt from tmp_max_dschrg_dt),-1))
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2



Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2

Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2

Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2
);

/*
OB Training for Clinical Outcome Score
*/

drop table stg_tmp_ob_trn_fct if exists;
create  table stg_tmp_ob_trn_fct as 
(SELECT 

case when otf.fcy_nm in ('Bay','McLaren Bay Region') then 'McLaren Bay Region'
when otf.fcy_nm in  ('Central','McLaren Central Michigan') then 'McLaren Central Michigan' 
when otf.fcy_nm in  ('Flint','McLaren Flint') then 'McLaren Flint'
when otf.fcy_nm in  ('Lansing','McLaren Greater Lansing') then 'McLaren Greater Lansing' 
when otf.fcy_nm in  ('Lapeer','McLaren Lapeer Region') then 'McLaren Lapeer Region' 
when otf.fcy_nm in  ('Macomb','McLaren Macomb') then 'McLaren Macomb'
when otf.fcy_nm in  ('Northern','McLaren Northern Michigan') then 'McLaren Northern Michigan'
when otf.fcy_nm in  ('Port Huron','McLaren Port Huron') then 'McLaren Port Huron'
else otf.fcy_nm end
as fcy_nm,

case when fcy_nm in ('Bay','McLaren Bay Region') then 'MI2191'
when fcy_nm in  ('Central','McLaren Central Michigan') then 'MI2061'
when fcy_nm in  ('Flint','McLaren Flint') then 'MI2302'
when fcy_nm in  ('Lansing','McLaren Greater Lansing') then 'MI5020'
when fcy_nm in  ('Lapeer','McLaren Lapeer Region') then 'MI2001'
when fcy_nm in  ('Macomb','McLaren Macomb') then 'MI2048'
when fcy_nm in  ('Northern','McLaren Northern Michigan') then '637619'
when fcy_nm in  ('Port Huron','McLaren Port Huron') then '600816'
else null
end as fcy_num,
date(otf.rpt_mo)  as rpt_dt,
sum(msr_numr) as numr,
sum(msr_dnmr) as dnmr,
sum(msr_numr)/sum(msr_dnmr) as ob_rt
  FROM pce_qe16_prd..ob_trn_fct otf
  group by 1,2,3
);

/*
Patient Experience Percentile Composite

*/



DROP TABLE tmp_sf IF EXISTS;
;create temp table  tmp_sf as
(SELECT sf.client_id, 
case sf.client_id  when 331 then 'Lapeer'
when 398 then 'Flint'
when 453 then 'Northern'
when 1193 then 'Lansing'
when 1411 then 'Bay'
when 2766 then 'Oakland'
when 4062 then 'Karmanos'
when 5380 then 'Port Huron'
when 7841 then 'Central'
when 9123 then 'Macomb'
when 12705 then 'MMG'
when 24594 then 'Northern Michigan-MD'
when 26040 then 'Caro'
when 32475 then 'Thumb'
ELSE CAST(sf.client_id AS VARCHAR(20))
END AS FCY_NM,
sf.survey_id, 
sf.service, 
sf.disdate, 
sf.recdate, 
sf.resp_val, 
sf.varname, 
sf.question_text, 
sf.section, 
sf.standard, 
sf.screening, 
sf.top_box_answer, 
sf.top_box_scale, 
sf.survey_type, 
sf.sentiment, 

cast((case when length(disdate)<8 then disdate||'-01'
  		else disdate
 	 end) as date) as dschrg_dt,
	 'a' as join_key
  FROM pce_qe16_pressganey_prd_zoom..survey_fact sf);
  
  DROP TABLE tmp_maxd IF EXISTS;
;create temp table  tmp_maxd as
  (Select last_day(max(tsf.dschrg_dt)) as dschrg_dt,
  'a' as join_key
  FROM tmp_sf tsf);
  
-----03/15 Start
DROP TABLE tmp_hcaps_karmanos_sub_ptnt_exrnc_msr_fct IF EXISTS;
CREATE TABLE tmp_hcaps_karmanos_sub_ptnt_exrnc_msr_fct as 
with karmanos_tmp_sf as 
(select *,
CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL then 0
ELSE 0
END as top_box_resp_cnt
 FROM tmp_sf Z
 WHERE Z.service in ('IN') 
 and fcy_nm='Karmanos' 
 AND varname IN ('CMS_24','OSC_24') 
 AND Z.dschrg_dt >= (select add_months(date(dschrg_dt),-36) From tmp_maxd )
 --AND Z.dschrg_dt between '2020-10-01' AND '2020-12-31'
)
,grouped_data as 
(
  select 
 case when Ef.fcy_nm in ('Bay','McLaren Bay Region') then 'McLaren Bay Region'
when Ef.fcy_nm in  ('Central','McLaren Central Michigan') then 'McLaren Central Michigan' 
when Ef.fcy_nm in  ('Flint','McLaren Flint') then 'McLaren Flint'
when Ef.fcy_nm in  ('Lansing','McLaren Greater Lansing') then 'McLaren Greater Lansing' 
when Ef.fcy_nm in  ('Lapeer','McLaren Lapeer Region') then 'McLaren Lapeer Region' 
when Ef.fcy_nm in  ('Macomb','McLaren Macomb') then 'McLaren Macomb'
when Ef.fcy_nm in  ('Oakland','McLaren Oakland') then 'McLaren Oakland'
when Ef.fcy_nm in  ('Northern','McLaren Northern Michigan') then 'McLaren Northern Michigan'
when Ef.fcy_nm in  ('Karmanos','McLaren Karmanos Cancer Center') then 'Karmanos'
when Ef.fcy_nm in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then 'McLaren Port Huron Hospital'
else NULL  end
as fcy_nm,
case when EF.fcy_nm  in ('Bay','McLaren Bay Region') then 'MI2191'
when EF.fcy_nm  in  ('Central','McLaren Central Michigan') then 'MI2061'
when EF.fcy_nm  in  ('Flint','McLaren Flint') then 'MI2302'
when EF.fcy_nm  in  ('Lansing','McLaren Greater Lansing') then 'MI5020'
when EF.fcy_nm  in  ('Lapeer','McLaren Lapeer Region') then 'MI2001'
when EF.fcy_nm  in  ('Macomb','McLaren Macomb') then 'MI2048'
when EF.fcy_nm  in  ('Northern','McLaren Northern Michigan') then '637619'
when EF.fcy_nm  in ('Oakland','McLaren Oakland') then 'MI2055'
when Ef.fcy_nm in  ('Karmanos','McLaren Karmanos Cancer Center') then '634342'
when EF.fcy_nm  in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then '600816'
else null
end as fcy_num,
date_trunc('month',dschrg_dt) as report_month,
EF.service as svc_nm,
sum(EF.top_box_resp_cnt) as agg_top_box_resp_cnt,
count(distinct EF.survey_id) as agg_total_resp_cnt
FROM karmanos_tmp_sf EF
group by 1,2,3,4
order by 1 ASC,2 asc,3 DESC, 4
)
--select * from grouped_data
select fcy_nm,
fcy_num,
--svc_nm,
date(report_month) as rpt_dt,
svc_nm,
sum(agg_top_box_resp_cnt) OVER (PARTITION BY fcy_nm , svc_nm ORDER BY date(report_month) ROWS BETWEEN 2 preceding and current row) as numrtr,
sum(agg_total_resp_cnt) OVER (PARTITION BY fcy_nm, svc_nm ORDER BY date(report_month) ROWS BETWEEN 2 preceding and current row) as dnmntr
,nvl(round((numrtr/dnmntr)* 100,1),0) as numrtr_dnmntr_rt
from grouped_data
where fcy_nm is NOT NULL and fcy_num is NOT NULL and rpt_dt is NOT NULL 
--order by 1 ASC,2 ASC,3,4 DESC;
order by 1 ASC,2 ASC,3 DESC,4;

--03/15 End
   
DROP TABLE tmp_sub_ptnt_exrnc_msr_fct IF EXISTS;
;create temp table  tmp_sub_ptnt_exrnc_msr_fct as

(SELECT distinct service, 
 tsf.client_id,
tsf.FCY_NM,
tm.dschrg_dt as rprt_dt,
sum((CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-2))
and (select last_day(max(dschrg_dt)) from tmp_sf)
group by 1,2,3,4

UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-1) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-3))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-1))
group by 1,2,3,4

UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-2) as rprt_dt,
sum(  (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-4))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-2))
group by 1,2,3,4


UNION

 SELECT distinct tsf.service, 
 
 tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-3) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-5))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-3))
group by 1,2,3,4

UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-4) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-6))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-4))
group by 1,2,3,4


UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-5) as rprt_dt,
sum(  (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-7))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-5))
group by 1,2,3,4

UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-6) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-8))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-6))
group by 1,2,3,4



UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-7) as rprt_dt,
sum(  (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-9))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-7))
group by 1,2,3,4


UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-8) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-10))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-8))
group by 1,2,3,4

UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-9) as rprt_dt,
sum(  (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-11))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-9))
group by 1,2,3,4



UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-10) as rprt_dt,
sum(  (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-12))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-10))
group by 1,2,3,4


UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-11) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-13))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-11))
group by 1,2,3,4

UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-12) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where varname in ('CMS_24','OSC_24','F4') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-14))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-12))
group by 1,2,3,4);



DROP TABLE tmp_ptnt_exrnc_pct_msr_fct IF EXISTS;
;create temp table  tmp_ptnt_exrnc_pct_msr_fct as 
(select tspe.service,
tspe.client_id,
tspe.FCY_NM,
tspe.rprt_dt,
sum(tspe.resp_cnt) as resp_cnt,
sum(tspe.top_box_resp_cnt) as top_box_resp_cnt,
(sum(tspe.top_box_resp_cnt)/sum(tspe.resp_cnt))*100 as sub_ptnt_exrnc_pct,
round(((sum(tspe.top_box_resp_cnt)/sum(tspe.resp_cnt))*100),1) as ptnt_exrnc_pct
from tmp_sub_ptnt_exrnc_msr_fct tspe
group by 1,2,3,4);

select * from tmp_ptnt_exrnc_pct_msr_fct; 

select trunc(73.686,1) , round(73.686,1);

drop table  stg_ptnt_exrnc_pct_msr_fct if exists ;
create  table stg_ptnt_exrnc_pct_msr_fct as 
(select tmppm.*,
case when service= 'IN' then 0.5
when service = 'AS' then 0.2
when service = 'ER' then 0.3
end as msr_wt,
ppep.prgny_pct,
ppep.prgny_pct*msr_wt as prgny_pct_scor
from tmp_ptnt_exrnc_pct_msr_fct tmppm
--left join pce_qe16_pressganey_prd_zoom..prgny_ptnt_exrnc_pct_dim ppep on tmppm.service = ppep.svc_cd and tmppm.ptnt_exrnc_pct = ppep.scor);
left join pce_qe16_pressganey_prd_zoom..prgny_ptnt_exrnc_pct_dim ppep on tmppm.service = ppep.svc_cd and tmppm.ptnt_exrnc_pct = ppep.scor and ppep.reporting_year='2021');

select * from tmp_hcaps_karmanos_sub_ptnt_exrnc_msr_fct;

drop  table stg_hcaphs_ptnt_exrnc_pct_msr_fct if exists ;
create  table stg_hcaphs_ptnt_exrnc_pct_msr_fct as 
(select tmppm.*,
tmppm.numrtr_dnmntr_rt as prgny_pct,
tmppm.numrtr_dnmntr_rt as prgny_pct_scor
from tmp_hcaps_karmanos_sub_ptnt_exrnc_msr_fct tmppm
);

select * from pce_qe16_pressganey_prd_zoom..prgny_ptnt_exrnc_pct_dim where reporting_year = '2021' and svc_Cd='IN' and scor='80.2' UNION
select * from pce_qe16_pressganey_prd_zoom..prgny_ptnt_exrnc_pct_dim where reporting_year = '2021' and svc_Cd='AS' and scor='85.0' ;
select * from tmp_hcaps_karmanos_sub_ptnt_exrnc_msr_fct;
select * from stg_hcaphs_ptnt_exrnc_pct_msr_fct;

drop table stg_hcaphs_karmanos_tmp_ptnt_exrnc_pct_msr_fct if exists;
create  table stg_hcaphs_karmanos_tmp_ptnt_exrnc_pct_msr_fct as
(SELECT distinct pepm.fcy_nm, 

case when pepm.fcy_nm = 'Bay' then 'MI2191' 
when pepm.fcy_nm = 'Central' then 'MI2061' 
when pepm.fcy_nm= 'Flint' then  'MI2302' 
when pepm.fcy_nm ='Karmanos' then '634342' 
when pepm.fcy_nm='Lansing' then 'MI5020' 
when pepm.fcy_nm = 'Lapeer' then 'MI2001'
when pepm.fcy_nm = 'Macomb' then 'MI2048' 
when pepm.fcy_nm ='Northern' then '637619' 
when pepm.fcy_nm ='Oakland' then 'MI2055' 
when pepm.fcy_nm ='Port Huron' then '600816'
else null end as fcy_num,
pepm.rpt_dt as rprt_dt,
sum(pepm.prgny_pct) as prgny_pct,
sum(pepm.prgny_pct_scor) as prgny_pct_scor,
sum(pepm.numrtr_dnmntr_rt) as ptnt_exrnc_pct,
sum(pepm.numrtr) as top_box_cnt,
sum(pepm.dnmntr) as resp_cnt
--FROM pce_qe16_pressganey_prd_zoom..ptnt_exrnc_pct_msr_fct pepm
FROM  stg_hcaphs_ptnt_exrnc_pct_msr_fct pepm
where fcy_num is not null
group by 1,2,3);

-------Patient Experience Composite for Clinical Outcome Score
drop table stg_tmp_ptnt_exrnc_pct_msr_fct if exists;
create  table stg_tmp_ptnt_exrnc_pct_msr_fct as
(SELECT distinct pepm.fcy_nm, 

case when pepm.fcy_nm = 'Bay' then 'MI2191' 
when pepm.fcy_nm = 'Central' then 'MI2061' 
when pepm.fcy_nm= 'Flint' then  'MI2302' 
when pepm.fcy_nm ='Karmanos' then '634342' 
when pepm.fcy_nm='Lansing' then 'MI5020' 
when pepm.fcy_nm = 'Lapeer' then 'MI2001'
when pepm.fcy_nm = 'Macomb' then 'MI2048' 
when pepm.fcy_nm ='Northern' then '637619' 
when pepm.fcy_nm ='Oakland' then 'MI2055' 
when pepm.fcy_nm ='Port Huron' then '600816'
else null end as fcy_num,
pepm.rprt_dt,
sum(pepm.prgny_pct) as prgny_pct,
sum(pepm.prgny_pct_scor) as prgny_pct_scor,
sum(pepm.ptnt_exrnc_pct) as ptnt_exrnc_pct,
sum(pepm.resp_cnt) as resp_cnt
--FROM pce_qe16_pressganey_prd_zoom..ptnt_exrnc_pct_msr_fct pepm
FROM  stg_ptnt_exrnc_pct_msr_fct pepm
where fcy_num is not null
group by 1,2,3);

/*
MMG Overall Patient Experience

*/

DROP TABLE tmp_sf IF EXISTS;
;create temp table  tmp_sf as
(SELECT sf.client_id, 
case sf.client_id  when 331 then 'Lapeer'
when 398 then 'Flint'
when 453 then 'Northern'
when 1193 then 'Lansing'
when 1411 then 'Bay'
when 2766 then 'Oakland'
when 4062 then 'Karmanos'
when 5380 then 'Port Huron'
when 7841 then 'Central'
when 9123 then 'Macomb'
when 12705 then 'MMG'
when 24594 then 'Northern Michigan-MD'
when 26040 then 'Caro'
when 32475 then 'Thumb'
ELSE CAST(sf.client_id AS VARCHAR(20))
END AS FCY_NM,
sf.survey_id, 
sf.service, 
sf.disdate, 
sf.recdate, 
sf.resp_val, 
sf.varname, 
sf.question_text, 
sf.section, 
sf.standard, 
sf.screening, 
sf.top_box_answer, 
sf.top_box_scale, 
sf.survey_type, 
sf.sentiment, 

cast((case when length(disdate)<8 then disdate||'-01'
  		else disdate
 	 end) as date) as dschrg_dt,
	 'a' as join_key
  FROM pce_qe16_pressganey_prd_zoom..survey_fact sf);
  
select distinct client_id  from pce_qe16_pressganey_prd_zoom..survey_fact where service ='ON'; 

--select max(disdate) from tmp_sf where fcy_nm='Karmanos' and length(disdate) =10
--
--and date(disdate) between '2020-12-01' and '2020-12-31' and length(disdate) =10;
  

DROP TABLE tmp_sub_ptntexp IF EXISTS;
;create temp table  tmp_sub_ptntexp as 
(SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
tm.dschrg_dt as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-2))
and (select last_day(max(dschrg_dt)) from tmp_sf)
group by 1,2,3,4,5

union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-1) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-3))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-1))
group by 1,2,3,4,5

union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-2) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-4))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-2))
group by 1,2,3,4,5

union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-3) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-5))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-3))
group by 1,2,3,4,5

union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-4) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-6))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-4))
group by 1,2,3,4,5

union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-5) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-7))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-5))
group by 1,2,3,4,5

union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-6) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-8))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-6))
group by 1,2,3,4,5

union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-7) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-9))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-7))
group by 1,2,3,4,5


union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-8) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-10))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-8))
group by 1,2,3,4,5

union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-9) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-11))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-9))
group by 1,2,3,4,5


union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-10) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-12))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-10))
group by 1,2,3,4,5

union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-11) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-13))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-11))
group by 1,2,3,4,5

union

SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
add_months(tm.dschrg_dt,-12) as rprt_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-14))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-12))
group by 1,2,3,4,5);


drop table stg_mmg_ovrl_ptnt_exrnc_msr_fct if exists ;
create  table stg_mmg_ovrl_ptnt_exrnc_msr_fct as
(select tsp.* , 
tsp.top_box_resp_cnt/tsp.resp_cnt as ptnt_exrnc_scor,
case when hsptl_rgon='Bay' and ROUND(ptnt_exrnc_scor,3) <0.799 then 'R'
when hsptl_rgon='Bay' and ROUND(ptnt_exrnc_scor,3) >=0.799 then 'G'
when hsptl_rgon='Central' and ROUND(ptnt_exrnc_scor,3) <0.805 then 'R'
when hsptl_rgon='Central' and ROUND(ptnt_exrnc_scor,3) >=0.805 then 'G'
when hsptl_rgon='Flint' and ROUND(ptnt_exrnc_scor,3) <0.758 then 'R'
when hsptl_rgon='Flint' and ROUND(ptnt_exrnc_scor,3) >=0.758 then 'G'
when hsptl_rgon='Lansing' and ROUND(ptnt_exrnc_scor,3)<0.757 then 'R'
when hsptl_rgon='Lansing' and ROUND(ptnt_exrnc_scor,3) >=0.757 then 'G'
when hsptl_rgon='Lapeer' and ROUND(ptnt_exrnc_scor,3) <0.790 then 'R'
when hsptl_rgon='Lapeer' and ROUND(ptnt_exrnc_scor,3) >=0.790 then 'G'
when hsptl_rgon='Macomb' and ROUND(ptnt_exrnc_scor,3) <0.819 then 'R'
when hsptl_rgon='Macomb' and ROUND(ptnt_exrnc_scor,3) >=0.819 then 'G'
when hsptl_rgon='Northern' and ROUND(ptnt_exrnc_scor,3) <0.787 then 'R'
when hsptl_rgon='Northern' and ROUND(ptnt_exrnc_scor,3) >=0.787 then 'G'
when hsptl_rgon='Oakland' and ROUND(ptnt_exrnc_scor,3) <0.831 then 'R'
when hsptl_rgon='Oakland' and ROUND(ptnt_exrnc_scor,3) >=0.831 then 'G'
when hsptl_rgon='Port Huron' and ROUND(ptnt_exrnc_scor,3) <0.855 then 'R'
when hsptl_rgon='Port Huron' and ROUND(ptnt_exrnc_scor,3) >=0.855 then 'G'
END as ptnt_exrnc_clr_cdg,
case when hsptl_rgon  = 'Bay' then 'MI2191' 
when hsptl_rgon = 'Central' then 'MI2061' 
when hsptl_rgon= 'Flint' then  'MI2302' 
when hsptl_rgon ='Karmanos' then '634342' 
when hsptl_rgon='Lansing' then 'MI5020' 
when hsptl_rgon = 'Lapeer' then 'MI2001'
when hsptl_rgon = 'Macomb' then 'MI2048' 
when hsptl_rgon ='Northern' then '637619' 
when hsptl_rgon ='Oakland' then 'MI2055' 
when hsptl_rgon ='Port Huron' then '600816'
else null end as fcy_num
from tmp_sub_ptntexp tsp
WHERE client_id =12705);

select hsptl_rgon, sum(top_box_resp_cnt) as total_top_box_resp_cnt, sum(resp_cnt) as total_resp_cnt,
total_top_box_resp_cnt/total_resp_cnt as ratio
from tmp_sub_ptntexp where hsptl_rgon = 'Flint' and rprt_dt = '2020-07-31 00:00:00'
GROUP BY 1;

--select * FROM stg_mmg_ovrl_ptnt_exrnc_msr_fct WHERE fcy_num = 'MI5020' ;
--select * FROM stg_tmp_mmg_ovrl_ptnt_exrnc_msr_fct  WHERE fcy_num = 'MI5020' ;; 
----MMG Overall Patient Experience for Clinical Outcome Score

drop table stg_tmp_mmg_ovrl_ptnt_exrnc_msr_fct if exists;
create  table stg_tmp_mmg_ovrl_ptnt_exrnc_msr_fct as
(select distinct 
mmof.hsptl_rgon, 
case when mmof.hsptl_rgon  = 'Bay' then 'MI2191' 
when mmof.hsptl_rgon = 'Central' then 'MI2061' 
when mmof.hsptl_rgon= 'Flint' then  'MI2302' 
when mmof.hsptl_rgon ='Karmanos' then '634342' 
when mmof.hsptl_rgon='Lansing' then 'MI5020' 
when mmof.hsptl_rgon = 'Lapeer' then 'MI2001'
when mmof.hsptl_rgon = 'Macomb' then 'MI2048' 
when mmof.hsptl_rgon ='Northern' then '637619' 
when mmof.hsptl_rgon ='Oakland' then 'MI2055' 
when mmof.hsptl_rgon ='Port Huron' then '600816'
else null end as fcy_num,
mmof.rprt_dt,
mmof.top_box_resp_cnt,
mmof.resp_cnt,
mmof.ptnt_exrnc_scor,
mmof.ptnt_exrnc_clr_cdg
from pce_qe16_pressganey_prd_zoom..mmg_ovrl_ptnt_exrnc_msr_fct mmof
where fcy_num is not null);

/*
ACO MPP Measure

*/
DROP TABLE tmp_aco_sub_msr IF EXISTS;
;create temp table  tmp_aco_sub_msr as 
(SELECT asm.*,
case when asm.rgon='Bay' then  'MI2191'
 when asm.rgon='Central' then  'MI2061'
 when asm.rgon='Flint' then 'MI2302'
 when asm.rgon='Greater Lansing' then  'MI5020'
 when asm.rgon='Lapeer' then  'MI2001'
 when asm.rgon='Macomb' then  'MI2048'
 when asm.rgon='Northern' then  '637619'
 when asm.rgon='Oakland' then  'MI2055'
 when asm.rgon='Port Huron' then  '600816'
 end as fcy_num,

-------PBPY Scoring for Bay------------------------------
case when asm.rgon = 'Bay' and pbpy<=11445.55 then 150
when asm.rgon = 'Bay' and pbpy>11445.55 and pbpy<=11566.03 then 125
when asm.rgon = 'Bay' and pbpy>11566.03 and pbpy<=11686.51 then 100
when asm.rgon = 'Bay' and pbpy>11686.51 and pbpy<=12047.95 then 75
when asm.rgon = 'Bay' and pbpy>12047.95 and pbpy<=12650.35 then 50
-------PBPY Scoring for Central------------------------------
when asm.rgon = 'Central' and pbpy<=10496.24 then 150
when asm.rgon = 'Central' and pbpy>10496.24 and pbpy<=10606.72 then 125
when asm.rgon = 'Central' and pbpy>10606.72 and pbpy<=10717.21 then 100
when asm.rgon = 'Central' and pbpy>10717.21 and pbpy<=11048.67 then 75
when asm.rgon = 'Central' and pbpy>11048.67 and pbpy<=11601.1 then 50
-------PBPY Scoring for Flint--------------------------------
when asm.rgon = 'Flint' and pbpy<=11243.53 then 150
when asm.rgon = 'Flint' and pbpy>11243.53 and pbpy<=11361.88 then 125
when asm.rgon = 'Flint' and pbpy>11361.88 and pbpy<=11480.23 then 100
when asm.rgon = 'Flint' and pbpy>11480.23 and pbpy<=11835.29 then 75
when asm.rgon = 'Flint' and pbpy>11835.29 and pbpy<=12427.05 then 50

-------PBPY Scoring for Lansing--------------------------------
when asm.rgon = 'Greater Lansing' and pbpy<=9819.32 then 150
when asm.rgon = 'Greater Lansing' and pbpy>9819.32 and pbpy<=9922.68 then 125
when asm.rgon = 'Greater Lansing' and pbpy>9922.68 and pbpy<=10026.05 then 100
when asm.rgon = 'Greater Lansing' and pbpy>10026.05 and pbpy<=10336.13 then 75
when asm.rgon = 'Greater Lansing'and pbpy>10336.13 and pbpy<=10852.94 then 50

-------PBPY Scoring for Lansing--------------------------------
when asm.rgon = 'Lapeer' and pbpy<=9800.52 then 150
when asm.rgon = 'Lapeer' and pbpy>9800.52 and pbpy<=9903.69 then 125
when asm.rgon = 'Lapeer' and pbpy>9903.69 and pbpy<=10006.85 then 100
when asm.rgon = 'Lapeer' and pbpy>10006.85 and pbpy<=10316.34 then 75
when asm.rgon = 'Lapeer' and pbpy>10316.34 and pbpy<=10832.16 then 50

-------PBPY Scoring for Macomb--------------------------------
when asm.rgon = 'Macomb' and pbpy<=11175.12 then 150
when asm.rgon = 'Macomb' and pbpy>11175.12 and pbpy<=11292.75 then 125
when asm.rgon = 'Macomb' and pbpy>11292.75 and pbpy<=11410.38 then 100
when asm.rgon = 'Macomb' and pbpy>11410.38 and pbpy<=11763.28 then 75
when asm.rgon = 'Macomb' and pbpy>11763.28 and pbpy<=12351.44 then 50
-------PBPY Scoring for McLaren Healthcare--------------------------------
--when asm.rgon = 'Mclaren Health Care' and pbpy<=10700 then 150
--when asm.rgon = 'Mclaren Health Care' and pbpy>10700 and pbpy<=10812 then 125
--when asm.rgon = 'Mclaren Health Care' and pbpy>10812 and pbpy<=10925 then 100
--when asm.rgon = 'Mclaren Health Care' and pbpy>10925 and pbpy<=11263 then 75
--when asm.rgon = 'Mclaren Health Care' and pbpy>11263 and pbpy<=11826 then 50

-------PBPY Scoring for Northern--------------------------------
when asm.rgon = 'Northern' and pbpy<=9308.22 then 150
when asm.rgon = 'Northern' and pbpy>9308.22  and pbpy<=9406.2 then 125
when asm.rgon = 'Northern' and pbpy>9406.2 and pbpy<=9504.19 then 100
when asm.rgon = 'Northern' and pbpy>9504.19 and pbpy<=9798.13 then 75
when asm.rgon = 'Northern' and pbpy>9798.13 and pbpy<=10288.04 then 50

-------PBPY Scoring for Oakland--------------------------------
when asm.rgon = 'Oakland' and pbpy<=10896.28 then 150
when asm.rgon = 'Oakland' and pbpy>10896.28  and pbpy<=11010.98 then 125
when asm.rgon = 'Oakland' and pbpy>11010.98 and pbpy<=11125.68 then 100
when asm.rgon = 'Oakland' and pbpy>11125.68 and pbpy<=11469.77 then 75
when asm.rgon = 'Oakland' and pbpy>11469.77 and pbpy<=12043.26 then 50

-------PBPY Scoring for Port Huron--------------------------------
when asm.rgon = 'Port Huron' and pbpy<=8814 then 150
when asm.rgon = 'Port Huron' and pbpy>8814  and pbpy<=8906.77 then 125
when asm.rgon = 'Port Huron' and pbpy>8906.77 and pbpy<=8999.55 then 100
when asm.rgon = 'Port Huron' and pbpy>8999.55 and pbpy<=9277.89 then 75
when asm.rgon = 'Port Huron' and pbpy>9277.89 and pbpy<=9741.78 then 50
else 0
end as pbpy_pts,

-------HCC Scoring for Bay------------------------------
case when asm.rgon = 'Bay' and round(avg_hcc_scr,2)>=1.16 then 150 
when asm.rgon = 'Bay' and round(avg_hcc_scr,2)>=1.13 and round(avg_hcc_scr,2) < 1.16 then 125
when asm.rgon = 'Bay' and round(avg_hcc_scr,2)>=1.12 and round(avg_hcc_scr,2)<1.13 then 100
when asm.rgon = 'Bay' and round(avg_hcc_scr,2)>=1.1 and round(avg_hcc_scr,2)<1.12 then 75
when asm.rgon = 'Bay' and round(avg_hcc_scr,2)>=1.05 and round(avg_hcc_scr,2)<1.1 then 50
when asm.rgon = 'Bay' and round(avg_hcc_scr,2)<1.05 then 0
-------HCC Scoring for Central------------------------------
when asm.rgon = 'Central' and round(avg_hcc_scr,2)>=1.1 then 150
when asm.rgon = 'Central' and round(avg_hcc_scr,2)>=1.08 and round(avg_hcc_scr,2)<1.1 then 125
when asm.rgon = 'Central' and round(avg_hcc_scr,2)>=1.07 and round(avg_hcc_scr,2)<1.08 then 100
when asm.rgon = 'Central' and round(avg_hcc_scr,2)>=1.05 and round(avg_hcc_scr,2)<1.07 then 75
when asm.rgon = 'Central' and round(avg_hcc_scr,2)>=1 and round(avg_hcc_scr,2)<1.05 then 50
when asm.rgon = 'Central' and round(avg_hcc_scr,2)<1 then 0
-------HCC Scoring for Flint--------------------------------
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)>=1.07  then 150
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)>=1.05 and round(avg_hcc_scr,2)<1.07 then 125
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)>=1.04 and round(avg_hcc_scr,2)<1.05 then 100
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)>=1.02 and round(avg_hcc_scr,2)<1.04 then 75
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)>=0.97 and round(avg_hcc_scr,2)<1.02 then 50
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)<0.97 then 0

-------HCC Scoring for Lansing--------------------------------
when asm.rgon = 'Greater Lansing' and round(avg_hcc_scr,2)>=1  then 150
when asm.rgon = 'Greater Lansing' and round(avg_hcc_scr,2)>=0.98  and round(avg_hcc_scr,2)<1  then 125
when asm.rgon = 'Greater Lansing' and round(avg_hcc_scr,2)>=0.97 and round(avg_hcc_scr,2)<0.98 then 100
when asm.rgon = 'Greater Lansing' and round(avg_hcc_scr,2)>=0.95 and round(avg_hcc_scr,2)<0.97 then 75
when asm.rgon = 'Greater Lansing' and round(avg_hcc_scr,2)>=0.9 and round(avg_hcc_scr,2)<0.95 then 50
when asm.rgon = 'Greater Lansing'and round(avg_hcc_scr,2)<0.9 then 0

-------HCC Scoring for Lansing--------------------------------
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)>=1.07  then 150
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)>=1.05 and round(avg_hcc_scr,2)<1.07 then 125
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)>=1.04 and round(avg_hcc_scr,2)<1.05 then 100
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)>=1.02 and round(avg_hcc_scr,2)<1.04 then 75
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)>=0.97 and round(avg_hcc_scr,2)<1.02 then 50
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)<0.97 then 0

-------HCC Scoring for Macomb--------------------------------
when asm.rgon = 'Macomb' and round(avg_hcc_scr,2)>=1.21 then 150
when asm.rgon = 'Macomb' and round(avg_hcc_scr,2)>=1.18 and round(avg_hcc_scr,2)<1.21 then 125
when asm.rgon = 'Macomb' and round(avg_hcc_scr,2)>=1.17 and round(avg_hcc_scr,2)<1.18 then 100
when asm.rgon = 'Macomb' and round(avg_hcc_scr,2)>=1.15 and round(avg_hcc_scr,2)<1.17 then 75
when asm.rgon = 'Macomb' and round(avg_hcc_scr,2)>=1.09 and round(avg_hcc_scr,2)<1.15 then 50
when asm.rgon = 'Macomb' and  round(avg_hcc_scr,2)<1.09 then 0
-------HCC Scoring for McLaren Healthcare--------------------------------
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)>=1.07  then 150
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)>=1.05 and round(avg_hcc_scr,2)<1.07 then 125
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)>=1.04 and round(avg_hcc_scr,2)<1.05 then 100
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)>=1.02 and round(avg_hcc_scr,2)<1.04 then 75
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)>=0.97 and round(avg_hcc_scr,2)<1.02 then 50
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)<0.97 then 0

-------HCC Scoring for Northern--------------------------------
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)>=0.99 then 150
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)>=0.97 and round(avg_hcc_scr,2)<0.99 then 125
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)>=0.96  and round(avg_hcc_scr,2)<0.97 then 100
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)>=0.94 and round(avg_hcc_scr,2)<0.96 then 75
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)>=0.89 and round(avg_hcc_scr,2)<0.94 then 50
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)<0.89 then 0

-------HCC Scoring for Oakland--------------------------------
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)>=1.08 then 150
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)>=1.06 and round(avg_hcc_scr,2)<1.08 then 125
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)>=1.05  and round(avg_hcc_scr,2)<1.06 then 100
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)>=1.03 and round(avg_hcc_scr,2)<1.05 then 75
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)>=0.98 and round(avg_hcc_scr,2)<1.03 then 50
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)<0.98 then 0

-------HCC Scoring for Port Huron--------------------------------
when asm.rgon = 'Port Huron' and round(avg_hcc_scr,2)>=1.04 then 150
when asm.rgon = 'Port Huron' and round(avg_hcc_scr,2)>=1.02 and round(avg_hcc_scr,2)<1.04 then 125
when asm.rgon = 'Port Huron' and round(avg_hcc_scr,2)>=1.01  and round(avg_hcc_scr,2)<1.02 then 100
when asm.rgon = 'Port Huron' and round(avg_hcc_scr,2)>=0.99 and round(avg_hcc_scr,2)<1.01 then 75
when asm.rgon = 'Port Huron' and round(avg_hcc_scr,2)>=0.94 and round(avg_hcc_scr,2)<0.99 then 50
when asm.rgon = 'Port Huron'  and round(avg_hcc_scr,2)<0.94 then 0
else 0
end as hcc_pts,

-------Leakage Scoring for Bay------------------------------
case when asm.rgon = 'Bay' and lkg_amt_xld<=50.9 then 150
when asm.rgon = 'Bay' and lkg_amt_xld>50.9 and lkg_amt_xld<=51.2 then 125
when asm.rgon = 'Bay' and lkg_amt_xld>51.2 and lkg_amt_xld<=51.5 then 100
when asm.rgon = 'Bay' and lkg_amt_xld>51.5 and lkg_amt_xld<=52.5 then 75
when asm.rgon = 'Bay' and lkg_amt_xld>52.5 and lkg_amt_xld<=55.1 then 50
-------Leakage Scoring for Central------------------------------
when asm.rgon = 'Central' and lkg_amt_xld<=51.2 then 150
when asm.rgon = 'Central' and lkg_amt_xld>51.2 and lkg_amt_xld<=51.5 then 125
when asm.rgon = 'Central' and lkg_amt_xld>51.5 and lkg_amt_xld<=51.7 then 100
when asm.rgon = 'Central' and lkg_amt_xld>51.7 and lkg_amt_xld<=52.8 then 75
when asm.rgon = 'Central' and lkg_amt_xld>52.8 and lkg_amt_xld<=55.4 then 50
-------Leakage Scoring for Flint--------------------------------
when asm.rgon = 'Flint' and lkg_amt_xld<=45.3 then 150
when asm.rgon = 'Flint' and lkg_amt_xld>45.3 and lkg_amt_xld<=45.5 then 125
when asm.rgon = 'Flint' and lkg_amt_xld>45.5 and lkg_amt_xld<=45.8 then 100
when asm.rgon = 'Flint' and lkg_amt_xld>45.8 and lkg_amt_xld<=46.7 then 75
when asm.rgon = 'Flint' and lkg_amt_xld>46.7 and lkg_amt_xld<=49 then 50

-------Leakage Scoring for Lansing--------------------------------
when asm.rgon = 'Greater Lansing' and lkg_amt_xld<=62.6 then 150
when asm.rgon = 'Greater Lansing' and lkg_amt_xld>62.6 and lkg_amt_xld<=62.9 then 125
when asm.rgon = 'Greater Lansing' and lkg_amt_xld>62.9 and lkg_amt_xld<=63.2 then 100
when asm.rgon = 'Greater Lansing' and lkg_amt_xld>63.2 and lkg_amt_xld<=64.5 then 75
when asm.rgon = 'Greater Lansing'and lkg_amt_xld>64.5 and lkg_amt_xld<=67.7 then 50

-------Leakage Scoring for Lansing--------------------------------
when asm.rgon = 'Lapeer' and lkg_amt_xld<=37.6 then 150
when asm.rgon = 'Lapeer' and lkg_amt_xld>37.6 and lkg_amt_xld<=37.8 then 125
when asm.rgon = 'Lapeer' and lkg_amt_xld>37.8 and lkg_amt_xld<=38 then 100
when asm.rgon = 'Lapeer' and lkg_amt_xld>38 and lkg_amt_xld<=38.8 then 75
when asm.rgon = 'Lapeer' and lkg_amt_xld>38.8 and lkg_amt_xld<=40.7 then 50

-------Leakage Scoring for Macomb--------------------------------
when asm.rgon = 'Macomb' and lkg_amt_xld<=50.7 then 150
when asm.rgon = 'Macomb' and lkg_amt_xld>50.7 and lkg_amt_xld<=51 then 125
when asm.rgon = 'Macomb' and lkg_amt_xld>51 and lkg_amt_xld<=51.3 then 100
when asm.rgon = 'Macomb' and lkg_amt_xld>51.3 and lkg_amt_xld<=52.3 then 75
when asm.rgon = 'Macomb' and lkg_amt_xld>52.3 and lkg_amt_xld<=54.9 then 50
-------Leakage Scoring for McLaren Healthcare--------------------------------
--when asm.rgon = 'Mclaren Health Care' and lkg_amt_xld<=58.1 then 150
--when asm.rgon = 'Mclaren Health Care' and lkg_amt_xld>58.1 and lkg_amt_xld<=58.4 then 125
--when asm.rgon = 'Mclaren Health Care' and lkg_amt_xld>58.4 and lkg_amt_xld<=58.7 then 100
--when asm.rgon = 'Mclaren Health Care' and lkg_amt_xld>58.7 and lkg_amt_xld<=59.9 then 75
--when asm.rgon = 'Mclaren Health Care' and lkg_amt_xld>59.9 and lkg_amt_xld<=62.9 then 50

-------Leakage Scoring for Northern--------------------------------
when asm.rgon = 'Northern' and lkg_amt_xld<=48.3 then 150
when asm.rgon = 'Northern' and lkg_amt_xld>48.3 and lkg_amt_xld<=48.6 then 125
when asm.rgon = 'Northern' and lkg_amt_xld>48.6 and lkg_amt_xld<=48.8 then 100
when asm.rgon = 'Northern' and lkg_amt_xld>48.8 and lkg_amt_xld<=49.8 then 75
when asm.rgon = 'Northern' and lkg_amt_xld>49.8 and lkg_amt_xld<=52.3 then 50

-------Leakage Scoring for Oakland--------------------------------
when asm.rgon = 'Oakland' and lkg_amt_xld<=58.4 then 150
when asm.rgon = 'Oakland' and lkg_amt_xld>58.4  and lkg_amt_xld<=58.7 then 125
when asm.rgon = 'Oakland' and lkg_amt_xld>58.7 and lkg_amt_xld<=59 then 100
when asm.rgon = 'Oakland' and lkg_amt_xld>59 and lkg_amt_xld<=60.2 then 75
when asm.rgon = 'Oakland' and lkg_amt_xld>60.2 and lkg_amt_xld<=63.2 then 50

-------Leakage Scoring for Port Huron--------------------------------
when asm.rgon = 'Port Huron' and lkg_amt_xld<=49.4 then 150
when asm.rgon = 'Port Huron' and lkg_amt_xld>49.4  and lkg_amt_xld<=49.6 then 125
when asm.rgon = 'Port Huron' and lkg_amt_xld>49.6 and lkg_amt_xld<=49.9 then 100
when asm.rgon = 'Port Huron' and lkg_amt_xld>49.9 and lkg_amt_xld<=50.9 then 75
when asm.rgon = 'Port Huron' and lkg_amt_xld>50.9 and lkg_amt_xld<=53.4 then 50
else 0
end as Leakage_pts,

-------SNF Admissions Scoring for Bay------------------------------
case when asm.rgon = 'Bay' and snf_dschrg<=61.5 then 150
when asm.rgon = 'Bay' and snf_dschrg>61.5 and snf_dschrg<=62.1 then 125
when asm.rgon = 'Bay' and snf_dschrg>62.1 and snf_dschrg<=63.4 then 100
when asm.rgon = 'Bay' and snf_dschrg>63.4 and snf_dschrg<=64.7 then 75
when asm.rgon = 'Bay' and snf_dschrg>64.7 and snf_dschrg<=67.9 then 50
-------SNF Admissions Scoring for Central------------------------------
when asm.rgon = 'Central' and snf_dschrg<=65.4 then 150
when asm.rgon = 'Central' and snf_dschrg>65.4 and snf_dschrg<=66 then 125
when asm.rgon = 'Central' and snf_dschrg>66 and snf_dschrg<=67.4 then 100
when asm.rgon = 'Central' and snf_dschrg>67.4 and snf_dschrg<=68.8 then 75
when asm.rgon = 'Central' and snf_dschrg>68.8 and snf_dschrg<=72.2 then 50
-------SNF Admissions Scoring for Flint--------------------------------
when asm.rgon = 'Flint' and snf_dschrg<=46.3 then 150
when asm.rgon = 'Flint' and snf_dschrg>46.3 and snf_dschrg<=46.8 then 125
when asm.rgon = 'Flint' and snf_dschrg>46.8 and snf_dschrg<=47.7 then 100
when asm.rgon = 'Flint' and snf_dschrg>47.7 and snf_dschrg<=48.7 then 75
when asm.rgon = 'Flint' and snf_dschrg>48.7 and snf_dschrg<=51.1 then 50

-------SNF Admissions Scoring for Lansing--------------------------------
when asm.rgon = 'Greater Lansing' and snf_dschrg<=46 then 150
when asm.rgon = 'Greater Lansing' and snf_dschrg>46 and snf_dschrg<=46.5 then 125
when asm.rgon = 'Greater Lansing' and snf_dschrg>46.5 and snf_dschrg<=47.4 then 100
when asm.rgon = 'Greater Lansing' and snf_dschrg>47.4 and snf_dschrg<=48.4 then 75
when asm.rgon = 'Greater Lansing'and snf_dschrg>48.4 and snf_dschrg<=50.8 then 50

-------SNF Admissions Scoring for Lansing--------------------------------
when asm.rgon = 'Lapeer' and snf_dschrg<=39.7 then 150
when asm.rgon = 'Lapeer' and snf_dschrg>39.7 and snf_dschrg<=40.1 then 125
when asm.rgon = 'Lapeer' and snf_dschrg>40.1 and snf_dschrg<=41 then 100
when asm.rgon = 'Lapeer' and snf_dschrg>41 and snf_dschrg<=41.8 then 75
when asm.rgon = 'Lapeer' and snf_dschrg>41.8 and snf_dschrg<=43.9 then 50

-------SNF Admissions Scoring for Macomb--------------------------------
when asm.rgon = 'Macomb' and snf_dschrg<=65.6 then 150
when asm.rgon = 'Macomb' and snf_dschrg>65.6 and snf_dschrg<=66.2then 125
when asm.rgon = 'Macomb' and snf_dschrg>66.2 and snf_dschrg<=67.6 then 100
when asm.rgon = 'Macomb' and snf_dschrg>67.6 and snf_dschrg<=69 then 75
when asm.rgon = 'Macomb' and snf_dschrg>69 and snf_dschrg<=72.5 then 50
-------SNF Admissions Scoring for McLaren Healthcare--------------------------------
--when asm.rgon = 'Mclaren Health Care' and snf_dschrg<=60.8 then 150
--when asm.rgon = 'Mclaren Health Care' and snf_dschrg>60.8 and snf_dschrg<=61.4 then 125
--when asm.rgon = 'Mclaren Health Care' and snf_dschrg>61.4 and snf_dschrg<=62.7 then 100
--when asm.rgon = 'Mclaren Health Care' and snf_dschrg>62.7 and snf_dschrg<=64 then 75
--when asm.rgon = 'Mclaren Health Care' and snf_dschrg>64 and snf_dschrg<=67.2 then 50

-------SNF Admissions Scoring for Northern--------------------------------
when asm.rgon = 'Northern' and snf_dschrg<=24.6 then 150
when asm.rgon = 'Northern' and snf_dschrg>24.6 and snf_dschrg<=24.9 then 125
when asm.rgon = 'Northern' and snf_dschrg>24.9 and snf_dschrg<=25.4 then 100
when asm.rgon = 'Northern' and snf_dschrg>25.4 and snf_dschrg<=25.9 then 75
when asm.rgon = 'Northern' and snf_dschrg>25.9 and snf_dschrg<=27.2 then 50

-------SNF Admissions Scoring for Oakland--------------------------------
when asm.rgon = 'Oakland' and snf_dschrg<=54.2 then 150
when asm.rgon = 'Oakland' and snf_dschrg>54.2  and snf_dschrg<=54.7 then 125
when asm.rgon = 'Oakland' and snf_dschrg>54.7 and snf_dschrg<=55.9 then 100
when asm.rgon = 'Oakland' and snf_dschrg>55.9 and snf_dschrg<=57 then 75
when asm.rgon = 'Oakland' and snf_dschrg>57 and snf_dschrg<=59.9 then 50

-------SNF Admissions Scoring for Port Huron--------------------------------
when asm.rgon = 'Port Huron' and snf_dschrg<=57.9 then 150
when asm.rgon = 'Port Huron' and snf_dschrg>57.9 and snf_dschrg<=58.5 then 125
when asm.rgon = 'Port Huron' and snf_dschrg>58.5 and snf_dschrg<=59.7 then 100
when asm.rgon = 'Port Huron' and snf_dschrg>59.7 and snf_dschrg<=60.9 then 75
when asm.rgon = 'Port Huron' and snf_dschrg>60.9 and snf_dschrg<=63.9 then 50
else 0
end as snf_adm_pts,

------------------ED Visits---------------------------------------
-------ED Visits Scoring for Bay------------------------------
case when asm.rgon = 'Bay' and ed_vst_ind<=747 then 150
when asm.rgon = 'Bay' and ed_vst_ind>747 and ed_vst_ind<=755 then 125
when asm.rgon = 'Bay' and ed_vst_ind>755 and ed_vst_ind<=762 then 100
when asm.rgon = 'Bay' and ed_vst_ind>762 and ed_vst_ind<=786 then 75
when asm.rgon = 'Bay' and ed_vst_ind>786 and ed_vst_ind<=825 then 50
-------ED Visits Scoring for Central------------------------------
when asm.rgon = 'Central' and ed_vst_ind<=867 then 150
when asm.rgon = 'Central' and ed_vst_ind>867 and ed_vst_ind<=876 then 125
when asm.rgon = 'Central' and ed_vst_ind>876 and ed_vst_ind<=886 then 100
when asm.rgon = 'Central' and ed_vst_ind>886 and ed_vst_ind<=913 then 75
when asm.rgon = 'Central' and ed_vst_ind>913 and ed_vst_ind<=959 then 50
-------ED Visits Scoring for Flint--------------------------------
when asm.rgon = 'Flint' and ed_vst_ind<=629 then 150
when asm.rgon = 'Flint' and ed_vst_ind>629 and ed_vst_ind<=636 then 125
when asm.rgon = 'Flint' and ed_vst_ind>636 and ed_vst_ind<=642 then 100
when asm.rgon = 'Flint' and ed_vst_ind>642 and ed_vst_ind<=662 then 75
when asm.rgon = 'Flint' and ed_vst_ind>662 and ed_vst_ind<=695 then 50

-------ED Visits Scoring for Lansing--------------------------------
when asm.rgon = 'Greater Lansing' and ed_vst_ind<=598 then 150
when asm.rgon = 'Greater Lansing' and ed_vst_ind>598 and ed_vst_ind<=604 then 125
when asm.rgon = 'Greater Lansing' and ed_vst_ind>604 and ed_vst_ind<=610 then 100
when asm.rgon = 'Greater Lansing' and ed_vst_ind>610 and ed_vst_ind<=629 then 75
when asm.rgon = 'Greater Lansing'and ed_vst_ind>629 and ed_vst_ind<=660 then 50

-------ED Visits Scoring for Lansing--------------------------------
when asm.rgon = 'Lapeer' and ed_vst_ind<=703 then 150
when asm.rgon = 'Lapeer' and ed_vst_ind>703 and ed_vst_ind<=710 then 125
when asm.rgon = 'Lapeer' and ed_vst_ind>710 and ed_vst_ind<=718 then 100
when asm.rgon = 'Lapeer' and ed_vst_ind>718 and ed_vst_ind<=740 then 75
when asm.rgon = 'Lapeer' and ed_vst_ind>740 and ed_vst_ind<=777 then 50

-------ED Visits Scoring for Macomb--------------------------------
when asm.rgon = 'Macomb' and ed_vst_ind<=745 then 150
when asm.rgon = 'Macomb' and ed_vst_ind>745 and ed_vst_ind<=753 then 125
when asm.rgon = 'Macomb' and ed_vst_ind>753 and ed_vst_ind<=760 then 100
when asm.rgon = 'Macomb' and ed_vst_ind>760 and ed_vst_ind<=784 then 75
when asm.rgon = 'Macomb' and ed_vst_ind>784 and ed_vst_ind<=823 then 50
-----ED Visits Scoring for McLaren Healthcare--------------------------------
--when asm.rgon = 'Mclaren Health Care' and ed_vst_ind<=663 then 150
--when asm.rgon = 'Mclaren Health Care' and ed_vst_ind>663 and ed_vst_ind<=670 then 125
--when asm.rgon = 'Mclaren Health Care' and ed_vst_ind>670 and ed_vst_ind<=677 then 100
--when asm.rgon = 'Mclaren Health Care' and ed_vst_ind>677 and ed_vst_ind<=698 then 75
--when asm.rgon = 'Mclaren Health Care' and ed_vst_ind>698 and ed_vst_ind<=733 then 50

-------ED Visits Scoring for Northern--------------------------------
when asm.rgon = 'Northern' and ed_vst_ind<=581 then 150
when asm.rgon = 'Northern' and ed_vst_ind>581 and ed_vst_ind=588 then 125
when asm.rgon = 'Northern' and ed_vst_ind>588 and ed_vst_ind<=594 then 100
when asm.rgon = 'Northern' and ed_vst_ind>594 and ed_vst_ind<=612 then 75
when asm.rgon = 'Northern' and ed_vst_ind>612 and ed_vst_ind<=643 then 50

-------ED Visits Scoring for Oakland--------------------------------
when asm.rgon = 'Oakland' and ed_vst_ind<=733 then 150
when asm.rgon = 'Oakland' and ed_vst_ind>733  and ed_vst_ind<=741 then 125
when asm.rgon = 'Oakland' and ed_vst_ind>741 and ed_vst_ind<=749 then 100
when asm.rgon = 'Oakland' and ed_vst_ind>749 and ed_vst_ind<=772 then 75
when asm.rgon = 'Oakland' and ed_vst_ind>772 and ed_vst_ind<=811 then 50

-------ED Visits Scoring for Port Huron--------------------------------
when asm.rgon = 'Port Huron' and ed_vst_ind<=569 then 150
when asm.rgon = 'Port Huron' and ed_vst_ind>569 and ed_vst_ind<=575 then 125
when asm.rgon = 'Port Huron' and ed_vst_ind>575 and ed_vst_ind<=581 then 100
when asm.rgon = 'Port Huron' and ed_vst_ind>581 and ed_vst_ind<=599 then 75
when asm.rgon = 'Port Huron' and ed_vst_ind>599 and ed_vst_ind<=629 then 50
else 0
end as ed_vst_pts
FROM pce_qe16_prd..aco_smry_msr asm);

---Added 12/28/2020: FY21 Dashboard
--MPP Latest logic

DROP TABLE tmp_aco_sub_msr_achvmnt IF EXISTS;
;create temp table  tmp_aco_sub_msr_achvmnt as 
(SELECT asm.*,
case when asm.rgon='Bay' then  'MI2191'
 when asm.rgon='Central' then  'MI2061'
 when asm.rgon='Flint' then 'MI2302'
 when asm.rgon='Greater Lansing' then  'MI5020'
 when asm.rgon='Lapeer' then  'MI2001'
 when asm.rgon='Macomb' then  'MI2048'
 when asm.rgon='Northern' then  '637619'
 when asm.rgon='Oakland' then  'MI2055'
 when asm.rgon='Port Huron' then  '600816'
 end as fcy_num,

0 as pbpy_pts,

-------HCC Scoring (Achievements)------------------------------
case when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and round(avg_hcc_scr,2)>=1.15 then 150 
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  round(avg_hcc_scr,2)>=1.13 and round(avg_hcc_scr,2) < 1.15 then 125
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  round(avg_hcc_scr,2)>=1.11 and round(avg_hcc_scr,2) <1.13  then 100
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  round(avg_hcc_scr,2)>=1.09 and round(avg_hcc_scr,2) <1.11  then 75
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  round(avg_hcc_scr,2)>=1.02 and round(avg_hcc_scr,2) <1.09  then 50

else 0
end as hcc_pts,

-------Leakage Scoring------------------------------
0 as  Leakage_pts,

-------SNF Admissions Scoring  (Achievements)------------------------------

case when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and snf_dschrg  <= 28 then 150
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  snf_dschrg > 28 and snf_dschrg <= 38.5 then 125
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  snf_dschrg > 38.5  and snf_dschrg <= 49 then 100
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  snf_dschrg > 49 and snf_dschrg <= 55.5 then 75
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  snf_dschrg > 55.5 and snf_dschrg <= 62 then 50

else 0
end as snf_adm_pts,

------------------ED Visits (Achievements)---------------------------------------
-------ED Visits Scoring ------------------------------
case when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and ed_vst_ind  <= 518 then 150
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  ed_vst_ind > 518 and ed_vst_ind <= 565 then 125
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  ed_vst_ind > 565 and ed_vst_ind <= 613 then 100
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  ed_vst_ind > 613 and ed_vst_ind <= 660 then 75
when asm.rgon IN ('Bay', 'Central','Flint','Greater Lansing','Lapeer','Macomb','Oakland','Northern','Port Huron') and  ed_vst_ind > 660 and ed_vst_ind <= 714 then 50

else 0
end as ed_vst_pts
FROM pce_qe16_prd..aco_smry_msr asm);

DROP TABLE tmp_aco_sub_msr_imprvmnt IF EXISTS;
;create temp table  tmp_aco_sub_msr_imprvmnt as 
(SELECT asm.*,
case when asm.rgon='Bay' then  'MI2191'
 when asm.rgon='Central' then  'MI2061'
 when asm.rgon='Flint' then 'MI2302'
 when asm.rgon='Greater Lansing' then  'MI5020'
 when asm.rgon='Lapeer' then  'MI2001'
 when asm.rgon='Macomb' then  'MI2048'
 when asm.rgon='Northern' then  '637619'
 when asm.rgon='Oakland' then  'MI2055'
 when asm.rgon='Port Huron' then  '600816'
 end as fcy_num,
 0 as pbpy_pts,

-------HCC Scoring for Bay------------------------------
case when asm.rgon = 'Bay' and round(avg_hcc_scr,2)>=1.19 then 150 
when asm.rgon = 'Bay' and round(avg_hcc_scr,2)>=1.16 and round(avg_hcc_scr,2) < 1.19 then 125
when asm.rgon = 'Bay' and round(avg_hcc_scr,2)>=1.15 and round(avg_hcc_scr,2)<1.16 then 100
when asm.rgon = 'Bay' and round(avg_hcc_scr,2)>=1.13  and round(avg_hcc_scr,2)<1.15 then 75
when asm.rgon = 'Bay' and round(avg_hcc_scr,2)>=1.07 and round(avg_hcc_scr,2)<1.13 then 50
when asm.rgon = 'Bay' and round(avg_hcc_scr,2)<1.07 then 0
-------HCC Scoring for Central------------------------------
when asm.rgon = 'Central' and round(avg_hcc_scr,2)>=1.14 then 150
when asm.rgon = 'Central' and round(avg_hcc_scr,2)>=1.12 and round(avg_hcc_scr,2)<1.14 then 125
when asm.rgon = 'Central' and round(avg_hcc_scr,2)>=1.11 and round(avg_hcc_scr,2)<1.12 then 100
when asm.rgon = 'Central' and round(avg_hcc_scr,2)>=1.09 and round(avg_hcc_scr,2)<1.11 then 75
when asm.rgon = 'Central' and round(avg_hcc_scr,2)>=1.04 and round(avg_hcc_scr,2)<1.09 then 50
when asm.rgon = 'Central' and round(avg_hcc_scr,2)<1.04 then 0
-------HCC Scoring for Flint--------------------------------
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)>=1.18  then 150
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)>=1.15 and round(avg_hcc_scr,2)<1.18 then 125
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)>=1.14 and round(avg_hcc_scr,2)<1.15 then 100
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)>=1.12 and round(avg_hcc_scr,2)<1.14 then 75
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)>=1.06 and round(avg_hcc_scr,2)<1.12 then 50
when asm.rgon = 'Flint' and round(avg_hcc_scr,2)<1.06 then 0

-------HCC Scoring for Lansing--------------------------------
when asm.rgon = 'Greater Lansing' and round(avg_hcc_scr,2)>= 1.03  then 150
when asm.rgon = 'Greater Lansing' and round(avg_hcc_scr,2)>=1.01  and round(avg_hcc_scr,2)< 1.03  then 125
when asm.rgon = 'Greater Lansing' and round(avg_hcc_scr,2)>=1.00 and round(avg_hcc_scr,2)<1.01 then 100
when asm.rgon = 'Greater Lansing' and round(avg_hcc_scr,2)>=0.98 and round(avg_hcc_scr,2)<1.00 then 75
when asm.rgon = 'Greater Lansing' and round(avg_hcc_scr,2)>=0.93 and round(avg_hcc_scr,2)<0.98 then 50
when asm.rgon = 'Greater Lansing'and round(avg_hcc_scr,2)<0.93 then 0

-------HCC Scoring for Lapeer--------------------------------
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)>=1.12  then 150
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)>=1.10 and round(avg_hcc_scr,2)<1.12 then 125
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)>=1.09 and round(avg_hcc_scr,2)<1.10 then 100
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)>=1.07 and round(avg_hcc_scr,2)<1.09 then 75
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)>= 1.02  and round(avg_hcc_scr,2)<1.07 then 50
when asm.rgon = 'Lapeer' and round(avg_hcc_scr,2)<1.02 then 0

-------HCC Scoring for Macomb--------------------------------
when asm.rgon = 'Macomb' and round(avg_hcc_scr,2)>=1.20 then 150
when asm.rgon = 'Macomb' and round(avg_hcc_scr,2)>=1.17 and round(avg_hcc_scr,2)<1.20 then 125
when asm.rgon = 'Macomb' and round(avg_hcc_scr,2)>=1.16 and round(avg_hcc_scr,2)<1.17 then 100
when asm.rgon = 'Macomb' and round(avg_hcc_scr,2)>=1.14 and round(avg_hcc_scr,2)<1.16 then 75
when asm.rgon = 'Macomb' and round(avg_hcc_scr,2)>=1.08 and round(avg_hcc_scr,2)<1.14 then 50
when asm.rgon = 'Macomb' and  round(avg_hcc_scr,2)<1.08 then 0
-------HCC Scoring for McLaren Healthcare--------------------------------
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)>=1.07  then 150
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)>=1.05 and round(avg_hcc_scr,2)<1.07 then 125
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)>=1.04 and round(avg_hcc_scr,2)<1.05 then 100
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)>=1.02 and round(avg_hcc_scr,2)<1.04 then 75
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)>=0.97 and round(avg_hcc_scr,2)<1.02 then 50
--when asm.rgon = 'Mclaren Health Care' and round(avg_hcc_scr,2)<0.97 then 0

-------HCC Scoring for Northern--------------------------------
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)>= 1.03 then 150
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)>= 1.01 and round(avg_hcc_scr,2)<1.03 then 125
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)>= 1  and round(avg_hcc_scr,2)<1.01 then 100
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)>= 0.98 and round(avg_hcc_scr,2)<1 then 75
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)>= 0.93 and round(avg_hcc_scr,2)<0.98 then 50
when asm.rgon = 'Northern' and round(avg_hcc_scr,2)<0.93 then 0

-------HCC Scoring for Oakland--------------------------------
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)>=1.19 then 150
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)>=1.16 and round(avg_hcc_scr,2)<1.19 then 125
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)>=1.15  and round(avg_hcc_scr,2)<1.16 then 100
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)>=1.13 and round(avg_hcc_scr,2)<1.15 then 75
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)>= 1.07 and round(avg_hcc_scr,2)<1.13 then 50
when asm.rgon = 'Oakland' and round(avg_hcc_scr,2)< 1.07 then 0

-------HCC Scoring for Port Huron--------------------------------
when asm.rgon = 'Port Huron' and round(avg_hcc_scr,2)>=1.1 then 150
when asm.rgon = 'Port Huron' and round(avg_hcc_scr,2)>=1.08 and round(avg_hcc_scr,2)<1.10 then 125
when asm.rgon = 'Port Huron' and round(avg_hcc_scr,2)>=1.07  and round(avg_hcc_scr,2)<1.08 then 100
when asm.rgon = 'Port Huron' and round(avg_hcc_scr,2)>= 1.05 and round(avg_hcc_scr,2)<1.07 then 75
when asm.rgon = 'Port Huron' and round(avg_hcc_scr,2)>=1  and round(avg_hcc_scr,2)< 1.05 then 50
when asm.rgon = 'Port Huron'  and round(avg_hcc_scr,2)<1 then 0
else 0
end as hcc_pts,

-------Leakage Scoring for Bay------------------------------
0 as Leakage_pts,

-------SNF Admissions Scoring for Bay------------------------------
case when asm.rgon = 'Bay' and snf_dschrg<= 54.4 then 150
when asm.rgon = 'Bay' and snf_dschrg> 54.4 and snf_dschrg<=55.0 then 125
when asm.rgon = 'Bay' and snf_dschrg> 55.0 and snf_dschrg<=55.6  then 100
when asm.rgon = 'Bay' and snf_dschrg> 55.6 and snf_dschrg<=57.3 then 75
when asm.rgon = 'Bay' and snf_dschrg> 57.3 and snf_dschrg<= 60.2 then 50
-------SNF Admissions Scoring for Central------------------------------
when asm.rgon = 'Central' and snf_dschrg<= 39.8 then 150
when asm.rgon = 'Central' and snf_dschrg>39.8 and snf_dschrg<=40.2 then 125
when asm.rgon = 'Central' and snf_dschrg>40.2 and snf_dschrg<=40.6 then 100
when asm.rgon = 'Central' and snf_dschrg>40.6 and snf_dschrg<=41.9 then 75
when asm.rgon = 'Central' and snf_dschrg> 41.9 and snf_dschrg<=44 then 50
-------SNF Admissions Scoring for Flint--------------------------------
when asm.rgon = 'Flint' and snf_dschrg<=41.2 then 150
when asm.rgon = 'Flint' and snf_dschrg>41.2 and snf_dschrg<=41.7 then 125
when asm.rgon = 'Flint' and snf_dschrg>41.7 and snf_dschrg<=42.1 then 100
when asm.rgon = 'Flint' and snf_dschrg>42.1 and snf_dschrg<=43.4 then 75
when asm.rgon = 'Flint' and snf_dschrg>43.4 and snf_dschrg<=45.6 then 50

-------SNF Admissions Scoring for Lansing--------------------------------
when asm.rgon = 'Greater Lansing' and snf_dschrg<=44.6 then 150
when asm.rgon = 'Greater Lansing' and snf_dschrg>44.6 and snf_dschrg<=45.0 then 125
when asm.rgon = 'Greater Lansing' and snf_dschrg>45.0 and snf_dschrg<=45.5 then 100
when asm.rgon = 'Greater Lansing' and snf_dschrg>45.5 and snf_dschrg<=46.9 then 75
when asm.rgon = 'Greater Lansing'and snf_dschrg>46.9 and snf_dschrg<=49.2 then 50

-------SNF Admissions Scoring for Lapeer--------------------------------
when asm.rgon = 'Lapeer' and snf_dschrg<= 65.2 then 150
when asm.rgon = 'Lapeer' and snf_dschrg>65.2 and snf_dschrg<=65.9 then 125
when asm.rgon = 'Lapeer' and snf_dschrg>65.9 and snf_dschrg<=66.5 then 100
when asm.rgon = 'Lapeer' and snf_dschrg>66.5 and snf_dschrg<=68.6 then 75
when asm.rgon = 'Lapeer' and snf_dschrg>68.6 and snf_dschrg<=72 then 50

-------SNF Admissions Scoring for Macomb--------------------------------
when asm.rgon = 'Macomb' and snf_dschrg<=50.1 then 150
when asm.rgon = 'Macomb' and snf_dschrg>50.1 and snf_dschrg<=50.6 then 125
when asm.rgon = 'Macomb' and snf_dschrg>50.6 and snf_dschrg<=51.1 then 100
when asm.rgon = 'Macomb' and snf_dschrg>51.1 and snf_dschrg<=52.7 then 75
when asm.rgon = 'Macomb' and snf_dschrg>52.7 and snf_dschrg<=55.3 then 50
-------SNF Admissions Scoring for McLaren Healthcare--------------------------------
--when asm.rgon = 'Mclaren Health Care' and snf_dschrg<=60.8 then 150
--when asm.rgon = 'Mclaren Health Care' and snf_dschrg>60.8 and snf_dschrg<=61.4 then 125
--when asm.rgon = 'Mclaren Health Care' and snf_dschrg>61.4 and snf_dschrg<=62.7 then 100
--when asm.rgon = 'Mclaren Health Care' and snf_dschrg>62.7 and snf_dschrg<=64 then 75
--when asm.rgon = 'Mclaren Health Care' and snf_dschrg>64 and snf_dschrg<=67.2 then 50

-------SNF Admissions Scoring for Northern--------------------------------
when asm.rgon = 'Northern' and snf_dschrg<=26.9 then 150
when asm.rgon = 'Northern' and snf_dschrg>26.9 and snf_dschrg<=27.2 then 125
when asm.rgon = 'Northern' and snf_dschrg>27.2 and snf_dschrg<=27.5 then 100
when asm.rgon = 'Northern' and snf_dschrg>27.5 and snf_dschrg<=28.3 then 75
when asm.rgon = 'Northern' and snf_dschrg>28.3 and snf_dschrg<=29.7 then 50

-------SNF Admissions Scoring for Oakland--------------------------------
when asm.rgon = 'Oakland' and snf_dschrg<= 68.1 then 150
when asm.rgon = 'Oakland' and snf_dschrg>68.1  and snf_dschrg<=68.8 then 125
when asm.rgon = 'Oakland' and snf_dschrg>68.8 and snf_dschrg<=69.5 then 100
when asm.rgon = 'Oakland' and snf_dschrg>69.5 and snf_dschrg<=71.7 then 75
when asm.rgon = 'Oakland' and snf_dschrg>71.7 and snf_dschrg<=75.3 then 50

-------SNF Admissions Scoring for Port Huron--------------------------------
when asm.rgon = 'Port Huron' and snf_dschrg<=40.4 then 150
when asm.rgon = 'Port Huron' and snf_dschrg>40.4 and snf_dschrg<=40.8 then 125
when asm.rgon = 'Port Huron' and snf_dschrg>40.8 and snf_dschrg<=41.2 then 100
when asm.rgon = 'Port Huron' and snf_dschrg>41.2 and snf_dschrg<=42.5 then 75
when asm.rgon = 'Port Huron' and snf_dschrg>42.5 and snf_dschrg<=44.6 then 50
else 0
end as snf_adm_pts,

------------------ED Visits---------------------------------------
-------ED Visits Scoring for Bay------------------------------
case when asm.rgon = 'Bay' and ed_vst_ind<= 667 then 150
when asm.rgon = 'Bay' and ed_vst_ind> 667 and ed_vst_ind<=674 then 125
when asm.rgon = 'Bay' and ed_vst_ind> 674 and ed_vst_ind<=681 then 100
when asm.rgon = 'Bay' and ed_vst_ind> 681 and ed_vst_ind<=702 then 75
when asm.rgon = 'Bay' and ed_vst_ind> 702 and ed_vst_ind<=737 then 50
-------ED Visits Scoring for Central------------------------------
when asm.rgon = 'Central' and ed_vst_ind<= 775 then 150
when asm.rgon = 'Central' and ed_vst_ind> 775 and ed_vst_ind<=783 then 125
when asm.rgon = 'Central' and ed_vst_ind> 783 and ed_vst_ind<=792 then 100
when asm.rgon = 'Central' and ed_vst_ind> 792  and ed_vst_ind<=816 then 75
when asm.rgon = 'Central' and ed_vst_ind> 816 and ed_vst_ind<= 857 then 50
-------ED Visits Scoring for Flint--------------------------------
when asm.rgon = 'Flint' and ed_vst_ind<= 627 then 150
when asm.rgon = 'Flint' and ed_vst_ind> 627 and ed_vst_ind<=634 then 125
when asm.rgon = 'Flint' and ed_vst_ind> 634 and ed_vst_ind<=640 then 100
when asm.rgon = 'Flint' and ed_vst_ind> 640 and ed_vst_ind<=660 then 75
when asm.rgon = 'Flint' and ed_vst_ind> 660 and ed_vst_ind<=693 then 50

-------ED Visits Scoring for Lansing--------------------------------
when asm.rgon = 'Greater Lansing' and ed_vst_ind<= 575 then 150
when asm.rgon = 'Greater Lansing' and ed_vst_ind> 575 and ed_vst_ind<=581 then 125
when asm.rgon = 'Greater Lansing' and ed_vst_ind> 581 and ed_vst_ind<=587 then 100
when asm.rgon = 'Greater Lansing' and ed_vst_ind> 587 and ed_vst_ind<=605 then 75
when asm.rgon = 'Greater Lansing'and ed_vst_ind> 605 and ed_vst_ind<=635 then 50

-------ED Visits Scoring for Lapeer--------------------------------
when asm.rgon = 'Lapeer' and ed_vst_ind<= 631 then 150
when asm.rgon = 'Lapeer' and ed_vst_ind> 631 and ed_vst_ind<= 637 then 125
when asm.rgon = 'Lapeer' and ed_vst_ind> 637 and ed_vst_ind<= 644 then 100
when asm.rgon = 'Lapeer' and ed_vst_ind> 644 and ed_vst_ind<= 664 then 75
when asm.rgon = 'Lapeer' and ed_vst_ind> 664 and ed_vst_ind<= 697 then 50

-------ED Visits Scoring for Macomb--------------------------------
when asm.rgon = 'Macomb' and ed_vst_ind<= 625 then 150
when asm.rgon = 'Macomb' and ed_vst_ind>625 and ed_vst_ind<=632 then 125
when asm.rgon = 'Macomb' and ed_vst_ind> 632 and ed_vst_ind<=638 then 100
when asm.rgon = 'Macomb' and ed_vst_ind>638 and ed_vst_ind<=658 then 75
when asm.rgon = 'Macomb' and ed_vst_ind>658 and ed_vst_ind<=691 then 50
-----ED Visits Scoring for McLaren Healthcare--------------------------------
--when asm.rgon = 'Mclaren Health Care' and ed_vst_ind<=663 then 150
--when asm.rgon = 'Mclaren Health Care' and ed_vst_ind>663 and ed_vst_ind<=670 then 125
--when asm.rgon = 'Mclaren Health Care' and ed_vst_ind>670 and ed_vst_ind<=677 then 100
--when asm.rgon = 'Mclaren Health Care' and ed_vst_ind>677 and ed_vst_ind<=698 then 75
--when asm.rgon = 'Mclaren Health Care' and ed_vst_ind>698 and ed_vst_ind<=733 then 50

-------ED Visits Scoring for Northern--------------------------------
when asm.rgon = 'Northern' and ed_vst_ind<= 492 then 150
when asm.rgon = 'Northern' and ed_vst_ind>492 and ed_vst_ind=497 then 125
when asm.rgon = 'Northern' and ed_vst_ind>497 and ed_vst_ind<=502 then 100
when asm.rgon = 'Northern' and ed_vst_ind>502 and ed_vst_ind<=518 then 75
when asm.rgon = 'Northern' and ed_vst_ind>518 and ed_vst_ind<=544 then 50

-------ED Visits Scoring for Oakland--------------------------------
when asm.rgon = 'Oakland' and ed_vst_ind<= 750 then 150
when asm.rgon = 'Oakland' and ed_vst_ind>750  and ed_vst_ind<=757 then 125
when asm.rgon = 'Oakland' and ed_vst_ind>757 and ed_vst_ind<=765 then 100
when asm.rgon = 'Oakland' and ed_vst_ind>765 and ed_vst_ind<=789 then 75
when asm.rgon = 'Oakland' and ed_vst_ind>789 and ed_vst_ind<=828 then 50

-------ED Visits Scoring for Port Huron--------------------------------
when asm.rgon = 'Port Huron' and ed_vst_ind<= 493 then 150
when asm.rgon = 'Port Huron' and ed_vst_ind>493 and ed_vst_ind<=498 then 125
when asm.rgon = 'Port Huron' and ed_vst_ind>498 and ed_vst_ind<=503 then 100
when asm.rgon = 'Port Huron' and ed_vst_ind>503 and ed_vst_ind<=519 then 75
when asm.rgon = 'Port Huron' and ed_vst_ind>519 and ed_vst_ind<=545 then 50
else 0
end as ed_vst_pts
FROM pce_qe16_prd..aco_smry_msr asm);

DROP TABLE tmp_aco_sub_msr_combined IF EXISTS;
;create temp table  tmp_aco_sub_msr_combined as 
with grouped_data as 
(select  * from tmp_aco_sub_msr_achvmnt 
UNION ALL 
select  *  from tmp_aco_sub_msr_imprvmnt 
)
select rpt_prd_strt_dt, rpt_prd_end_dt, rgon, 
max(prsn_yrs) as prsn_yrs,
max(mbr_cnt ) as mbr_cnt,
max(pbpy) as pbpy,
max(avg_hcc_scr) as avg_hcc_scr, 
max(ed_vst_ind) as ed_vst_ind,
max(snf_dschrg) as snf_dschrg, 
max(lkg_amt) as lkg_amt, 
max(lkg_amt_xld) as lkg_amt_xld, 
fcy_num, 
max(pbpy_pts) as pbpy_pts, 
max(hcc_pts) as hcc_pts, 
max(leakage_pts) as leakage_pts, 
max(snf_adm_pts) as snf_adm_pts, 
max(ed_vst_pts) as ed_vst_pts
from grouped_data
group by 1,2,3,12;

---------

drop table stg_aco_mpp_msr_fct IF EXISTS;

create  table stg_aco_mpp_msr_fct as 
(select distinct taco.*,
taco.pbpy_pts*0.25 as pbpy_scr,
--UPDATED : 12/15/2O20: FY21 DASHBOARD
--taco.hcc_pts*0.25 as hcc_scr,
--taco.Leakage_pts*0.125 as leakage_scr,
--taco.snf_adm_pts*0.25 as sbf_adm_scr,
--taco.ed_vst_pts*0.125 as ed_vst_scr,
taco.hcc_pts*0.333 as hcc_scr,
taco.Leakage_pts*0.125 as leakage_scr,
taco.snf_adm_pts*0.333 as sbf_adm_scr,
taco.ed_vst_pts*0.333 as ed_vst_scr,
--pbpy_scr+hcc_scr+leakage_scr+sbf_adm_scr+ed_vst_scr as mpp_scr,
hcc_scr+sbf_adm_scr+ed_vst_scr as mpp_scr,
case when round(mpp_scr) <100 then 'R'
--WHEN MPP_SCR >=100 and MPP_SCR <150 then 'G'
--WHEN MPP_SCR>=150 then 'B'
WHEN round(MPP_SCR) >=100 then 'G'
END AS MPP_SCR_COLOR_ATTR,
CD.mo_and_yr_nm AS DATE_PARAMETER
--Added on 12/28/2020: FY 21 Dashboard
--from tmp_aco_sub_msr taco
from tmp_aco_sub_msr_combined taco
INNER JOIN  pce_ae00_aco_prd_cdr..cdr_dim CD ON taco.rpt_prd_end_dt= CD.CDR_DT);

--select * from  stg_aco_mpp_msr_fct; 

/*Other Tables for Clinical Outcome Score
*/

----Cardiac Rehab for Clinical Outcome Score
DROP TABLE tmp_cardiac_rehab_fct IF EXISTS;
create  table  tmp_cardiac_rehab_fct as
with cdr_dim_data AS 
(select cdr_dt,qtr_and_yr_abbr FROM (select cdr_Dt,qtr_and_yr_abbr,
row_number() over(partition by qtr_and_yr_abbr ORDER BY cdr_Dt ) as rank_num
from  pce_ae00_aco_prd_cdr..cdr_dim cd
) X 
WHERE
date_part('day',cdr_dt) = '01')
--X.rank_num =1 )
--select * from cdr_dim_data
(SELECT 

distinct clientname as fcy_nm,
case when ncdr.clientname = 'McLaren Bay Region' then 'MI2191' 
 when ncdr.clientname = 'McLaren Central' then 'MI2061' 
--when p.company_id = 'Central' then 'MI2061' 
when ncdr.clientname= 'McLaren Flint' then  'MI2302' 
---when p.company_id ='Karmanos' then '634342' 
when ncdr.clientname='McLaren Greater Lansing' then 'MI5020' 
--when p.company_id = 'Lapeer' then 'MI2001'
when ncdr.clientname = 'McLaren Lapeer Region' then 'MI2001' 
when ncdr.clientname = 'McLaren Macomb' then 'MI2048' 
when ncdr.clientname ='McLaren Northern Michigan' then '637619' 
when ncdr.clientname ='McLaren Oakland' then 'MI2055' 
when ncdr.clientname ='McLaren Port Huron Hospital' then '600816'
else null end as fcy_num,
ncdr.timeframecode,
add_months(cd.cdr_dt,4)   as event_dt,
1 as Join_key,
--min(cd.cdr_dt) as eent_dt,
sum(snpsr4qnumerator) as snpsr4qnumerator, 
sum(snpsr4qdenominator) as snpsr4qdenominator
FROM pce_qe16_prd..ncdr_hsptl_fct_vw ncdr
inner join cdr_dim_data cd on replace(cd.qtr_and_yr_abbr,' ','') = ncdr.timeframecode 
where ncdr.market_name = 'My Group' and ncdr.metricid = 45 and ncdr.clientname is not null
group by 1,2,3,4);

DROP TABLE tmp_cardiac_rehab IF EXISTS;
create temp table  tmp_cardiac_rehab as
with cdr_dim_data AS 
(select cdr_dt,qtr_and_yr_abbr FROM (select cdr_Dt,qtr_and_yr_abbr,
row_number() over(partition by qtr_and_yr_abbr ORDER BY cdr_Dt ) as rank_num
from  pce_ae00_aco_prd_cdr..cdr_dim cd
) X 
WHERE
--date_part('day',cdr_dt) = '01')
X.rank_num =1 )
--select * from cdr_dim_data
(SELECT 

distinct clientname as fcy_nm,
case when ncdr.clientname = 'McLaren Bay Region' then 'MI2191' 
--when p.company_id = 'Central' then 'MI2061' 
when ncdr.clientname= 'McLaren Flint' then  'MI2302' 
--when p.company_id ='Karmanos' then '634342' 
when ncdr.clientname='McLaren Greater Lansing' then 'MI5020' 
--when p.company_id = 'Lapeer' then 'MI2001'
when ncdr.clientname = 'McLaren Macomb' then 'MI2048' 
when ncdr.clientname ='McLaren Northern Michigan' then '637619' 
when ncdr.clientname ='McLaren Oakland' then 'MI2055' 
when ncdr.clientname ='McLaren Port Huron Hospital' then '600816'
else null end as fcy_num,
ncdr.timeframecode,
add_months(min(cd.cdr_dt),4) as event_dt,
1 as Join_key,
sum(snpsr4qnumerator) as snpsr4qnumerator, 
sum(snpsr4qdenominator) as snpsr4qdenominator
FROM pce_qe16_prd..ncdr_hsptl_fct_vw ncdr
inner join cdr_dim_data cd on replace(cd.qtr_and_yr_abbr,' ','') = ncdr.timeframecode 
where ncdr.market_name = 'My Group' and ncdr.metricid = 45 and ncdr.clientname is not null
group by 1,2,3);

DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt,1 as Join_key from stg_encntr_qs_anl_fct_vw ef);

---New Code----------------------

drop table stg_tmp_card_rehab_fct if exists;
create  table stg_tmp_card_rehab_fct as 
select tcr.fcy_nm,
tcr.fcy_num,
tcr.event_dt as rpt_dt,
tcr.snpsr4qnumerator as snpsr4qnumerator,
tcr.snpsr4qdenominator as snpsr4qdenominator,
ROUND(tcr.snpsr4qnumerator/tcr.snpsr4qdenominator,4) as cardiac_rt
from tmp_cardiac_rehab_fct tcr;


---FY2021 Dashboard
DROP TABLE stg_tmp_pci_radial_access_fct IF EXISTS;
CREATE TABLE stg_tmp_pci_radial_access_fct as 
(SELECT 

case when pci.facility in ('Bay','McLaren Bay Region') then 'McLaren Bay Region'
when pci.facility in  ('Central','McLaren Central Michigan') then 'McLaren Central Michigan' 
when pci.facility in  ('Flint','McLaren Flint') then 'McLaren Flint'
when pci.facility in  ('Lansing','McLaren Greater Lansing') then 'McLaren Greater Lansing' 
when pci.facility in  ('Lapeer','McLaren Lapeer Region') then 'McLaren Lapeer Region' 
when pci.facility in  ('Macomb','McLaren Macomb') then 'McLaren Macomb'
when pci.facility in  ('Northern','McLaren Northern Michigan') then 'McLaren Northern Michigan'
when pci.facility in  ('Oakland','McLaren Oakland') then 'McLaren Oakland'
when pci.facility in  ('Port Huron','McLaren Port Huron') then 'McLaren Port Huron Hospital'
else pci.facility end
as fcy_nm,

case when facility in ('Bay','McLaren Bay Region') then 'MI2191'
when facility in  ('Central','McLaren Central Michigan') then 'MI2061'
when facility in  ('Flint','McLaren Flint') then 'MI2302'
when facility in  ('Lansing','McLaren Greater Lansing') then 'MI5020'
when facility in  ('Lapeer','McLaren Lapeer Region') then 'MI2001'
when facility in  ('Macomb','McLaren Macomb') then 'MI2048'
when facility in  ('Northern','McLaren Northern Michigan') then '637619'
when facility in  ('Oakland','McLaren Oakland') then 'MI2055'
when facility in  ('Port Huron','McLaren Port Huron') then '600816'
else null
end as fcy_num,
date(pci.report_month)  as rpt_dt,
sum(numerator) as pci_numr,
sum(denominator) as pci_dnmr,
round(sum(numerator)/sum(denominator),3) as pci_radial_access_rt
--FROM pce_qe16_slp_prd_dm..mclaren_ncdr_pciradial_20200908_00000000_dev pci
FROM pce_qe16_misc_prd_lnd..ncdr_pciradial_fct  pci
  group by 1,2,3
);

select * from stg_tmp_pci_radial_access_fct; 

DROP TABLE stg_tmp_pci_radial_access_r12m_fct IF EXISTS;
CREATE TABLE stg_tmp_pci_radial_access_r12m_fct as 
with grouped_data as 
(
  select 
 case when pci.facility in ('Bay','McLaren Bay Region') then 'McLaren Bay Region'
when pci.facility in  ('Central','McLaren Central Michigan') then 'McLaren Central Michigan' 
when pci.facility in  ('Flint','McLaren Flint') then 'McLaren Flint'
when pci.facility in  ('Lansing','McLaren Greater Lansing') then 'McLaren Greater Lansing' 
when pci.facility in  ('Lapeer','McLaren Lapeer Region') then 'McLaren Lapeer Region' 
when pci.facility in  ('Macomb','McLaren Macomb') then 'McLaren Macomb'
when pci.facility in  ('Oakland','McLaren Oakland') then 'McLaren Oakland'
when pci.facility in  ('Northern','McLaren Northern Michigan') then 'McLaren Northern Michigan'
when pci.facility in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then 'McLaren Port Huron Hospital'
else pci.facility end
as fcy_nm,
case when facility in ('Bay','McLaren Bay Region') then 'MI2191'
when facility in  ('Central','McLaren Central Michigan') then 'MI2061'
when facility in  ('Flint','McLaren Flint') then 'MI2302'
when facility in  ('Lansing','McLaren Greater Lansing') then 'MI5020'
when facility in  ('Lapeer','McLaren Lapeer Region') then 'MI2001'
when facility in  ('Macomb','McLaren Macomb') then 'MI2048'
when facility in  ('Northern','McLaren Northern Michigan') then '637619'
when facility in ('Oakland','McLaren Oakland') then 'MI2055'
when facility in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then '600816'
else null
end as fcy_num,
  date_trunc('month',report_month) as report_month,
 sum(numerator) as pci_agg_numr,
 sum(denominator) as pci_agg_dnmr 
from pce_qe16_misc_prd_lnd..ncdr_pciradial_fct  pci --where facility = 'Bay' --and report_month between '2019-05-01' and '2020-05-01'
group by 1,2,3
order by 1 ASC,2 asc,3 desc
)
select fcy_nm,
fcy_num,
date(report_month) as rpt_dt,
sum(pci_agg_numr) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as pci_numr,
sum(pci_agg_dnmr) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as pci_dnmr,
nvl(round((pci_numr/pci_dnmr) *100,3),0) as pci_radial_access_rt
from grouped_data
order by 1 ASC,2 asc,3 desc;

select * From stg_tmp_pci_radial_access_fct where fcy_nm = 'McLaren Bay Region';
select * From stg_tmp_pci_radial_access_r12m_fct where fcy_nm = 'McLaren Bay Region';

DROP TABLE stg_tmp_onc_awbi_fct IF EXISTS;
CREATE TABLE stg_tmp_onc_awbi_fct as 
with onc_awbi_fct_copy as
(SELECT  onc_region
       , report_month
       , case when denom = 0 then NULL else denom end as denom 
       , case when numerator =0 then 0 else numerator end as numerator
  FROM pce_qe16_misc_prd_lnd.prmretlp.onc_awbi_fct
 )
   select 
case when onc_region in ('Bay','McLaren Bay Region') then 'McLaren Bay Region'
when onc_region in  ('Central','McLaren Central Michigan') then 'McLaren Central Michigan' 
when onc_region in  ('Flint','McLaren Flint') then 'McLaren Flint'
when onc_region in  ('Lansing','McLaren Greater Lansing') then 'McLaren Greater Lansing' 
when onc_region in  ('Lapeer','McLaren Lapeer Region') then 'McLaren Lapeer Region' 
when onc_region in  ('Macomb','McLaren Macomb') then 'McLaren Macomb'
when onc_region in  ('Oakland','McLaren Oakland') then 'McLaren Oakland'
when onc_region in  ('Karmanos','McLaren Karmanos') then 'McLaren Karmanos'
when onc_region in  ('Northern','McLaren Northern Michigan') then 'McLaren Northern Michigan'
when onc_region in  ('Port Huron','McLaren Port Huron','McLaren Port Huron Hospital') then 'McLaren Port Huron'
else onc_region end
as fcy_nm,

case when onc_region in ('Bay','McLaren Bay Region') then 'MI2191'
when onc_region in  ('Central','McLaren Central Michigan') then 'MI2061'
when onc_region in  ('Flint','McLaren Flint') then 'MI2302'
when onc_region in  ('Lansing','McLaren Greater Lansing') then 'MI5020'
when onc_region in  ('Lapeer','McLaren Lapeer Region') then 'MI2001'
when onc_region in  ('Macomb','McLaren Macomb') then 'MI2048'
when onc_region in  ('Northern','McLaren Northern Michigan') then '637619'
when onc_region in ('Karmanos','McLaren Karmanos') then '634342'
when onc_region in ('Oakland','McLaren Oakland') then 'MI2055'
when onc_region in  ('Port Huron','McLaren Port Huron', 'McLaren Port Huron Hospital') then '600816'
else null
end as fcy_num,
   date(report_month)  as rpt_dt,
   sum(numerator) as awbi_numr,
   sum(denom) as awbi_dnmr,
   round(sum(numerator)/sum(denom),3) as onc_awbi_rt
  -- from pce_qe16_misc_prd_lnd..onc_awbi_fct
   from onc_awbi_fct_copy   -- TODO Need to replace onc_awbi_fct_copy with actual table  onc_awbi_fct since Denom = 0 and sql is failing 
   GROUP BY 1,2,3
;
select * from stg_tmp_onc_awbi_fct; 
select * from pce_qe16_misc_prd_lnd..onc_awbi_fct;

DROP TABLE stg_tmp_onc_awbi_r12m_fct IF EXISTS;
CREATE TABLE stg_tmp_onc_awbi_r12m_fct as 
with onc_awbi_fct_copy as
(SELECT  onc_region
       , report_month
       , case when denom = 0 then NULL else denom end as denom 
       , case when numerator =0 then 0 else numerator end as numerator
  FROM pce_qe16_misc_prd_lnd.prmretlp.onc_awbi_fct
 )
,grouped_data as 
(
  select 
 case when awbi.onc_region in ('Bay','McLaren Bay Region') then 'McLaren Bay Region'
when awbi.onc_region in  ('Central','McLaren Central Michigan') then 'McLaren Central Michigan' 
when awbi.onc_region in  ('Flint','McLaren Flint') then 'McLaren Flint'
when awbi.onc_region in  ('Lansing','McLaren Greater Lansing') then 'McLaren Greater Lansing' 
when awbi.onc_region in  ('Lapeer','McLaren Lapeer Region') then 'McLaren Lapeer Region' 
when awbi.onc_region in  ('Macomb','McLaren Macomb') then 'McLaren Macomb'
when awbi.onc_region in  ('Northern','McLaren Northern Michigan') then 'McLaren Northern Michigan'
when awbi.onc_region in  ('Oakland','McLaren Oakland') then 'McLaren Oakland'
when awbi.onc_region in  ('Port Huron','McLaren Port Huron', 'McLaren Port Huron Hospital') then 'McLaren Port Huron'
else awbi.onc_region end
as fcy_nm,
case when awbi.onc_region in ('Bay','McLaren Bay Region') then 'MI2191'
when awbi.onc_region in  ('Central','McLaren Central Michigan') then 'MI2061'
when awbi.onc_region in  ('Flint','McLaren Flint') then 'MI2302'
when awbi.onc_region in  ('Lansing','McLaren Greater Lansing') then 'MI5020'
when awbi.onc_region in  ('Lapeer','McLaren Lapeer Region') then 'MI2001'
when awbi.onc_region in  ('Macomb','McLaren Macomb') then 'MI2048'
when awbi.onc_region in  ('Northern','McLaren Northern Michigan') then '637619'
when awbi.onc_region in  ('Oakland','McLaren Oakland') then 'MI2055'
when awbi.onc_region in  ('Port Huron','McLaren Port Huron', 'McLaren Port Huron Hospital') then '600816'
when awbi.onc_region in ('Karmanos','McLaren Karmanos') then '634342'
else null
end as fcy_num,
  date_trunc('month',report_month) as report_month,
 sum(numerator) as awbi_agg_numr,
 sum(denom) as awbi_agg_dnmr 
from onc_awbi_fct_copy  awbi --where facility = 'Bay' --and report_month between '2019-05-01' and '2020-05-01'
group by 1,2,3
order by 1 ASC,2 asc,3 desc
)
--select * from grouped_data where fcy_nm = 'Karmanos'
select fcy_nm,
fcy_num,
date(report_month) as rpt_dt,
sum(awbi_agg_numr) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as awbi_numr,
sum(awbi_agg_dnmr) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as awbi_dnmr,
nvl(round((awbi_numr/awbi_dnmr)* 100,3),0) as awbi_radial_access_rt
from grouped_data
order by 1 ASC,2 asc,3 desc;
  
  ------------------------OB Training Fact for Clinical Outcome Score
  drop table stg_tmp_ob_trn_fct if exists;
create  table stg_tmp_ob_trn_fct as 
(SELECT 

case when otf.fcy_nm in ('Bay','McLaren Bay Region') then 'McLaren Bay Region'
when otf.fcy_nm in  ('Central','McLaren Central Michigan') then 'McLaren Central Michigan' 
when otf.fcy_nm in  ('Flint','McLaren Flint') then 'McLaren Flint'
when otf.fcy_nm in  ('Lansing','McLaren Greater Lansing') then 'McLaren Greater Lansing' 
when otf.fcy_nm in  ('Lapeer','McLaren Lapeer Region') then 'McLaren Lapeer Region' 
when otf.fcy_nm in  ('Macomb','McLaren Macomb') then 'McLaren Macomb'
when otf.fcy_nm in  ('Northern','McLaren Northern Michigan') then 'McLaren Northern Michigan'
when otf.fcy_nm in  ('Port Huron','McLaren Port Huron', 'McLaren Port Huron Hospital') then 'McLaren Port Huron'
else otf.fcy_nm end
as fcy_nm,

case when fcy_nm in ('Bay','McLaren Bay Region') then 'MI2191'
when fcy_nm in  ('Central','McLaren Central Michigan') then 'MI2061'
when fcy_nm in  ('Flint','McLaren Flint') then 'MI2302'
when fcy_nm in  ('Lansing','McLaren Greater Lansing') then 'MI5020'
when fcy_nm in  ('Lapeer','McLaren Lapeer Region') then 'MI2001'
when fcy_nm in  ('Macomb','McLaren Macomb') then 'MI2048'
when fcy_nm in  ('Northern','McLaren Northern Michigan') then '637619'
when fcy_nm in  ('Port Huron','McLaren Port Huron', 'McLaren Port Huron Hospital') then '600816'
else null
end as fcy_num,
date(otf.rpt_mo)  as rpt_dt,
sum(msr_numr) as numr,
sum(msr_dnmr) as dnmr,
sum(msr_numr)/sum(msr_dnmr) as ob_rt
  FROM pce_qe16_prd..ob_trn_fct otf
  group by 1,2,3
);

DROP TABLE tmp_sep_compl IF EXISTS;
;create temp table  tmp_sep_compl as
(SELECT csf.fcy_nm,  
csf.fcy_num,
csf.dschrg_dt,
1 as join_key,
count(distinct(case when csf.sep1_cgy = 'E' then csf.patient_id else null end)) as numr,
count(distinct(case when csf.sep1_cgy = 'D' or csf.sep1_cgy = 'E' then csf.patient_id else null end)) as dnmr

FROM pce_qe16_misc_prd_lnd.prmretlp.cm_sep1_fct csf
group by 1,2,3);

DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt, 1 as join_key from stg_encntr_qs_anl_fct_vw ef);

drop table stg_tmp_sep_compl_fct if exists;
create  table stg_tmp_sep_compl_fct as 
(select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and last_day(add_months((select max_dt from tmp_max_dschrg_dt),-1))
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2



Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2

Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2

Union

select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
--and tsc.fcy_nm<>'Greater Lansing'
group by 1,2
);
-------Added on 12/21/2020: FY21 Dashboard Oncology Related Measures 
select count(distinct survey_id)
FROM tmp_sf where fcy_nm = 'Karmanos' and service='ON' and date(dschrg_dt) between '2020-08-01' and '2020-10-31' and varname in ('O3');  --642 Unique Survey ID's

select count(*)  FROm 
(select *
FROM tmp_sf where fcy_nm = 'Karmanos' and service='ON' and date(dschrg_dt) between '2020-12-01' and '2020-12-31' and varname in ('O3')  ) Z;


DROP TABLE tmp_sub_onc_ptnt_exrnc_msr_fct IF EXISTS;
;create temp table  tmp_sub_onc_ptnt_exrnc_msr_fct as
(
SELECT distinct service, 
 tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-1) as rprt_dt,
sum((CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-3))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-1))
group by 1,2,3,4

UNION

SELECT distinct service, 
 tsf.client_id,
tsf.FCY_NM,
tm.dschrg_dt as rprt_dt,
sum((CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-2))
and (select last_day(max(dschrg_dt)) from tmp_sf)
group by 1,2,3,4

UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-2) as rprt_dt,
sum(  (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-4))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-2))
group by 1,2,3,4


UNION

 SELECT distinct tsf.service, 
 
 tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-3) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-5))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-3))
group by 1,2,3,4

UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-4) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-6))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-4))
group by 1,2,3,4


UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-5) as rprt_dt,
sum(  (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-7))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-5))
group by 1,2,3,4

UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-6) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-8))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-6))
group by 1,2,3,4



UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-7) as rprt_dt,
sum(  (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-9))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-7))
group by 1,2,3,4


UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-8) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-10))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-8))
group by 1,2,3,4

UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-9) as rprt_dt,
sum(  (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-11))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-9))
group by 1,2,3,4



UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-10) as rprt_dt,
sum(  (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-12))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-10))
group by 1,2,3,4


UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-11) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-13))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-11))
group by 1,2,3,4

UNION

 SELECT distinct service, 
  tsf.client_id,
tsf.FCY_NM,
add_months(tm.dschrg_dt,-12) as rprt_dt,
sum( (CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt
FROM tmp_sf tsf
inner join tmp_maxd tm  on tsf.join_key = tm.join_key
where service='ON' and varname in ('O3') and tsf.dschrg_dt between date_trunc('month',add_months((select max(dschrg_dt) from tmp_sf),-14))
and last_day(add_months((select max(dschrg_dt) from tmp_sf),-12))
group by 1,2,3,4);

select * from tmp_sub_onc_ptnt_exrnc_msr_fct where service = 'ON' and date(rprt_Dt) between '2019-11-01' and '2020-10-31';

DROP TABLE stg_onc_ptnt_exrnc_pct_msr_fct IF EXISTS;
;create  table  stg_onc_ptnt_exrnc_pct_msr_fct as 
(select tspe.service,
tspe.client_id,
tspe.FCY_NM,
date_trunc('month',tspe.rprt_dt) as rprt_dt,
sum(tspe.resp_cnt) as resp_cnt,
sum(tspe.top_box_resp_cnt) as top_box_resp_cnt,
(sum(tspe.top_box_resp_cnt)/sum(tspe.resp_cnt))*100 as sub_ptnt_exrnc_pct,
round(((sum(tspe.top_box_resp_cnt)/sum(tspe.resp_cnt))*100),1) as ptnt_exrnc_pct
from tmp_sub_onc_ptnt_exrnc_msr_fct tspe
where service = 'ON'
group by 1,2,3,4);

select * from stg_onc_ptnt_exrnc_pct_msr_fct; 

--drop  table stg_onc_ptnt_exrnc_pct_msr_fct if exists ;
--create  table stg_onc_ptnt_exrnc_pct_msr_fct as 
--(select tmppm.*,
--case when service= 'ON' then 0.1
--end as msr_wt,
--ppep.prgny_pct,
--ppep.prgny_pct*msr_wt as prgny_pct_scor
--from tmp_onc_ptnt_exrnc_pct_msr_fct tmppm
----left join pce_qe16_pressganey_prd_zoom..prgny_ptnt_exrnc_pct_dim ppep on tmppm.service = ppep.svc_cd and tmppm.ptnt_exrnc_pct = ppep.scor);
--left join pce_qe16_pressganey_prd_zoom..prgny_ptnt_exrnc_pct_dim ppep on tmppm.service = ppep.svc_cd and tmppm.ptnt_exrnc_pct = ppep.scor);


select * from stg_onc_ptnt_exrnc_pct_msr_fct where date(rprt_dt) = '2020-05-01'; 
select * from stg_onc_ptnt_exrnc_pct_msr_fct; 
select distinct svc_cd from pce_qe16_pressganey_prd_zoom..prgny_ptnt_exrnc_pct_dim; 
------------

/*
Final Clinical Outcome Score Query
*/

drop table stg_clncl_outc_scor_fct if exists;
create  table stg_clncl_outc_scor_fct as 
(select distinct tmi.fcy_nm,
tmi.fcy_num,
tmi.rpt_dt,
max(tmi.mort_obs_rt) as mort_obs_rt, 
max(tmi.mort_expc_rt) as mort_expc_rt, 
max(tmi.mort_oe_rt) as mort_oe_rt, 
max(pcf.cmplc_obsr_rt) as cmplc_obsr_rt,
max(pcf.cmplc_expc_rt) as cmplc_expc_rt,
max(cast(pcf.cmplc_obsr_rt as decimal)/cast(pcf.cmplc_expc_rt as decimal)) as comp_oe_rt,
max(pcf.obsr_cases) as cmplc_obsr_cases,
max(pcf.outc_cases) as cmplc_outc_cases,
round(max(tri.readm_obs_rt),3) as readm_obs_rt, 
max(tri.readm_expc_rt) as readm_expc_rt, 
max(tri.readm_oe_rt) as readm_oe_rt,
max(tlu.qty) as qty, 
max(tlu.ptnt_days) as ptnt_days, 
max(tlu.lab_utlz) as lab_utlz,
max(tcrf.snpsr4qnumerator) as snpsr4qnumerator,
max(tcrf.snpsr4qdenominator) as snpsr4qdenominator,
max(tcrf.cardiac_rt) as cardiac_rt,
--Added 12/10/2020: FY21 Dashboard Metric
max(tpci.pci_numr) as pci_numr,
max(tpci.pci_dnmr) as pci_demr,
max(tpci.pci_radial_access_rt) as pci_radial_access_rt,
max(tot.numr) as ob_numr,
max(tot.dnmr) as ob_dnmr,
max(tot.ob_rt) as ob_rt,
max(tscf.numr) as complc_numr,
max(tscf.dnmr) as complc_dnmr,
max(tscf.complc) as complc_rt,
max(acof.mpp_scr) as mpp_scr,
max(thef.harm_events_cnt) as harm_events_cnt,
max(tzef.zero_events_numr) as zero_events_numr,
max(tzef.zero_events_dnmr) as zero_events_dnmr,
max(tzef.zero_events_rt)*100 as zero_events_rt,
max(pepm.ptnt_exrnc_pct) as ptnt_exrnc_pct,
max(pepm.prgny_pct_scor) as prgny_pct_scor,
max(pepm.prgny_pct) AS prgny_pct,
max(tpep.top_box_resp_cnt) as top_box_resp_cnt,
max(tpep.resp_cnt) as resp_cnt,
max(tpep.ptnt_exrnc_scor) as ptnt_exrnc_scor,
max(tpep.ptnt_exrnc_clr_cdg) as ptnt_exrnc_clr_cdg,
max(tsm.mort_obs_rt) as sep_mort_obs_rt,
max(tsm.mort_expc_rt) as sep_mort_expc_rt,
max(tsm.mort_oe_rt) as sep_mort_oe_rt,
max(tscf.numr) as sep_compliance_numr,
max(tscf.dnmr) as sep_compliance_dnmr,
max(tscf.numr)/max(tscf.dnmr) as sep_compliance_rt,
--Updated on 12/10/2020 FY 21 Dashboard
(case 
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3) > 0.135 then 0
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.123 and round(max(tri.readm_obs_rt),3) <= 0.135 then 50
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.121 and round(max(tri.readm_obs_rt),3) <= 0.123 then 75
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.118 and round(max(tri.readm_obs_rt),3) <= 0.121 then 100
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.116 and round(max(tri.readm_obs_rt),3) <= 0.118 then 125
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)<=0.116 then 150

when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3) > 0.08 then 0
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.073 and round(max(tri.readm_obs_rt),3) <= 0.08 then 50
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.072 and round(max(tri.readm_obs_rt),3) <= 0.073 then 75
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.07 and round(max(tri.readm_obs_rt),3) <= 0.072 then 100
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.069 and round(max(tri.readm_obs_rt),3) <= 0.07 then 125
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)<=0.069 then 150

when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3) > 0.14 then 0
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.127 and round(max(tri.readm_obs_rt),3) <= 0.14 then 50
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.124 and round(max(tri.readm_obs_rt),3) <= 0.127 then 75
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.122 and round(max(tri.readm_obs_rt),3) <= 0.124 then 100
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.119 and round(max(tri.readm_obs_rt),3) <= 0.122 then 125
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)<=0.119 then 150


when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3) > 0.118 then 0
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.107 and round(max(tri.readm_obs_rt),3) <= 0.118 then 50
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.105 and round(max(tri.readm_obs_rt),3) <= 0.107 then 75
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.103 and round(max(tri.readm_obs_rt),3) <= 0.105 then 100
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.101 and round(max(tri.readm_obs_rt),3) <= 0.103 then 125
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)<=0.101 then 150

when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3) > 0.124 then 0
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.113 and round(max(tri.readm_obs_rt),3) <= 0.124 then 50
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.111 and round(max(tri.readm_obs_rt),3) <= 0.113 then 75
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.108 and round(max(tri.readm_obs_rt),3) <= 0.111 then 100
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.106 and round(max(tri.readm_obs_rt),3) <= 0.108 then 125
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)<=0.106 then 150

when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3) > 0.130 then 0
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.118 and round(max(tri.readm_obs_rt),3) <= 0.130 then 50
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.116 and round(max(tri.readm_obs_rt),3) <= 0.118 then 75
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.113 and round(max(tri.readm_obs_rt),3) <= 0.116 then 100
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.111 and round(max(tri.readm_obs_rt),3) <= 0.113 then 125
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)<=0.111 then 150

when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3) > 0.099 then 0
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.090 and round(max(tri.readm_obs_rt),3) <= 0.099 then 50
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.088 and round(max(tri.readm_obs_rt),3) <= 0.090 then 75
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.086 and round(max(tri.readm_obs_rt),3) <= 0.088 then 100
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.085 and round(max(tri.readm_obs_rt),3) <= 0.086 then 125
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)<=0.085 then 150

when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3) > 0.124 then 0
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.113 and round(max(tri.readm_obs_rt),3) <= 0.124 then 50
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.111 and round(max(tri.readm_obs_rt),3) <= 0.113 then 75
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.108 and round(max(tri.readm_obs_rt),3) <= 0.111 then 100
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.106 and round(max(tri.readm_obs_rt),3) <= 0.108 then 125
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)<=0.106 then 150

when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3) > 0.152 then 0
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.138 and round(max(tri.readm_obs_rt),3) <= 0.152 then 50
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.135 and round(max(tri.readm_obs_rt),3) <= 0.138 then 75
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.132 and round(max(tri.readm_obs_rt),3) <= 0.135 then 100
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.130 and round(max(tri.readm_obs_rt),3) <= 0.132 then 125
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)<=0.130 then 150

else 0
end) as readm_obs_pts,
--Updated on 12/10/2020 FY 21 Dashboard
(case 
when  round(max(tri.readm_oe_rt),2) > 1.07 then 0
when  round(max(tri.readm_oe_rt),2)> 0.97 and round(max(tri.readm_oe_rt),2) <= 1.07 then 50
when  round(max(tri.readm_oe_rt),2)> 0.95 and round(max(tri.readm_oe_rt),2) <= 0.97 then 75
when  round(max(tri.readm_oe_rt),2)> 0.93 and round(max(tri.readm_oe_rt),2) <= 0.95 then 100
when round(max(tri.readm_oe_rt),2)> 0.91 and round(max(tri.readm_oe_rt),2) <= 0.93 then 125
when  round(max(tri.readm_oe_rt),2)<=0.91 then 150
else 0
end) as readm_oe_pts,

(case 
when  round(max(tmi.mort_oe_rt),2) > 1.0 then 0
when  round(max(tmi.mort_oe_rt),2)  > 0.87 and round(max(tmi.mort_oe_rt),2)  <= 1.0 then 50
when  round(max(tmi.mort_oe_rt),2)  > 0.77 and round(max(tmi.mort_oe_rt),2)  <= 0.87 then 75
when  round(max(tmi.mort_oe_rt),2)  > 0.7 and  round(max(tmi.mort_oe_rt),2)  <= 0.77 then 100
when  round(max(tmi.mort_oe_rt),2)  > 0.67 and round(max(tmi.mort_oe_rt),2)  <= 0.7 then 125
when  round(max(tmi.mort_oe_rt),2)  <=0.67 then 150
else 0
end) as mort_oe_pts,

--Updated 12/10/2020: FY 21 Dashboard
(
--Bay 
case 
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)> 1.2 then 0
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)> 1.11 and round(max(tsm.mort_oe_rt),2) <= 1.2 then 50
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)> 1.09 and round(max(tsm.mort_oe_rt),2) <= 1.11 then 75
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)> 1.07 and round(max(tsm.mort_oe_rt),2) <= 1.09 then 100
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)> 1.04 and round(max(tsm.mort_oe_rt),2) <= 1.07 then 125
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)<= 1.04 then 150
--Central
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2) > 0.91 then 0
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2)> 0.84 and round(max(tsm.mort_oe_rt),2) <= 0.91 then 50
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2)> 0.82 and round(max(tsm.mort_oe_rt),2) <= 0.84 then 75
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2)> 0.81 and round(max(tsm.mort_oe_rt),2) <= 0.82 then 100
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2)> 0.79 and round(max(tsm.mort_oe_rt),2) <= 0.81 then 125
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2)<= 0.79 then 150
--Flint
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2) > 1.64 then 0
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2)> 1.52 and round(max(tsm.mort_oe_rt),2) <= 1.64 then 50
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2)> 1.49  and round(max(tsm.mort_oe_rt),2) <= 1.52 then 75
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2)> 1.46 and round(max(tsm.mort_oe_rt),2) <= 1.49 then 100
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2)> 1.43 and round(max(tsm.mort_oe_rt),2) <= 1.46 then 125
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2)<= 1.43 then 150
--Lansing
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2) > 1.94 then 0
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2)> 1.8 and round(max(tsm.mort_oe_rt),2) <= 1.94 then 50
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2)> 1.76 and round(max(tsm.mort_oe_rt),2) <= 1.8 then 75
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2)> 1.73 and round(max(tsm.mort_oe_rt),2) <= 1.76 then 100
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2)> 1.69 and round(max(tsm.mort_oe_rt),2) <= 1.73 then 125
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2)<= 1.69 then 150
--Lapeer
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2) > 0.7 then 0
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2)> 0.65 and round(max(tsm.mort_oe_rt),2) <= 0.7 then 50
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2)> 0.64 and round(max(tsm.mort_oe_rt),2) <= 0.65 then 75
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2)> 0.62 and round(max(tsm.mort_oe_rt),2) <= 0.64 then 100
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2)> 0.61 and round(max(tsm.mort_oe_rt),2) <= 0.62 then 125
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2)<= 0.61 then 150
--Macomb
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2) > 1.36 then 0
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2)> 1.26 and round(max(tsm.mort_oe_rt),2) <= 1.36 then 50
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2)> 1.23 and round(max(tsm.mort_oe_rt),2) <= 1.26 then 75
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2)> 1.21 and round(max(tsm.mort_oe_rt),2) <= 1.23 then 100
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2)> 1.18 and round(max(tsm.mort_oe_rt),2) <= 1.21 then 125
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2)<= 1.18 then 150
--Northern
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2) > 0.67 then 0
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2)> 0.62 and round(max(tsm.mort_oe_rt),2) <= 0.67 then 50
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2)> 0.61 and round(max(tsm.mort_oe_rt),2) <= 0.62 then 75
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2)> 0.6 and round(max(tsm.mort_oe_rt),2) <= 0.61 then 100
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2)> 0.58 and round(max(tsm.mort_oe_rt),2) <= 0.6 then 125
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2)<= 0.58 then 150

--Oakland
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2) > 1.39 then 0
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2)> 1.29 and round(max(tsm.mort_oe_rt),2) <= 1.39 then 50
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2)> 1.26 and round(max(tsm.mort_oe_rt),2) <= 1.29 then 75
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2)> 1.24 and round(max(tsm.mort_oe_rt),2) <= 1.26 then 100
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2)> 1.21 and round(max(tsm.mort_oe_rt),2) <= 1.24 then 125
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2)<= 1.21 then 150
--POrt Huron
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2) > 1.26 then 0
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2)> 1.17 and round(max(tsm.mort_oe_rt),2) <= 1.26 then 50
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2)> 1.15 and round(max(tsm.mort_oe_rt),2) <= 1.17 then 75
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2)> 1.12 and round(max(tsm.mort_oe_rt),2) <= 1.15 then 100
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2)> 1.1 and round(max(tsm.mort_oe_rt),2) <= 1.12 then 125
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2)<= 1.1 then 150

else 0
end) as sep_mort_oe_pts,

--(case 
--when  round(max(tsm.mort_oe_rt),2) > 0.97 then 0
--when  round(max(tsm.mort_oe_rt),2)> 0.94 and round(max(tsm.mort_oe_rt),2) <= 0.97 then 50
--when  round(max(tsm.mort_oe_rt),2)> 0.9 and round(max(tsm.mort_oe_rt),2) <= 0.94 then 75
--when  round(max(tsm.mort_oe_rt),2)> 0.87 and round(max(tsm.mort_oe_rt),2) <= 0.9 then 100
--when  round(max(tsm.mort_oe_rt),2)> 0.85 and round(max(tsm.mort_oe_rt),2) <= 0.87 then 125
--when  round(max(tsm.mort_oe_rt),2)<=0.85 then 150
--else 0
--end) as sep_mort_oe_pts,

--Added 12/10/2020: FY21 Dashboard metric
--(case 
--when  max(tpci.pci_radial_access_rt) <= 50.01 then 0
--when  max(tpci.pci_radial_access_rt)> 50.01 and max(tpci.pci_radial_access_rt) <= 52.4 then 50
--when  max(tpci.pci_radial_access_rt)> 52.4 and max(tpci.pci_radial_access_rt) <= 63.5 then 75
--when  max(tpci.pci_radial_access_rt)> 63.5 and max(tpci.pci_radial_access_rt) <= 68.2 then 100
--when  max(tpci.pci_radial_access_rt)> 68.2 and max(tpci.pci_radial_access_rt) <= 75 then 125
--when  max(tpci.pci_radial_access_rt)> 75 then 150
--else 0
--end) as pci_radial_access_pts,

 case 
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) >= 50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) >= 52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) >= 62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) >= 68.2 and round(max(tpci.pci_radial_access_rt),2) < 75.0 then 125
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) >= 75.0 then 150

when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) >= 50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) >= 52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) >= 62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) >= 68.2 and round(max(tpci.pci_radial_access_rt),2) < 75 then 125
when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) >= 75 then 150

when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) >= 50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) >= 52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) >= 62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) >= 68.2 and round(max(tpci.pci_radial_access_rt),2) < 75 then 125
when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) >= 75 then 150

when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) >=50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) >=52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) >=62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) >=68.2 and round(max(tpci.pci_radial_access_rt),2) < 75 then 125
when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) >=75 then 150

when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) >=50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) >=52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) >=62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) >=68.2 and round(max(tpci.pci_radial_access_rt),2) < 75 then 125
when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) >=75 then 150

when tmi.fcy_num IN ('600816' ) and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('600816' ) and round(max(tpci.pci_radial_access_rt),2) >=50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('600816' ) and round(max(tpci.pci_radial_access_rt),2) >=52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('600816' ) and round(max(tpci.pci_radial_access_rt),2) >=62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('600816' ) and  round(max(tpci.pci_radial_access_rt),2) >=68.2 and round(max(tpci.pci_radial_access_rt),2) < 75 then 125
when tmi.fcy_num IN ('600816' ) and round(max(tpci.pci_radial_access_rt),2) >=75 then 150

else 0
end as pci_radial_access_pts,

(case 
when  max(tcrf.cardiac_rt) <=0.6891 then 0
when  max(tcrf.cardiac_rt)> 0.6897 and max(tcrf.cardiac_rt) <= 0.791 then 50
when  max(tcrf.cardiac_rt)> 0.791 and max(tcrf.cardiac_rt) <= 0.8122 then 75
when  max(tcrf.cardiac_rt)> 0.8122 and max(tcrf.cardiac_rt) <= 0.9555 then 100
when  max(tcrf.cardiac_rt)> 0.9555 and max(tcrf.cardiac_rt) <= 0.9813 then 125
when  max(tcrf.cardiac_rt)> 0.9813 then 150
else 0
end) as cardiac_rehab_pts,

(case 
when  round(max(tot.ob_rt),2) <0.95 then 0
when  round(max(tot.ob_rt),2)>= 0.95 and round(max(tot.ob_rt),2) <1 then 100
when  round(max(tot.ob_rt),2)>= 1 then 150
else 0
end) as ob_training_pts,
--Updated on 12/15/2020: FY 21 Dashboard
(case 
when  round(comp_oe_rt,2) > 0.76 then 0
when  round(comp_oe_rt,2) > 0.72 and round(comp_oe_rt,2)  <=0.76 then 50
when  round(comp_oe_rt,2) > 0.69 and round(comp_oe_rt,2)  <= 0.72 then 75
when round(comp_oe_rt,2) > 0.64 and round(comp_oe_rt,2)  <= 0.69 then 100
when  round(comp_oe_rt,2) > 0.58 and round(comp_oe_rt,2)  <= 0.64 then 125
when  round(comp_oe_rt,2)  <= 0.58 then 150
else 0
end) as comp_oe_rt_pts,
--Updated on 12/15/2020: FY 21 Dashboard
(case 
when  round(sep_compliance_rt,2) < 0.54 then 0
when  round(sep_compliance_rt,2) >= 0.54 and round(sep_compliance_rt,2) <0.59 then 50
when  round(sep_compliance_rt,2) >= 0.59 and round(sep_compliance_rt,2) < 0.62 then 75
when  round(sep_compliance_rt,2) >= 0.62 and round(sep_compliance_rt,2) < 0.67 then 100
when  round(sep_compliance_rt,2) >= 0.67 and round(sep_compliance_rt,2) < 0.71 then 125
when  round(sep_compliance_rt,2) >= 0.71 then 150
else 0
end) as sep_compliance_rt_pts,
--Updated on 12/15/2020: FY 21 Dashboard
(case 
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2) > 4.75 then 0
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2)> 4.52 and round(max(tlu.lab_utlz),2) <= 4.75 then 50
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2)> 4.47 and round(max(tlu.lab_utlz),2) <= 4.52 then 75
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2)> 4.43 and round(max(tlu.lab_utlz),2) <= 4.47 then 100
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2)> 4.38 and round(max(tlu.lab_utlz),2) <= 4.43 then 125
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2)<=4.38 then 150

when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2) > 5.38 then 0
when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2)> 5.12 and round(max(tlu.lab_utlz),2) <= 5.38 then 50
when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2)> 5.07 and round(max(tlu.lab_utlz),2) <= 5.12 then 75
when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2)> 5.02 and round(max(tlu.lab_utlz),2) <= 5.07 then 100
when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2)> 4.97 and round(max(tlu.lab_utlz),2) <= 5.02 then 125
when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2)<=4.97 then 150

when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2) > 5.54 then 0
when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2)> 5.28 and round(max(tlu.lab_utlz),2) <= 5.54 then 50
when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2)> 5.23 and round(max(tlu.lab_utlz),2) <= 5.28 then 75
when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2)> 5.17 and round(max(tlu.lab_utlz),2) <= 5.23 then 100
when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2)> 5.12 and round(max(tlu.lab_utlz),2) <= 5.17 then 125
when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2)<=5.12 then 150


when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2) > 6.01 then 0
when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2)> 5.72 and round(max(tlu.lab_utlz),2) <= 6.01 then 50
when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2)> 5.66 and round(max(tlu.lab_utlz),2) <= 5.72 then 75
when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2)> 5.61 and round(max(tlu.lab_utlz),2) <= 5.66 then 100
when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2)> 5.55 and round(max(tlu.lab_utlz),2) <= 5.61 then 125
when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2)<=5.55 then 150

when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2) > 5.53 then 0
when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2)> 5.27 and round(max(tlu.lab_utlz),2) <= 5.53 then 50
when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2)> 5.22 and round(max(tlu.lab_utlz),2) <= 5.27 then 75
when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2)> 5.16 and round(max(tlu.lab_utlz),2) <= 5.22 then 100
when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2)> 5.11 and round(max(tlu.lab_utlz),2) <= 5.16 then 125
when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2)<=5.11 then 150

when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2) > 6.94 then 0
when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2)> 6.61 and round(max(tlu.lab_utlz),2) <= 6.94 then 50
when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2)> 6.54 and round(max(tlu.lab_utlz),2) <=  6.61 then 75
when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2)> 6.48 and round(max(tlu.lab_utlz),2) <= 6.54 then 100
when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2)> 6.41 and round(max(tlu.lab_utlz),2) <= 6.48 then 125
when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2)<= 6.41 then 150
--
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2) > 6.41 then 0
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2)> 6.1 and round(max(tlu.lab_utlz),2) <= 6.41 then 50
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2)> 6.04 and round(max(tlu.lab_utlz),2) <= 6.1 then 75
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2)> 5.98 and round(max(tlu.lab_utlz),2) <= 6.04 then 100
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2)> 5.92 and round(max(tlu.lab_utlz),2) <= 5.98 then 125
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2)<= 5.92 then 150

when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2) > 4.82 then 0
when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2)> 4.59 and round(max(tlu.lab_utlz),2) <= 4.82 then 50
when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2)> 4.54 and round(max(tlu.lab_utlz),2) <= 4.59 then 75
when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2)> 4.5 and round(max(tlu.lab_utlz),2) <= 4.54 then 100
when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2)> 4.45 and round(max(tlu.lab_utlz),2) <= 4.5 then 125
when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2)<=4.45 then 150

when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2) > 5.18 then 0
when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2)> 4.93 and round(max(tlu.lab_utlz),2) <= 5.18 then 50
when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2)> 4.88 and round(max(tlu.lab_utlz),2) <= 4.93 then 75
when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2)> 4.83 and round(max(tlu.lab_utlz),2) <= 4.88 then 100
when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2)> 4.78 and round(max(tlu.lab_utlz),2) <= 4.83 then 125
when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2)<=4.78 then 150
else 0
end) as lab_utlz_pts,
--Updated on 12/15/2020: FY 21 Dashboard (McLaren Patient Experience Percentile Composite)
(case 
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor) < 17 then 0
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 17 and max(pepm.prgny_pct_scor) < 20 then 50
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 20 and max(pepm.prgny_pct_scor) < 21 then 75
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 21 and max(pepm.prgny_pct_scor) < 21.6 then 100
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 21.6 and max(pepm.prgny_pct_scor) < 22 then 125
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>=22 then 150

when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor) < 25.3 then 0
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 25.3 and max(pepm.prgny_pct_scor) < 29.8 then 50
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 29.8 and max(pepm.prgny_pct_scor) < 31.3 then 75
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 31.3 and max(pepm.prgny_pct_scor) < 32.2 then 100
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 32.2 and max(pepm.prgny_pct_scor) < 32.8 then 125
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 32.8 then 150

when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor) < 20.4 then 0
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 20.4 and max(pepm.prgny_pct_scor) < 24 then 50
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 24 and max(pepm.prgny_pct_scor) < 25.2 then 75
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 25.2 and max(pepm.prgny_pct_scor) < 25.9 then 100
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 25.9 and max(pepm.prgny_pct_scor) < 26.4 then 125
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 26.4 then 150


when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor) <= 25.7 then 0
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 25.7 and max(pepm.prgny_pct_scor) <= 30.2 then 50
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 30.2 and max(pepm.prgny_pct_scor) <= 31.7 then 75
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 31.7 and max(pepm.prgny_pct_scor) <= 32.6 then 100
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 32.6 and max(pepm.prgny_pct_scor) <= 33.2 then 125
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)>33.2 then 150

when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor) < 17 then 0
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 17 and max(pepm.prgny_pct_scor) < 20 then 50
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 20 and max(pepm.prgny_pct_scor) < 21 then 75
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 21 and max(pepm.prgny_pct_scor) < 21.6 then 100
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 21.6 and max(pepm.prgny_pct_scor) < 22 then 125
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 22 then 150

when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor) < 17.9 then 0
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 17.9 and max(pepm.prgny_pct_scor) < 21.1 then 50
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 21.1 and max(pepm.prgny_pct_scor) < 22.2 then 75
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 22.2 and max(pepm.prgny_pct_scor) < 22.8 then 100
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 22.8 and max(pepm.prgny_pct_scor) < 23.2 then 125
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 23.2 then 150
--
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor) < 35.7 then 0
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 35.7 and max(pepm.prgny_pct_scor) < 42 then 50
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 42 and max(pepm.prgny_pct_scor) < 44.1 then 75
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 44.1 and max(pepm.prgny_pct_scor) < 45.4 then 100
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 45.4 and max(pepm.prgny_pct_scor) < 46.2 then 125
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 46.2 then 150

when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor) < 23.4 then 0
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 23.4 and max(pepm.prgny_pct_scor) < 27.5 then 50
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 27.5 and max(pepm.prgny_pct_scor) < 28.9 then 75
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 28.9 and max(pepm.prgny_pct_scor) < 29.7 then 100
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 29.7 and max(pepm.prgny_pct_scor) < 30.3 then 125
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 30.3 then 150

when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor) < 26.4 then 0
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 26.4 and max(pepm.prgny_pct_scor) < 31 then 50
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 31 and max(pepm.prgny_pct_scor) < 32.6 then 75
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 32.6 and max(pepm.prgny_pct_scor) < 33.5 then 100
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 33.5 and max(pepm.prgny_pct_scor) < 34.1 then 125
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>=34.1 then 150



else 0
end) as prgny_pct_scor_pts,
--Updated on 12/15/2020: FY21 Dashboard
(case 
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3) < 0.779 then 0
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.779 and round(max(tpep.ptnt_exrnc_scor),3) < 0.795 then 50
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.795 and round(max(tpep.ptnt_exrnc_scor),3) < 0.799 then 75
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.799 and round(max(tpep.ptnt_exrnc_scor),3) < 0.803 then 100
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.803 and round(max(tpep.ptnt_exrnc_scor),3) < 0.807 then 125
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>=0.807 then 150

when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3) < 0.785 then 0
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.785 and round(max(tpep.ptnt_exrnc_scor),3) < 0.801 then 50
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.801 and round(max(tpep.ptnt_exrnc_scor),3) < 0.805 then 75
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.805 and round(max(tpep.ptnt_exrnc_scor),3) < 0.809 then 100
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.809 and round(max(tpep.ptnt_exrnc_scor),3) < 0.813 then 125
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>=0.813 then 150

when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.739 then 0
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.739 and round(max(tpep.ptnt_exrnc_scor),3) < 0.754 then 50
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.754 and round(max(tpep.ptnt_exrnc_scor),3) < 0.758 then 75
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.758 and round(max(tpep.ptnt_exrnc_scor),3) < 0.762 then 100
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.762 and round(max(tpep.ptnt_exrnc_scor),3) < 0.765 then 125
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>=0.765 then 150


when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.738 then 0
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.738 and round(max(tpep.ptnt_exrnc_scor),3) < 0.753 then 50
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.753 and round(max(tpep.ptnt_exrnc_scor),3) < 0.757 then 75
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.757 and round(max(tpep.ptnt_exrnc_scor),3) < 0.761 then 100
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.761 and round(max(tpep.ptnt_exrnc_scor),3) < 0.764 then 125
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.764 then 150

when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.77 then 0
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.77 and round(max(tpep.ptnt_exrnc_scor),3) < 0.786 then 50
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.786 and round(max(tpep.ptnt_exrnc_scor),3) < 0.79 then 75
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.79 and round(max(tpep.ptnt_exrnc_scor),3) < 0.794 then 100
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.794 and round(max(tpep.ptnt_exrnc_scor),3) < 0.798 then 125
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.798 then 150

when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3) <0.799 then 0
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.799 and round(max(tpep.ptnt_exrnc_scor),3) < 0.815 then 50
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.815 and round(max(tpep.ptnt_exrnc_scor),3) < 0.819 then 75
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.819 and round(max(tpep.ptnt_exrnc_scor),3) < 0.823 then 100
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.823 and round(max(tpep.ptnt_exrnc_scor),3)< 0.827 then 125
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.827 then 150
--
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.767 then 0
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.767 and round(max(tpep.ptnt_exrnc_scor),3) < 0.783 then 50
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.783 and round(max(tpep.ptnt_exrnc_scor),3) < 0.787 then 75
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.787 and round(max(tpep.ptnt_exrnc_scor),3) < 0.791 then 100
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.791 and round(max(tpep.ptnt_exrnc_scor),3) < 0.795 then 125
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>=0.795 then 150

when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3) <0.81 then 0
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.81 and round(max(tpep.ptnt_exrnc_scor),3) < 0.827 then 50
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.827 and round(max(tpep.ptnt_exrnc_scor),3) < 0.831 then 75
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.831 and round(max(tpep.ptnt_exrnc_scor),3) < 0.835 then 100
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.835 and round(max(tpep.ptnt_exrnc_scor),3) < 0.839 then 125
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.839 then 150

when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.834 then 0
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3) >= 0.834 and round(max(tpep.ptnt_exrnc_scor),3) < 0.851 then 50
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.851 and round(max(tpep.ptnt_exrnc_scor),3) < 0.855 then 75
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.855 and round(max(tpep.ptnt_exrnc_scor),3) < 0.86 then 100
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.86 and round(max(tpep.ptnt_exrnc_scor),3) < 0.864 then 125
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>=0.864 then 150

else 0
end) as ptnt_exrnc_scor_pts
,
case 
when tmi.rpt_dt='2019-08-01 00:00:00' then 1
when tmi.rpt_dt='2019-09-01 00:00:00' then 2
when tmi.rpt_dt='2019-10-01 00:00:00' then 3
when tmi.rpt_dt='2019-11-01 00:00:00' then 4
when tmi.rpt_dt='2019-12-01 00:00:00' then 5
when tmi.rpt_dt='2020-01-01 00:00:00' then 6
when tmi.rpt_dt='2020-02-01 00:00:00' then 7
when tmi.rpt_dt='2020-03-01 00:00:00' then 8
when tmi.rpt_dt='2020-04-01 00:00:00' then 9
when tmi.rpt_dt='2020-05-01 00:00:00' then 10
when tmi.rpt_dt='2020-06-01 00:00:00' then 11
when tmi.rpt_dt ='2019-07-01 00:00:00' then 12
else null
end as no_of_mnths,

(case 
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt) > 77 then 0
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)>  73 and max(thef.harm_events_cnt) <= 77 then 50
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)> 69 and max(thef.harm_events_cnt) <= 73 then 75
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)> 68 and max(thef.harm_events_cnt) <= 69 then 100
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)> 66 and max(thef.harm_events_cnt) <= 68 then 125
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)<= 66 then 150

when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt) > 5  then 0
when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 4 and max(thef.harm_events_cnt) <= 5 then 75
--when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 0.78 and max(thef.harm_events_cnt) <= 0.784 then 75
--when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 0.784 and max(thef.harm_events_cnt) <= 0.788 then 100
when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 3 and max(thef.harm_events_cnt) <= 4 then 125
when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)<=3 then 150

when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt) > 102 then 0
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 97 and max(thef.harm_events_cnt) <= 102 then 50
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 92 and max(thef.harm_events_cnt) <= 97 then 75
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 90 and max(thef.harm_events_cnt) <= 92 then 100
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 87 and max(thef.harm_events_cnt) <= 90 then 125
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)<= 87 then 150


when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) > 63 then 0
when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) > 60 and max(thef.harm_events_cnt) <= 63 then 50
when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) > 57 and max(thef.harm_events_cnt) <= 60 then 75
when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) > 56 and max(thef.harm_events_cnt) <= 57 then 100
when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) > 54 and max(thef.harm_events_cnt) <= 56 then 125
when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) <=54 then 150

when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt) > 14 then 0
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 13 and max(thef.harm_events_cnt) <= 14 then 50
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 13 and max(thef.harm_events_cnt) <= 13 then 75
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 12 and max(thef.harm_events_cnt) <= 13 then 100
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 12 and max(thef.harm_events_cnt) <= 12 then 125
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)<=12 then 150
--
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt) > 50 then 0
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 48 and max(thef.harm_events_cnt) <= 50 then 50
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 46 and max(thef.harm_events_cnt) <= 48 then 75
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 45 and max(thef.harm_events_cnt) <= 46 then 100
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 43 and max(thef.harm_events_cnt) <= 45 then 125
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)<= 43 then 150
--
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt) > 41 then 0
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 39 and max(thef.harm_events_cnt) <= 41 then 50
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 37 and max(thef.harm_events_cnt) <= 39 then 75
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 36 and max(thef.harm_events_cnt) <= 37 then 100
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 35 and max(thef.harm_events_cnt) <= 36 then 125
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)<= 35 then 150
--
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt) > 26 then 0
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 25 and max(thef.harm_events_cnt) <= 26 then 50
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 24 and max(thef.harm_events_cnt) <= 25 then 75
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 23 and max(thef.harm_events_cnt) <= 24 then 100
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 22 and max(thef.harm_events_cnt) <= 23 then 125
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)<= 22 then 150
--
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt) > 36 then 0
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 34 and max(thef.harm_events_cnt) <= 36 then 50
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 33 and max(thef.harm_events_cnt) <= 34 then 75
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 32 and max(thef.harm_events_cnt) <= 33 then 100
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 31 and max(thef.harm_events_cnt) <= 32 then 125
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)<= 31 then 150



else 0
end) as harm_events_pts
,
--Updated on 12/10/2020 FY 21 Dashboard
(case 
when  max(ROUND(tzef.zero_events_rt,2)) < 0.79 then 0
when  max(ROUND(tzef.zero_events_rt,2)) >= 0.79 and max(ROUND(tzef.zero_events_rt,2)) < 0.86 then 50
when  max(ROUND(tzef.zero_events_rt,2)) >= 0.86 and max(ROUND(tzef.zero_events_rt,2)) < 0.87 then 75
when  max(ROUND(tzef.zero_events_rt,2)) >= 0.87 and max(ROUND(tzef.zero_events_rt,2)) < 0.89 then 100
when  max(ROUND(tzef.zero_events_rt,2)) >= 0.89 and  max(ROUND(tzef.zero_events_rt,2)) <= 0.9 then 125
when  max(ROUND(tzef.zero_events_rt,2)) >=0.9 then 150
else 0
end
) as zero_events_rt_pts,

case when harm_events_pts > zero_events_rt_pts then harm_events_pts else zero_events_rt_pts end as harm_zero_events_pts,

case when readm_obs_pts > readm_oe_pts then readm_obs_pts else readm_oe_pts end as readm_obs_oe_pts,

((nvl(mort_oe_pts,0)*0.05 )+
(nvl(comp_oe_rt_pts,0)*0.05)+
(nvl(harm_zero_events_pts,0)*0.15)+
(nvl(readm_obs_oe_pts,0)*0.15)+ 
(nvl(sep_mort_oe_pts,0)*0.055)+
(nvl(sep_compliance_rt_pts,0)*0.055)+
(nvl(lab_utlz_pts,0)*0.13)+
(nvl(prgny_pct_scor_pts,0)*0.05)+
(max(nvl(round(acof.mpp_scr),0))*0.15)   +
(nvl(pci_radial_access_pts,0)*0.1) +
(nvl(ptnt_exrnc_scor_pts,0)*0.06)) as weighted_pts,

--Updated on 12/15/2020 FY 21 Dashboard
case when tmi.fcy_num = 'MI2191' then 1
when tmi.fcy_num = 'MI2061' then 0.9
when tmi.fcy_num = 'MI2302' then 1
when tmi.fcy_num = 'MI5020' then 1
when tmi.fcy_num = 'MI2001' then 0.9
when tmi.fcy_num = 'MI2048' then 1
when tmi.fcy_num = '637619' then 1
when tmi.fcy_num = 'MI2055' then 0.9
when tmi.fcy_num = '600816' then 1
else 1
end as weight,

weighted_pts/weight as clncl_outc_scor


from 
 stg_tmp_mrtly_ind tmi
left join  stg_pqsd_cmplc_idnx_fct pcf on tmi.rpt_dt = date_trunc('month',pcf.end_of_month) and pcf.end_of_month <> pcf.first_of_month and tmi.fcy_num = pcf.fcy_num and pcf.msr_nm = 'Compl_R12M'
left join  stg_tmp_readm_ind tri on tmi.rpt_dt = tri.rpt_dt and tmi.fcy_num=tri.fcy_num
left join  stg_tmp_lab_utlz_fct tlu on tmi.rpt_dt = date(tlu.rpt_dt) and tmi.fcy_num = tlu.fcy_num
left join  stg_tmp_card_rehab_fct tcrf on tmi.rpt_dt = tcrf.rpt_dt and tmi.fcy_num = tcrf.fcy_num
left join  stg_tmp_ob_trn_fct tot on tmi.rpt_dt = tot.rpt_dt and tmi.fcy_num = tot.fcy_num
left join  stg_tmp_sep_mrtly_ind tsm on tmi.rpt_dt = tsm.rpt_dt and tmi.fcy_num = tsm.fcy_num
left join  stg_tmp_sep_compl_fct tscf on tmi.rpt_dt = tscf.rpt_dt and tmi.fcy_num = tscf.fcy_num
left join  stg_aco_mpp_msr_fct acof on tmi.rpt_dt = date_trunc('month',acof.rpt_prd_end_dt) and tmi.fcy_num = acof.fcy_num
left join  stg_TMP_HARM_EVENTS_FCT thef on tmi.rpt_dt = thef.rpt_dt and tmi.fcy_num = thef.fcy_num
left join  stg_tmp_zero_events_fct tzef on tmi.rpt_dt = tzef.rpt_dt and tmi.fcy_num = tzef.fcy_num
LEFT JOIN  stg_tmp_ptnt_exrnc_pct_msr_fct pepm on tmi.rpt_dt = date_trunc('month',pepm.rprt_dt) and tmi.fcy_num = pepm.fcy_num
--left join  stg_tmp_mmg_ovrl_ptnt_exrnc_msr_fct tpep on tmi.rpt_dt = date_trunc('month',tpep.rprt_dt) and tmi.fcy_num = tpep.fcy_num
left join  stg_mmg_ovrl_ptnt_exrnc_msr_fct tpep on tmi.rpt_dt = date_trunc('month',tpep.rprt_dt) and tmi.fcy_num = tpep.fcy_num
--Added 12/10/2020 : FY21 Metric
left join  stg_tmp_pci_radial_access_r12m_fct tpci on tmi.rpt_dt = tpci.rpt_dt and tmi.fcy_num = tpci.fcy_num
group by 1,2,3

 );
 
 select * from  stg_tmp_pci_radial_access_r12m_fct where fcy_nm = 'McLaren Flint' and rpt_dt = '2020-05-01 00:00:00'; 
-- select tpci.*, 
-- case 
--when tmi.fcy_num IN ('MI2191', 'MI2302', 'MI5020', 'MI2048', '637619' , '600816' ) and round(max(tpci.pci_radial_access_rt),2) <=  50.1 then 0
--when tmi.fcy_num IN ('MI2191', 'MI2302', 'MI5020', 'MI2048', '637619' , '600816' ) and round(max(tpci.pci_radial_access_rt),2) > 50.1 and round(max(tpci.pci_radial_access_rt),2) <= 52.4 then 50
--when tmi.fcy_num IN ('MI2191', 'MI2302', 'MI5020', 'MI2048', '637619' , '600816' ) and round(max(tpci.pci_radial_access_rt),2) > 52.4 and round(max(tpci.pci_radial_access_rt),2) <= 63.5 then 75
--when tmi.fcy_num IN ('MI2191', 'MI2302', 'MI5020', 'MI2048', '637619' , '600816' ) and round(max(tpci.pci_radial_access_rt),2) > 63.5 and round(max(tpci.pci_radial_access_rt),2) <= 68.2 then 100
--when tmi.fcy_num IN ('MI2191', 'MI2302', 'MI5020', 'MI2048', '637619' , '600816' ) and round(max(tpci.pci_radial_access_rt),2) > 68.2 and round(max(tpci.pci_radial_access_rt),2) <= 75 then 125
--when tmi.fcy_num IN ('MI2191', 'MI2302', 'MI5020', 'MI2048', '637619' , '600816' ) and round(max(tpci.pci_radial_access_rt),2) > 75 then 150
--else 0
--end as pci_radial_access_pts
--
-- from  stg_tmp_mrtly_ind  tmi 
-- INNER JOIN  stg_tmp_pci_radial_access_r12m_fct tpci
-- ON tmi.rpt_dt = tpci.rpt_dt and tmi.fcy_num = tpci.fcy_num 
 -----> --CODE CHANGE: 08/24/2020 MLH-579 Excluding COVID _cases 
 drop table stg_clncl_outc_scor_fct_wo_covid if exists;
create  table stg_clncl_outc_scor_fct_wo_covid as 
(select distinct tmi.fcy_nm,
tmi.fcy_num,
tmi.rpt_dt,
max(tmi.mort_obs_rt) as mort_obs_rt, 
max(tmi.mort_expc_rt) as mort_expc_rt, 
max(tmi.mort_oe_rt) as mort_oe_rt, 
max(pcf.cmplc_obsr_rt) as cmplc_obsr_rt,
max(pcf.cmplc_expc_rt) as cmplc_expc_rt,
max(cast(pcf.cmplc_obsr_rt as decimal)/cast(pcf.cmplc_expc_rt as decimal)) as comp_oe_rt,
max(pcf.obsr_cases) as cmplc_obsr_cases,
max(pcf.outc_cases) as cmplc_outc_cases,
round(max(tri.readm_obs_rt),3) as readm_obs_rt, 
max(tri.readm_expc_rt) as readm_expc_rt, 
max(tri.readm_oe_rt) as readm_oe_rt,
max(tlu.qty) as qty, 
max(tlu.ptnt_days) as ptnt_days, 
max(tlu.lab_utlz) as lab_utlz,
max(tcrf.snpsr4qnumerator) as snpsr4qnumerator,
max(tcrf.snpsr4qdenominator) as snpsr4qdenominator,
max(tcrf.cardiac_rt) as cardiac_rt,
--Added 12/10/2020: FY21 Dashboard Metric
max(tpci.pci_numr) as pci_numr,
max(tpci.pci_dnmr) as pci_demr,
max(tpci.pci_radial_access_rt) as pci_radial_access_rt,
max(tot.numr) as ob_numr,
max(tot.dnmr) as ob_dnmr,
max(tot.ob_rt) as ob_rt,
max(tscf.numr) as complc_numr,
max(tscf.dnmr) as complc_dnmr,
max(tscf.complc) as complc_rt,
max(acof.mpp_scr) as mpp_scr,
max(thef.harm_events_cnt) as harm_events_cnt,
max(tzef.zero_events_numr) as zero_events_numr,
max(tzef.zero_events_dnmr) as zero_events_dnmr,
max(tzef.zero_events_rt)*100 as zero_events_rt,
max(pepm.ptnt_exrnc_pct) as ptnt_exrnc_pct,
max(pepm.prgny_pct_scor) as prgny_pct_scor,
max(pepm.prgny_pct) AS prgny_pct,
max(tpep.top_box_resp_cnt) as top_box_resp_cnt,
max(tpep.resp_cnt) as resp_cnt,
max(tpep.ptnt_exrnc_scor) as ptnt_exrnc_scor,
max(tpep.ptnt_exrnc_clr_cdg) as ptnt_exrnc_clr_cdg,
max(tsm.mort_obs_rt) as sep_mort_obs_rt,
max(tsm.mort_expc_rt) as sep_mort_expc_rt,
max(tsm.mort_oe_rt) as sep_mort_oe_rt,
max(tscf.numr) as sep_compliance_numr,
max(tscf.dnmr) as sep_compliance_dnmr,
max(tscf.numr)/max(tscf.dnmr) as sep_compliance_rt,
--Updated on 12/10/2020 FY 21 Dashboard
(case 
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3) > 0.135 then 0
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.123 and round(max(tri.readm_obs_rt),3) <= 0.135 then 50
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.121 and round(max(tri.readm_obs_rt),3) <= 0.123 then 75
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.118 and round(max(tri.readm_obs_rt),3) <= 0.121 then 100
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.116 and round(max(tri.readm_obs_rt),3) <= 0.118 then 125
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)<=0.116 then 150

when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3) > 0.08 then 0
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.073 and round(max(tri.readm_obs_rt),3) <= 0.08 then 50
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.072 and round(max(tri.readm_obs_rt),3) <= 0.073 then 75
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.07 and round(max(tri.readm_obs_rt),3) <= 0.072 then 100
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.069 and round(max(tri.readm_obs_rt),3) <= 0.07 then 125
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)<=0.069 then 150

when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3) > 0.14 then 0
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.127 and round(max(tri.readm_obs_rt),3) <= 0.14 then 50
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.124 and round(max(tri.readm_obs_rt),3) <= 0.127 then 75
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.122 and round(max(tri.readm_obs_rt),3) <= 0.124 then 100
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.119 and round(max(tri.readm_obs_rt),3) <= 0.122 then 125
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)<=0.119 then 150


when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3) > 0.118 then 0
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.107 and round(max(tri.readm_obs_rt),3) <= 0.118 then 50
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.105 and round(max(tri.readm_obs_rt),3) <= 0.107 then 75
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.103 and round(max(tri.readm_obs_rt),3) <= 0.105 then 100
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.101 and round(max(tri.readm_obs_rt),3) <= 0.103 then 125
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)<=0.101 then 150

when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3) > 0.124 then 0
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.113 and round(max(tri.readm_obs_rt),3) <= 0.124 then 50
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.111 and round(max(tri.readm_obs_rt),3) <= 0.113 then 75
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.108 and round(max(tri.readm_obs_rt),3) <= 0.111 then 100
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.106 and round(max(tri.readm_obs_rt),3) <= 0.108 then 125
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)<=0.106 then 150

when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3) > 0.130 then 0
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.118 and round(max(tri.readm_obs_rt),3) <= 0.130 then 50
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.116 and round(max(tri.readm_obs_rt),3) <= 0.118 then 75
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.113 and round(max(tri.readm_obs_rt),3) <= 0.116 then 100
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.111 and round(max(tri.readm_obs_rt),3) <= 0.113 then 125
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)<=0.111 then 150

when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3) > 0.099 then 0
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.090 and round(max(tri.readm_obs_rt),3) <= 0.099 then 50
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.088 and round(max(tri.readm_obs_rt),3) <= 0.090 then 75
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.086 and round(max(tri.readm_obs_rt),3) <= 0.088 then 100
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.085 and round(max(tri.readm_obs_rt),3) <= 0.086 then 125
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)<=0.085 then 150

when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3) > 0.124 then 0
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.113 and round(max(tri.readm_obs_rt),3) <= 0.124 then 50
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.111 and round(max(tri.readm_obs_rt),3) <= 0.113 then 75
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.108 and round(max(tri.readm_obs_rt),3) <= 0.111 then 100
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.106 and round(max(tri.readm_obs_rt),3) <= 0.108 then 125
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)<=0.106 then 150

when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3) > 0.152 then 0
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.138 and round(max(tri.readm_obs_rt),3) <= 0.152 then 50
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.135 and round(max(tri.readm_obs_rt),3) <= 0.138 then 75
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.132 and round(max(tri.readm_obs_rt),3) <= 0.135 then 100
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.130 and round(max(tri.readm_obs_rt),3) <= 0.132 then 125
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)<=0.130 then 150

else 0
end) as readm_obs_pts,
--Updated on 12/10/2020 FY 21 Dashboard

(case 
when  round(max(tri.readm_oe_rt),2) > 1.07 then 0
when  round(max(tri.readm_oe_rt),2)> 0.97 and round(max(tri.readm_oe_rt),2) <= 1.07 then 50
when  round(max(tri.readm_oe_rt),2)> 0.95 and round(max(tri.readm_oe_rt),2) <= 0.97 then 75
when  round(max(tri.readm_oe_rt),2)> 0.93 and round(max(tri.readm_oe_rt),2) <= 0.95 then 100
when round(max(tri.readm_oe_rt),2)> 0.91 and round(max(tri.readm_oe_rt),2) <= 0.93 then 125
when  round(max(tri.readm_oe_rt),2)<=0.91 then 150
else 0
end) as readm_oe_pts,

(case 
when  round(max(tmi.mort_oe_rt),2) > 1.0 then 0
when  round(max(tmi.mort_oe_rt),2)  > 0.87 and round(max(tmi.mort_oe_rt),2)  <= 1.0 then 50
when  round(max(tmi.mort_oe_rt),2)  > 0.77 and round(max(tmi.mort_oe_rt),2)  <= 0.87 then 75
when  round(max(tmi.mort_oe_rt),2)  > 0.7 and  round(max(tmi.mort_oe_rt),2)  <= 0.77 then 100
when  round(max(tmi.mort_oe_rt),2)  > 0.67 and round(max(tmi.mort_oe_rt),2)  <= 0.7 then 125
when  round(max(tmi.mort_oe_rt),2)  <=0.67 then 150
else 0
end) as mort_oe_pts,

--Updated 12/10/2020: FY 21 Dashboard
(
--Bay 
case 
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)> 1.2 then 0
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)> 1.11 and round(max(tsm.mort_oe_rt),2) <= 1.2 then 50
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)> 1.09 and round(max(tsm.mort_oe_rt),2) <= 1.11 then 75
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)> 1.07 and round(max(tsm.mort_oe_rt),2) <= 1.09 then 100
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)> 1.04 and round(max(tsm.mort_oe_rt),2) <= 1.07 then 125
when tmi.fcy_num = 'MI2191' and round(max(tsm.mort_oe_rt),2)<= 1.04 then 150
--Central
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2) > 0.91 then 0
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2)> 0.84 and round(max(tsm.mort_oe_rt),2) <= 0.91 then 50
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2)> 0.82 and round(max(tsm.mort_oe_rt),2) <= 0.84 then 75
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2)> 0.81 and round(max(tsm.mort_oe_rt),2) <= 0.82 then 100
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2)> 0.79 and round(max(tsm.mort_oe_rt),2) <= 0.81 then 125
when tmi.fcy_num = 'MI2061' and round(max(tsm.mort_oe_rt),2)<= 0.79 then 150
--Flint
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2) > 1.64 then 0
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2)> 1.52 and round(max(tsm.mort_oe_rt),2) <= 1.64 then 50
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2)> 1.49  and round(max(tsm.mort_oe_rt),2) <= 1.52 then 75
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2)> 1.46 and round(max(tsm.mort_oe_rt),2) <= 1.49 then 100
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2)> 1.43 and round(max(tsm.mort_oe_rt),2) <= 1.46 then 125
when tmi.fcy_num = 'MI2302'  and round(max(tsm.mort_oe_rt),2)<= 1.43 then 150
--Lansing
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2) > 1.94 then 0
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2)> 1.8 and round(max(tsm.mort_oe_rt),2) <= 1.94 then 50
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2)> 1.76 and round(max(tsm.mort_oe_rt),2) <= 1.8 then 75
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2)> 1.73 and round(max(tsm.mort_oe_rt),2) <= 1.76 then 100
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2)> 1.69 and round(max(tsm.mort_oe_rt),2) <= 1.73 then 125
when tmi.fcy_num = 'MI5020'  and round(max(tsm.mort_oe_rt),2)<= 1.69 then 150
--Lapeer
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2) > 0.7 then 0
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2)> 0.65 and round(max(tsm.mort_oe_rt),2) <= 0.7 then 50
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2)> 0.64 and round(max(tsm.mort_oe_rt),2) <= 0.65 then 75
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2)> 0.62 and round(max(tsm.mort_oe_rt),2) <= 0.64 then 100
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2)> 0.61 and round(max(tsm.mort_oe_rt),2) <= 0.62 then 125
when tmi.fcy_num = 'MI2001'  and round(max(tsm.mort_oe_rt),2)<= 0.61 then 150
--Macomb
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2) > 1.36 then 0
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2)> 1.26 and round(max(tsm.mort_oe_rt),2) <= 1.36 then 50
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2)> 1.23 and round(max(tsm.mort_oe_rt),2) <= 1.26 then 75
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2)> 1.21 and round(max(tsm.mort_oe_rt),2) <= 1.23 then 100
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2)> 1.18 and round(max(tsm.mort_oe_rt),2) <= 1.21 then 125
when tmi.fcy_num = 'MI2048'  and round(max(tsm.mort_oe_rt),2)<= 1.18 then 150
--Northern
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2) > 0.67 then 0
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2)> 0.62 and round(max(tsm.mort_oe_rt),2) <= 0.67 then 50
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2)> 0.61 and round(max(tsm.mort_oe_rt),2) <= 0.62 then 75
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2)> 0.6 and round(max(tsm.mort_oe_rt),2) <= 0.61 then 100
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2)> 0.58 and round(max(tsm.mort_oe_rt),2) <= 0.6 then 125
when tmi.fcy_num = '637619'  and round(max(tsm.mort_oe_rt),2)<= 0.58 then 150

--Oakland
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2) > 1.39 then 0
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2)> 1.29 and round(max(tsm.mort_oe_rt),2) <= 1.39 then 50
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2)> 1.26 and round(max(tsm.mort_oe_rt),2) <= 1.29 then 75
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2)> 1.24 and round(max(tsm.mort_oe_rt),2) <= 1.26 then 100
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2)> 1.21 and round(max(tsm.mort_oe_rt),2) <= 1.24 then 125
when tmi.fcy_num = 'MI2055'  and round(max(tsm.mort_oe_rt),2)<= 1.21 then 150
--POrt Huron
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2) > 1.26 then 0
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2)> 1.17 and round(max(tsm.mort_oe_rt),2) <= 1.26 then 50
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2)> 1.15 and round(max(tsm.mort_oe_rt),2) <= 1.17 then 75
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2)> 1.12 and round(max(tsm.mort_oe_rt),2) <= 1.15 then 100
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2)> 1.1 and round(max(tsm.mort_oe_rt),2) <= 1.12 then 125
when tmi.fcy_num = '600816'  and round(max(tsm.mort_oe_rt),2)<= 1.1 then 150

else 0
end) as sep_mort_oe_pts,

--(case 
--when  round(max(tsm.mort_oe_rt),2) > 0.97 then 0
--when  round(max(tsm.mort_oe_rt),2)> 0.94 and round(max(tsm.mort_oe_rt),2) <= 0.97 then 50
--when  round(max(tsm.mort_oe_rt),2)> 0.9 and round(max(tsm.mort_oe_rt),2) <= 0.94 then 75
--when  round(max(tsm.mort_oe_rt),2)> 0.87 and round(max(tsm.mort_oe_rt),2) <= 0.9 then 100
--when  round(max(tsm.mort_oe_rt),2)> 0.85 and round(max(tsm.mort_oe_rt),2) <= 0.87 then 125
--when  round(max(tsm.mort_oe_rt),2)<=0.85 then 150
--else 0
--end) as sep_mort_oe_pts,

--Added 12/10/2020: FY21 Dashboard metric
--(case 
--when  max(tpci.pci_radial_access_rt) <= 50.01 then 0
--when  max(tpci.pci_radial_access_rt)> 50.01 and max(tpci.pci_radial_access_rt) <= 52.4 then 50
--when  max(tpci.pci_radial_access_rt)> 52.4 and max(tpci.pci_radial_access_rt) <= 63.5 then 75
--when  max(tpci.pci_radial_access_rt)> 63.5 and max(tpci.pci_radial_access_rt) <= 68.2 then 100
--when  max(tpci.pci_radial_access_rt)> 68.2 and max(tpci.pci_radial_access_rt) <= 75 then 125
--when  max(tpci.pci_radial_access_rt)> 75 then 150
--else 0
--end) as pci_radial_access_pts,

 case 
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) >= 50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) >= 52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) >= 62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) >= 68.2 and round(max(tpci.pci_radial_access_rt),2) < 75.0 then 125
when tmi.fcy_num IN ('MI2191')  and round(max(tpci.pci_radial_access_rt),2) >= 75.0 then 150

when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) >= 50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) >= 52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) >= 62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) >= 68.2 and round(max(tpci.pci_radial_access_rt),2) < 75 then 125
when tmi.fcy_num IN ('MI2302') and round(max(tpci.pci_radial_access_rt),2) >= 75 then 150

when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) >= 50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) >= 52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) >= 62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) >= 68.2 and round(max(tpci.pci_radial_access_rt),2) < 75 then 125
when tmi.fcy_num IN ('MI5020') and round(max(tpci.pci_radial_access_rt),2) >= 75 then 150

when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) >=50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) >=52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) >=62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) >=68.2 and round(max(tpci.pci_radial_access_rt),2) < 75 then 125
when tmi.fcy_num IN ('MI2048' ) and round(max(tpci.pci_radial_access_rt),2) >=75 then 150

when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) >=50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) >=52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) >=62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) >=68.2 and round(max(tpci.pci_radial_access_rt),2) < 75 then 125
when tmi.fcy_num IN ('637619'  ) and round(max(tpci.pci_radial_access_rt),2) >=75 then 150

when tmi.fcy_num IN ('600816' ) and round(max(tpci.pci_radial_access_rt),2) <  50.1 then 0
when tmi.fcy_num IN ('600816' ) and round(max(tpci.pci_radial_access_rt),2) >=50.1 and round(max(tpci.pci_radial_access_rt),2) < 52.4 then 50
when tmi.fcy_num IN ('600816' ) and round(max(tpci.pci_radial_access_rt),2) >=52.4 and round(max(tpci.pci_radial_access_rt),2) < 62.6 then 75
when tmi.fcy_num IN ('600816' ) and round(max(tpci.pci_radial_access_rt),2) >=62.6 and round(max(tpci.pci_radial_access_rt),2) < 68.2 then 100
when tmi.fcy_num IN ('600816' ) and  round(max(tpci.pci_radial_access_rt),2) >=68.2 and round(max(tpci.pci_radial_access_rt),2) < 75 then 125
when tmi.fcy_num IN ('600816' ) and round(max(tpci.pci_radial_access_rt),2) >=75 then 150

else 0
end as pci_radial_access_pts,

(case 
when  max(tcrf.cardiac_rt) <=0.6891 then 0
when  max(tcrf.cardiac_rt)> 0.6897 and max(tcrf.cardiac_rt) <= 0.791 then 50
when  max(tcrf.cardiac_rt)> 0.791 and max(tcrf.cardiac_rt) <= 0.8122 then 75
when  max(tcrf.cardiac_rt)> 0.8122 and max(tcrf.cardiac_rt) <= 0.9555 then 100
when  max(tcrf.cardiac_rt)> 0.9555 and max(tcrf.cardiac_rt) <= 0.9813 then 125
when  max(tcrf.cardiac_rt)> 0.9813 then 150
else 0
end) as cardiac_rehab_pts,

(case 
when  round(max(tot.ob_rt),2) <0.95 then 0
when  round(max(tot.ob_rt),2)>= 0.95 and round(max(tot.ob_rt),2) <1 then 100
when  round(max(tot.ob_rt),2)>= 1 then 150
else 0
end) as ob_training_pts,
--Updated on 12/15/2020: FY 21 Dashboard
(case 
when  round(comp_oe_rt,2) > 0.76 then 0
when  round(comp_oe_rt,2) > 0.72 and round(comp_oe_rt,2)  <=0.76 then 50
when  round(comp_oe_rt,2) > 0.69 and round(comp_oe_rt,2)  <= 0.72 then 75
when round(comp_oe_rt,2) > 0.64 and round(comp_oe_rt,2)  <= 0.69 then 100
when  round(comp_oe_rt,2) > 0.58 and round(comp_oe_rt,2)  <= 0.64 then 125
when  round(comp_oe_rt,2)  <= 0.58 then 150
else 0
end) as comp_oe_rt_pts,
--Updated on 12/15/2020: FY 21 Dashboard
(case 
when  round(sep_compliance_rt,2) < 0.54 then 0
when  round(sep_compliance_rt,2) >= 0.54 and round(sep_compliance_rt,2) <0.59 then 50
when  round(sep_compliance_rt,2) >= 0.59 and round(sep_compliance_rt,2) < 0.62 then 75
when  round(sep_compliance_rt,2) >= 0.62 and round(sep_compliance_rt,2) < 0.67 then 100
when  round(sep_compliance_rt,2) >= 0.67 and round(sep_compliance_rt,2) < 0.71 then 125
when  round(sep_compliance_rt,2) >= 0.71 then 150
else 0
end) as sep_compliance_rt_pts,
--Updated on 12/15/2020: FY 21 Dashboard
(case 
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2) > 4.75 then 0
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2)> 4.52 and round(max(tlu.lab_utlz),2) <= 4.75 then 50
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2)> 4.47 and round(max(tlu.lab_utlz),2) <= 4.52 then 75
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2)> 4.43 and round(max(tlu.lab_utlz),2) <= 4.47 then 100
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2)> 4.38 and round(max(tlu.lab_utlz),2) <= 4.43 then 125
when tmi.fcy_num = 'MI2191' and round(max(tlu.lab_utlz),2)<=4.38 then 150

when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2) > 5.38 then 0
when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2)> 5.12 and round(max(tlu.lab_utlz),2) <= 5.38 then 50
when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2)> 5.07 and round(max(tlu.lab_utlz),2) <= 5.12 then 75
when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2)> 5.02 and round(max(tlu.lab_utlz),2) <= 5.07 then 100
when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2)> 4.97 and round(max(tlu.lab_utlz),2) <= 5.02 then 125
when tmi.fcy_num = 'MI2061' and round(max(tlu.lab_utlz),2)<=4.97 then 150

when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2) > 5.54 then 0
when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2)> 5.28 and round(max(tlu.lab_utlz),2) <= 5.54 then 50
when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2)> 5.23 and round(max(tlu.lab_utlz),2) <= 5.28 then 75
when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2)> 5.17 and round(max(tlu.lab_utlz),2) <= 5.23 then 100
when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2)> 5.12 and round(max(tlu.lab_utlz),2) <= 5.17 then 125
when tmi.fcy_num = 'MI2302'  and round(max(tlu.lab_utlz),2)<=5.12 then 150


when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2) > 6.01 then 0
when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2)> 5.72 and round(max(tlu.lab_utlz),2) <= 6.01 then 50
when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2)> 5.66 and round(max(tlu.lab_utlz),2) <= 5.72 then 75
when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2)> 5.61 and round(max(tlu.lab_utlz),2) <= 5.66 then 100
when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2)> 5.55 and round(max(tlu.lab_utlz),2) <= 5.61 then 125
when tmi.fcy_num = 'MI5020'  and round(max(tlu.lab_utlz),2)<=5.55 then 150

when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2) > 5.53 then 0
when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2)> 5.27 and round(max(tlu.lab_utlz),2) <= 5.53 then 50
when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2)> 5.22 and round(max(tlu.lab_utlz),2) <= 5.27 then 75
when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2)> 5.16 and round(max(tlu.lab_utlz),2) <= 5.22 then 100
when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2)> 5.11 and round(max(tlu.lab_utlz),2) <= 5.16 then 125
when tmi.fcy_num = 'MI2001'  and round(max(tlu.lab_utlz),2)<=5.11 then 150

when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2) > 6.94 then 0
when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2)> 6.61 and round(max(tlu.lab_utlz),2) <= 6.94 then 50
when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2)> 6.54 and round(max(tlu.lab_utlz),2) <=  6.61 then 75
when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2)> 6.48 and round(max(tlu.lab_utlz),2) <= 6.54 then 100
when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2)> 6.41 and round(max(tlu.lab_utlz),2) <= 6.48 then 125
when tmi.fcy_num = 'MI2048'  and round(max(tlu.lab_utlz),2)<= 6.41 then 150
--
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2) > 6.41 then 0
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2)> 6.1 and round(max(tlu.lab_utlz),2) <= 6.41 then 50
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2)> 6.04 and round(max(tlu.lab_utlz),2) <= 6.1 then 75
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2)> 5.98 and round(max(tlu.lab_utlz),2) <= 6.04 then 100
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2)> 5.92 and round(max(tlu.lab_utlz),2) <= 5.98 then 125
when tmi.fcy_num = '637619'  and round(max(tlu.lab_utlz),2)<= 5.92 then 150

when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2) > 4.82 then 0
when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2)> 4.59 and round(max(tlu.lab_utlz),2) <= 4.82 then 50
when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2)> 4.54 and round(max(tlu.lab_utlz),2) <= 4.59 then 75
when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2)> 4.5 and round(max(tlu.lab_utlz),2) <= 4.54 then 100
when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2)> 4.45 and round(max(tlu.lab_utlz),2) <= 4.5 then 125
when tmi.fcy_num = 'MI2055'  and round(max(tlu.lab_utlz),2)<=4.45 then 150

when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2) > 5.18 then 0
when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2)> 4.93 and round(max(tlu.lab_utlz),2) <= 5.18 then 50
when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2)> 4.88 and round(max(tlu.lab_utlz),2) <= 4.93 then 75
when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2)> 4.83 and round(max(tlu.lab_utlz),2) <= 4.88 then 100
when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2)> 4.78 and round(max(tlu.lab_utlz),2) <= 4.83 then 125
when tmi.fcy_num = '600816'  and round(max(tlu.lab_utlz),2)<=4.78 then 150
else 0
end) as lab_utlz_pts,
--Updated on 12/15/2020: FY 21 Dashboard
(case 
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor) < 17 then 0
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 17 and max(pepm.prgny_pct_scor) < 20 then 50
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 20 and max(pepm.prgny_pct_scor) < 21 then 75
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 21 and max(pepm.prgny_pct_scor) < 21.6 then 100
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 21.6 and max(pepm.prgny_pct_scor) < 22 then 125
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>=22 then 150

when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor) < 25.3 then 0
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 25.3 and max(pepm.prgny_pct_scor) < 29.8 then 50
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 29.8 and max(pepm.prgny_pct_scor) < 31.3 then 75
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 31.3 and max(pepm.prgny_pct_scor) < 32.2 then 100
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 32.2 and max(pepm.prgny_pct_scor) < 32.8 then 125
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 32.8 then 150

when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor) < 20.4 then 0
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 20.4 and max(pepm.prgny_pct_scor) < 24 then 50
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 24 and max(pepm.prgny_pct_scor) < 25.2 then 75
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 25.2 and max(pepm.prgny_pct_scor) < 25.9 then 100
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 25.9 and max(pepm.prgny_pct_scor) < 26.4 then 125
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 26.4 then 150


when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor) <= 25.7 then 0
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 25.7 and max(pepm.prgny_pct_scor) <= 30.2 then 50
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 30.2 and max(pepm.prgny_pct_scor) <= 31.7 then 75
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 31.7 and max(pepm.prgny_pct_scor) <= 32.6 then 100
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 32.6 and max(pepm.prgny_pct_scor) <= 33.2 then 125
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)>33.2 then 150

when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor) < 17 then 0
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 17 and max(pepm.prgny_pct_scor) < 20 then 50
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 20 and max(pepm.prgny_pct_scor) < 21 then 75
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 21 and max(pepm.prgny_pct_scor) < 21.6 then 100
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 21.6 and max(pepm.prgny_pct_scor) < 22 then 125
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 22 then 150

when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor) < 17.9 then 0
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 17.9 and max(pepm.prgny_pct_scor) < 21.1 then 50
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 21.1 and max(pepm.prgny_pct_scor) < 22.2 then 75
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 22.2 and max(pepm.prgny_pct_scor) < 22.8 then 100
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 22.8 and max(pepm.prgny_pct_scor) < 23.2 then 125
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 23.2 then 150
--
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor) < 35.7 then 0
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 35.7 and max(pepm.prgny_pct_scor) < 42 then 50
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 42 and max(pepm.prgny_pct_scor) < 44.1 then 75
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 44.1 and max(pepm.prgny_pct_scor) < 45.4 then 100
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 45.4 and max(pepm.prgny_pct_scor) < 46.2 then 125
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 46.2 then 150

when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor) < 23.4 then 0
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 23.4 and max(pepm.prgny_pct_scor) < 27.5 then 50
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 27.5 and max(pepm.prgny_pct_scor) < 28.9 then 75
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 28.9 and max(pepm.prgny_pct_scor) < 29.7 then 100
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 29.7 and max(pepm.prgny_pct_scor) < 30.3 then 125
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 30.3 then 150

when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor) < 26.4 then 0
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 26.4 and max(pepm.prgny_pct_scor) < 31 then 50
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 31 and max(pepm.prgny_pct_scor) < 32.6 then 75
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 32.6 and max(pepm.prgny_pct_scor) < 33.5 then 100
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 33.5 and max(pepm.prgny_pct_scor) < 34.1 then 125
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>=34.1 then 150



else 0
end) as prgny_pct_scor_pts,
--Updated on 12/15/2020: FY21 Dashboard
(case 
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3) < 0.779 then 0
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.779 and round(max(tpep.ptnt_exrnc_scor),3) < 0.795 then 50
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.795 and round(max(tpep.ptnt_exrnc_scor),3) < 0.799 then 75
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.799 and round(max(tpep.ptnt_exrnc_scor),3) < 0.803 then 100
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.803 and round(max(tpep.ptnt_exrnc_scor),3) < 0.807 then 125
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>=0.807 then 150

when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3) < 0.785 then 0
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.785 and round(max(tpep.ptnt_exrnc_scor),3) < 0.801 then 50
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.801 and round(max(tpep.ptnt_exrnc_scor),3) < 0.805 then 75
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.805 and round(max(tpep.ptnt_exrnc_scor),3) < 0.809 then 100
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.809 and round(max(tpep.ptnt_exrnc_scor),3) < 0.813 then 125
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>=0.813 then 150

when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.739 then 0
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.739 and round(max(tpep.ptnt_exrnc_scor),3) < 0.754 then 50
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.754 and round(max(tpep.ptnt_exrnc_scor),3) < 0.758 then 75
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.758 and round(max(tpep.ptnt_exrnc_scor),3) < 0.762 then 100
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.762 and round(max(tpep.ptnt_exrnc_scor),3) < 0.765 then 125
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>=0.765 then 150


when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.738 then 0
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.738 and round(max(tpep.ptnt_exrnc_scor),3) < 0.753 then 50
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.753 and round(max(tpep.ptnt_exrnc_scor),3) < 0.757 then 75
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.757 and round(max(tpep.ptnt_exrnc_scor),3) < 0.761 then 100
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.761 and round(max(tpep.ptnt_exrnc_scor),3) < 0.764 then 125
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.764 then 150

when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.77 then 0
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.77 and round(max(tpep.ptnt_exrnc_scor),3) < 0.786 then 50
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.786 and round(max(tpep.ptnt_exrnc_scor),3) < 0.79 then 75
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.79 and round(max(tpep.ptnt_exrnc_scor),3) < 0.794 then 100
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.794 and round(max(tpep.ptnt_exrnc_scor),3) < 0.798 then 125
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.798 then 150

when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3) <0.799 then 0
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.799 and round(max(tpep.ptnt_exrnc_scor),3) < 0.815 then 50
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.815 and round(max(tpep.ptnt_exrnc_scor),3) < 0.819 then 75
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.819 and round(max(tpep.ptnt_exrnc_scor),3) < 0.823 then 100
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.823 and round(max(tpep.ptnt_exrnc_scor),3)< 0.827 then 125
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.827 then 150
--
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.767 then 0
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.767 and round(max(tpep.ptnt_exrnc_scor),3) < 0.783 then 50
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.783 and round(max(tpep.ptnt_exrnc_scor),3) < 0.787 then 75
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.787 and round(max(tpep.ptnt_exrnc_scor),3) < 0.791 then 100
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.791 and round(max(tpep.ptnt_exrnc_scor),3) < 0.795 then 125
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>=0.795 then 150

when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3) <0.81 then 0
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.81 and round(max(tpep.ptnt_exrnc_scor),3) < 0.827 then 50
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.827 and round(max(tpep.ptnt_exrnc_scor),3) < 0.831 then 75
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.831 and round(max(tpep.ptnt_exrnc_scor),3) < 0.835 then 100
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.835 and round(max(tpep.ptnt_exrnc_scor),3) < 0.839 then 125
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.839 then 150

when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.834 then 0
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3) >= 0.834 and round(max(tpep.ptnt_exrnc_scor),3) < 0.851 then 50
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.851 and round(max(tpep.ptnt_exrnc_scor),3) < 0.855 then 75
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.855 and round(max(tpep.ptnt_exrnc_scor),3) < 0.86 then 100
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.86 and round(max(tpep.ptnt_exrnc_scor),3) < 0.864 then 125
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>=0.864 then 150

else 0
end) as ptnt_exrnc_scor_pts
,
case 
when tmi.rpt_dt='2019-08-01 00:00:00' then 1
when tmi.rpt_dt='2019-09-01 00:00:00' then 2
when tmi.rpt_dt='2019-10-01 00:00:00' then 3
when tmi.rpt_dt='2019-11-01 00:00:00' then 4
when tmi.rpt_dt='2019-12-01 00:00:00' then 5
when tmi.rpt_dt='2020-01-01 00:00:00' then 6
when tmi.rpt_dt='2020-02-01 00:00:00' then 7
when tmi.rpt_dt='2020-03-01 00:00:00' then 8
when tmi.rpt_dt='2020-04-01 00:00:00' then 9
when tmi.rpt_dt='2020-05-01 00:00:00' then 10
when tmi.rpt_dt='2020-06-01 00:00:00' then 11
when tmi.rpt_dt ='2019-07-01 00:00:00' then 12
else null
end as no_of_mnths,

(case 
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt) > 77 then 0
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)>  73 and max(thef.harm_events_cnt) <= 77 then 50
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)> 69 and max(thef.harm_events_cnt) <= 73 then 75
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)> 68 and max(thef.harm_events_cnt) <= 69 then 100
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)> 66 and max(thef.harm_events_cnt) <= 68 then 125
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)<= 66 then 150

when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt) > 5  then 0
when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 4 and max(thef.harm_events_cnt) <= 5 then 75
--when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 0.78 and max(thef.harm_events_cnt) <= 0.784 then 75
--when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 0.784 and max(thef.harm_events_cnt) <= 0.788 then 100
when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 3 and max(thef.harm_events_cnt) <= 4 then 125
when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)<=3 then 150

when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt) > 102 then 0
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 97 and max(thef.harm_events_cnt) <= 102 then 50
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 92 and max(thef.harm_events_cnt) <= 97 then 75
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 90 and max(thef.harm_events_cnt) <= 92 then 100
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 87 and max(thef.harm_events_cnt) <= 90 then 125
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)<= 87 then 150

when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) > 63 then 0
when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) > 60 and max(thef.harm_events_cnt) <= 63 then 50
when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) > 57 and max(thef.harm_events_cnt) <= 60 then 75
when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) > 56 and max(thef.harm_events_cnt) <= 57 then 100
when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) > 54 and max(thef.harm_events_cnt) <= 56 then 125
when tmi.fcy_num = 'MI5020'  and max(thef.harm_events_cnt) <=54 then 150

when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt) > 14 then 0
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 13 and max(thef.harm_events_cnt) <= 14 then 50
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 13 and max(thef.harm_events_cnt) <= 13 then 75
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 12 and max(thef.harm_events_cnt) <= 13 then 100
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 12 and max(thef.harm_events_cnt) <= 12 then 125
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)<=12 then 150
--
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt) > 50 then 0
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 48 and max(thef.harm_events_cnt) <= 50 then 50
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 46 and max(thef.harm_events_cnt) <= 48 then 75
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 45 and max(thef.harm_events_cnt) <= 46 then 100
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 43 and max(thef.harm_events_cnt) <= 45 then 125
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)<= 43 then 150
--
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt) > 41 then 0
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 39 and max(thef.harm_events_cnt) <= 41 then 50
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 37 and max(thef.harm_events_cnt) <= 39 then 75
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 36 and max(thef.harm_events_cnt) <= 37 then 100
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 35 and max(thef.harm_events_cnt) <= 36 then 125
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)<= 35 then 150
--
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt) > 26 then 0
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 25 and max(thef.harm_events_cnt) <= 26 then 50
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 24 and max(thef.harm_events_cnt) <= 25 then 75
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 23 and max(thef.harm_events_cnt) <= 24 then 100
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 22 and max(thef.harm_events_cnt) <= 23 then 125
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)<= 22 then 150
--
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt) > 36 then 0
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 34 and max(thef.harm_events_cnt) <= 36 then 50
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 33 and max(thef.harm_events_cnt) <= 34 then 75
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 32 and max(thef.harm_events_cnt) <= 33 then 100
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 31 and max(thef.harm_events_cnt) <= 32 then 125
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)<= 31 then 150
else 0
end) as harm_events_pts
,
--Updated on 12/10/2020 FY 21 Dashboard
(case 
when  max(ROUND(tzef.zero_events_rt,2)) < 0.79 then 0
when  max(ROUND(tzef.zero_events_rt,2)) >= 0.79 and max(ROUND(tzef.zero_events_rt,2)) < 0.86 then 50
when  max(ROUND(tzef.zero_events_rt,2)) >= 0.86 and max(ROUND(tzef.zero_events_rt,2)) < 0.87 then 75
when  max(ROUND(tzef.zero_events_rt,2)) >= 0.87 and max(ROUND(tzef.zero_events_rt,2)) < 0.89 then 100
when  max(ROUND(tzef.zero_events_rt,2)) >= 0.89 and  max(ROUND(tzef.zero_events_rt,2)) < 0.9 then 125
when  max(ROUND(tzef.zero_events_rt,2)) >=0.9 then 150
else 0
end
) as zero_events_rt_pts,
case when harm_events_pts > zero_events_rt_pts then harm_events_pts else zero_events_rt_pts end as harm_zero_events_pts,

case when readm_obs_pts > readm_oe_pts then readm_obs_pts else readm_oe_pts end as readm_obs_oe_pts,

((nvl(mort_oe_pts,0)*0.05 )+
(nvl(comp_oe_rt_pts,0)*0.05)+
(nvl(harm_zero_events_pts,0)*0.15)+
(nvl(readm_obs_oe_pts,0)*0.15)+ 
(nvl(sep_mort_oe_pts,0)*0.055)+
(nvl(sep_compliance_rt_pts,0)*0.055)+
(nvl(lab_utlz_pts,0)*0.13)+
(nvl(prgny_pct_scor_pts,0)*0.05)+
(max(nvl(round(acof.mpp_scr),0))*0.15)   +
(nvl(pci_radial_access_pts,0)*0.1) +
(nvl(ptnt_exrnc_scor_pts,0)*0.06)) as weighted_pts,

--Updated on 12/15/2020 FY 21 Dashboard
case when tmi.fcy_num = 'MI2191' then 1
when tmi.fcy_num = 'MI2061' then 0.9
when tmi.fcy_num = 'MI2302' then 1
when tmi.fcy_num = 'MI5020' then 1
when tmi.fcy_num = 'MI2001' then 0.9
when tmi.fcy_num = 'MI2048' then 1
when tmi.fcy_num = '637619' then 1
when tmi.fcy_num = 'MI2055' then 0.9
when tmi.fcy_num = '600816' then 1
else 1
end as weight,

weighted_pts/weight as clncl_outc_scor

from 
stg_tmp_mrtly_ind_wo_covid tmi
left join stg_pqsd_cmplc_idnx_fct pcf on tmi.rpt_dt = date_trunc('month',pcf.end_of_month) and pcf.end_of_month <> pcf.first_of_month and tmi.fcy_num = pcf.fcy_num and pcf.msr_nm = 'Compl_R12M'
left join stg_tmp_readm_ind_wo_covid tri on tmi.rpt_dt = tri.rpt_dt and tmi.fcy_num=tri.fcy_num
left join stg_tmp_lab_utlz_fct_wo_covid tlu on tmi.rpt_dt = date(tlu.rpt_dt) and tmi.fcy_num = tlu.fcy_num
left join stg_tmp_card_rehab_fct tcrf on tmi.rpt_dt = tcrf.rpt_dt and tmi.fcy_num = tcrf.fcy_num
left join stg_tmp_ob_trn_fct tot on tmi.rpt_dt = tot.rpt_dt and tmi.fcy_num = tot.fcy_num
left join stg_tmp_sep_mrtly_ind_wo_covid tsm on tmi.rpt_dt = tsm.rpt_dt and tmi.fcy_num = tsm.fcy_num
left join stg_tmp_sep_compl_fct tscf on tmi.rpt_dt = tscf.rpt_dt and tmi.fcy_num = tscf.fcy_num
left join stg_aco_mpp_msr_fct acof on tmi.rpt_dt = date_trunc('month',acof.rpt_prd_end_dt) and tmi.fcy_num = acof.fcy_num
left join stg_TMP_HARM_EVENTS_FCT thef on tmi.rpt_dt = thef.rpt_dt and tmi.fcy_num = thef.fcy_num
left join stg_tmp_zero_events_fct tzef on tmi.rpt_dt = tzef.rpt_dt and tmi.fcy_num = tzef.fcy_num
LEFT JOIN stg_tmp_ptnt_exrnc_pct_msr_fct pepm on tmi.rpt_dt = date_trunc('month',pepm.rprt_dt) and tmi.fcy_num = pepm.fcy_num
--left join  stg_tmp_mmg_ovrl_ptnt_exrnc_msr_fct tpep on tmi.rpt_dt = date_trunc('month',tpep.rprt_dt) and tmi.fcy_num = tpep.fcy_num
left join stg_mmg_ovrl_ptnt_exrnc_msr_fct tpep on tmi.rpt_dt = date_trunc('month',tpep.rprt_dt) and tmi.fcy_num = tpep.fcy_num 
--Added 12/10/2020 : FY21 Metric
left join stg_tmp_pci_radial_access_r12m_fct tpci on tmi.rpt_dt = tpci.rpt_dt and tmi.fcy_num = tpci.fcy_num
group by 1,2,3

 );
 
 ----> 
 
--select *, zero_events_rt, ROUND(zero_events_rt,2) as x  from stg_tmp_zero_events_fct WHERE fcy_nm ='McLaren Greater Lansing' AND rpt_dt = '2020-03-01 00:00:00'   LIMIT 100 ; 
-- select attname || ',' from _v_relation_column where name='stg_clncl_outc_scor_fct' and attname like '%pts'
-- 
-- select *,
-- readm_obs_rt,
-- readm_obs_pts, --125
--readm_oe_pts,
--mort_oe_pts,
--sep_mort_oe_pts,
--cardiac_rehab_pts,
--ob_training_pts,
--comp_oe_rt_pts,
--sep_compliance_rt_pts,
--lab_utlz_pts,
--prgny_pct_scor_pts,
--ptnt_exrnc_scor_pts,
--harm_events_pts,
--zero_events_rt_pts,
--harm_zero_events_pts,
--readm_obs_oe_pts,--125
--weighted_pts
--from pce_qe16_slp_prd_dm..clncl_outc_scor_fct Z where rpt_dt = '2020-02-01 00:00:00' and fcy_nm ='McLaren Flint';

--select Z.fcy_nm, readm_obs_rt *100 from  stg_clncl_outc_scor_fct Z where rpt_dt = '2020-02-01 00:00:00';
--
-- 
-- select fcy_nm,
--ROUND(mort_oe_rt,2) as mort_oe_rt,
--ROUND(comp_oe_rt,2) as comp_oe_rt,
--ROUND(lab_utlz, 2) as lab_utlz,
--ROUND(clncl_outc_scor, 2) as clncl_outc_scor, 
--ROUND(weighted_pts, 2) as weighted_pts,
--ROUND(sep_compliance_rt, 2) as sep_compliance_rt,
--ROUND(sep_mort_oe_rt, 2) as sep_mort_oe_rt ,
--ROUND(zero_events_rt, 2) as zero_events_rt, 
--ROUND(cardiac_rt, 2  ) as cardiac_rt,
--cardiac_rt as cardiac_rt_actual,
--ROUND(Z.snpsr4qnumerator,2) as snpsr4qnumerator,
--ROUND(Z.snpsr4qdenominator,2) as snpsr4qdenominator,
--Z.*
--from pce_qe16_slp_prd_dm..clncl_outc_scor_fct Z where rpt_dt = '2020-02-01 00:00:00' and fcy_nm ='McLaren Greater Lansing';
-- select * from  stg_mmg_ovrl_ptnt_exrnc_msr_fct LIMIT 100; 
--  select distinct rpt_dt from  stg_tmp_mrtly_ind LIMIT 100; 
-- 
 /* Month over Month Tables*/
 
 
 ----Cardiac Rehab Month over Month
 
 DROP TABLE TMP_CARDIAC_REHAB_BASE IF EXISTS;
;create temp table  TMP_CARDIAC_REHAB_BASE AS 
(SELECT clientid, clientname, timeframecode, reportsection, Snpstfpercentage,snpstfnumerator, snpstfdenominator
  FROM pce_qe16_prd..ncdr_hsptl_fct_vw
  where metricid=45 and market_name = 'My Group'
  
  UNION
  SELECT DISTINCT clientid, clientname,'2019Q1' AS timeframecode, NULL AS reportsection, NULL AS Snpstfpercentage, 0 AS snpstfnumerator, 0 AS snpstfdenominator
    FROM pce_qe16_prd..ncdr_hsptl_fct_vw
  where metricid=45 and market_name = 'My Group'
  UNION
    SELECT DISTINCT clientid, clientname,'2019Q2' AS timeframecode, NULL AS reportsection, NULL AS Snpstfpercentage, 0 AS snpstfnumerator, 0 AS snpstfdenominator
    FROM pce_qe16_prd..ncdr_hsptl_fct_vw
  where metricid=45 and market_name = 'My Group'
  UNION
    SELECT DISTINCT clientid, clientname,'2019Q4' AS timeframecode, NULL AS reportsection, NULL AS Snpstfpercentage,0 AS snpstfnumerator, 0 AS snpstfdenominator
    FROM pce_qe16_prd..ncdr_hsptl_fct_vw
  where metricid=45 and market_name = 'My Group'
  UNION
    SELECT DISTINCT clientid, clientname,'2020Q1' AS timeframecode, NULL AS reportsection, NULL AS Snpstfpercentage, 0 AS snpstfnumerator, 0 AS snpstfdenominator
    FROM pce_qe16_prd..ncdr_hsptl_fct_vw
  where metricid=45 and market_name = 'My Group'
   UNION
    SELECT DISTINCT clientid, clientname,'2020Q2' AS timeframecode, NULL AS reportsection, NULL AS Snpstfpercentage, 0 AS snpstfnumerator, 0 AS snpstfdenominator
    FROM pce_qe16_prd..ncdr_hsptl_fct_vw
  where metricid=45 and market_name = 'My Group'
    UNION
    SELECT DISTINCT clientid, clientname,'2020Q3' AS timeframecode, NULL AS reportsection, NULL AS Snpstfpercentage, 0 AS snpstfnumerator, 0 AS snpstfdenominator
    FROM pce_qe16_prd..ncdr_hsptl_fct_vw
  where metricid=45 and market_name = 'My Group'
    UNION
    SELECT DISTINCT clientid, clientname,'2020Q4' AS timeframecode, NULL AS reportsection, NULL AS Snpstfpercentage, 0 AS snpstfnumerator, 0 AS snpstfdenominator
    FROM pce_qe16_prd..ncdr_hsptl_fct_vw
  where metricid=45 and market_name = 'My Group');


drop table stg_card_rhb_movm_fct if exists; 
create  table stg_card_rhb_movm_fct as
--CHANGE : Feb 2021 Done the required changes as per the ACO DB cdr_dim changes
(Select distinct tcrb.*, cd.frst_day_of_mo as rpt_mnth
from TMP_CARDIAC_REHAB_BASE tcrb
inner join  pce_ae00_aco_prd_cdr..cdr_dim cd on tcrb.timeframecode = replace(cd.qtr_and_yr_abbr, ' ','')); 

-----MMG Overall Patient Experience Month over Month

DROP TABLE tmp_sf IF EXISTS;
;create temp table  tmp_sf as
(SELECT sf.client_id, 
case sf.client_id  when 331 then 'Lapeer'
when 398 then 'Flint'
when 453 then 'Northern'
when 1193 then 'Lansing'
when 1411 then 'Bay'
when 2766 then 'Oakland'
when 4062 then 'Karmanos'
when 5380 then 'Port Huron'
when 7841 then 'Central'
when 9123 then 'Macomb'
when 12705 then 'MMG'
when 24594 then 'Northern Michigan-MD'
when 26040 then 'Caro'
when 32475 then 'Thumb'
ELSE CAST(sf.client_id AS VARCHAR(20))
END AS FCY_NM,
sf.survey_id, 
sf.service, 
sf.disdate, 
sf.recdate, 
sf.resp_val, 
sf.varname, 
sf.question_text, 
sf.section, 
sf.standard, 
sf.screening, 
sf.top_box_answer, 
sf.top_box_scale, 
sf.survey_type, 
sf.sentiment, 

cast((case when length(disdate)<8 then disdate||'-01'
  		else disdate
 	 end) as date) as dschrg_dt,
	 'a' as join_key
  FROM pce_qe16_pressganey_prd_zoom..survey_fact sf);
  
  DROP TABLE stg_mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct IF EXISTS; 

create  table stg_mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct as 
(SELECT distinct tsf.service, 
tsf.client_id,
tsf.FCY_NM,
pfd.hsptl_rgon,
tsf.dschrg_dt,
sum((CASE WHEN tsf.survey_type = 'CAHPS' and tsf.top_box_answer IS NOT NULL and tsf.resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
then 1
WHEN tsf.survey_type = 'CAHPS' and   tsf.top_box_answer IS NOT NULL then 0
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val = '5' then 1
WHEN tsf.survey_type = 'OTHER' and tsf.resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
sum( case when tsf.resp_val is not null then 1 else 0 end) as resp_cnt
FROM tmp_sf tsf
left join pce_qe16_pressganey_prd_zoom..cv_survey_demographics_tr csd on tsf.survey_id = csd.survey_id
left join pce_qe16_pressganey_prd_zoom..prgney_fcy_dim pfd on csd.siteid = pfd.site_id
where tsf.varname in ('A1','A91','CP2','CP3','CP4','CP10','CP112','I3','I60','N2','N48','O4','O15','V2','V7','V60') AND tsf.survey_type = 'OTHER' and tsf.service = 'MD'
group by 1,2,3,4,5);

-------Patient Experience Month over Month


DROP TABLE tmp_sf IF EXISTS;
;create temp table  tmp_sf as
(SELECT sf.client_id, 
case sf.client_id  when 331 then 'Lapeer'
when 398 then 'Flint'
when 453 then 'Northern'
when 1193 then 'Lansing'
when 1411 then 'Bay'
when 2766 then 'Oakland'
when 4062 then 'Karmanos'
when 5380 then 'Port Huron'
when 7841 then 'Central'
when 9123 then 'Macomb'
when 12705 then 'MMG'
when 24594 then 'Northern Michigan-MD'
when 26040 then 'Caro'
when 32475 then 'Thumb'
ELSE CAST(sf.client_id AS VARCHAR(20))
END AS FCY_NM,
sf.survey_id, 
sf.service, 
sf.disdate, 
sf.recdate, 
sf.resp_val, 
sf.varname, 
sf.question_text, 
sf.section, 
sf.standard, 
sf.screening, 
sf.top_box_answer, 
sf.top_box_scale, 
sf.survey_type, 
sf.sentiment, 

cast((case when length(disdate)<8 then disdate||'-01'
  		else disdate
 	 end) as date) as dschrg_dt,
	 'a' as join_key
  FROM pce_qe16_pressganey_prd_zoom..survey_fact sf);
  
  DROP TABLE tmp_maxd IF EXISTS;
;create temp table  tmp_maxd as
  (Select last_day(max(tsf.dschrg_dt)) as dschrg_dt,
  'a' as join_key
  FROM tmp_sf tsf);
  
  drop table stg_ptnt_exrnc_pct_msr_fct_MNTH_OVER_MNTH if exists;
 
 create  table stg_ptnt_exrnc_pct_msr_fct_MNTH_OVER_MNTH AS 
(
select q.*,
ppep.prgny_pct*q.msr_wt as prgny_pct_scor
from
(SELECT distinct service, 
 tsf.client_id,
tsf.FCY_NM,
date_trunc('month',tsf.dschrg_dt) as rprt_dt,
case when service= 'IN' then 0.5
when service = 'AS' then 0.2
when service = 'ER' then 0.3
end as msr_wt,
sum((CASE WHEN survey_type = 'CAHPS' and top_box_answer IS NOT NULL and 
resp_val IN ( 'Yes' , 'Always','Strongly agree','Definitely yes', 
'Yes, definitely', 'Yes', '9' ,'10-Best possible', '10-Best facility possible', '10-Best provider')
  then 1
WHEN survey_type = 'CAHPS' and   top_box_answer IS NOT NULL then 0
WHEN survey_type = 'OTHER' and resp_val = '5' then 1
WHEN survey_type = 'OTHER' and resp_val IN ('1' , '2' , '3' , '4') then 0
END)) as top_box_resp_cnt,
count(distinct tsf.survey_id) as resp_cnt,
--top_box_resp_cnt/resp_cnt as ptnt_exrnc_pct
ROUND((top_box_resp_cnt/resp_cnt) * 100) as ptnt_exrnc_pct

FROM tmp_sf tsf
where varname in ('CMS_24','OSC_24','F4')
group by 1,2,3,4
) as q
--left join pce_qe16_pressganey_prd_zoom..prgny_ptnt_exrnc_pct_dim ppep on q.service = ppep.svc_cd and q.ptnt_exrnc_pct = ppep.scor
left join pce_qe16_pressganey_prd_zoom..prgny_ptnt_exrnc_pct_dim ppep on q.service = ppep.svc_cd and q.ptnt_exrnc_pct = ppep.scor and  ppep.reporting_year='2021'
);

DROP TABLE stg_tmp_hghlyemtgnc_wthantmtc_r12m_fct IF EXISTS;
CREATE TABLE stg_tmp_hghlyemtgnc_wthantmtc_r12m_fct as 
with prd_encntr_onc_anl_fct_hghlyemtgnc_wthantmtc_only as 
(select 
EOAF.oncology_region1 as fcy_nm,  EOAF.dschrg_dt, EOAF.cases_with_antiemetic_ind, EOAF.highemeto_antiecases_denom_ind
FROM pce_qe16_slp_prd_dm..prd_encntr_oncology_anl_fct EOAF 
INNER JOIN  pce_qe16_slp_prd_dm..prd_encntr_anl_fct EF USING (fcy_nm, encntr_num)
WHERE EF.tot_chrg_ind =1 and fcy_nm NOT IN  ('CARO','Caro','MMG','Thumb') and EOAF.oncology_region1 <> 'UNKNOWN' and EF.dschrg_dt is NOT NULL 
AND EF.in_or_out_patient_ind ='O'
)
,grouped_data as 
(
  select 
 case when Ef.fcy_nm in ('Bay','McLaren Bay Region') then 'McLaren Bay Region'
when Ef.fcy_nm in  ('Central','McLaren Central Michigan') then 'McLaren Central Michigan' 
when Ef.fcy_nm in  ('Flint','McLaren Flint') then 'McLaren Flint'
when Ef.fcy_nm in  ('Lansing','McLaren Greater Lansing') then 'McLaren Greater Lansing' 
when Ef.fcy_nm in  ('Lapeer','McLaren Lapeer Region') then 'McLaren Lapeer Region' 
when Ef.fcy_nm in  ('Macomb','McLaren Macomb') then 'McLaren Macomb'
when Ef.fcy_nm in  ('Oakland','McLaren Oakland') then 'McLaren Oakland'
when Ef.fcy_nm in  ('Northern','McLaren Northern Michigan') then 'McLaren Northern Michigan'
when Ef.fcy_nm in  ('Karmanos','McLaren Karmanos Cancer Center') then 'Karmanos Cancer Center'
when Ef.fcy_nm in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then 'McLaren Port Huron Hospital'
else NULL  end
as fcy_nm,
case when EF.fcy_nm  in ('Bay','McLaren Bay Region') then 'MI2191'
when EF.fcy_nm  in  ('Central','McLaren Central Michigan') then 'MI2061'
when EF.fcy_nm  in  ('Flint','McLaren Flint') then 'MI2302'
when EF.fcy_nm  in  ('Lansing','McLaren Greater Lansing') then 'MI5020'
when EF.fcy_nm  in  ('Lapeer','McLaren Lapeer Region') then 'MI2001'
when EF.fcy_nm  in  ('Macomb','McLaren Macomb') then 'MI2048'
when EF.fcy_nm  in  ('Northern','McLaren Northern Michigan') then '637619'
when EF.fcy_nm  in ('Oakland','McLaren Oakland') then 'MI2055'
when Ef.fcy_nm in  ('Karmanos','McLaren Karmanos Cancer Center') then '634342'
when EF.fcy_nm  in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then '600816'
else null
end as fcy_num,
date_trunc('month',dschrg_dt) as report_month,   
sum(EF.cases_with_antiemetic_ind) as antmtc_agg_csc_numr,
sum(case when EF.highemeto_antiecases_denom_ind =0 then NULL else EF.highemeto_antiecases_denom_ind end) as hghlyemeto_antmtc_agg_csc_dnmr
FROM prd_encntr_onc_anl_fct_hghlyemtgnc_wthantmtc_only EF
group by 1,2,3
order by 1 ASC,2 asc,3 desc
)
select fcy_nm,
fcy_num,
date(report_month) as rpt_dt,
sum(antmtc_agg_csc_numr) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as antmtc_csc_numr,
sum(hghlyemeto_antmtc_agg_csc_dnmr) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as hghlyemeto_antmtc_csc_dnmr
,nvl(round((antmtc_csc_numr/hghlyemeto_antmtc_csc_dnmr)* 100,3),0) as emto_with_antmtc_csc_rt
from grouped_data
where fcy_nm is NOT NULL and fcy_num is NOT NULL and rpt_dt is NOT NULL 
order by 1 ASC,2 asc,3 desc;

DROP TABLE stg_tmp_admssn_w30_dayschemo_r12m_fct IF EXISTS;
CREATE TABLE stg_tmp_admssn_w30_dayschemo_r12m_fct as 
with prd_encntr_onc_anl_fct_chemo_only as 
(select 
EOAF.oncology_region1 as fcy_nm,EF.fcy_num, EOAF.dschrg_dt, EOAF.chemo_denom_ind, EOAF.ip_visit_after_30_days_of_op_chemo_ind
FROM pce_qe16_slp_prd_dm..prd_encntr_oncology_anl_fct EOAF 
INNER JOIN  pce_qe16_slp_prd_dm..prd_encntr_anl_fct EF USING (fcy_nm, encntr_num)
WHERE EF.tot_chrg_ind =1 and fcy_nm NOT IN  ('CARO','Caro','MMG','Thumb') and EF.dschrg_dt is NOT NULL 
)
,grouped_data as 
(
  select 
 case when Ef.fcy_nm in ('Bay','McLaren Bay Region') then 'McLaren Bay Region'
when Ef.fcy_nm in  ('Central','McLaren Central Michigan') then 'McLaren Central Michigan' 
when Ef.fcy_nm in  ('Flint','McLaren Flint') then 'McLaren Flint'
when Ef.fcy_nm in  ('Lansing','McLaren Greater Lansing') then 'McLaren Greater Lansing' 
when Ef.fcy_nm in  ('Lapeer','McLaren Lapeer Region') then 'McLaren Lapeer Region' 
when Ef.fcy_nm in  ('Macomb','McLaren Macomb') then 'McLaren Macomb'
when Ef.fcy_nm in  ('Oakland','McLaren Oakland') then 'McLaren Oakland'
when Ef.fcy_nm in  ('Northern','McLaren Northern Michigan') then 'McLaren Northern Michigan'
when Ef.fcy_nm in  ('Karmanos','McLaren Karmanos Cancer Center') then 'Karmanos Cancer Center'
when Ef.fcy_nm in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then 'McLaren Port Huron Hospital'
else NULL  end
as fcy_nm,
case when EF.fcy_nm  in ('Bay','McLaren Bay Region') then 'MI2191'
when EF.fcy_nm  in  ('Central','McLaren Central Michigan') then 'MI2061'
when EF.fcy_nm  in  ('Flint','McLaren Flint') then 'MI2302'
when EF.fcy_nm  in  ('Lansing','McLaren Greater Lansing') then 'MI5020'
when EF.fcy_nm  in  ('Lapeer','McLaren Lapeer Region') then 'MI2001'
when EF.fcy_nm  in  ('Macomb','McLaren Macomb') then 'MI2048'
when EF.fcy_nm  in  ('Northern','McLaren Northern Michigan') then '637619'
when EF.fcy_nm  in ('Oakland','McLaren Oakland') then 'MI2055'
when Ef.fcy_nm in  ('Karmanos','McLaren Karmanos Cancer Center') then '634342'
when EF.fcy_nm  in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then '600816'
else null
end as fcy_num,
date_trunc('month',dschrg_dt) as report_month,
sum(EF.ip_visit_after_30_days_of_op_chemo_ind) as ip_visit_agg_csc_numr,
sum(case when EF.chemo_denom_ind =0 then NULL else EF.chemo_denom_ind end) as op_chemo_agg_csc_dnmr
FROM prd_encntr_onc_anl_fct_chemo_only EF
group by 1,2,3
order by 1 ASC,2 asc,3 desc
)
select fcy_nm,
fcy_num,
date(report_month) as rpt_dt,
sum(ip_visit_agg_csc_numr) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as hsptl_vsts_numr,
sum(op_chemo_agg_csc_dnmr) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as op_chemo_dnmr
,nvl(round((hsptl_vsts_numr/op_chemo_dnmr)* 100,3),0) as adm_w30days_chemo_rt
from grouped_data
where fcy_nm is NOT NULL and fcy_num is NOT NULL and rpt_dt is NOT NULL 
order by 1 ASC,2 asc,3 desc;


DROP TABLE stg_tmp_lung_cancer_screening_r12m_fct IF EXISTS;
CREATE TABLE stg_tmp_lung_cancer_screening_r12m_fct as 
with prd_encntr_anl_fct_lung_cancer_only as 
(select 
EF.fcy_nm, EF.fcy_num,EF.dschrg_dt, EF.lung_cancer_scrn_ind
FROM pce_qe16_slp_prd_dm..prd_encntr_anl_fct EF 
WHERE EF.tot_chrg_ind =1 and fcy_nm NOT IN  ('CARO','Caro','MMG','Thumb') and EF.dschrg_dt is NOT NULL 
)
,grouped_data as 
(
  select 
 case when Ef.fcy_nm in ('Bay','McLaren Bay Region') then 'McLaren Bay Region'
when Ef.fcy_nm in  ('Central','McLaren Central Michigan') then 'McLaren Central Michigan' 
when Ef.fcy_nm in  ('Flint','McLaren Flint') then 'McLaren Flint'
when Ef.fcy_nm in  ('Lansing','McLaren Greater Lansing') then 'McLaren Greater Lansing' 
when Ef.fcy_nm in  ('Lapeer','McLaren Lapeer Region') then 'McLaren Lapeer Region' 
when Ef.fcy_nm in  ('Macomb','McLaren Macomb') then 'McLaren Macomb'
when Ef.fcy_nm in  ('Oakland','McLaren Oakland') then 'McLaren Oakland'
when Ef.fcy_nm in  ('Northern','McLaren Northern Michigan') then 'McLaren Northern Michigan'
when Ef.fcy_nm in  ('Karmanos','McLaren Karmanos Cancer Center') then 'Karmanos Cancer Center'
when Ef.fcy_nm in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then 'McLaren Port Huron Hospital'
else NULL  end
as fcy_nm,
case when EF.fcy_nm  in ('Bay','McLaren Bay Region') then 'MI2191'
when EF.fcy_nm  in  ('Central','McLaren Central Michigan') then 'MI2061'
when EF.fcy_nm  in  ('Flint','McLaren Flint') then 'MI2302'
when EF.fcy_nm  in  ('Lansing','McLaren Greater Lansing') then 'MI5020'
when EF.fcy_nm  in  ('Lapeer','McLaren Lapeer Region') then 'MI2001'
when EF.fcy_nm  in  ('Macomb','McLaren Macomb') then 'MI2048'
when EF.fcy_nm  in  ('Northern','McLaren Northern Michigan') then '637619'
when EF.fcy_nm  in ('Oakland','McLaren Oakland') then 'MI2055'
when Ef.fcy_nm in  ('Karmanos','McLaren Karmanos Cancer Center') then '634342'
when EF.fcy_nm  in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then '600816'
else null
end as fcy_num,
date_trunc('month',dschrg_dt) as report_month,
sum(EF.lung_cancer_scrn_ind) as lung_cancer_screen_cases
FROM prd_encntr_anl_fct_lung_cancer_only EF
group by 1,2,3
order by 1 ASC,2 asc,3 desc
)
select fcy_nm,
fcy_num,
date(report_month) as rpt_dt,
sum(lung_cancer_screen_cases) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as lung_cancer_screen_agg_cases
from grouped_data
where fcy_nm is NOT NULL and fcy_num is NOT NULL and rpt_dt is NOT NULL 
order by 1 ASC,2 asc,3 desc;

--Highly Emetogenic Chemo with Targeted Antiemetic Treatment 
--SUM([Antiemetic Treatment Cases])/SUM([Cases With High Emeto and Antiemetic (Denominator)]))*100
--
DROP TABLE stg_tmp_lung_cancer_screening_r12m_fct IF EXISTS;
CREATE TABLE stg_tmp_lung_cancer_screening_r12m_fct as 
with prd_encntr_anl_fct_lung_cancer_only as 
(select 
EF.fcy_nm, EF.fcy_num,EF.dschrg_dt, EF.lung_cancer_scrn_ind
FROM pce_qe16_slp_prd_dm..prd_encntr_anl_fct EF 
WHERE EF.tot_chrg_ind =1 and fcy_nm NOT IN  ('CARO','Caro','MMG','Thumb') and EF.dschrg_dt is NOT NULL 
)
,grouped_data as 
(
  select 
 case when Ef.fcy_nm in ('Bay','McLaren Bay Region') then 'McLaren Bay Region'
when Ef.fcy_nm in  ('Central','McLaren Central Michigan') then 'McLaren Central Michigan' 
when Ef.fcy_nm in  ('Flint','McLaren Flint') then 'McLaren Flint'
when Ef.fcy_nm in  ('Lansing','McLaren Greater Lansing') then 'McLaren Greater Lansing' 
when Ef.fcy_nm in  ('Lapeer','McLaren Lapeer Region') then 'McLaren Lapeer Region' 
when Ef.fcy_nm in  ('Macomb','McLaren Macomb') then 'McLaren Macomb'
when Ef.fcy_nm in  ('Oakland','McLaren Oakland') then 'McLaren Oakland'
when Ef.fcy_nm in  ('Northern','McLaren Northern Michigan') then 'McLaren Northern Michigan'
when Ef.fcy_nm in  ('Karmanos','McLaren Karmanos Cancer Center') then 'Karmanos Cancer Center'
when Ef.fcy_nm in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then 'McLaren Port Huron Hospital'
else NULL  end
as fcy_nm,
case when EF.fcy_nm  in ('Bay','McLaren Bay Region') then 'MI2191'
when EF.fcy_nm  in  ('Central','McLaren Central Michigan') then 'MI2061'
when EF.fcy_nm  in  ('Flint','McLaren Flint') then 'MI2302'
when EF.fcy_nm  in  ('Lansing','McLaren Greater Lansing') then 'MI5020'
when EF.fcy_nm  in  ('Lapeer','McLaren Lapeer Region') then 'MI2001'
when EF.fcy_nm  in  ('Macomb','McLaren Macomb') then 'MI2048'
when EF.fcy_nm  in  ('Northern','McLaren Northern Michigan') then '637619'
when EF.fcy_nm  in ('Oakland','McLaren Oakland') then 'MI2055'
when Ef.fcy_nm in  ('Karmanos','McLaren Karmanos Cancer Center') then '634342'
when EF.fcy_nm  in  ('Port Huron','McLaren Port Huron Hospital','McLaren Port Huron') then '600816'
else null
end as fcy_num,
date_trunc('month',dschrg_dt) as report_month,
sum(EF.lung_cancer_scrn_ind) as lung_cancer_screen_cases
FROM prd_encntr_anl_fct_lung_cancer_only EF
group by 1,2,3
order by 1 ASC,2 asc,3 desc
)
select fcy_nm,
fcy_num,
date(report_month) as rpt_dt,
sum(lung_cancer_screen_cases) OVER (PARTITION BY fcy_nm ORDER BY date(report_month) ROWS BETWEEN 11 preceding and current row) as lung_cancer_screen_agg_cases
from grouped_data
where fcy_nm is NOT NULL and fcy_num is NOT NULL and rpt_dt is NOT NULL 
order by 1 ASC,2 asc,3 desc;

drop table stg_onc_ptnt_exrnc_pct_msr_fct_new if exists;
CREATE TABLE stg_onc_ptnt_exrnc_pct_msr_fct_new as 
select service, client_id, 
case when fcy_nm = 'Karmanos' then 'Karmanos Cancer Center' end as fcy_nm,
case when fcy_nm = 'Karmanos' then '634342' end as fcy_num,
rprt_dt, resp_cnt, top_box_resp_cnt, sub_ptnt_exrnc_pct, ptnt_exrnc_pct
FROM  stg_onc_ptnt_exrnc_pct_msr_fct;

drop table stg_onc_clncl_outc_scor_fct_wo_covid if exists;

create table stg_onc_clncl_outc_scor_fct_wo_covid AS 
select  
distinct tmi.fcy_nm,
tmi.fcy_num,
tmi.rpt_dt,
max(tmi.mort_obs_rt) as mort_obs_rt, 
max(tmi.mort_expc_rt) as mort_expc_rt, 
max(tmi.mort_oe_rt) as mort_oe_rt, 
max(pcf.cmplc_obsr_rt) as cmplc_obsr_rt,
max(pcf.cmplc_expc_rt) as cmplc_expc_rt,
max(cast(pcf.cmplc_obsr_rt as decimal)/cast(pcf.cmplc_expc_rt as decimal)) as comp_oe_rt,
max(pcf.obsr_cases) as cmplc_obsr_cases,
max(pcf.outc_cases) as cmplc_outc_cases,
round(max(tri.readm_obs_rt),3) as readm_obs_rt, 
max(tri.readm_expc_rt) as readm_expc_rt, 
max(tri.readm_oe_rt) as readm_oe_rt
,max(tlu.lung_cancer_screen_agg_cases) as lng_cncr_scrn_csc_cnt
,max(thef.harm_events_cnt) as harm_events_cnt
,max(tzef.zero_events_numr) as zero_events_numr
,max(tzef.zero_events_dnmr) as zero_events_dnmr
,max(tzef.zero_events_rt)*100 as zero_events_rt
,max(tpep.hsptl_vsts_numr) as hsptl_vsts_numr
,max(tpep.adm_w30days_chemo_rt) as adm_w30days_chemo_rt
,max(tpep.op_chemo_dnmr) as op_chemo_dnmr
,max(pepm.resp_cnt) as resp_cnt 
,max(pepm.top_box_resp_cnt) as top_box_resp_cnt 
,max(pepm.sub_ptnt_exrnc_pct) as sub_ptnt_exrnc_pct
,max(pepm.ptnt_exrnc_pct) as ptnt_exrnc_pct
,max(tbrstcncr.awbi_dnmr) as awbi_dnmr
,max(tbrstcncr.awbi_numr) as awbi_numr
,max(tbrstcncr.awbi_radial_access_rt) as awbi_radial_access_rt
,max(temto.antmtc_csc_numr) as antmtc_csc_numr
,max(temto.emto_with_antmtc_csc_rt) as emto_with_antmtc_csc_rt
,max(temto.hghlyemeto_antmtc_csc_dnmr) as hghlyemeto_antmtc_csc_dnmr
,max(thcaphs.numrtr) as hcaphs_numrtr
,max(thcaphs.dnmntr) as hcpahs_dnmntr
,max(thcaphs.prgny_pct_scor) as hcaphs_prgny_pct_scor
,max(thcaphs.prgny_pct) AS hcaphs_prgny_pct
--Oncology Measure (30-Days Readmission Index Improvement)
,(case 
when tmi.fcy_num = '634342' and round(max(tri.readm_obs_rt),3) > 0.199 then 0
when tmi.fcy_num = '634342' and round(max(tri.readm_obs_rt),3)> 0.190 and round(max(tri.readm_obs_rt),3) <= 0.199 then 50
when tmi.fcy_num = '634342' and round(max(tri.readm_obs_rt),3)> 0.175 and round(max(tri.readm_obs_rt),3) <= 0.190 then 75
when tmi.fcy_num = '634342' and round(max(tri.readm_obs_rt),3)> 0.160 and round(max(tri.readm_obs_rt),3) <= 0.175 then 100
when tmi.fcy_num = '634342' and round(max(tri.readm_obs_rt),3)> 0.149 and round(max(tri.readm_obs_rt),3) <= 0.160 then 125
when tmi.fcy_num = '634342' and round(max(tri.readm_obs_rt),3)<=0.149 then 150
else 0
end) as readm_obs_pts,
--Oncology Measure (30-Days Readmission Index Achievement)
(case 
when tmi.fcy_num = '634342' and round(max(tri.readm_oe_rt),2) > 1.08 then 0
when tmi.fcy_num = '634342' and round(max(tri.readm_oe_rt),2)> 0.98 and round(max(tri.readm_oe_rt),2) <= 1.08 then 50
when tmi.fcy_num = '634342' and round(max(tri.readm_oe_rt),2)> 0.96 and round(max(tri.readm_oe_rt),2) <= 0.98 then 75
when tmi.fcy_num = '634342' and round(max(tri.readm_oe_rt),2)> 0.94 and round(max(tri.readm_oe_rt),2) <= 0.96 then 100
when tmi.fcy_num = '634342' and round(max(tri.readm_oe_rt),2)> 0.92 and round(max(tri.readm_oe_rt),2) <= 0.94 then 125
when tmi.fcy_num = '634342' and round(max(tri.readm_oe_rt),2)<=0.92 then 150
else 0
end) as readm_oe_pts,
--Oncology Measure (Mortality Index)
(case 
when tmi.fcy_num = '634342' and  round(max(tmi.mort_oe_rt),2)  > 1.09 then 0
when tmi.fcy_num = '634342' and  round(max(tmi.mort_oe_rt),2)  > 0.99 and round(max(tmi.mort_oe_rt),2)  <= 1.09 then 50
when tmi.fcy_num = '634342' and  round(max(tmi.mort_oe_rt),2)  > 0.97 and round(max(tmi.mort_oe_rt),2)  <= 0.99 then 75
when tmi.fcy_num = '634342' and  round(max(tmi.mort_oe_rt),2)  > 0.95 and  round(max(tmi.mort_oe_rt),2) <= 0.97 then 100
when tmi.fcy_num = '634342' and  round(max(tmi.mort_oe_rt),2)  > 0.93 and round(max(tmi.mort_oe_rt),2)  <= 0.95 then 125
when tmi.fcy_num = '634342' and  round(max(tmi.mort_oe_rt),2)  <=0.93 then 150
else 0
end) as mort_oe_pts,

--Oncology Measure (Complication Index)
(case 
when tmi.fcy_num = '634342' and    round(comp_oe_rt,2) > 1.51 then 0
when tmi.fcy_num = '634342' and    round(comp_oe_rt,2) > 1.44 and round(comp_oe_rt,2)  <= 1.51 then 50
when tmi.fcy_num = '634342' and    round(comp_oe_rt,2) > 1.41 and round(comp_oe_rt,2)  <= 1.44 then 75
when tmi.fcy_num = '634342' and    round(comp_oe_rt,2) > 1.35 and round(comp_oe_rt,2)  <= 1.41 then 100
when tmi.fcy_num = '634342' and    round(comp_oe_rt,2) > 1.3  and round(comp_oe_rt,2)  <= 1.35 then 125
when tmi.fcy_num = '634342' and    round(comp_oe_rt,2) <= 1.3 then 150
else 0
end) as comp_oe_rt_pts,
--Oncology Measure (Lung Cancer Screneeing Volume)
(
case 
--Bay
when tmi.fcy_num = 'MI2191' and max(tlu.lung_cancer_screen_agg_cases)> 440 and  max(tlu.lung_cancer_screen_agg_cases) <= 463 then 50
when tmi.fcy_num = 'MI2191' and max(tlu.lung_cancer_screen_agg_cases)> 463 and max(tlu.lung_cancer_screen_agg_cases) <= 468 then 75
when tmi.fcy_num = 'MI2191' and max(tlu.lung_cancer_screen_agg_cases)> 468 and max(tlu.lung_cancer_screen_agg_cases) <= 477 then 100
when tmi.fcy_num = 'MI2191' and max(tlu.lung_cancer_screen_agg_cases)> 477 and max(tlu.lung_cancer_screen_agg_cases) <= 486 then 125 
when tmi.fcy_num = 'MI2191' and max(tlu.lung_cancer_screen_agg_cases)>=486 then 150
--Central 
when tmi.fcy_num = 'MI2061' and max(tlu.lung_cancer_screen_agg_cases)> 230 and  max(tlu.lung_cancer_screen_agg_cases) <= 242 then 50
when tmi.fcy_num = 'MI2061' and max(tlu.lung_cancer_screen_agg_cases)> 242 and max(tlu.lung_cancer_screen_agg_cases) <= 244 then 75
when tmi.fcy_num = 'MI2061' and max(tlu.lung_cancer_screen_agg_cases)> 244 and max(tlu.lung_cancer_screen_agg_cases) <= 249 then 100
when tmi.fcy_num = 'MI2061' and max(tlu.lung_cancer_screen_agg_cases)> 249 and max(tlu.lung_cancer_screen_agg_cases) <= 254 then 125 
when tmi.fcy_num = 'MI2061' and max(tlu.lung_cancer_screen_agg_cases)>=254 then 150
--Flint
when tmi.fcy_num = 'MI2302' and max(tlu.lung_cancer_screen_agg_cases)> 1021 and  max(tlu.lung_cancer_screen_agg_cases) <= 1075 then 50
when tmi.fcy_num = 'MI2302' and max(tlu.lung_cancer_screen_agg_cases)> 1075 and max(tlu.lung_cancer_screen_agg_cases) <= 1086 then 75
when tmi.fcy_num = 'MI2302' and max(tlu.lung_cancer_screen_agg_cases)> 1086 and max(tlu.lung_cancer_screen_agg_cases) <= 1107 then 100
when tmi.fcy_num = 'MI2302' and max(tlu.lung_cancer_screen_agg_cases)> 1107 and max(tlu.lung_cancer_screen_agg_cases) <= 1129 then 125 
when tmi.fcy_num = 'MI2302' and max(tlu.lung_cancer_screen_agg_cases)>=1129 then 150
--Lansing
when tmi.fcy_num = 'MI5020' and max(tlu.lung_cancer_screen_agg_cases)> 441 and  max(tlu.lung_cancer_screen_agg_cases) <= 464 then 50
when tmi.fcy_num = 'MI5020' and max(tlu.lung_cancer_screen_agg_cases)> 464 and max(tlu.lung_cancer_screen_agg_cases) <= 469 then 75
when tmi.fcy_num = 'MI5020' and max(tlu.lung_cancer_screen_agg_cases)> 469 and max(tlu.lung_cancer_screen_agg_cases) <= 478 then 100
when tmi.fcy_num = 'MI5020' and max(tlu.lung_cancer_screen_agg_cases)> 478 and max(tlu.lung_cancer_screen_agg_cases) <= 487 then 125 
when tmi.fcy_num = 'MI5020' and max(tlu.lung_cancer_screen_agg_cases)>=487 then 150
--Lapeer
when tmi.fcy_num = 'MI2001' and max(tlu.lung_cancer_screen_agg_cases)> 366 and  max(tlu.lung_cancer_screen_agg_cases) <= 385 then 50
when tmi.fcy_num = 'MI2001' and max(tlu.lung_cancer_screen_agg_cases)> 385 and max(tlu.lung_cancer_screen_agg_cases) <= 389 then 75
when tmi.fcy_num = 'MI2001' and max(tlu.lung_cancer_screen_agg_cases)> 389 and max(tlu.lung_cancer_screen_agg_cases) <= 397 then 100
when tmi.fcy_num = 'MI2001' and max(tlu.lung_cancer_screen_agg_cases)> 397 and max(tlu.lung_cancer_screen_agg_cases) <= 404 then 125 
when tmi.fcy_num = 'MI2001' and max(tlu.lung_cancer_screen_agg_cases)>=404 then 150
--Macomb
when tmi.fcy_num = 'MI2048' and max(tlu.lung_cancer_screen_agg_cases)> 637 and  max(tlu.lung_cancer_screen_agg_cases) <= 671 then 50
when tmi.fcy_num = 'MI2048' and max(tlu.lung_cancer_screen_agg_cases)> 671 and max(tlu.lung_cancer_screen_agg_cases)  <= 678 then 75
when tmi.fcy_num = 'MI2048' and max(tlu.lung_cancer_screen_agg_cases)> 678 and max(tlu.lung_cancer_screen_agg_cases)  <= 691 then 100
when tmi.fcy_num = 'MI2048' and max(tlu.lung_cancer_screen_agg_cases)> 691 and max(tlu.lung_cancer_screen_agg_cases)  <= 705 then 125 
when tmi.fcy_num = 'MI2048' and max(tlu.lung_cancer_screen_agg_cases)>=705 then 150
--Northern
when tmi.fcy_num = '637619' and max(tlu.lung_cancer_screen_agg_cases)> 433 and  max(tlu.lung_cancer_screen_agg_cases) <= 456 then 50
when tmi.fcy_num = '637619' and max(tlu.lung_cancer_screen_agg_cases)> 456 and max(tlu.lung_cancer_screen_agg_cases) <= 461 then 75
when tmi.fcy_num = '637619' and max(tlu.lung_cancer_screen_agg_cases)> 461 and max(tlu.lung_cancer_screen_agg_cases) <= 470 then 100
when tmi.fcy_num = '637619' and max(tlu.lung_cancer_screen_agg_cases)> 470 and max(tlu.lung_cancer_screen_agg_cases) <= 479 then 125 
when tmi.fcy_num = '637619' and max(tlu.lung_cancer_screen_agg_cases)>=479 then 150
--Oakland
when tmi.fcy_num = 'MI2055' and max(tlu.lung_cancer_screen_agg_cases)> 414 and  max(tlu.lung_cancer_screen_agg_cases) <= 436 then 50
when tmi.fcy_num = 'MI2055' and max(tlu.lung_cancer_screen_agg_cases)> 436 and max(tlu.lung_cancer_screen_agg_cases) <= 440 then 75
when tmi.fcy_num = 'MI2055' and max(tlu.lung_cancer_screen_agg_cases)> 440 and max(tlu.lung_cancer_screen_agg_cases) <= 449 then 100
when tmi.fcy_num = 'MI2055' and max(tlu.lung_cancer_screen_agg_cases)> 449 and max(tlu.lung_cancer_screen_agg_cases) <= 458 then 125 
when tmi.fcy_num = 'MI2055' and max(tlu.lung_cancer_screen_agg_cases)>=458 then 150

--Port Huron
when tmi.fcy_num = '600816' and max(tlu.lung_cancer_screen_agg_cases)> 248 and  max(tlu.lung_cancer_screen_agg_cases) <= 261 then 50
when tmi.fcy_num = '600816' and max(tlu.lung_cancer_screen_agg_cases)> 261 and max(tlu.lung_cancer_screen_agg_cases) <= 264 then 75
when tmi.fcy_num = '600816' and max(tlu.lung_cancer_screen_agg_cases)> 264 and max(tlu.lung_cancer_screen_agg_cases) <= 269 then 100
when tmi.fcy_num = '600816' and max(tlu.lung_cancer_screen_agg_cases)> 269 and max(tlu.lung_cancer_screen_agg_cases) <= 274 then 125 
when tmi.fcy_num = '600816' and max(tlu.lung_cancer_screen_agg_cases)>=274 then 150

--Karmanos
when tmi.fcy_num = '634342' and max(tlu.lung_cancer_screen_agg_cases)> 74 and  max(tlu.lung_cancer_screen_agg_cases) <= 78 then 50
when tmi.fcy_num = '634342' and max(tlu.lung_cancer_screen_agg_cases)> 78 and max(tlu.lung_cancer_screen_agg_cases) <= 79 then 75
when tmi.fcy_num = '634342' and max(tlu.lung_cancer_screen_agg_cases)> 79 and max(tlu.lung_cancer_screen_agg_cases) <= 80 then 100
when tmi.fcy_num = '634342' and max(tlu.lung_cancer_screen_agg_cases)> 80 and max(tlu.lung_cancer_screen_agg_cases) <= 82 then 125 
when tmi.fcy_num = '634342' and max(tlu.lung_cancer_screen_agg_cases)>=82 then 150

else 0
end) as lng_scrn_scor_pts,
--Oncology Measure(O/P Oncology Likelihood to recommend)
(case 
when tmi.fcy_num = '634342' and max(pepm.ptnt_exrnc_pct) <  78.6 then 0
when tmi.fcy_num = '634342' and max(pepm.ptnt_exrnc_pct) < 82.7 and max(pepm.ptnt_exrnc_pct) >= 78.6 then 50
when tmi.fcy_num = '634342' and max(pepm.ptnt_exrnc_pct) < 84.3 and max(pepm.ptnt_exrnc_pct) >= 82.7 then 75
when tmi.fcy_num = '634342' and max(pepm.ptnt_exrnc_pct) < 85.8 and max(pepm.ptnt_exrnc_pct) >= 84.3 then 100
when tmi.fcy_num = '634342' and max(pepm.ptnt_exrnc_pct) < 87.3 and max(pepm.ptnt_exrnc_pct) >= 85.8 then 125
when tmi.fcy_num = '634342' and max(pepm.ptnt_exrnc_pct) >= 87.3 then 150
else 0
end) as onc_ptnt_exrnc_scor_pts,
--Oncology Measure(Admission within 30-days of Outpuatient Chemo )
(
case 
--Bay
when tmi.fcy_num = 'MI2191' and round(max(tpep.adm_w30days_chemo_rt),4) >  1.40 then 0
when tmi.fcy_num = 'MI2191' and round(max(tpep.adm_w30days_chemo_rt),4) >  1.33 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.40 then 50
when tmi.fcy_num = 'MI2191' and round(max(tpep.adm_w30days_chemo_rt),4) >  1.30 and round(max(tpep.adm_w30days_chemo_rt),4)  <= 1.33 then 75
when tmi.fcy_num = 'MI2191' and round(max(tpep.adm_w30days_chemo_rt),4) >  1.28 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.30 then 100
when tmi.fcy_num = 'MI2191' and round(max(tpep.adm_w30days_chemo_rt),4) >  1.25 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.28 then 125
when tmi.fcy_num = 'MI2191' and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.25  then 150
--Central
when tmi.fcy_num = 'MI2061' and round(max(tpep.adm_w30days_chemo_rt),4) >  0.17 then 0
when tmi.fcy_num = 'MI2061' and round(max(tpep.adm_w30days_chemo_rt),4) >  0.16 and round(max(tpep.adm_w30days_chemo_rt),4)  <= 0.17 then 50
when tmi.fcy_num = 'MI2061' and round(max(tpep.adm_w30days_chemo_rt),4) >  0.15 and round(max(tpep.adm_w30days_chemo_rt),4)  <= 0.16 then 75
when tmi.fcy_num = 'MI2061' and round(max(tpep.adm_w30days_chemo_rt),4) >  0.14 and round(max(tpep.adm_w30days_chemo_rt),4)  <= 0.15 then 100
when tmi.fcy_num = 'MI2061' and round(max(tpep.adm_w30days_chemo_rt),4) >  0.13 and round(max(tpep.adm_w30days_chemo_rt),4)  <= 0.14 then 125
when tmi.fcy_num = 'MI2061' and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.13  then 150
--Flint
--Lansing
when tmi.fcy_num = 'MI5020' and round(max(tpep.adm_w30days_chemo_rt),4) >  0.92 then 0
when tmi.fcy_num = 'MI5020' and round(max(tpep.adm_w30days_chemo_rt),4) >  0.88 and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.92 then 50
when tmi.fcy_num = 'MI5020' and round(max(tpep.adm_w30days_chemo_rt),4) >  0.86 and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.88 then 75
when tmi.fcy_num = 'MI5020' and round(max(tpep.adm_w30days_chemo_rt),4) >  0.84 and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.86 then 100
when tmi.fcy_num = 'MI5020' and round(max(tpep.adm_w30days_chemo_rt),4) >  0.83 and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.84 then 125
when tmi.fcy_num = 'MI5020' and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.83  then 150
--Lapeer
when tmi.fcy_num = 'MI2001'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.59 then 0
when tmi.fcy_num = 'MI2001'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.51 and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.0159 then 50
when tmi.fcy_num = 'MI2001'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.48 and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.0151 then 75
when tmi.fcy_num = 'MI2001'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.45 and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.0148 then 100
when tmi.fcy_num = 'MI2001'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.42 and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.0145 then 125
when tmi.fcy_num = 'MI2001'  and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.42 then 150
--Macomb
when tmi.fcy_num = 'MI2048'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.13 then 0
when tmi.fcy_num = 'MI2048'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.08 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.13 then 50
when tmi.fcy_num = 'MI2048'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.06 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.08 then 75
when tmi.fcy_num = 'MI2048'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.04 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.06 then 100
when tmi.fcy_num = 'MI2048'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.02 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.04 then 125
when tmi.fcy_num = 'MI2048'  and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.02 then 150
--Northern
when tmi.fcy_num = '637619'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.11 then 0
when tmi.fcy_num = '637619'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.06 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.11 then 50
when tmi.fcy_num = '637619'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.04 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.06 then 75
when tmi.fcy_num = '637619'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.02 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.04 then 100
when tmi.fcy_num = '637619'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.00   and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.02 then 125
when tmi.fcy_num = '637619'  and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.00 then 150
--Oakland
when tmi.fcy_num = 'MI2055'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.11 then 0
when tmi.fcy_num = 'MI2055'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.06 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.11 then 50
when tmi.fcy_num = 'MI2055'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.04 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.06 then 75
when tmi.fcy_num = 'MI2055'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.02 and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.04 then 100
when tmi.fcy_num = 'MI2055'  and round(max(tpep.adm_w30days_chemo_rt),4) >  1.00   and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.02 then 125
when tmi.fcy_num = 'MI2055'  and round(max(tpep.adm_w30days_chemo_rt),4) <= 1.00 then 150
--Port Huron
--Karmanos
when tmi.fcy_num = '634342'  and round(max(tpep.adm_w30days_chemo_rt),4) >  0.44 then 0
when tmi.fcy_num = '634342'  and round(max(tpep.adm_w30days_chemo_rt),4) >  0.42 and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.44 then 50
when tmi.fcy_num = '634342'  and round(max(tpep.adm_w30days_chemo_rt),4) >  0.41 and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.42 then 75
when tmi.fcy_num = '634342'  and round(max(tpep.adm_w30days_chemo_rt),4) >  0.4  and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.41 then 100
when tmi.fcy_num = '634342'  and round(max(tpep.adm_w30days_chemo_rt),4) >  0.39 and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.4 then 125
when tmi.fcy_num = '634342'  and round(max(tpep.adm_w30days_chemo_rt),4) <= 0.39 then 150
else 0
end) as admn_w30_chemo_scor_pts
,
--Oncology Measures (Conventional Breast Cancer Irradition (NOT AWBI))
(
CASE
when round(max(tbrstcncr.awbi_radial_access_rt),3) > 35.000 then 0 
when round(max(tbrstcncr.awbi_radial_access_rt),3) > 30.000 AND round(max(tbrstcncr.awbi_radial_access_rt),3) <= 35.000 then 50
when round(max(tbrstcncr.awbi_radial_access_rt),3) > 25.000 AND round(max(tbrstcncr.awbi_radial_access_rt),3) <= 30.000 then 75
when round(max(tbrstcncr.awbi_radial_access_rt),3) > 15.000 AND round(max(tbrstcncr.awbi_radial_access_rt),3) <= 25.000 then 100
when round(max(tbrstcncr.awbi_radial_access_rt),3) > 05.000 AND round(max(tbrstcncr.awbi_radial_access_rt),3) <= 15.000 then 125
when round(max(tbrstcncr.awbi_radial_access_rt),3) <= 05.000 then 150
else 0
END) as cnvtnl_brst_cncr_scor_pts,
--Oncology Measure (HCAPHS )
(
CASE
when tmi.fcy_num = '634342'  and round(max(thcaphs.prgny_pct_scor),3) >  71.600 AND round(max(thcaphs.prgny_pct_scor),3) <= 78.600 then 50
when tmi.fcy_num = '634342'  and round(max(thcaphs.prgny_pct_scor),3) >  78.600 AND round(max(thcaphs.prgny_pct_scor),3) <= 80.000 then 75
when tmi.fcy_num = '634342'  and round(max(thcaphs.prgny_pct_scor),3) >  80.000 AND round(max(thcaphs.prgny_pct_scor),3) <= 82.000 then 100
when tmi.fcy_num = '634342'  and round(max(thcaphs.prgny_pct_scor),3) >  82.000 AND round(max(thcaphs.prgny_pct_scor),3) <= 84.300 then 125
when tmi.fcy_num = '634342'  and round(max(thcaphs.prgny_pct_scor),3) >= 84.300 then 150
else 0 
END) as hcaphs_scor_pts,
--Oncology Measure Highly Emetogenic Chemo with Targeted Antiemetic Treatment
(
CASE
when round(max(temto.emto_with_antmtc_csc_rt),3) >  30.000 AND round(max(temto.emto_with_antmtc_csc_rt),3) <= 35.000 then 50
when round(max(temto.emto_with_antmtc_csc_rt),3) >  35.000 AND round(max(temto.emto_with_antmtc_csc_rt),3) <= 40.000 then 75
when round(max(temto.emto_with_antmtc_csc_rt),3) >  40.000 AND round(max(temto.emto_with_antmtc_csc_rt),3) <= 45.000 then 100
when round(max(temto.emto_with_antmtc_csc_rt),3) >  45.000 AND round(max(temto.emto_with_antmtc_csc_rt),3) <= 55.000 then 125
when round(max(temto.emto_with_antmtc_csc_rt),3) >= 55.000 then 150
else 0 
END) as hglyemto_antmtc_scor_pts,
--Oncology Measures (Harm Events Improvement)
case 
when tmi.rpt_dt='2019-08-01 00:00:00' then 1
when tmi.rpt_dt='2019-09-01 00:00:00' then 2
when tmi.rpt_dt='2019-10-01 00:00:00' then 3
when tmi.rpt_dt='2019-11-01 00:00:00' then 4
when tmi.rpt_dt='2019-12-01 00:00:00' then 5
when tmi.rpt_dt='2020-01-01 00:00:00' then 6
when tmi.rpt_dt='2020-02-01 00:00:00' then 7
when tmi.rpt_dt='2020-03-01 00:00:00' then 8
when tmi.rpt_dt='2020-04-01 00:00:00' then 9
when tmi.rpt_dt='2020-05-01 00:00:00' then 10
when tmi.rpt_dt='2020-06-01 00:00:00' then 11
when tmi.rpt_dt ='2019-07-01 00:00:00' then 12
else null
end as no_of_mnths,

(case 
when tmi.fcy_num = '634342' and max(thef.harm_events_cnt) > 60 then 0
when tmi.fcy_num = '634342' and max(thef.harm_events_cnt) > 57 and max(thef.harm_events_cnt) <= 60 then 50
when tmi.fcy_num = '634342' and max(thef.harm_events_cnt) > 56 and max(thef.harm_events_cnt) <= 57 then 75
when tmi.fcy_num = '634342' and max(thef.harm_events_cnt) > 54 and max(thef.harm_events_cnt) <= 56 then 100
when tmi.fcy_num = '634342' and max(thef.harm_events_cnt) > 51 and max(thef.harm_events_cnt) <= 54 then 125
when tmi.fcy_num = '634342' and max(thef.harm_events_cnt) <=51 then 150

else 0
end) as harm_events_pts
,
--Oncology Measures (ZERO Harm Events Improvement)
(case 
when tmi.fcy_num = '634342' and max(ROUND(tzef.zero_events_rt,2)) < 0.8 then 0
when tmi.fcy_num = '634342' and max(ROUND(tzef.zero_events_rt,2)) >= 0.80 and max(ROUND(tzef.zero_events_rt,2)) < 0.87 then 50
when tmi.fcy_num = '634342' and max(ROUND(tzef.zero_events_rt,2)) >= 0.87 and max(ROUND(tzef.zero_events_rt,2)) < 0.88 then 75
when tmi.fcy_num = '634342' and max(ROUND(tzef.zero_events_rt,2)) >= 0.88 and max(ROUND(tzef.zero_events_rt,2)) < 0.9 then 100
when tmi.fcy_num = '634342' and max(ROUND(tzef.zero_events_rt,2)) >= 0.9 and  max(ROUND(tzef.zero_events_rt,2)) <= 0.92 then 125
when tmi.fcy_num = '634342' and max(ROUND(tzef.zero_events_rt,2)) >=0.92 then 150
else 0
end
) as zero_events_rt_pts,
case when harm_events_pts > zero_events_rt_pts then harm_events_pts else zero_events_rt_pts end as harm_zero_events_pts,
case when readm_obs_pts > readm_oe_pts then readm_obs_pts else readm_oe_pts end as readm_obs_oe_pts,
case when tmi.fcy_num= '634342' then
((nvl(mort_oe_pts,0)*0.1 )+
(nvl(comp_oe_rt_pts,0)*0.15)+
(nvl(harm_zero_events_pts,0)*0.15)+
(nvl(readm_obs_oe_pts,0)*0.15)+ 
(nvl(hcaphs_scor_pts,0)*0.05) + 
(nvl(onc_ptnt_exrnc_scor_pts,0)* 0.1 )+
(nvl(admn_w30_chemo_scor_pts,0)* 0.1 )+
(nvl(cnvtnl_brst_cncr_scor_pts,0)* 0.05) +
(nvl(lng_scrn_scor_pts,0)* 0.1) +
(nvl(hglyemto_antmtc_scor_pts,0)* 0.05 )) 
ELSE
((nvl(mort_oe_pts,0)*0.1 )+
(nvl(comp_oe_rt_pts,0)*0.15)+
(nvl(harm_zero_events_pts,0)*0.15)+
(nvl(readm_obs_oe_pts,0)*0.15)+ 
(nvl(hcaphs_scor_pts,0)*0.05) + 
(nvl(onc_ptnt_exrnc_scor_pts,0)* 0 )+
(nvl(admn_w30_chemo_scor_pts,0)* 0.35 )+
(nvl(cnvtnl_brst_cncr_scor_pts,0)* 0.15) +
(nvl(lng_scrn_scor_pts,0)* 0.35) +
(nvl(hglyemto_antmtc_scor_pts,0)* 0.15 )) 
END 
as weighted_pts,
----Updated on 12/15/2020 FY 21 Dashboard
case when tmi.fcy_num = 'MI2191' then 1
when tmi.fcy_num = 'MI2061' then 1
when tmi.fcy_num = 'MI2302' then 0.5
when tmi.fcy_num = 'MI5020' then 1
when tmi.fcy_num = 'MI2001' then 1
when tmi.fcy_num = 'MI2048' then 1
when tmi.fcy_num = '637619' then 1
when tmi.fcy_num = 'MI2055' then 1
when tmi.fcy_num = '600816' then 0.5
when tmi.fcy_num = '634342' then 1
else 1
end as weight,
weighted_pts/weight as clncl_outc_scor
FROM 
 stg_tmp_mrtly_ind_wo_covid tmi
--Complication Index
left join stg_pqsd_cmplc_idnx_fct pcf on tmi.rpt_dt = date_trunc('month',pcf.end_of_month) and pcf.end_of_month <> pcf.first_of_month and tmi.fcy_num = pcf.fcy_num and pcf.msr_nm = 'Compl_R12M'
--30 Day Readmission Rate(Improvement)
--30 Day Readmission Index(Achievement)
left join stg_tmp_readm_ind_wo_covid tri on tmi.rpt_dt = tri.rpt_dt and tmi.fcy_num=tri.fcy_num
--Harm Events (Improvement)
left join stg_TMP_HARM_EVENTS_FCT thef on tmi.rpt_dt = thef.rpt_dt and tmi.fcy_num = thef.fcy_num
--Zero Harm (Achivement)
left join stg_tmp_zero_events_fct tzef on tmi.rpt_dt = tzef.rpt_dt and tmi.fcy_num = tzef.fcy_num
--HCAPHS Likelihood to recommend
left join stg_hcaphs_ptnt_exrnc_pct_msr_fct thcaphs on tmi.rpt_dt = date_trunc('month',thcaphs.rpt_dt) and tmi.fcy_num = thcaphs.fcy_num
--Added 03/01/2021: FY21 Oncology Measure (O/P Oncology Likelihood to recommend)
left join stg_onc_ptnt_exrnc_pct_msr_fct_new pepm on tmi.rpt_dt = pepm.rprt_dt and tmi.fcy_num = pepm.fcy_num
--Added 03/01/2021: FY21 Oncology Measure (Admission within 30-days of Outpuatient Chemo )
left join stg_tmp_admssn_w30_dayschemo_r12m_fct tpep on tmi.rpt_dt = tpep.rpt_dt and tmi.fcy_num = tpep.fcy_num
--Added 03/01/2021: FY21 Oncology Measure (Conventional Breast Cancer Irradition (NOT AWBI))
left join stg_tmp_onc_awbi_r12m_fct tbrstcncr on tmi.rpt_dt = tbrstcncr.rpt_dt and tmi.fcy_num = tbrstcncr.fcy_num
--Added 03/01/2021: FY21 Oncology Measure (Lung Cancer Screening Volume)
left join stg_tmp_lung_cancer_screening_r12m_fct tlu on tmi.rpt_dt = tlu.rpt_dt and tmi.fcy_num = tlu.fcy_num
--Added 03/01/2021: FY21 Oncology Measure (Highly Emetogenic Chemo with Targeted Antiemetic Treatment)
left join stg_tmp_hghlyemtgnc_wthantmtc_r12m_fct temto on tmi.rpt_dt = temto.rpt_dt and tmi.fcy_num = temto.fcy_num
--WHERE tmi.rpt_dt = '2020-11-01 00:00:00'
GROUP BY 1,2,3;
\unset ON_ERROR_STOP
