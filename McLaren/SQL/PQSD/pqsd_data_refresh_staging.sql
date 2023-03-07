\set ON_ERROR_STOP ON;
/*

***************Quality Advisor Data***************

*/
 

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
,case when (p.company_id in ('Oakland','Port Huron') and p.dischargeservice in ('BEH','GERI','REHAB','PSYCH'))then 0 else 1 end as dschrg_svc_excl

from 
pce_qe16_oper_prd_zoom..cv_patdisch p

);

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
  where p.inpatient_outpatient_flag='I '
  );
  
  
  


------------------------------------------------------QA Attributes------------------------------------------------------------------------------------------------------------------------------------------
drop table tmp_qadv if exists;
DROP TABLE tmp_qadv IF EXISTS;
;create temp table  tmp_qadv as
(select ef.encntr_num
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
,dr.icd_diag_descr as prim_diag_descr
,ef.icd10_proc_code
,procd.icd_proc_descr
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
,max(pf.postop_derangemnts_obs_num) AS PSI10_pat_msr_obs_num_a
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
left join pce_qe16_prd_qadv..ptnt_saft_ind_obs_exp pf ON ef.encntr_num = pf.encntr_num and ef.fcy_num = pf.fcy_num and  pf.psi_iqi_version = '2018' and pf.method_type = 'STD'
left outer join pce_qe16_prd_qadv..ptnt_icd_proc_cd_asgnt i on ef.encntr_num = i.encntr_num and ef.fcy_num = i.fcy_num and i.icd_cd = '0W8NXZZ'
left join pce_qe16_prd_qadv..ptnt_icd_diag_cd_asgnt  ic on ef.encntr_num = ic.encntr_num and ef.fcy_num = ic.fcy_num
left join pce_qe16_prd_qadv..val_set_dim ccs on (replace(ic.icd_cd,'.','')) = ccs.cd and ic.icd_diag_poa_cd in ('N','U')  and ic.icd_cl_cd ='S'
left outer join pce_qe16_prd_qadv..stnd_adm_src_ref as ad on ef.adm_src_cd = ad.adm_src_cd
left outer join pce_qe16_prd_qadv..stnd_adm_type_ref as at on ef.adm_type_cd = at.adm_type_cd
left outer join pce_qe16_prd_qadv..dschrg_sts_ref as ds on ef.dschrg_sts_cd = ds.dschrg_sts_cd
left outer join pce_qe16_prd_qadv..ms_drg_ref as ms on ef.ms_drg_icd10 = ms.ms_drg_cd and ef.ms_drg_mdc_icd10 = ms.ms_drg_mdc_cd
left outer join pce_qe16_prd_qadv..pract_ref as pr on ef.fcy_attnd_pract_cd = pr.fcy_pract_cd and ef.fcy_num = pr.fcy_num
left outer join pce_qe16_prd_qadv..icd_diag_cd_ref dr on ef.icd10_diag_code = dr.icd_diag_cd
left outer join pce_qe16_prd_qadv..icd_proc_cd_ref procd on ef.icd10_proc_code = procd.icd_proc_cd
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
drop table pce_qe16_prd..stg_encntr_qs_anl_fct_vw if exists;
create table pce_qe16_prd..stg_encntr_qs_anl_fct_vw as 
(SELECT 
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
	, case when (tqa.dschrg_dt >= '10/01/2018' and pds.updateid = 'Incarcerated' and pds.company_id ='Lansing') then 0 else 1 end as crnr_lnsg_prsnr_excl_ind
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

	
	
	WHERE  
	
	 year(pds.zm_dschrg_dt) >= 2017

     group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61
);

/*

***************Complication Index***************

*/
drop table pce_qe16_prd..stg_pqsd_cmplc_idnx_fct if exists;
create table pce_qe16_prd..stg_pqsd_cmplc_idnx_fct as 

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
column__complications_of_care___result_ as msr1,
column__complications_of_care___comparison_ as msr2,
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
column__complications_of_care___result_ as msr1,
column__complications_of_care___comparison_ as msr2,
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
cast(cmplc_of_care_rslt as varchar(10)) as msr1,
cast(cmplc_of_care_cmpr as varchar(10)) as msr2,
cmplc_of_care_occr as obsv_cases, 
cmplc_of_care_case_cnt as outcome_cases, 
'Compl_R12M' as msr_nm
  FROM pce_qe16_misc_prd_lnd..cmplc_roll_12mo_smy_fct
  union
  
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
cast(prd as date) AS  first_of_month, 
cast(prd as date) AS end_of_month,
cast(prd as varchar(20)) as period ,
cast(cmplc_of_care_rslt as varchar(10)) as msr1,
cast(cmplc_of_care_cmpr as varchar(10)) as msr2,
cmplc_of_care_occr as obsv_cases, 
cmplc_of_care_case_cnt as outcome_cases, 
'Compl_R12M' as msr_nm
  FROM pce_qe16_misc_prd_lnd..cmplc_1mo_smy_fct
) as scmp;
/*

***************Harm Events***************

*/
DROP TABLE tmp_cdr_dim IF EXISTS;
;create temp table  tmp_cdr_dim as
(select distinct cd.mo_and_yr_abbr,
min(cd.cdr_dt) as cdr_dt,
1 as join_key
from pce_qe16_prd..cdr_dim cd
where cd.cdr_dt between add_months((select max(eq.dschrg_dt)from pce_qe16_prd..encntr_fct eq ),-36) and (select max(eq.dschrg_dt)from pce_qe16_prd..encntr_fct eq )
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
cd.mo_and_yr_abbr ,
date_trunc('month',cd.cdr_dt) as cdr_dt,
SUM(CASE when nf.c_diff  is not null then nf.c_diff else 0 end) as cdiff,
SUM(CASE when nf.cauti_events  is not null then nf.cauti_events  else 0 end) as cauti_events, 
SUM(CASE when nf.clabsi_events is not null then nf.clabsi_events else 0 end) as clabsi_events, 
SUM(CASE when nf.mrsa is not null then nf.mrsa else 0 end) as mrsa,
SUM(CASE when nf.ssi_colo is not null then nf.ssi_colo else 0 end) as ssi_colo, 
SUM(CASE when nf.ssi_hyst is not null then nf.ssi_hyst else 0 end) as ssi_hyst

FROM pce_qe16_misc_prd_lnd.prmradmp.nhsn_msr_fct nf 
inner JOIN pce_qe16_prd..cdr_dim cd ON nf.event_dt = cd.cdr_dt
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

DROP TABLE tmp_hac_dummy IF EXISTS;
;create temp table  tmp_hac_dummy as 
(select distinct
fd.fcy_nm,
fd.fcy_num,
0 as cms5_hac_adm_ind,
0 as cms6_hac_adm_ind,
0 as cms7_hac_adm_ind,
1 as join_key
from pce_Qe16_prd..fcy_prfl_dim fd);


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
cd.mo_and_yr_abbr,
date_trunc('month',cd.cdr_dt) as event_dt,
sum(eqv.cms5_hac_adm_ind) as cms5_hac_adm_ind,
sum(eqv.cms6_hac_adm_ind) as cms6_hac_adm_ind,
sum(eqv.cms7_hac_adm_ind) as cms7_hac_adm_ind

from encntr_qs_anl_fct_vw eqv
inner join pce_Qe16_prd..cdr_dim cd on eqv.dschrg_dt = cd.cdr_dt
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
cd.mo_and_yr_abbr,
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




from pce_qe16_prd..stg_encntr_qs_anl_fct_vw pf
inner join pce_qe16_prd..cdr_dim cd on pf.dschrg_dt = cd.cdr_dt
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
drop table pce_qe16_prd..stg_harm_events_fct if exists;
create table pce_qe16_prd..stg_harm_events_fct as 
(select  thf.fcy_nm,
thf.fcy_num,
thf.cdr_dt as event_dt,
1 as join_key,
sum(thf.event_cnt) as events_cnt

from temp_harm_events_fct_a thf
group by 1,2,3);

DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt, 1 as join_key from pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef);

drop table stg_TMP_HARM_EVENTS_FCT if exists;
create table pce_qe16_prd..stg_TMP_HARM_EVENTS_FCT AS 
(SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where hf.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and last_day(add_months((select max_dt from tmp_max_dschrg_dt),-2))
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
and hf.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(add_months(date('06/01/2019'),1)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),1)
and hf.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(add_months(date('06/01/2019'),2)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),2)
and hf.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(add_months(date('06/01/2019'),3)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),3)
and hf.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(add_months(date('06/01/2019'),4)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),4)
and hf.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(add_months(date('06/01/2019'),5)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),5)
and hf.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(add_months(date('06/01/2019'),6)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),6)
and hf.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(add_months(date('06/01/2019'),7)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),7)
and hf.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(add_months(date('06/01/2019'),8)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),8)
and hf.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(add_months(date('06/01/2019'),9)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),9)
and hf.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(add_months(date('06/01/2019'),10)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),10)
and hf.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct hf.fcy_nm,
hf.fcy_num,
max(add_months(date('06/01/2019'),11)) as rpt_dt,
sum(events_cnt) as harm_events_cnt
FROM pce_qe16_prd..stg_harm_events_fct hf
left join tmp_max_dschrg_dt tmd on hf.join_key=tmd.join_key
where  HF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),11)
and hf.fcy_nm ='McLaren Greater Lansing'
group by 1,2);

/*

***************Zero Events***************

*/
DROP TABLE tmp_cdr_dim IF EXISTS;
;create temp table  tmp_cdr_dim as
(select distinct cd.mo_and_yr_abbr,
min(cd.cdr_dt) as cdr_dt,
1 as join_key
from pce_qe16_prd..cdr_dim cd
where cd.cdr_dt between add_months((select max(eq.dschrg_dt)from pce_qe16_prd..encntr_fct eq ),-36) and (select max(eq.dschrg_dt)from pce_qe16_prd..encntr_fct eq )
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
cd.mo_and_yr_abbr ,
date_trunc('month',cd.cdr_dt) as cdr_dt,
SUM(CASE when nf.c_diff  is not null then nf.c_diff else 0 end) as cdiff,
SUM(CASE when nf.cauti_events  is not null then nf.cauti_events  else 0 end) as cauti_events, 
SUM(CASE when nf.clabsi_events is not null then nf.clabsi_events else 0 end) as clabsi_events, 
SUM(CASE when nf.mrsa is not null then nf.mrsa else 0 end) as mrsa,
SUM(CASE when nf.ssi_colo is not null then nf.ssi_colo else 0 end) as ssi_colo, 
SUM(CASE when nf.ssi_hyst is not null then nf.ssi_hyst else 0 end) as ssi_hyst

FROM pce_qe16_misc_prd_lnd.prmradmp.nhsn_msr_fct nf 
inner JOIN pce_qe16_prd..cdr_dim cd ON nf.event_dt = cd.cdr_dt
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

drop table pce_qe16_prd..stg_nhsn_zero_event_fct if exists;
create table pce_qe16_prd..stg_nhsn_zero_event_fct as 
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
cd.mo_and_yr_abbr,
date_trunc('month',cd.cdr_dt) as event_dt,
sum(eqv.cms5_hac_adm_ind) as cms5_hac_adm_ind,
sum(eqv.cms6_hac_adm_ind) as cms6_hac_adm_ind,
sum(eqv.cms7_hac_adm_ind) as cms7_hac_adm_ind

from encntr_qs_anl_fct_vw eqv
inner join pce_qe16_prd..cdr_dim cd on eqv.dschrg_dt = cd.cdr_dt
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

drop table pce_qe16_prd..stg_hac_zero_event_fct if exists;
create table pce_qe16_prd..stg_hac_zero_event_fct as 
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
cd.mo_and_yr_abbr,
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




from pce_qe16_prd..stg_encntr_qs_anl_fct_vw pf
inner join pce_qe16_prd..cdr_dim cd on pf.dschrg_dt = cd.cdr_dt
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

drop table pce_qe16_prd..stg_psi_zero_event_fct if exists;
create table pce_qe16_prd..stg_psi_zero_event_fct as 
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

drop table pce_qe16_prd..stg_zero_event_fct if exists;
create table pce_qe16_prd..stg_zero_event_fct as

(SELECT fcy_nm, fcy_num, mo_and_yr_abbr, event_dt, psi03_pat_msr_obs_rate+psi06_pat_msr_obs_rate+psi08_pat_msr_obs_rate+psi09_pat_msr_obs_rate+psi10_pat_msr_obs_rate+psi11_pat_msr_obs_rate+
psi12_pat_msr_obs_rate+psi13_pat_msr_obs_rate+psi14_pat_msr_obs_rate+psi15_pat_msr_obs_rate as msr_val,
'PSI Zero Events' as msr_nm
FROM pce_qe16_prd..stg_psi_zero_event_fct

union 

SELECT distinct fcy_nm, 
fcy_num, 
mo_and_yr_abbr, 
event_dt, 
cdiff_zero_event_ind+cauti_zero_event_ind+clabsi_zero_event_ind+mrsa_zero_event_ind+ssi_colo_zero_event_ind+ssi_hyst_zero_event_ind as msr_val,
'NHSN Zero Events' as msr_nm
FROM pce_qe16_prd..stg_nhsn_zero_event_fct

union

SELECT distinct fcy_nm, 
fcy_num, 
mo_and_yr_abbr, 
event_dt,
cms5_hac_adm_ind+cms6_hac_adm_ind+cms7_hac_adm_ind as msr_val,
'HAC Zero Events' as msr_nm
from pce_qe16_prd..stg_hac_zero_event_fct 
);

--Zero Events Fact for Clinical Outcome Score
DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt, 1 as join_key from pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef);

drop table pce_qe16_prd..stg_tmp_zero_events_fct if exists;
create table pce_qe16_prd..stg_TMP_ZERO_EVENTS_FCT AS 
(SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr, 
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2



UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
sum(msr_val) as Zero_events_numr,
228 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
and ZF.fcy_nm <>'McLaren Greater Lansing'
group by 1,2


UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(add_months(date('06/01/2019'),1)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
19*2 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),1)
and ZF.fcy_nm ='McLaren Greater Lansing'
group by 1,2



UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(add_months(date('06/01/2019'),2)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
19*3 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),2)
and ZF.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(add_months(date('06/01/2019'),3)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
19*4 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),3)
and ZF.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(add_months(date('06/01/2019'),4)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
19*5 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),4)
and ZF.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(add_months(date('06/01/2019'),5)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
19*6 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),5)
and ZF.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(add_months(date('06/01/2019'),6)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
19*7 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),6)
and ZF.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(add_months(date('06/01/2019'),7)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
19*8 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),7)
and ZF.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(add_months(date('06/01/2019'),8)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
19*9 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),8)
and ZF.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(add_months(date('06/01/2019'),9)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
19*10 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),9)
and ZF.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(add_months(date('06/01/2019'),10)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
19*11 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),10)
and ZF.fcy_nm ='McLaren Greater Lansing'
group by 1,2

UNION

SELECT distinct ZF.fcy_nm,
zf.fcy_num,
max(add_months(date('06/01/2019'),11)) as rpt_dt,
sum(msr_val) as Zero_events_numr,
19*12 as zero_events_dnmr,
1 as join_key,
Zero_events_numr/zero_events_dnmr as Zero_events_rt
FROM pce_qe16_prd..stg_zero_event_fct zf
left join tmp_max_dschrg_dt tmd on join_key= tmd.join_key
where  ZF.event_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),11)
and ZF.fcy_nm ='McLaren Greater Lansing'
group by 1,2);

/*

Quality Advisor Measure needed for Calculating Clinical Outcome Score

*/

----Mortality Index for Clinical Outcome Score

drop table pce_qe16_prd..tmp_mort if exists;
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
1 as join_key,
( case when mrtly_outc_case_w_excl=0 then null 
 when mrtly_excl_ind=1  then mrtly_cnt_w_excl
 else null
 end) as mort_obs_cnt,
  (case when mrtly_outc_case_w_excl=0 then null
 when mrtly_excl_ind=1 and hspc_pyr_excl =1 then apr_expc_mrtly_cnt
 else null end
 ) as mort_expc_cnt
FROM pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef
where ef.stnd_ptnt_type_cd = '08')
;
 
 DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
 
 (select max(ef.dschrg_dt) as max_dt, 1 as join_key from pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef);
 drop table pce_qe16_prd..stg_tmp_mrtly_ind if exists;
 create table pce_qe16_prd..stg_tmp_mrtly_ind as 
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
   UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(date('06/01/2019'),1))) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),1)
 and fcy_nm = 'McLaren Greater Lansing'
 group by 1,2
 
 
   UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(date('06/01/2019'),2))) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),2)
 and fcy_nm = 'McLaren Greater Lansing'
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(date('06/01/2019'),3))) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),3)
 and fcy_nm = 'McLaren Greater Lansing'
 group by 1,2
 
   UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(date('06/01/2019'),4))) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),4)
 and fcy_nm = 'McLaren Greater Lansing'
 group by 1,2
 
   UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(date('06/01/2019'),5))) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),5)
 and fcy_nm = 'McLaren Greater Lansing'
 group by 1,2
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(date('06/01/2019'),6))) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),6)
 and fcy_nm = 'McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(date('06/01/2019'),7))) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),7)
 and fcy_nm = 'McLaren Greater Lansing'
 group by 1,2
 
 
  UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(date('06/01/2019'),8))) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),8)
 and fcy_nm = 'McLaren Greater Lansing'
 group by 1,2
 
 UNION
 
 select tm.fcy_nm,tm.fcy_num,
 max(date_trunc('month',add_months(date('06/01/2019'),9))) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),9)
 and fcy_nm = 'McLaren Greater Lansing'
 group by 1,2;
 
 drop table pce_qe16_prd..tmp_mort if exists;
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
1 as join_key

FROM pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef
where ef.stnd_ptnt_type_cd = '08' and ef.ms_drg_icd10 not in ('837','838','846','847','848')

);
DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt, 1 as join_key from pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef);
 
 drop table pce_qe16_prd..stg_tmp_sep_mrtly_ind if exists;
 create table pce_qe16_prd..stg_tmp_sep_mrtly_ind as
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 
   
   UNION
--------------------------------------------------------------Lansing------------------------------------------------------------------------------------------------------ 
  
   select tm.fcy_nm,
 tm.fcy_num,
 max(add_months(date('06/01/2019'),1)) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),1)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 
 union
 
 
    select tm.fcy_nm,
 tm.fcy_num,
 max(add_months(date('06/01/2019'),2)) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),2)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 union
 
 
    select tm.fcy_nm,
 tm.fcy_num,
 max(add_months(date('06/01/2019'),3)) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),3)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 union
 
 
    select tm.fcy_nm,
 tm.fcy_num,
 max(add_months(date('06/01/2019'),4)) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),4)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 
 union
 
 
    select tm.fcy_nm,
 tm.fcy_num,
 max(add_months(date('06/01/2019'),5)) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),5)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 union
 
 
    select tm.fcy_nm,
 tm.fcy_num,
 max(add_months(date('06/01/2019'),6)) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),6)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 
 union
 
 
    select tm.fcy_nm,
 tm.fcy_num,
 max(add_months(date('06/01/2019'),7)) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),7)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 union
 
 
    select tm.fcy_nm,
 tm.fcy_num,
 max(add_months(date('06/01/2019'),8)) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),8)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 union
 
 
    select tm.fcy_nm,
 tm.fcy_num,
 max(add_months(date('06/01/2019'),9)) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),9)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
  union
 
 
    select tm.fcy_nm,
 tm.fcy_num,
 max(add_months(date('06/01/2019'),10)) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),10)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
  union
 
 
    select tm.fcy_nm,
 tm.fcy_num,
 max(add_months(date('06/01/2019'),11)) as rpt_dt,
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
 where tm.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),11)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2);
 
 drop table pce_qe16_prd..tmp_readm if exists;
;create temp table  tmp_readm as 
(
SELECT distinct ef.encntr_num,
ef.fcy_nm,
ef.fcy_num,
ef.stnd_ptnt_type_cd,
ef.dschrg_dt,
max(EF.readm_excl_ind) as readm_excl_ind,
max(ef.prs_readm_30day_rsk_out_case_cnt) as readm_outc_case,
max(case when EF.readm_excl_ind=1 then ef.prs_readm_30day_rsk_out_case_cnt else null end) as readm_outc_case_w_excl,
max(ef.acute_readmit_days_key) as acute_readmit_days_key,
max(EF.csa_obs_readm_rsk_adj_cnt) as csa_obs_readm_rsk_adj_cnt,
max(ef.csa_expc_prs_readm_30day_rsk) as csa_expc_prs_readm_30day_rsk,
1 as join_key


FROM pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef
where ef.stnd_ptnt_type_cd = '08'
group by 1,2,3,4,5
);

drop table pce_qe16_prd..tmp_max_dschrg_dt if exists;
;create temp table  tmp_max_dschrg_dt as 
 
 (select max(ef.dschrg_dt) as max_dt,1 as join_key from pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef);
 
 
 drop table pce_qe16_prd..stg_tmp_readm_ind if exists;
 create table pce_qe16_prd..stg_tmp_readm_ind as 
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
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
 and fcy_nm <>'McLaren Greater Lansing'
 group by 1,2
 
 
 ----------------lANSING Readmission Measures-------------------------------------------------------------------------------------------------------
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(add_months(date('06/01/2019'),1)) as rpt_dt,
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
 where  tr.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),1)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(add_months(date('06/01/2019'),2)) as rpt_dt,
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
 where  tr.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),2)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(add_months(date('06/01/2019'),3)) as rpt_dt,
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
 where  tr.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),3)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(add_months(date('06/01/2019'),4)) as rpt_dt,
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
 where  tr.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),4)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
  UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(add_months(date('06/01/2019'),5)) as rpt_dt,
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
 where  tr.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),5)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
  UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(add_months(date('06/01/2019'),6)) as rpt_dt,
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
 where  tr.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),6)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
  UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(add_months(date('06/01/2019'),7)) as rpt_dt,
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
 where  tr.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),7)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(add_months(date('06/01/2019'),8)) as rpt_dt,
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
 where  tr.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),8)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(add_months(date('06/01/2019'),9)) as rpt_dt,
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
 where  tr.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),9)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(add_months(date('06/01/2019'),10)) as rpt_dt,
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
 where  tr.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),10)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2
 
 
 UNION
 
   select tr.fcy_nm,
 tr.fcy_num,
  max(add_months(date('06/01/2019'),11)) as rpt_dt,
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
 where  tr.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),11)
 and fcy_nm ='McLaren Greater Lansing'
 group by 1,2);
 
 /*
 Lab Utilization for Clinical Outcomes
 */
 DROP table pce_qe16_prd..stg_lab_utlz_fct IF EXISTS; 
 create table pce_qe16_prd..stg_lab_utlz_fct as 
(SELECT 
EF.encntr_num,
ef.fcy_nm, 
ef.fcy_num, 
ef.dschrg_dt,
CF.department_group,
CF.persp_clncl_dtl_descr,
ef.dschrg_svc,
ef.src_prim_pyr_cd,
EF.ptnt_tp_cd,
ef.src_prim_payor_grp3,
max(CASE WHEN ( (UPPER(EF.ptnt_tp_cd) <> 'BSCH' OR UPPER(EF.ptnt_tp_cd) <> 'BSCHO' OR upper(ef.src_prim_pyr_cd)<>'SELECT' OR upper(ef.src_prim_pyr_cd)<>'SELEC')

OR (upper(ef.src_prim_payor_grp3) <> ('HOSPICE'))

OR (upper(ef.dschrg_svc)<>'NB' OR upper(ef.dschrg_svc)<>'NBN' OR upper(ef.dschrg_svc)<>'OIN' OR upper(ef.dschrg_svc)<>'SCN' OR upper(ef.dschrg_svc)<>'L1N'
OR upper(ef.dschrg_svc)<>'BBN' OR upper(ef.dschrg_svc)<>'NURS'))
then 1 else 0 end) as qty_incl_ind,

max(case when CF.persp_clncl_dtl_descr in ('R&B CICU/CCU (CORONARY CARE)',
'R&B ICU',
'R&B ISOLATION PRIVATE',
'R&B MED/SURG DELUXE',
'R&B MED/SURG PRIVATE',
'R&B MED/SURG SEMI PRIVATE',
'R&B NURSERY',
'R&B NURSERY INTENSIVE LEVEL III(NICU)',
'R&B NURSERY INTERMEDIATE LEVEL II',
'R&B OB',
'R&B ONCOLOGY',
'R&B PEDIATRIC',
'R&B PSYCH ISOLATION',
'R&B PSYCH PRIVATE',
'R&B PSYCH SEMI PRIVATE',
'R&B REHAB ISOLATION',
'R&B REHAB PRIVATE',
'R&B REHAB SEMI PRIVATE',
'R&B STEP DOWN SEMI PRIVATE (PCU)',
'R&B TCU DELUXE','R&B TCU PRIVATE',
'R&B TCU SEMI PRIVATE',
'R&B TELEMETRY PRIVATE',
'R&B TELEMETRY SEMI PRIVATE',
'R&B TRAUMA ICU') then 1 else 0 end) as randb_incl_ind,

max(case when cf.cpt_code in ('36415',
'36430',
'38212',
'38221',
'81371',
'81372',
'81373',
'81374',
'81376',
'81379',
'81380',
'81381',
'81382',
'81479',
'82947',
'82948',
'82962',
'86160',
'86161',
'86162',
'86812',
'86829',
'86832',
'86833',
'86850',
'86860',
'86870',
'86880',
'86885',
'86886',
'86900',
'86901',
'86902',
'86904',
'86905',
'86906',
'86920',
'86921',
'86922',
'86923',
'86927',
'86945',
'86965',
'86970',
'86971',
'86978',
'87207',
'87220',
'88104',
'88108',
'88112',
'88142',
'88160',
'88161',
'88172',
'88173',
'88175',
'88177',
'88184',
'88185',
'88188',
'88189',
'88300',
'88302',
'88304',
'88305',
'88307',
'88309',
'88311',
'88312',
'88313',
'88314',
'88329',
'88331',
'88332',
'88333',
'88334',
'88341',
'88342',
'88344',
'88348',
'88360',
'88361',
'93005',
'G0123',
'G0145',
'G0364',
'G0416',
'J2790',
'J2791',
'P9011',
'P9012',
'P9016',
'P9017',
'P9019',
'P9021',
'P9031',
'P9033',
'P9034',
'P9035',
'P9037',
'P9039',
'P9040',
'P9044',
'P9052',
'P9059',
'P9604'
) then 1 else 0 end) as cpt_excl_ind,
sum(CF.quantity) as quantity, 
sum(cf.total_charge) as total_charge,
1 as join_key

FROM PCE_QE16_SLP_PRD_DM..prd_encntr_anl_fct EF
INNER JOIN PCE_QE16_SLP_PRD_DM..prd_chrg_fct CF ON EF.encntr_num = CF.src_patient_id AND EF.fcy_nm = CF.src_company_id
WHERE EF.in_or_out_patient_ind = 'I' 
group by 1,2,3,4,5,6,7,8,9,10
);

DROP TABLE tmp_lab_utlz IF EXISTS;
;create temp table  tmp_lab_utlz as 
(SELECT 

ef.fcy_nm, 
ef.fcy_num, 
ef.dschrg_dt,
sum(case when CF.department_group= 'Lab' then CF.quantity else 0 end) as qty, 
1 as join_key,
sum(CASE WHEN (UPPER(EF.ptnt_tp_cd) NOT IN ('BSCH','BSCHO') or upper(ef.src_prim_pyr_cd) not in ('SELECT','SELEC') or upper(ef.src_prim_payor_grp3) not in ('HOSPICE')

or upper(ef.dschrg_svc) not in ('NB','NBN','OIN','SCN','L1N','BBN','NURS'))
AND CF.persp_clncl_dtl_descr in ('R&B CICU/CCU (CORONARY CARE)','R&B ICU','R&B ISOLATION PRIVATE','R&B MED/SURG DELUXE','R&B MED/SURG PRIVATE','R&B MED/SURG SEMI PRIVATE',
'R&B NURSERY','R&B NURSERY INTENSIVE LEVEL III(NICU)','R&B NURSERY INTERMEDIATE LEVEL II','R&B OB','R&B ONCOLOGY','R&B PEDIATRIC','R&B PSYCH ISOLATION','R&B PSYCH PRIVATE',
'R&B PSYCH SEMI PRIVATE','R&B REHAB ISOLATION','R&B REHAB PRIVATE','R&B REHAB SEMI PRIVATE','R&B STEP DOWN SEMI PRIVATE (PCU)','R&B TCU DELUXE','R&B TCU PRIVATE','R&B TCU SEMI PRIVATE',
'R&B TELEMETRY PRIVATE','R&B TELEMETRY SEMI PRIVATE','R&B TRAUMA ICU') then cf.quantity else 0 end) as ptnt_days

FROM PCE_QE16_SLP_PRD_DM..prd_encntr_anl_fct EF
INNER JOIN PCE_QE16_SLP_PRD_DM..prd_chrg_fct CF ON EF.encntr_num = CF.src_patient_id AND EF.fcy_nm = CF.src_company_id
WHERE EF.in_or_out_patient_ind = 'I' AND CF.total_charge <>0 
and cf.cpt_code not in ('36415',
'36430',
'38212',
'38221',
'81371',
'81372',
'81373',
'81374',
'81376',
'81379',
'81380',
'81381',
'81382',
'81479',
'82947',
'82948',
'82962',
'86160',
'86161',
'86162',
'86812',
'86829',
'86832',
'86833',
'86850',
'86860',
'86870',
'86880',
'86885',
'86886',
'86900',
'86901',
'86902',
'86904',
'86905',
'86906',
'86920',
'86921',
'86922',
'86923',
'86927',
'86945',
'86965',
'86970',
'86971',
'86978',
'87207',
'87220',
'88104',
'88108',
'88112',
'88142',
'88160',
'88161',
'88172',
'88173',
'88175',
'88177',
'88184',
'88185',
'88188',
'88189',
'88300',
'88302',
'88304',
'88305',
'88307',
'88309',
'88311',
'88312',
'88313',
'88314',
'88329',
'88331',
'88332',
'88333',
'88334',
'88341',
'88342',
'88344',
'88348',
'88360',
'88361',
'93005',
'G0123',
'G0145',
'G0364',
'G0416',
'J2790',
'J2791',
'P9011',
'P9012',
'P9016',
'P9017',
'P9019',
'P9021',
'P9031',
'P9033',
'P9034',
'P9035',
'P9037',
'P9039',
'P9040',
'P9044',
'P9052',
'P9059',
'P9604'
)
group by 1,2,3
);



DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt,1 as join_key from pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef);

drop table pce_qe16_prd..stg_tmp_lab_utlz_fct if exists;
create table pce_qe16_prd..stg_tmp_lab_utlz_fct as 

(select tlu.fcy_nm,
tlu.fcy_num,
max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
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
 and tlu.fcy_nm <>'Lansing'
 group by 1,2
 
 
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
 max(add_months(date('06/01/2019'),1)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),1)
 and tlu.fcy_nm = 'Lansing'
 group by 1,2
 
   UNION

select tlu.fcy_nm,
tlu.fcy_num,
 max(add_months(date('06/01/2019'),2)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),2)
 and tlu.fcy_nm = 'Lansing'
 group by 1,2
 
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
 max(add_months(date('06/01/2019'),3)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),3)
 and tlu.fcy_nm = 'Lansing'
 group by 1,2
 
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
 max(add_months(date('06/01/2019'),4)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),4)
 and tlu.fcy_nm = 'Lansing'
 group by 1,2
 
 UNION

select tlu.fcy_nm,
tlu.fcy_num,
 max(add_months(date('06/01/2019'),5)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),5)
 and tlu.fcy_nm = 'Lansing'
 group by 1,2
 
 UNION

select tlu.fcy_nm,
tlu.fcy_num,
 max(add_months(date('06/01/2019'),6)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),6)
 and tlu.fcy_nm = 'Lansing'
 group by 1,2
 
 UNION

select tlu.fcy_nm,
tlu.fcy_num,
 max(add_months(date('06/01/2019'),7)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),7)
 and tlu.fcy_nm = 'Lansing'
 group by 1,2
 
 
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
 max(add_months(date('06/01/2019'),8)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),8)
 and tlu.fcy_nm = 'Lansing'
 group by 1,2
 
  
  UNION

select tlu.fcy_nm,
tlu.fcy_num,
 max(add_months(date('06/01/2019'),9)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),9)
 and tlu.fcy_nm = 'Lansing'
 group by 1,2
 
   UNION

select tlu.fcy_nm,
tlu.fcy_num,
 max(add_months(date('06/01/2019'),10)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),10)
 and tlu.fcy_nm = 'Lansing'
 group by 1,2
 
    UNION

select tlu.fcy_nm,
tlu.fcy_num,
 max(add_months(date('06/01/2019'),11)) as rpt_dt,
sum( tlu.qty) as qty,
sum(tlu.ptnt_days) as ptnt_days,
sum( tlu.qty)/sum(tlu.ptnt_days) as lab_utlz

from tmp_lab_utlz tlu
left join tmp_max_dschrg_dt tmd on tlu.join_key=tmd.join_key
where tlu.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),11)
 and tlu.fcy_nm = 'Lansing'
 group by 1,2);

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
(select max(ef.dschrg_dt) as max_dt, 1 as join_key from pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef);

drop table pce_qe16_prd..stg_tmp_sep_compl_fct if exists;
create table pce_qe16_prd..stg_tmp_sep_compl_fct as 
(select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),1)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),1)
and tsc.fcy_nm='Greater Lansing'
group by 1,2

union 



select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),2)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),2)
and tsc.fcy_nm='Greater Lansing'
group by 1,2

union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),3)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),3)
and tsc.fcy_nm='Greater Lansing'
group by 1,2

union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),4)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),4)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),5)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),5)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),6)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),6)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),7)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),7)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),8)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),8)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),9)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),9)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),10)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),10)
and tsc.fcy_nm='Greater Lansing'
group by 1,2

union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),11)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),11)
and tsc.fcy_nm='Greater Lansing'
group by 1,2);

/*
OB Training for Clinical Outcome Score
*/

drop table pce_qe16_prd..stg_tmp_ob_trn_fct if exists;
create table pce_qe16_prd..stg_tmp_ob_trn_fct as 
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

--drop table tmp_ptnt_exrnc_pct_msr_fct;
DROP TABLE tmp_ptnt_exrnc_pct_msr_fct IF EXISTS;
;create temp table  tmp_ptnt_exrnc_pct_msr_fct as 
(select tspe.service,
tspe.client_id,
tspe.FCY_NM,
tspe.rprt_dt,
sum(tspe.resp_cnt) as resp_cnt,
sum(tspe.top_box_resp_cnt) as top_box_resp_cnt,
(sum(tspe.top_box_resp_cnt)/sum(tspe.resp_cnt))*100 as sub_ptnt_exrnc_pct,

trunc((sum(tspe.top_box_resp_cnt)/sum(tspe.resp_cnt))*100,1) as ptnt_exrnc_pct

from tmp_sub_ptnt_exrnc_msr_fct tspe
group by 1,2,3,4);

drop table  pce_qe16_prd..stg_ptnt_exrnc_pct_msr_fct if exists ;
create table pce_qe16_prd..stg_ptnt_exrnc_pct_msr_fct as 
(select tmppm.*,
case when service= 'IN' then 0.5
when service = 'AS' then 0.2
when service = 'ER' then 0.3
end as msr_wt,


ppep.prgny_pct,

ppep.prgny_pct*msr_wt as prgny_pct_scor
from tmp_ptnt_exrnc_pct_msr_fct tmppm
left join pce_qe16_pressganey_prd_zoom..prgny_ptnt_exrnc_pct_dim ppep on tmppm.service = ppep.svc_cd and tmppm.ptnt_exrnc_pct = ppep.scor);


-------Patient Experience Composite for Clinical Outcome Score
drop table pce_qe16_prd..stg_tmp_ptnt_exrnc_pct_msr_fct if exists;
create table pce_qe16_prd..stg_tmp_ptnt_exrnc_pct_msr_fct as
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
FROM pce_qe16_pressganey_prd_zoom..ptnt_exrnc_pct_msr_fct pepm
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
  
  DROP TABLE tmp_maxd IF EXISTS;
;create temp table  tmp_maxd as
  (Select last_day(max(tsf.dschrg_dt)) as dschrg_dt,
  'a' as join_key
  FROM tmp_sf tsf);

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


drop table pce_qe16_prd..stg_mmg_ovrl_ptnt_exrnc_msr_fct if exists ;
create table pce_qe16_prd..stg_mmg_ovrl_ptnt_exrnc_msr_fct as
(select tsp.* , 
tsp.top_box_resp_cnt/tsp.resp_cnt as ptnt_exrnc_scor,
case when hsptl_rgon='Bay' and ROUND(ptnt_exrnc_scor,2) <0.76 then 'R'
when hsptl_rgon='Bay' and ROUND(ptnt_exrnc_scor,2) >=0.76 then 'G'
when hsptl_rgon='Central' and ROUND(ptnt_exrnc_scor,3) <0.784 then 'R'
when hsptl_rgon='Central' and ROUND(ptnt_exrnc_scor,3) >=0.784 then 'G'
when hsptl_rgon='Flint' and ROUND(ptnt_exrnc_scor,3) <0.741 then 'R'
when hsptl_rgon='Flint' and ROUND(ptnt_exrnc_scor,3) >=0.741 then 'G'
when hsptl_rgon='Lansing' and ROUND(ptnt_exrnc_scor,3)<0.721 then 'R'
when hsptl_rgon='Lansing' and ROUND(ptnt_exrnc_scor,3) >=0.721 then 'G'
when hsptl_rgon='Lapeer' and ROUND(ptnt_exrnc_scor,3) <0.797 then 'R'
when hsptl_rgon='Lapeer' and ROUND(ptnt_exrnc_scor,3) >=0.797 then 'G'
when hsptl_rgon='Macomb' and ROUND(ptnt_exrnc_scor,3) <0.808 then 'R'
when hsptl_rgon='Macomb' and ROUND(ptnt_exrnc_scor,3) >=0.808 then 'G'
when hsptl_rgon='Northern' and ROUND(ptnt_exrnc_scor,3) <0.794 then 'R'
when hsptl_rgon='Northern' and ROUND(ptnt_exrnc_scor,3) >=0.794 then 'G'
when hsptl_rgon='Oakland' and ROUND(ptnt_exrnc_scor,3) <0.828 then 'R'
when hsptl_rgon='Oakland' and ROUND(ptnt_exrnc_scor,3) >=0.828 then 'G'
when hsptl_rgon='Port Huron' and ROUND(ptnt_exrnc_scor,3) <0.845 then 'R'
when hsptl_rgon='Port Huron' and ROUND(ptnt_exrnc_scor,3) >=0.845 then 'G'
END as ptnt_exrnc_clr_cdg
from tmp_sub_ptntexp tsp);


----MMG Overall Patient Experience for Clinical Outcome Score

drop table pce_qe16_prd..stg_tmp_mmg_ovrl_ptnt_exrnc_msr_fct if exists;
create table pce_qe16_prd..stg_tmp_mmg_ovrl_ptnt_exrnc_msr_fct as
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
FROM pce_qe16_prd.rpola.aco_smry_msr asm);

DROP table pce_qe16_prd..stg_aco_mpp_msr_fct IF EXISTS;

create table pce_qe16_prd..stg_aco_mpp_msr_fct as 
(select distinct taco.*,
taco.pbpy_pts*0.25 as pbpy_scr,
taco.hcc_pts*0.25 as hcc_scr,
taco.Leakage_pts*0.125 as leakage_scr,
taco.snf_adm_pts*0.25 as sbf_adm_scr,
taco.ed_vst_pts*0.125 as ed_vst_scr,
pbpy_scr+hcc_scr+leakage_scr+sbf_adm_scr+ed_vst_scr as mpp_scr,
case when mpp_scr <100 then 'R'
WHEN MPP_SCR >=100 and MPP_SCR <150 then 'G'
WHEN MPP_SCR>=150 then 'B'
END AS MPP_SCR_COLOR_ATTR,
CD.mo_and_yr_nm AS DATE_PARAMETER
from tmp_aco_sub_msr taco
INNER JOIN pce_qe16_prd..CDR_DIM CD ON taco.rpt_prd_end_dt= CD.CDR_DT);

/*Other Tables for Clinical Outcome Score
*/

----Cardiac Rehab for Clinical Outcome Score
DROP TABLE tmp_cardiac_rehab IF EXISTS;
;create temp table  tmp_cardiac_rehab as
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
1 as Join_key,
min(cd.cdr_dt) as event_dt,
sum(snpsr4qnumerator) as snpsr4qnumerator, 
sum(snpsr4qdenominator) as snpsr4qdenominator
FROM pce_qe16_prd..ncdr_hsptl_fct_vw ncdr
inner join pce_qe16_prd..cdr_dim cd on replace(cd.qtr_and_yr_abbr,' ','') = ncdr.timeframecode
where ncdr.market_name = 'My Group' and ncdr.metricid = 45 and ncdr.clientname is not null
group by 1,2,3);

DROP TABLE tmp_max_dschrg_dt IF EXISTS;
;create temp table  tmp_max_dschrg_dt as 
(select max(ef.dschrg_dt) as max_dt,1 as Join_key from pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef);

drop table pce_qe16_prd..stg_tmp_card_rehab_fct if exists;
create table pce_qe16_prd..stg_tmp_card_rehab_fct as 
(select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
group by 1,2
 
UNION
 
select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-1))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-12) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-1)
group by 1,2

UNION

select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-2))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-13) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-2)
group by 1,2
  
UNION

select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-3))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-14) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-3)
group by 1,2
  
UNION

select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-4))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-15) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-4)
group by 1,2
  
UNION

select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-5))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-16) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-5)
group by 1,2

UNION

select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-6))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-17) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-6)
group by 1,2
  
UNION

select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-7))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-18) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-7)
group by 1,2
  
 UNION

select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-8))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-19) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-8)
group by 1,2   

UNION

select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-9))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-20) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-9)
group by 1,2   

UNION

select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-10))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-21) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-10)
group by 1,2   
  

UNION

select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-11))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-22) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-11)
group by 1,2   
  

UNION

select tcr.fcy_nm,
tcr.fcy_num,
max(date_trunc('month',add_months(tmd.max_dt,-12))) as rpt_dt,
sum(tcr.snpsr4qnumerator) as snpsr4qnumerator,
sum(tcr.snpsr4qdenominator) as snpsr4qdenominator,
sum(tcr.snpsr4qnumerator)/sum(tcr.snpsr4qdenominator) as cardiac_rt
from tmp_cardiac_rehab tcr
left join tmp_max_dschrg_dt tmd on tcr.join_key = tmd.join_key
where tcr.event_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-23) and add_months((select last_day(max_dt) from tmp_max_dschrg_dt),-12)
group by 1,2   
  
  );
  
  
  
  ------------------------OB Trainingg Fact for Clinical Outcome Score
  drop table pce_qe16_prd..stg_tmp_ob_trn_fct if exists;
create table pce_qe16_prd..stg_tmp_ob_trn_fct as 
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
(select max(ef.dschrg_dt) as max_dt, 1 as join_key from pce_qe16_prd..stg_encntr_qs_anl_fct_vw ef);

drop table pce_qe16_prd..stg_tmp_sep_compl_fct if exists;
create table pce_qe16_prd..stg_tmp_sep_compl_fct as 
(select tsc.fcy_nm,
tsc.fcy_num,
 max(date_trunc('month',tmd.max_dt)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between add_months((select date_trunc('month',max_dt) from tmp_max_dschrg_dt ),-11) and (select last_day(max_dt) from tmp_max_dschrg_dt)
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
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
and tsc.fcy_nm<>'Greater Lansing'
group by 1,2


Union

select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),1)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),1)
and tsc.fcy_nm='Greater Lansing'
group by 1,2

union 



select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),2)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),2)
and tsc.fcy_nm='Greater Lansing'
group by 1,2

union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),3)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),3)
and tsc.fcy_nm='Greater Lansing'
group by 1,2

union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),4)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),4)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),5)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),5)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),6)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),6)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),7)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),7)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),8)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),8)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),9)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),9)
and tsc.fcy_nm='Greater Lansing'
group by 1,2


union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),10)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),10)
and tsc.fcy_nm='Greater Lansing'
group by 1,2

union 


select tsc.fcy_nm,
tsc.fcy_num,
max(add_months(date('06/01/2019'),11)) as rpt_dt,
sum(tsc.numr) as numr,
sum(tsc.dnmr) as dnmr,
(sum(tsc.numr)/sum(tsc.dnmr))*100 as complc
from tmp_sep_compl tsc
 left join tmp_max_dschrg_dt tmd on tsc.join_key=tmd.join_key
where tsc.dschrg_dt between '06/01/2019' and add_months((select last_day(date('06/01/2019')) from tmp_max_dschrg_dt),11)
and tsc.fcy_nm='Greater Lansing'
group by 1,2);


/*
Final Clinical Outcome Score Query
*/

drop table pce_qe16_prd..stg_clncl_outc_scor_fct if exists;
create table pce_qe16_prd..stg_clncl_outc_scor_fct as 
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
(case 
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3) > 0.1334 then 0
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.1270 and round(max(tri.readm_obs_rt),3) <= 0.1334 then 50
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.1245 and round(max(tri.readm_obs_rt),3) <= 0.1270 then 75
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.1219 and round(max(tri.readm_obs_rt),3) <= 0.1245 then 100
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)> 0.1194 and round(max(tri.readm_obs_rt),3) <= 0.1219 then 125
when tmi.fcy_num = 'MI2191' and round(max(tri.readm_obs_rt),3)<=0.1194 then 150

when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3) > 0.0746 then 0
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.0710 and round(max(tri.readm_obs_rt),3) <= 0.0746 then 50
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.0696 and round(max(tri.readm_obs_rt),3) <= 0.0710 then 75
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.0682 and round(max(tri.readm_obs_rt),3) <= 0.0696 then 100
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)> 0.0667 and round(max(tri.readm_obs_rt),3) <= 0.0682 then 125
when tmi.fcy_num = 'MI2061' and round(max(tri.readm_obs_rt),3)<=0.0667 then 150

when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3) > 0.1397 then 0
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.1330 and round(max(tri.readm_obs_rt),3) <= 0.1397 then 50
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.1303 and round(max(tri.readm_obs_rt),3) <= 0.1330 then 75
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.1277 and round(max(tri.readm_obs_rt),3) <= 0.1303 then 100
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)> 0.1250 and round(max(tri.readm_obs_rt),3) <= 0.1277 then 125
when tmi.fcy_num = 'MI2302'  and round(max(tri.readm_obs_rt),3)<=0.1250 then 150


when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3) > 0.1124 then 0
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.1070 and round(max(tri.readm_obs_rt),3) <= 0.1124 then 50
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.1049 and round(max(tri.readm_obs_rt),3) <= 0.1070 then 75
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.1027 and round(max(tri.readm_obs_rt),3) <= 0.1049 then 100
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)> 0.1006 and round(max(tri.readm_obs_rt),3) <= 0.1027 then 125
when tmi.fcy_num = 'MI5020'  and round(max(tri.readm_obs_rt),3)<=0.1006 then 150

when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3) > 0.1134 then 0
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.1080 and round(max(tri.readm_obs_rt),3) <= 0.1134 then 50
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.1058 and round(max(tri.readm_obs_rt),3) <= 0.1080 then 75
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.1037 and round(max(tri.readm_obs_rt),3) <= 0.1058 then 100
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)> 0.1015 and round(max(tri.readm_obs_rt),3) <= 0.1037 then 125
when tmi.fcy_num = 'MI2001'  and round(max(tri.readm_obs_rt),3)<=0.1015 then 150

when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3) > 0.1313 then 0
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.1250 and round(max(tri.readm_obs_rt),3) <= 0.1313 then 50
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.1225 and round(max(tri.readm_obs_rt),3) <= 0.1250 then 75
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.1200 and round(max(tri.readm_obs_rt),3) <= 0.1225 then 100
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)> 0.1175 and round(max(tri.readm_obs_rt),3) <= 0.1200 then 125
when tmi.fcy_num = 'MI2048'  and round(max(tri.readm_obs_rt),3)<=0.1175 then 150

when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3) > 0.1008 then 0
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.0960 and round(max(tri.readm_obs_rt),3) <= 0.1008 then 50
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.0941 and round(max(tri.readm_obs_rt),3) <= 0.0960 then 75
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.0922 and round(max(tri.readm_obs_rt),3) <= 0.0941 then 100
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)> 0.0902 and round(max(tri.readm_obs_rt),3) <= 0.0922 then 125
when tmi.fcy_num = '637619'  and round(max(tri.readm_obs_rt),3)<=0.0902 then 150

when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3) > 0.1197 then 0
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.1140 and round(max(tri.readm_obs_rt),3) <= 0.1197 then 50
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.1117 and round(max(tri.readm_obs_rt),3) <= 0.1140 then 75
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.1094 and round(max(tri.readm_obs_rt),3) <= 0.1117 then 100
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)> 0.1072 and round(max(tri.readm_obs_rt),3) <= 0.1094 then 125
when tmi.fcy_num = 'MI2055'  and round(max(tri.readm_obs_rt),3)<=0.1072 then 150

when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3) > 0.1344 then 0
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.1280 and round(max(tri.readm_obs_rt),3) <= 0.1344 then 50
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.1254 and round(max(tri.readm_obs_rt),3) <= 0.1280 then 75
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.1229 and round(max(tri.readm_obs_rt),3) <= 0.1254 then 100
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)> 0.1203 and round(max(tri.readm_obs_rt),3) <= 0.1229 then 125
when tmi.fcy_num = '600816'  and round(max(tri.readm_obs_rt),3)<=0.1203 then 150



else 0
end) as readm_obs_pts,

(case 
when  round(max(tri.readm_oe_rt),2) > 1.1 then 0
when  round(max(tri.readm_oe_rt),2)> 1 and round(max(tri.readm_oe_rt),2) <= 1.1 then 50
when  round(max(tri.readm_oe_rt),2)> 0.98 and round(max(tri.readm_oe_rt),2) <= 1 then 75
when  round(max(tri.readm_oe_rt),2)> 0.96 and round(max(tri.readm_oe_rt),2) <= 0.98 then 100
when round(max(tri.readm_oe_rt),2)> 0.94 and round(max(tri.readm_oe_rt),2) <= 0.96 then 125
when  round(max(tri.readm_oe_rt),2)<=0.94 then 150
else 0
end) as readm_oe_pts,

(case 
when  max(tmi.mort_oe_rt) > 0.8 then 0
when  max(tmi.mort_oe_rt)> 0.77 and max(tmi.mort_oe_rt) <= 0.8 then 50
when  max(tmi.mort_oe_rt)> 0.74 and max(tmi.mort_oe_rt) <= 0.77 then 75
when  max(tmi.mort_oe_rt)> 0.7 and max(tmi.mort_oe_rt) <= 0.74 then 100
when  max(tmi.mort_oe_rt)> 0.67 and max(tmi.mort_oe_rt) <= 0.7 then 125
when  max(tmi.mort_oe_rt)<=0.67 then 150
else 0
end) as mort_oe_pts,

(case 
when  round(max(tsm.mort_oe_rt),2) > 0.97 then 0
when  round(max(tsm.mort_oe_rt),2)> 0.94 and round(max(tsm.mort_oe_rt),2) <= 0.97 then 50
when  round(max(tsm.mort_oe_rt),2)> 0.9 and round(max(tsm.mort_oe_rt),2) <= 0.94 then 75
when  round(max(tsm.mort_oe_rt),2)> 0.87 and round(max(tsm.mort_oe_rt),2) <= 0.9 then 100
when  round(max(tsm.mort_oe_rt),2)> 0.85 and round(max(tsm.mort_oe_rt),2) <= 0.87 then 125
when  round(max(tsm.mort_oe_rt),2)<=0.85 then 150
else 0
end) as sep_mort_oe_pts,

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

(case 
when  round(comp_oe_rt,2) > 0.92 then 0
when  round(comp_oe_rt,2) > 0.88 and round(comp_oe_rt,2)  <=0.92 then 50
when  round(comp_oe_rt,2) > 0.84 and round(comp_oe_rt,2)  <= 0.88 then 75
when round(comp_oe_rt,2) > 0.73 and round(comp_oe_rt,2)  <= 0.84 then 100
when  round(comp_oe_rt,2) > 0.62 and round(comp_oe_rt,2)  <= 0.73 then 125
when  round(comp_oe_rt,2)  <= 0.62 then 150
else 0
end) as comp_oe_rt_pts,

(case 
when  round(sep_compliance_rt,2) < 0.56 then 0
when  round(sep_compliance_rt,2)>= 0.56 and round(sep_compliance_rt,2) <0.59 then 50
when  round(sep_compliance_rt,2)>= 0.59 and round(sep_compliance_rt,2) < 0.63 then 75
when round(sep_compliance_rt,2)>= 0.63 and round(sep_compliance_rt,2) < 0.69 then 100
when  round(sep_compliance_rt,2)>= 0.69 and round(sep_compliance_rt,2) < 0.76 then 125
when  round(sep_compliance_rt,2) >= 0.76 then 150
else 0
end) as sep_compliance_rt_pts,

(case 
when tmi.fcy_num = 'MI2191' and max(tlu.lab_utlz) > 5.06 then 0
when tmi.fcy_num = 'MI2191' and max(tlu.lab_utlz)> 4.82 and max(tlu.lab_utlz) <= 5.06 then 50
when tmi.fcy_num = 'MI2191' and max(tlu.lab_utlz)> 4.72 and max(tlu.lab_utlz) <= 4.82 then 75
when tmi.fcy_num = 'MI2191' and max(tlu.lab_utlz)> 4.63 and max(tlu.lab_utlz) <= 4.72 then 100
when tmi.fcy_num = 'MI2191' and max(tlu.lab_utlz)> 4.53 and max(tlu.lab_utlz) <= 4.63 then 125
when tmi.fcy_num = 'MI2191' and max(tlu.lab_utlz)<=4.53 then 150

when tmi.fcy_num = 'MI2061' and max(tlu.lab_utlz) > 6.13 then 0
when tmi.fcy_num = 'MI2061' and max(tlu.lab_utlz)> 5.84 and max(tlu.lab_utlz) <= 6.13 then 50
when tmi.fcy_num = 'MI2061' and max(tlu.lab_utlz)> 5.72 and max(tlu.lab_utlz) <= 5.84 then 75
when tmi.fcy_num = 'MI2061' and max(tlu.lab_utlz)> 5.61 and max(tlu.lab_utlz) <= 5.72 then 100
when tmi.fcy_num = 'MI2061' and max(tlu.lab_utlz)> 5.49 and max(tlu.lab_utlz) <= 5.61 then 125
when tmi.fcy_num = 'MI2061' and max(tlu.lab_utlz)<=5.49 then 150

when tmi.fcy_num = 'MI2302'  and max(tlu.lab_utlz) > 5.76 then 0
when tmi.fcy_num = 'MI2302'  and max(tlu.lab_utlz)> 5.49 and max(tlu.lab_utlz) <= 5.76 then 50
when tmi.fcy_num = 'MI2302'  and max(tlu.lab_utlz)> 5.38 and max(tlu.lab_utlz) <= 5.49 then 75
when tmi.fcy_num = 'MI2302'  and max(tlu.lab_utlz)> 5.27 and max(tlu.lab_utlz) <= 5.38 then 100
when tmi.fcy_num = 'MI2302'  and max(tlu.lab_utlz)> 5.16 and max(tlu.lab_utlz) <= 5.27 then 125
when tmi.fcy_num = 'MI2302'  and max(tlu.lab_utlz)<=5.16 then 150


when tmi.fcy_num = 'MI5020'  and max(tlu.lab_utlz) > 6.22 then 0
when tmi.fcy_num = 'MI5020'  and max(tlu.lab_utlz)> 5.92 and max(tlu.lab_utlz) <= 6.22 then 50
when tmi.fcy_num = 'MI5020'  and max(tlu.lab_utlz)> 5.8 and max(tlu.lab_utlz) <= 5.92 then 75
when tmi.fcy_num = 'MI5020'  and max(tlu.lab_utlz)> 5.68 and max(tlu.lab_utlz) <= 5.8 then 100
when tmi.fcy_num = 'MI5020'  and max(tlu.lab_utlz)> 5.56 and max(tlu.lab_utlz) <= 5.68 then 125
when tmi.fcy_num = 'MI5020'  and max(tlu.lab_utlz)<=5.56 then 150

when tmi.fcy_num = 'MI2001'  and max(tlu.lab_utlz) > 5.23 then 0
when tmi.fcy_num = 'MI2001'  and max(tlu.lab_utlz)> 4.98 and max(tlu.lab_utlz) <= 5.23 then 50
when tmi.fcy_num = 'MI2001'  and max(tlu.lab_utlz)> 4.88 and max(tlu.lab_utlz) <= 4.98 then 75
when tmi.fcy_num = 'MI2001'  and max(tlu.lab_utlz)> 4.78 and max(tlu.lab_utlz) <= 4.88 then 100
when tmi.fcy_num = 'MI2001'  and max(tlu.lab_utlz)> 4.68 and max(tlu.lab_utlz) <= 4.78 then 125
when tmi.fcy_num = 'MI2001'  and max(tlu.lab_utlz)<=4.68 then 150

when tmi.fcy_num = 'MI2048'  and max(tlu.lab_utlz) > 7.06 then 0
when tmi.fcy_num = 'MI2048'  and max(tlu.lab_utlz)> 6.72 and max(tlu.lab_utlz) <= 7.06 then 50
when tmi.fcy_num = 'MI2048'  and max(tlu.lab_utlz)> 6.59 and max(tlu.lab_utlz) <= 6.72 then 75
when tmi.fcy_num = 'MI2048'  and max(tlu.lab_utlz)> 6.45 and max(tlu.lab_utlz) <= 6.59 then 100
when tmi.fcy_num = 'MI2048'  and max(tlu.lab_utlz)> 6.32 and max(tlu.lab_utlz) <= 6.45 then 125
when tmi.fcy_num = 'MI2048'  and max(tlu.lab_utlz)<= 6.32 then 150
--
when tmi.fcy_num = '637619'  and max(tlu.lab_utlz) > 6.97 then 0
when tmi.fcy_num = '637619'  and max(tlu.lab_utlz)> 6.64 and max(tlu.lab_utlz) <= 6.97 then 50
when tmi.fcy_num = '637619'  and max(tlu.lab_utlz)> 6.51 and max(tlu.lab_utlz) <= 6.64 then 75
when tmi.fcy_num = '637619'  and max(tlu.lab_utlz)> 6.37 and max(tlu.lab_utlz) <= 6.51 then 100
when tmi.fcy_num = '637619'  and max(tlu.lab_utlz)> 6.24 and max(tlu.lab_utlz) <= 6.37 then 125
when tmi.fcy_num = '637619'  and max(tlu.lab_utlz)<= 6.24 then 150

when tmi.fcy_num = 'MI2055'  and max(tlu.lab_utlz) > 4.94 then 0
when tmi.fcy_num = 'MI2055'  and max(tlu.lab_utlz)> 4.7 and max(tlu.lab_utlz) <= 4.94 then 50
when tmi.fcy_num = 'MI2055'  and max(tlu.lab_utlz)> 4.61 and max(tlu.lab_utlz) <= 4.7 then 75
when tmi.fcy_num = 'MI2055'  and max(tlu.lab_utlz)> 4.51 and max(tlu.lab_utlz) <= 4.61 then 100
when tmi.fcy_num = 'MI2055'  and max(tlu.lab_utlz)> 4.42 and max(tlu.lab_utlz) <= 4.51 then 125
when tmi.fcy_num = 'MI2055'  and max(tlu.lab_utlz)<=4.42 then 150

when tmi.fcy_num = '600816'  and max(tlu.lab_utlz) >5.62 then 0
when tmi.fcy_num = '600816'  and max(tlu.lab_utlz)> 5.35 and max(tlu.lab_utlz) <= 5.62 then 50
when tmi.fcy_num = '600816'  and max(tlu.lab_utlz)> 5.24 and max(tlu.lab_utlz) <= 5.35 then 75
when tmi.fcy_num = '600816'  and max(tlu.lab_utlz)> 5.14 and max(tlu.lab_utlz) <= 5.24 then 100
when tmi.fcy_num = '600816'  and max(tlu.lab_utlz)> 5.03 and max(tlu.lab_utlz) <= 5.14 then 125
when tmi.fcy_num = '600816'  and max(tlu.lab_utlz)<=5.03 then 150



else 0
end) as lab_utlz_pts,

(case 
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor) < 5.1 then 0
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 5.1 and max(pepm.prgny_pct_scor) <5.4 then 50
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 5.4 and max(pepm.prgny_pct_scor) < 5.6 then 75
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 5.6 and max(pepm.prgny_pct_scor) <5.7 then 100
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>= 5.7 and max(pepm.prgny_pct_scor) < 5.9 then 125
when tmi.fcy_num = 'MI2191' and max(pepm.prgny_pct_scor)>=5.9 then 150

when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor) < 48.4 then 0
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 48.4 and max(pepm.prgny_pct_scor) < 50.9 then 50
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 50.9 and max(pepm.prgny_pct_scor) < 52.4 then 75
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 52.4 and max(pepm.prgny_pct_scor) < 54 then 100
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>= 54 and max(pepm.prgny_pct_scor) < 56 then 125
when tmi.fcy_num = 'MI2061' and max(pepm.prgny_pct_scor)>=56 then 150

when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor) < 15.9 then 0
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 15.9 and max(pepm.prgny_pct_scor) < 16.7 then 50
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 16.7 and max(pepm.prgny_pct_scor) < 17.2 then 75
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 17.2 and max(pepm.prgny_pct_scor) < 17.7 then 100
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>= 17.7 and max(pepm.prgny_pct_scor) < 18.4 then 125
when tmi.fcy_num = 'MI2302'  and max(pepm.prgny_pct_scor)>=18.4 then 150


when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor) <= 29.3 then 0
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 29.3 and max(pepm.prgny_pct_scor) <= 30.8 then 50
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 30.8 and max(pepm.prgny_pct_scor) <= 31.7 then 75
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 31.7 and max(pepm.prgny_pct_scor) <= 32.6 then 100
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)> 32.6 and max(pepm.prgny_pct_scor) <= 33.9 then 125
when tmi.fcy_num = 'MI5020'  and max(pepm.prgny_pct_scor)>33.9 then 150

when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor) <22 then 0
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 22 and max(pepm.prgny_pct_scor) < 23.2 then 50
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 23.2 and max(pepm.prgny_pct_scor) < 23.9 then 75
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 23.9 and max(pepm.prgny_pct_scor) < 24.6 then 100
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 24.6 and max(pepm.prgny_pct_scor) < 25.5 then 125
when tmi.fcy_num = 'MI2001'  and max(pepm.prgny_pct_scor)>= 25.5 then 150

when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor) <18.7 then 0
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 18.7 and max(pepm.prgny_pct_scor) < 19.7 then 50
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 19.7 and max(pepm.prgny_pct_scor) < 20.3 then 75
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 20.3 and max(pepm.prgny_pct_scor) < 20.9 then 100
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 20.9 and max(pepm.prgny_pct_scor) < 21.7 then 125
when tmi.fcy_num = 'MI2048'  and max(pepm.prgny_pct_scor)>= 21.7 then 150
--
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor) < 50.2 then 0
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 50.2 and max(pepm.prgny_pct_scor) < 52.8 then 50
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 52.8 and max(pepm.prgny_pct_scor) < 54.4 then 75
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 54.4 and max(pepm.prgny_pct_scor) < 56 then 100
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>= 56 and max(pepm.prgny_pct_scor) < 58.1 then 125
when tmi.fcy_num = '637619'  and max(pepm.prgny_pct_scor)>=58.1 then 150

when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor) <33.7 then 0
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 33.7 and max(pepm.prgny_pct_scor) < 35.5 then 50
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 35.5 and max(pepm.prgny_pct_scor) < 36.6 then 75
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 36.6 and max(pepm.prgny_pct_scor) < 37.6 then 100
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 37.6 and max(pepm.prgny_pct_scor) < 39.1 then 125
when tmi.fcy_num = 'MI2055'  and max(pepm.prgny_pct_scor)>= 39.1 then 150

when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor) < 21.4 then 0
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 21.4 and max(pepm.prgny_pct_scor) < 22.5 then 50
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 22.5 and max(pepm.prgny_pct_scor) < 23.2 then 75
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 23.2 and max(pepm.prgny_pct_scor) < 23.9 then 100
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>= 23.9 and max(pepm.prgny_pct_scor) < 24.8 then 125
when tmi.fcy_num = '600816'  and max(pepm.prgny_pct_scor)>=24.8 then 150



else 0
end) as prgny_pct_scor_pts,
(case 
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3) < 0.718 then 0
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.718 and round(max(tpep.ptnt_exrnc_scor),3) < 0.756 then 50
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.756 and round(max(tpep.ptnt_exrnc_scor),3) < 0.76 then 75
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.76 and round(max(tpep.ptnt_exrnc_scor),3) < 0.764 then 100
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.764 and round(max(tpep.ptnt_exrnc_scor),3) < 0.767 then 125
when tmi.fcy_num = 'MI2191' and round(max(tpep.ptnt_exrnc_scor),3)>=0.767 then 150

when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3) < 0.741 then 0
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.741 and round(max(tpep.ptnt_exrnc_scor),3) < 0.78 then 50
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.78 and round(max(tpep.ptnt_exrnc_scor),3) < 0.784 then 75
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.784 and round(max(tpep.ptnt_exrnc_scor),3) < 0.788 then 100
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>= 0.788 and round(max(tpep.ptnt_exrnc_scor),3) < 0.792 then 125
when tmi.fcy_num = 'MI2061' and round(max(tpep.ptnt_exrnc_scor),3)>=0.792 then 150

when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.7 then 0
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.7 and round(max(tpep.ptnt_exrnc_scor),3) < 0.737 then 50
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.737 and round(max(tpep.ptnt_exrnc_scor),3) < 0.741 then 75
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.741 and round(max(tpep.ptnt_exrnc_scor),3) < 0.744 then 100
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.744 and round(max(tpep.ptnt_exrnc_scor),3) < 0.748 then 125
when tmi.fcy_num = 'MI2302'  and round(max(tpep.ptnt_exrnc_scor),3)>=0.748 then 150


when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.681 then 0
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.681 and round(max(tpep.ptnt_exrnc_scor),3) < 0.717 then 50
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.717 and round(max(tpep.ptnt_exrnc_scor),3) < 0.721 then 75
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.721 and round(max(tpep.ptnt_exrnc_scor),3) < 0.724 then 100
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.724 and round(max(tpep.ptnt_exrnc_scor),3) < 0.728 then 125
when tmi.fcy_num = 'MI5020'  and round(max(tpep.ptnt_exrnc_scor),3)>=0.728 then 150

when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3) <0.753 then 0
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.753 and round(max(tpep.ptnt_exrnc_scor),3) < 0.793 then 50
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.793 and round(max(tpep.ptnt_exrnc_scor),3) < 0.797 then 75
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.797 and round(max(tpep.ptnt_exrnc_scor),3) < 0.801 then 100
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.801 and round(max(tpep.ptnt_exrnc_scor),3) < 0.805 then 125
when tmi.fcy_num = 'MI2001'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.805 then 150

when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3) <0.764 then 0
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.764 and round(max(tpep.ptnt_exrnc_scor),3) < 0.804 then 50
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.804 and round(max(tpep.ptnt_exrnc_scor),3) < 0.808 then 75
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.808 and round(max(tpep.ptnt_exrnc_scor),3) < 0.812 then 100
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.812 and round(max(tpep.ptnt_exrnc_scor),3)< 0.816 then 125
when tmi.fcy_num = 'MI2048'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.816 then 150
--
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.751 then 0
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.751 and round(max(tpep.ptnt_exrnc_scor),3) < 0.79 then 50
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.79 and round(max(tpep.ptnt_exrnc_scor),3) < 0.794 then 75
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.794 and round(max(tpep.ptnt_exrnc_scor),3) < 0.798 then 100
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.798 and round(max(tpep.ptnt_exrnc_scor),3) < 0.802 then 125
when tmi.fcy_num = '637619'  and round(max(tpep.ptnt_exrnc_scor),3)>=0.802 then 150

when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3) <0.783 then 0
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.783 and round(max(tpep.ptnt_exrnc_scor),3) < 0.824 then 50
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.824 and round(max(tpep.ptnt_exrnc_scor),3) < 0.828 then 75
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.828 and round(max(tpep.ptnt_exrnc_scor),3) < 0.832 then 100
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.832 and round(max(tpep.ptnt_exrnc_scor),3) < 0.836 then 125
when tmi.fcy_num = 'MI2055'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.836 then 150

when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3) < 0.799 then 0
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3) >= 0.799 and round(max(tpep.ptnt_exrnc_scor),3) < 0.841 then 50
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.841 and round(max(tpep.ptnt_exrnc_scor),3) < 0.845 then 75
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.845 and round(max(tpep.ptnt_exrnc_scor),3) < 0.849 then 100
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>= 0.849 and round(max(tpep.ptnt_exrnc_scor),3) < 0.854 then 125
when tmi.fcy_num = '600816'  and round(max(tpep.ptnt_exrnc_scor),3)>=0.854 then 150



else 0
end) as ptnt_exrnc_scor_pts
,
case when tmi.rpt_dt = '2019-06-01 00:00:00' then 1
when tmi.rpt_dt = '2019-07-01 00:00:00' then 2
when tmi.rpt_dt='2019-08-01 00:00:00' then 3
when tmi.rpt_dt='2019-09-01 00:00:00' then 4
when tmi.rpt_dt='2019-10-01 00:00:00' then 5
when tmi.rpt_dt='2019-11-01 00:00:00' then 6
when tmi.rpt_dt='2019-12-01 00:00:00' then 7
when tmi.rpt_dt='2020-01-01 00:00:00' then 8
when tmi.rpt_dt='2020-02-01 00:00:00' then 9
when tmi.rpt_dt='2020-03-01 00:00:00' then 10
when tmi.rpt_dt='2020-04-01 00:00:00' then 11
when tmi.rpt_dt='2020-05-01 00:00:00' then 12
else null
end as no_of_mnths,

(case 
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt) > 89 then 0
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)> 85 and max(thef.harm_events_cnt) <= 89 then 50
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)> 73 and max(thef.harm_events_cnt) <= 85 then 75
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)> 68 and max(thef.harm_events_cnt) <= 73 then 100
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)> 62 and max(thef.harm_events_cnt) <= 68 then 125
when tmi.fcy_num = 'MI2191' and max(thef.harm_events_cnt)<=62 then 150

when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt) >5 then 0
when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 4 and max(thef.harm_events_cnt) <= 5 then 50
--when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 0.78 and max(thef.harm_events_cnt) <= 0.784 then 75
--when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 0.784 and max(thef.harm_events_cnt) <= 0.788 then 100
--when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)> 0.788 and max(thef.harm_events_cnt) <= 0.792 then 125
when tmi.fcy_num = 'MI2061' and max(thef.harm_events_cnt)<=4 then 150

when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt) >130 then 0
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 123 and max(thef.harm_events_cnt) <= 130 then 50
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 106 and max(thef.harm_events_cnt) <= 123 then 75
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 98 and max(thef.harm_events_cnt) <= 106 then 100
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)> 91 and max(thef.harm_events_cnt) <= 98 then 125
when tmi.fcy_num = 'MI2302'  and max(thef.harm_events_cnt)<=91 then 150


when tmi.fcy_num = 'MI5020'  and (round((max(thef.harm_events_cnt)/no_of_mnths),0)*12) >50 then 0
when tmi.fcy_num = 'MI5020'  and (round((max(thef.harm_events_cnt)/no_of_mnths),0)*12) > 47 and (round((max(thef.harm_events_cnt)/no_of_mnths),0)*12) <= 50 then 50
when tmi.fcy_num = 'MI5020'  and (round((max(thef.harm_events_cnt)/no_of_mnths),0)*12) > 41 and (round((max(thef.harm_events_cnt)/no_of_mnths),0)*12) <= 47 then 75
when tmi.fcy_num = 'MI5020'  and (round((max(thef.harm_events_cnt)/no_of_mnths),0)*12) > 38 and (round((max(thef.harm_events_cnt)/no_of_mnths),0)*12) <= 41 then 100
when tmi.fcy_num = 'MI5020'  and (round((max(thef.harm_events_cnt)/no_of_mnths),0)*12) > 35 and (round((max(thef.harm_events_cnt)/no_of_mnths),0)*12) <= 38 then 125
when tmi.fcy_num = 'MI5020'  and (round((max(thef.harm_events_cnt)/no_of_mnths),0)*12) <=35 then 150

when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt) >22 then 0
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 20 and max(thef.harm_events_cnt) <= 22 then 50
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 18 and max(thef.harm_events_cnt) <= 20 then 75
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 16 and max(thef.harm_events_cnt) <= 18 then 100
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)> 15 and max(thef.harm_events_cnt) <= 16 then 125
when tmi.fcy_num = 'MI2001'  and max(thef.harm_events_cnt)<=15 then 150
--
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt) > 53 then 0
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 50 and max(thef.harm_events_cnt) <= 53 then 50
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 43 and max(thef.harm_events_cnt) <= 50 then 75
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 40 and max(thef.harm_events_cnt) <= 43 then 100
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)> 37 and max(thef.harm_events_cnt) <= 40 then 125
when tmi.fcy_num = 'MI2048'  and max(thef.harm_events_cnt)<= 37 then 150
--
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt) > 39 then 0
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 37 and max(thef.harm_events_cnt) <= 39 then 50
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 32 and max(thef.harm_events_cnt) <= 37 then 75
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 29 and max(thef.harm_events_cnt) <= 32 then 100
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)> 27 and max(thef.harm_events_cnt) <= 29 then 125
when tmi.fcy_num = '637619'  and max(thef.harm_events_cnt)<= 27 then 150
--
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt) > 24 then 0
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 22 and max(thef.harm_events_cnt) <= 24 then 50
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 19 and max(thef.harm_events_cnt) <= 22 then 75
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 18 and max(thef.harm_events_cnt) <= 19 then 100
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)> 16 and max(thef.harm_events_cnt) <= 18 then 125
when tmi.fcy_num = 'MI2055'  and max(thef.harm_events_cnt)<=16 then 150
--
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt) > 38 then 0
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 36 and max(thef.harm_events_cnt) <= 38 then 50
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 31 and max(thef.harm_events_cnt) <= 36 then 75
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 29 and max(thef.harm_events_cnt) <= 31 then 100
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)> 26 and max(thef.harm_events_cnt) <= 29 then 125
when tmi.fcy_num = '600816'  and max(thef.harm_events_cnt)<= 26 then 150



else 0
end) as harm_events_pts
,

(case 
when  max(tzef.zero_events_rt) <= 0.75 then 0
when  max(tzef.zero_events_rt)> 0.75 and max(tzef.zero_events_rt) <= 0.82 then 50
when  max(tzef.zero_events_rt)> 0.82 and max(tzef.zero_events_rt) <= 0.86 then 75
when  max(tzef.zero_events_rt)> 0.86 and max(tzef.zero_events_rt) <= 0.9 then 100
when  max(tzef.zero_events_rt)> 0.9 and max(tzef.zero_events_rt) <= 0.95 then 125
when  max(tzef.zero_events_rt)>0.95 then 150
else 0
end
) as zero_events_rt_pts,

case when harm_events_pts > zero_events_rt_pts then harm_events_pts else zero_events_rt_pts end as harm_zero_events_pts,

case when readm_obs_pts > readm_oe_pts then readm_obs_pts else readm_oe_pts end as readm_obs_oe_pts,




((nvl(mort_oe_pts,0)*0.1)+(nvl(comp_oe_rt_pts,0)*0.1)+(nvl(harm_zero_events_pts,0)*0.15)+(nvl(readm_obs_oe_pts,0)*0.15)+ (nvl(sep_mort_oe_pts,0)*0.055)+(nvl(sep_compliance_rt_pts,0)*0.055)
+( nvl(ob_training_pts,0)*0.10)+(nvl(lab_utlz_pts,0)*0.1)+(nvl(prgny_pct_scor_pts,0)*0.05)+(nvl(cardiac_rehab_pts,0)*0.03)+(max(nvl(acof.mpp_scr,0))*0.08)   +(nvl(ptnt_exrnc_scor_pts,0)*0.03)) as weighted_pts,

case when tmi.fcy_num = 'MI2191' then 1
when tmi.fcy_num = 'MI2061' then 0.97
when tmi.fcy_num = 'MI2302' then 1
when tmi.fcy_num = 'MI5020' then 1
when tmi.fcy_num = 'MI2001' then 0.97
when tmi.fcy_num = 'MI2048' then 1
when tmi.fcy_num = '637619' then 1
when tmi.fcy_num = 'MI2055' then 0.87
when tmi.fcy_num = '600816' then 1
else 1
end as weight,

weighted_pts/weight as clncl_outc_scor


from 
pce_qe16_prd..stg_tmp_mrtly_ind tmi
left join pce_qe16_prd..stg_pqsd_cmplc_idnx_fct pcf on tmi.rpt_dt = date_trunc('month',pcf.end_of_month) and tmi.fcy_num = pcf.fcy_num and pcf.msr_nm = 'Compl_R12M'
left join pce_qe16_prd..stg_tmp_readm_ind tri on tmi.rpt_dt = tri.rpt_dt and tmi.fcy_num=tri.fcy_num
left join pce_qe16_prd..stg_tmp_lab_utlz_fct tlu on tmi.rpt_dt = date(tlu.rpt_dt) and tmi.fcy_num = tlu.fcy_num
left join pce_qe16_prd..stg_tmp_card_rehab_fct tcrf on tmi.rpt_dt = tcrf.rpt_dt and tmi.fcy_num = tcrf.fcy_num
left join pce_qe16_prd..stg_tmp_ob_trn_fct tot on tmi.rpt_dt = tot.rpt_dt and tmi.fcy_num = tot.fcy_num
left join pce_qe16_prd..stg_tmp_sep_mrtly_ind tsm on tmi.rpt_dt = tsm.rpt_dt and tmi.fcy_num = tsm.fcy_num
left join pce_qe16_prd..stg_tmp_sep_compl_fct tscf on tmi.rpt_dt = tscf.rpt_dt and tmi.fcy_num = tscf.fcy_num
left join pce_qe16_prd..aco_mpp_msr_fct acof on tmi.rpt_dt = date_trunc('month',acof.rpt_prd_end_dt) and tmi.fcy_num = acof.fcy_num
left join pce_qe16_prd..stg_TMP_HARM_EVENTS_FCT thef on tmi.rpt_dt = thef.rpt_dt and tmi.fcy_num = thef.fcy_num
left join pce_qe16_prd..stg_tmp_zero_events_fct tzef on tmi.rpt_dt = tzef.rpt_dt and tmi.fcy_num = tzef.fcy_num
LEFT JOIN pce_qe16_prd..stg_tmp_ptnt_exrnc_pct_msr_fct pepm on tmi.rpt_dt = date_trunc('month',pepm.rprt_dt) and tmi.fcy_num = pepm.fcy_num
left join pce_qe16_prd..stg_tmp_mmg_ovrl_ptnt_exrnc_msr_fct tpep on tmi.rpt_dt = date_trunc('month',tpep.rprt_dt) and tmi.fcy_num = tpep.fcy_num

group by 1,2,3

 );
 
 
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


drop table pce_qe16_prd..stg_card_rhb_movm_fct if exists; 
create table pce_qe16_prd..stg_card_rhb_movm_fct as
(Select distinct tcrb.*, cd.mo_and_yr_abbr as rpt_mnth
from TMP_CARDIAC_REHAB_BASE tcrb
inner join pce_qe16_prd..cdr_dim cd on tcrb.timeframecode = replace(cd.qtr_and_yr_abbr, ' ','')); 

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

create table pce_qe16_prd..stg_mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct as 
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
  
  drop table pce_qe16_prd..stg_ptnt_exrnc_pct_msr_fct_MNTH_OVER_MNTH if exists;
 create table pce_qe16_prd..stg_ptnt_exrnc_pct_msr_fct_MNTH_OVER_MNTH AS 
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
top_box_resp_cnt/resp_cnt as ptnt_exrnc_pct

FROM tmp_sf tsf
where varname in ('CMS_24','OSC_24','F4')
group by 1,2,3,4
) as q
left join pce_qe16_pressganey_prd_zoom..prgny_ptnt_exrnc_pct_dim ppep on q.service = ppep.svc_cd and q.ptnt_exrnc_pct = ppep.scor

 
);


 
 
 
