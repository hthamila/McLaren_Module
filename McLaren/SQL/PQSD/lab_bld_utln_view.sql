create or replace view lab_bld_utlzn
as
with temp_lab as 
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
)
,
temp_lab_ptnt as 
(select 
p.company_id
,p.patient_id
,to_date(b.postdate,'mmddyyyy') as post_dt
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
group by  
p.company_id
,p.patient_id
,post_dt
,inptnt_ind
)
--------------------union of facilities-------------------------------
 select
	 tp.* 
	,tp.company_id as f_nm
	,case tp.company_id when 'Bay' then 'MI2191' when 'Central' then 'MI2061'  when 'Flint' then  'MI2302' when 'Karmanos' then '634342' when 'Lansing' then 'MI5020' when 'Lapeer' then 'MI2001'
     when 'Macomb' then 'MI2048' when 'Northern' then '637619' when 'Oakland' then 'MI2055' when 'Port Huron' then '600816' else null end as f_num
	from temp_lab_ptnt  tp
	union 
	select tp1.* 
	,'McLaren Health Care' as f_nm    
	,'McLaren' as f_num
	from temp_lab_ptnt tp1
	union
	select tp2.* 
	,'McLaren Health Care (excl K)' as f_nm
	,'McLaren (excl))' as f_num
	from temp_lab_ptnt  tp2
	where tp2.company_id <> 'Karmanos'
	;
