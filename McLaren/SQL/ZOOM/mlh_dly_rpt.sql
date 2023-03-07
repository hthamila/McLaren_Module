select DATE_PART('YEAR',COALESCE(discharge_ts, admission_ts))
, pd.company_id
, pd.patient_id
, pd.inpatient_outpatient_flag
, primary_payer_code
, payer_description
, discharge_total_charges
, patbill_charges
from cv_patdisch pd
inner join
	(select b.company_id, b.patient_id, sum(total_charge) patbill_charges 
		from cv_patbill b
		join cv_patdisch using (company_id, patient_id)
	 group by 1,2)a 
on pd.company_id=a.company_id 
and pd.patient_id=a.patient_id
left join cv_paymstr pm 
on pd.company_id=pm.company_id and pd.primary_payer_code=pm.payer_code
where discharge_total_charges>0 
  and DATE_PART('YEAR',COALESCE(discharge_ts, admission_ts)) >=2019 
  and discharge_total_charges <> patbill_charges;
