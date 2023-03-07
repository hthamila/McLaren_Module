\set ON_ERROR_STOP ON;

insert into slp_audt_bal_cntl
(
run_dt_dk,
src,
tgt,
fcy_nm,
in_or_out_patient_ind,
zoom_total_cases,
zoom_dschrg_tot_chrg_amt,
zoom_total_charge,
slp_total_cases,
slp_dschrg_tot_chrg_amt,
slp_total_charge
)

select 
to_char(now(),'yyyymmddhh') as run_dt_dk,
src,
tgt,
fcy_nm,
in_or_out_patient_ind,
zoom_total_cases,
zoom_dschrg_tot_chrg_amt,
zoom_total_charge,
slp_total_cases,
slp_dschrg_tot_chrg_amt,
slp_total_charge
from 
(
with zoom_chrg as
(
select company_id, patient_id, sum(total_charge) as total_charge
from pce_qe16_oper_prd_zoom..cv_patbill
group by company_id, patient_id
)
select 'ZOOM' as src, company_id as fcy_nm, --patient_id as encntr_num,
trim(inpatient_outpatient_flag) as in_or_out_patient_ind, 
count(1) as zoom_total_cases, 
sum(discharge_total_charges) as zoom_dschrg_tot_chrg_amt,
sum(total_charge) as zoom_total_charge
from pce_qe16_oper_prd_zoom..cv_patdisch 
	left join zoom_chrg using (company_id,patient_id)
where (inpatient_outpatient_flag='I' and (cast(admission_ts AS DATE) BETWEEN DATE('2015-10-01') AND date(now()-1) OR cast(discharge_ts AS DATE) BETWEEN DATE('2015-10-01') AND date(now()-1)))
		OR 
	(inpatient_outpatient_flag='O' and (cast(admission_ts AS DATE) BETWEEN DATE('2015-10-01') AND date(now()-1)))
group by company_id, inpatient_outpatient_flag
)b 

inner join

--SLP Total cases Count
(
with slp_chrg as
(
select encntr_num as encntr_num, fcy_nm as fcy_nm, sum(total_charge) as total_charge
from pce_qe16_slp_prd_stg..prd_chrg_fct group by 
encntr_num, fcy_nm
)

select 'SLP' as tgt, fcy_nm, 
trim(in_or_out_patient_ind) as in_or_out_patient_ind,
count(1) as slp_total_cases, 
sum(dschrg_tot_chrg_amt) as slp_dschrg_tot_chrg_amt,
sum(total_charge) as slp_total_charge
from pce_qe16_slp_prd_stg..prd_encntr_anl_fct
	left join slp_chrg using (encntr_num, fcy_nm)
where (in_or_out_patient_ind='I' AND (cast(adm_ts AS DATE) BETWEEN DATE('2015-10-01') AND date(now()-1) OR cast(src_dschrg_ts AS DATE) BETWEEN DATE('2015-10-01') AND date(now()-1)))
		OR
	  (in_or_out_patient_ind='O' AND (cast(adm_ts AS DATE) BETWEEN DATE('2015-10-01') AND date(now()-1)))
group by fcy_nm, in_or_out_patient_ind)a using (fcy_nm, in_or_out_patient_ind)

;
GENERATE STATISTICS ON slp_audt_bal_cntl;
\unset ON_ERROR_STOP
