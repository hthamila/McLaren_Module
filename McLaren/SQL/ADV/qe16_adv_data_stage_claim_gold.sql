
--pce_qe16_slp_prd_dm
drop view pce_qe16_slp_prd_stg..stage_claim;
create view pce_qe16_slp_prd_stg..stage_claim as 
SELECT
	prd_encntr_anl_fct.encntr_num as Patient_Account_Number,
	prd_encntr_anl_fct.medical_record_number as Patient_Med_Rec_No,
	prd_encntr_anl_fct.dschrg_dt as Discharge_Date,
	prd_encntr_anl_fct.adm_dt as Check_in_Date,
	prd_encntr_anl_fct.qadv_prim_pyr_cd as Primary_Payor_Code,
	prd_encntr_anl_fct.qadv_prim_pyr_descr as Primary_Payor_Name,
	prd_encntr_anl_fct.acct_bal_amt as ZB_Charges,
	prd_encntr_anl_fct.dschrg_tot_chrg_amt as Charges,
    cast(null as int) as Coverage_Ratio,
	prd_encntr_anl_fct.brth_dt as Patient_Date_of_Birth,
	prd_encntr_anl_fct.ptnt_zip_cd as Pat_Home_Zip_Code,
	prd_encntr_anl_fct.apr_cd as APR_DRG,
	prd_encntr_anl_fct.fnc_cls as Financial_Class,
	prd_encntr_anl_fct.fnc_cls_descr as Financial_Class_Desc,
	prd_encntr_anl_fct.src_prim_payor_grp1,
	prd_encntr_anl_fct.src_prim_payor_grp2,
	prd_encntr_anl_fct.src_prim_payor_grp3,
	prd_encntr_anl_fct.pnt_of_orig_cd as Check_in_Source,
	prd_encntr_anl_fct.est_net_rev_amt as TOTAL_PAYMENTS,
	prd_encntr_anl_fct.ed_case_ind as ED_Non_ED,
	'Netezza pull 1' as src_file_name,
	cast(CURRENT_TIMESTAMP as datetime) as src_file_load_dt_tm,
	'Andy Allaway' as src_file_loaded_by,
	cast(1 as int) as src_file_id,
	cast(null as varchar(255)) as primary_cpt_hcpcs,
	prd_encntr_anl_fct.agg_rcc_based_indirect_cst_amt as TOT_FIX_CST_AMT,
	prd_encntr_anl_fct.agg_rcc_based_direct_cst_amt as TOT_VAR_CST_AMT,
	prd_encntr_anl_fct.fcy_nm as Discharge_Campus,
	cast(null as varchar(255)) as service_category,
	prd_encntr_anl_fct.in_or_out_patient_ind as ip_op,
	prd_encntr_anl_fct.ms_drg_cd as ms_drg,
	prd_encntr_anl_fct.adm_tp_cd as admit_type,
	cast(null as varchar(255)) as enc_type_cd,
	cast(null as varchar(255)) as ed_flag,
	prd_encntr_anl_fct.obsrv_stay_ind as observation_flag,
	year(prd_encntr_anl_fct.dschrg_dt) as dc_year,
	cast(null as varchar(255)) as outlier_flag,
	cast(null as varchar(255)) as zero_balance,
	cast(null as varchar(255)) as ms_drg_bus_line,
    cast(null as varchar(255)) as ms_drg_med_surg,
    cast(null as varchar(255)) as ms_drg_desc,
    cast(null as varchar(255)) as IP_McLaren_Service_Line,
    cast(null as varchar(255)) as Payor_Class,
    cast(1 as int) as id,
    case 
    when UPPER(ptnt_tp_cd) in ('BSCH','BSCHO','NB') then 1
    when UPPER(qadv_prim_pyr_cd)  in ('SELECT','SELEC') then 1
    when UPPER(src_prim_payor_grp3) in ('HOSPICE') then 1
    when UPPER(dschrg_svc) in ('NB','NBN','OIN','SCN','L1N','BBN','NURS') then 1 else 0 end as exclusion,
    cast(null as varchar(255)) ed_flag_ref,
    prd_encntr_anl_fct.expt_pymt_amt,
    prd_encntr_anl_fct.est_net_rev_amt,
    prd_encntr_anl_fct.attrb_physcn_nm,
    prd_encntr_anl_fct.attrb_physn_npi,
    prd_encntr_anl_fct.attrb_physcn_spcl_cd_descr,
    prd_encntr_anl_fct.ptnt_tp_cd as Patient_Type_Code,
    prd_encntr_anl_fct.dschrg_svc as Discharge_Service_Code,
    prd_encntr_anl_fct.e_svc_ln_nm as e_svc_ln_nm,
    prd_encntr_anl_fct.prim_pcd_cd,
    prd_encntr_anl_fct.prim_pcd_descr,
    prd_encntr_anl_fct.cal_svc_ln, --newlly added 2/10/2020
    prd_encntr_anl_fct.cal_sub_svc_ln, 
    prd_encntr_anl_fct.cal_svc_nm
        
FROM
	pce_qe16_slp_prd_dm.prmradmp.prd_encntr_anl_fct prd_encntr_anl_fct
WHERE
	year(prd_encntr_anl_fct.dschrg_dt)>=2016
	and prd_encntr_anl_fct.tot_chrg_ind=1
	and exclusion=0;


--select max(rcrd_isrt_ts) from pce_qe16_slp_prd_dm.prmradmp.prod_encntr_anl_fct prd_encntr_anl_fct

--pce_qe16_prd_ct
drop table aallaway.stage_claim_gold if exists;
create table aallaway.stage_claim_gold as 
SELECT distinct patient_account_number, patient_med_rec_no, discharge_date, check_in_date, primary_payor_code, primary_payor_name, zb_charges, charges, coverage_ratio, patient_date_of_birth, pat_home_zip_code, apr_drg, financial_class, financial_class_desc, src_prim_payor_grp1, src_prim_payor_grp2, src_prim_payor_grp3, check_in_source, total_payments, ed_non_ed, src_file_name, src_file_load_dt_tm, src_file_loaded_by, src_file_id, primary_cpt_hcpcs, tot_fix_cst_amt, tot_var_cst_amt, discharge_campus, service_category, ip_op, ms_drg, admit_type, enc_type_cd, ed_flag, observation_flag, dc_year, outlier_flag, zero_balance, ms_drg_bus_line, ms_drg_med_surg, ms_drg_desc, ip_mclaren_service_line, payor_class, id, exclusion, ed_flag_ref, expt_pymt_amt, est_net_rev_amt, attrb_physcn_nm, attrb_physn_npi, attrb_physcn_spcl_cd_descr, patient_type_code, discharge_service_code, e_svc_ln_nm, prim_pcd_cd, prim_pcd_descr, cal_svc_ln, cal_sub_svc_ln, cal_svc_nm 
from pce_qe16_slp_prd_stg..stage_claim;

--new 2/10/2021 - dedupe encs
delete from aallaway.stage_claim_gold where patient_account_number in(
select patient_account_number from aallaway.stage_claim_gold 
group by patient_account_number
having count(*) >1
);

alter table stage_claim_gold add column service_family varchar(255);

--select patient_account_number, check_in_date, discharge_date, count(1) from stage_claim_gold group by patient_account_number,check_in_date,discharge_date having count(1) > 1;



update stage_claim_gold a
SET service_family = b.service_family
FROM 
(
select s.patient_account_number, s.check_in_date, s.discharge_date, s.ip_op, s.ms_drg_med_surg, s.attrb_physcn_nm,
s.primary_cpt_hcpcs,s.prim_pcd_cd,s.ms_drg,

CASE
WHEN s.ip_op = 'O' 
AND h.svc_ln is null
THEN s.service_category
when s.ip_op = 'O'
AND h.svc_ln is not null
THEN h.svc_ln

WHEN s.ip_op = 'I' and s.prim_pcd_cd not in ('-100') 
and i.svc_ln is not null THEN i.svc_ln
else msdrg.ms_drg_fmly_descr

END as service_family

from stage_claim_gold s

left join pce_qe16_prd_ct.aallaway.dictionary_msdrg msdrg
on cast(s.ms_drg as int) = msdrg.ms_drg_cd

left join pce_qe16_prd_ct.rwidmaye.dictionary_service_line_hierarcy_final h --cpt codes for OP
on h.cd = s.primary_cpt_hcpcs

left join pce_qe16_prd_ct.rwidmaye.dictionary_service_line_hierarcy_final i --proc codes for surg ip
on i.cd = s.prim_pcd_cd

)b
WHERE 
a.Patient_Account_Number =b.patient_account_number
and
a.check_in_date=b.check_in_date 
and
a.discharge_date=b.discharge_date;
--and a.Patient_Account_Number not in('70000001326387','70000001190289','14102274','1002033711')

groom table stage_claim_gold versions;

alter table stage_claim_gold add column kpi_category varchar(255);

update stage_claim_gold a
SET kpi_category= b.kpi_category
FROM 
(
select
s.patient_account_number, s.check_in_date, s.discharge_date, s.ip_op,
case 
when observation_flag = 1 and ip_op = 'O' then 'Observation'
when ip_op = 'I' then 'Admits'
when ip_op = 'O' AND 
     ED_Non_ED = 1
THEN 'ER Visits'
else
'OP Reg'
end as kpi_category

from aallaway.stage_claim_gold s
)b

WHERE 
a.Patient_Account_Number =b.patient_account_number
and
a.check_in_date=b.check_in_date 
and
a.discharge_date=b.discharge_date;

alter table stage_claim_gold add column kpi_category2 varchar(255);

update stage_claim_gold a
SET kpi_category2= b.kpi_category2
FROM
(
select
distinct s.encntr_num, s.adm_dt, s.dschrg_dt, s.in_or_out_patient_ind,s.fcy_nm,
case
when s.dschrg_nbrn_ind = '1'THEN 'Births'
when s.srgl_case_ind = '1' and in_or_out_patient_ind = 'O' then 'OP Surgeries'
when s.srgl_case_ind = '1' and in_or_out_patient_ind = 'I' then 'IP Surgeries'
when cathlab_case_ind = '1' then 'Cath'
else NULL
end as kpi_category2
from pce_qe16_slp_prd_dm.prmretlp.prd_encntr_anl_fct s
)b
WHERE
a.Patient_Account_Number =b.encntr_num
and
a.discharge_campus = b.fcy_nm
and
a.check_in_date=b.adm_dt
and
a.discharge_date=b.dschrg_dt;

--pce_qe16_prd_ct
drop table aallaway.stage_billing_gold if exists;
create table aallaway.stage_billing_gold as 
    select * from pce_qe16_slp_prd_dm..prd_chrg_fct
    where patient_id in (select distinct patient_account_number from stage_claim_gold);
--DISTRIBUTE ON (src_patient_id,rec_num)

drop TABLE stage_billing_cpt_soft_hard_gold_cost if exists;
CREATE TABLE stage_billing_cpt_soft_hard_gold_cost  ( 
	src_patient_id	nvarchar(25) NULL,
	service_date  	date NULL,
	cpt_code      	nvarchar(20) NULL,
	src_company_id	nvarchar(25) NULL, 
	source_of_data	nvarchar(25) NULL,
	surg   nvarchar(25) NULL,
	surgical_non_surgical_cpt   nvarchar(25) NULL,
	total_charge float
	)
DISTRIBUTE ON (src_company_id,src_patient_id) 
;

insert into stage_billing_cpt_soft_hard_gold_cost (src_patient_id,service_date,cpt_code,src_company_id,source_of_data,total_charge)
(
select 
charge.patient_id,
charge.service_date,
charge.cpt_code,
charge.company_id,
'prd_chrg_fct' as source_of_data
,sum(charge.total_charge) as total_charge

from
pce_qe16_slp_prd_dm..prd_chrg_fct charge
where 
patient_id in ( select distinct patient_id from stage_billing_gold) and
cpt_code not in ('-100') 
--and patient_id='70000001175251'

group by 
charge.patient_id,
charge.service_date,
charge.cpt_code,
charge.company_id,
'prd_chrg_fct'
);

--81,755,505

insert into stage_billing_cpt_soft_hard_gold_cost (src_patient_id,service_date,cpt_code,src_company_id,source_of_data)
--select count(*) from
(
select distinct
prd_cpt_fct.patient_id,
get_charge_service_dt.service_date,
prd_cpt_fct.cpt_code,
prd_cpt_fct.company_id,
'prd_cpt_fct' as source_of_data
  
from
pce_qe16_slp_prd_dm..prd_cpt_fct
left join (
select distinct
patient_id,
min(service_date) as service_date
from
pce_qe16_slp_prd_dm..prd_chrg_fct
group by patient_id) get_charge_service_dt
on 
prd_cpt_fct.patient_id=get_charge_service_dt.patient_id
where 
prd_cpt_fct.patient_id in ( select distinct patient_id from stage_billing_gold) and
prd_cpt_fct.cpt_code not in ('-100') 
--and prd_cpt_fct.patient_id='000200755567'
and prd_cpt_fct.patient_id||prd_cpt_fct.cpt_code not in (select distinct src_patient_id||cpt_code from stage_billing_cpt_soft_hard_gold_cost)
);


--x

--752,037
--779,043

update stage_billing_cpt_soft_hard_gold_cost a
SET surgical_non_surgical_cpt = b.surgical_non_surgical
FROM
(
select hcpcs,surgical_non_surgical
from
pce_qe16_prd_ct..dictionary_cpt_hcpcs_mapping_service_line_2 -------***********FIND THE TABLE
)b
where
a.cpt_code =b.hcpcs;

update stage_billing_cpt_soft_hard_gold_cost a
SET surg= b.kpi_category2
FROM
(
select patient_account_number,discharge_campus,kpi_category2
from
stage_claim_gold
)b
where
a.src_patient_id =b.patient_account_number ;
--and a.src_patient_id not in('70000001326387','70000001190289','14102274','1002033711');

drop table stage_enc_by_top_rvu_cpt_hcpcs_all_gold_cost if exists;
create table stage_enc_by_top_rvu_cpt_hcpcs_all_gold_cost as
(
select distinct x.source_of_data,x.Patient_Account_Number,x.surgical_non_surgical_cpt,x.total_charge,x.cpt, x.rvu,
 RANK() OVER (PARTITION BY x.Patient_Account_Number ORDER BY x.surgical_non_surgical_cpt DESC,x.rvu DESC,x.total_charge DESC) as "Ranking",
 ROW_NUMBER() OVER (PARTITION BY x.Patient_Account_Number 
                             ORDER BY x.surgical_non_surgical_cpt DESC,x.rvu DESC,x.total_charge DESC) as "row_num"
 from (
select 
stage_billing_cpt_soft_hard_gold_cost.src_patient_id as Patient_Account_Number,
stage_billing_cpt_soft_hard_gold_cost.service_date,
case when 
stage_billing_cpt_soft_hard_gold_cost.cpt_code in ('99201','99202','99203','99204','99205','99206','99207','99208',
'99209','99210','99211','99212','99213','99214','99215')
then 0
else
stage_billing_cpt_soft_hard_gold_cost.total_charge end as total_charge,
stage_billing_cpt_soft_hard_gold_cost.source_of_data,
stage_billing_cpt_soft_hard_gold_cost.surgical_non_surgical_cpt,
year(stage_billing_cpt_soft_hard_gold_cost.service_date) as dc_year,
stage_billing_cpt_soft_hard_gold_cost.cpt_code as cpt,
stage_billing_cpt_soft_hard_gold_cost.src_company_id as Discharge_Campus,
xref_mclaren_locations.billing_discharge_campus as hospital_name,
xref_mclaren_locations.Zip_Code as zip,
xref_mclaren_locations.locality,
xref_mclaren_locations.carrier,
dictionary_gpci_by_year.pw_gpci_with_1_floor as work_gpci,
dictionary_gpci_by_year.pe_gpci as pract_expense_gpci,
dictionary_gpci_by_year.mp_gpci as malpract_gpci,
dictionary_rvu_by_year.work_rvu,
dictionary_rvu_by_year.facility_pe_rvu,
dictionary_rvu_by_year.mp_rvu,
case when 
stage_billing_cpt_soft_hard_gold_cost.cpt_code in ('99201','99202','99203','99204','99205','99206','99207','99208',
'99209','99210','99211','99212','99213','99214','99215')
then 0
else
(dictionary_rvu_by_year.work_rvu*dictionary_gpci_by_year.pw_gpci_with_1_floor)+
(dictionary_rvu_by_year.facility_pe_rvu * dictionary_gpci_by_year.pe_gpci) +
(dictionary_rvu_by_year.mp_rvu * dictionary_gpci_by_year.mp_gpci) end as rvu

from 
aallaway.stage_billing_cpt_soft_hard_gold_cost,
pce_qe16_prd_ct..xref_mclaren_locations,    -------***********FIND THE TABLE
pce_qe16_prd_ct..dictionary_gpci_by_year,   -------***********FIND THE TABLE
pce_qe16_prd_ct..dictionary_rvu_by_year     -------***********FIND THE TABLE

where 
stage_billing_cpt_soft_hard_gold_cost.src_company_id=xref_mclaren_locations.billing_discharge_campus
and stage_billing_cpt_soft_hard_gold_cost.cpt_code not in ('-100')
and dictionary_gpci_by_year.gpci_cal_year=year(stage_billing_cpt_soft_hard_gold_cost.service_date)
and dictionary_gpci_by_year.locality_number=xref_mclaren_locations.locality
and dictionary_gpci_by_year.carrier=cast(xref_mclaren_locations.carrier as int)
and dictionary_rvu_by_year.rvu_cal_year=year(stage_billing_cpt_soft_hard_gold_cost.service_date)
and dictionary_rvu_by_year.hcpcs=stage_billing_cpt_soft_hard_gold_cost.cpt_code
and dictionary_rvu_by_year.modifier is null
--and stage_billing_cpt_soft_hard_gold_cost.src_patient_id in ('57120808','25699692','25685255','25595577')
) as x
)

DISTRIBUTE ON (Patient_Account_Number,row_num);

--pce_qe16_prd_ct
groom table stage_enc_by_top_rvu_cpt_hcpcs_gold_cost;

drop table stage_enc_by_top_rvu_cpt_hcpcs_gold_cost if exists;
create table
stage_enc_by_top_rvu_cpt_hcpcs_gold_cost as
select * from 
stage_enc_by_top_rvu_cpt_hcpcs_all_gold_cost where
row_num =1
DISTRIBUTE ON (Patient_Account_Number);

--select * from stage_enc_by_top_rvu_cpt_hcpcs_all_gold where Patient_Account_Number='000200727002'

--pce_qe16_prd_ct
update stage_claim_gold A
set A.primary_cpt_hcpcs=B.cpt
from stage_enc_by_top_rvu_cpt_hcpcs_gold_cost B
where
A.patient_account_number=B.patient_account_number;
--9,011,669

--msdrg
update stage_claim_gold
set ms_drg_bus_line=x.MS_DRG_BSN_LINE_DESCR
from pce_qe16_prd_ct..dictionary_msdrg x     -------***********FIND THE TABLE
where 
stage_claim_gold.ms_drg=cast(x.MS_DRG_CD as int);

update stage_claim_gold
set ms_drg_med_surg=x.MS_DRG_CT_DESCR
from pce_qe16_prd_ct..dictionary_msdrg x     -------***********FIND THE TABLE
where 
stage_claim_gold.ms_drg=cast(x.MS_DRG_CD as int);

update stage_claim_gold
set ms_drg_desc=x.MS_DRG_DESCR
from pce_qe16_prd_ct..dictionary_msdrg x     -------***********FIND THE TABLE
where 
stage_claim_gold.ms_drg=cast(x.MS_DRG_CD as int);

--new service cat
update stage_claim_gold a
set a.service_category=x.service_category
from 
pce_qe16_prd_ct..dictionary_cpt_hcpcs_mapping_service_category x     -------***********FIND THE TABLE
WHERE a.primary_cpt_hcpcs= x.hcpcs and a.ip_op ='O';

update stage_claim_gold a
set a.service_category='ED'
WHERE a.ip_op ='O' and ed_non_ed =1;

update stage_claim_gold
set service_category='IP Surgery'
WHERE ip_op ='I' and ms_drg_med_surg='Surgical';

update stage_claim_gold
set service_category='ED'
WHERE ip_op ='I' and ms_drg_med_surg='Medical' and ed_non_ed=1;

update stage_claim_gold
set service_category='Non ED'
WHERE ip_op ='I' and ms_drg_med_surg='Medical' and ed_non_ed is null;

alter table stage_claim_gold add column cpt_hcpcs_desc varchar(255);

update stage_claim_gold a
set a.cpt_hcpcs_desc=x.hcpcs_code_description
from 
pce_qe16_prd_ct..dictionary_cpt_hcpcs_mapping_service_category x   -------***********FIND THE TABLE
WHERE a.primary_cpt_hcpcs= x.hcpcs ;
--and a.ip_op ='O'

update stage_claim_gold a
SET ed_flag = 'ED'
FROM (
select distinct patient_id
from stage_billing_gold
where revenue_code
 in ('0450','0451','0452','0456','0459')
)b
WHERE 
a.Patient_Account_Number =b.patient_id;


update stage_claim_gold
SET ed_flag = 'Non-ED'
WHERE
ed_flag is null;

groom table stage_claim_gold versions ;

alter table stage_claim_gold add column fiscal_year int;

update stage_claim_gold
set fiscal_year =2017 where discharge_date between '2016-10-01' and '2017-09-30';

update stage_claim_gold
set fiscal_year =2018 where discharge_date between '2017-10-01' and '2018-09-30';

update stage_claim_gold
set fiscal_year =2019 where discharge_date between '2018-10-01' and '2019-09-30';

update stage_claim_gold
set fiscal_year =2020 where discharge_date between '2019-10-01' and '2020-09-30';

update stage_claim_gold
set fiscal_year =2021 where discharge_date between '2020-10-01' and '2021-09-30';

update stage_claim_gold
set fiscal_year =2022 where discharge_date between '2021-10-01' and '2022-09-30';

