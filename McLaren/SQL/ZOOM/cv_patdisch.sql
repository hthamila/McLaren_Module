\set ON_ERROR_STOP ON;

update pce_qe16_oper_prd_zoom..stg_patdisch set primary_payer_code='SELF' where primary_payer_code is null;

generate statistics on pce_qe16_oper_prd_zoom..stg_patdisch;

exec drop_table_if_exists('cv_patdisch');

--Added below two fields to accomodate Total Charges greater than 0 and Admit Year greater than 2012
create table cv_patdisch as
select *,
case when discharge_total_charges > 0 then 1 
	else 0 end total_charges_ind 
,case when TO_DATE(admissionarrival_date,'MMDDYYYY') >= '2014-10-01 00:00:00' then 1 else 0 end admitdate_yr_ind
,to_timestamp((admissionarrival_date ||' '||nvl(substr(admit_time,1,2),'00')||':'||nvl(substr(replace(admit_time,':',''),3,2),'00')||':00') ,'MMDDYYYY HH24":"MI":"SS') as admission_ts
,to_timestamp((discharge_date ||' '||nvl(substr(discharge_time,1,2),'00')||':'||nvl(substr(replace(discharge_time,':',''),3,2),'00')||':00') ,'MMDDYYYY HH24":"MI":"SS') as discharge_ts

from pce_qe16_oper_prd_zoom..stg_patdisch;

generate statistics on cv_patdisch;

--------------------------
---Process to Load EMPI---
--------------------------

--Only Historicals
drop table stg_empi if exists;
create table stg_empi as
select * from (
select *, row_number() over (partition by facility, source_mrn order by extract_date desc, empi desc) as rnk from pce_qe16_inst_bill_prd_lnd..empi
)a where rnk=1
distribute on (facility, source_mrn, empi);


drop table stg_cv_empi if exists;

create table stg_cv_empi as
with ptnts as
(
select distinct company_id, patient_id, medical_record_number, trim(leading '0' from medical_record_number) src_mrn
        from cv_patdisch
)
select company_id, patient_id, medical_record_number, source_mrn, facility, empi,
        case when empi is null then 0 else 1 end as empi_ind
        from ptnts
        join stg_empi on src_mrn=source_mrn and company_id=facility

union

select company_id, patient_id, medical_record_number, source_mrn, facility, empi,
        case when empi is null then 0 else 1 end as empi_ind
        from ptnts
        join stg_empi on src_mrn=source_mrn and facility='EMPI'
        where company_id in ('MMG','Lansing') OR company_id like '%Prof'
distribute on (company_id, patient_id);

--Delete duplicates after creating table

--delete from stg_cv_empi where rowid in (select min(rowid) from stg_cv_empi group by company_id, patient_id having count(1)>1);

drop table cv_empi if exists;
alter table stg_cv_empi rename to cv_empi;

\unset ON_ERROR_STOP
