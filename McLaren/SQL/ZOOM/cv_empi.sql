\set ON_ERROR_STOP ON;

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
