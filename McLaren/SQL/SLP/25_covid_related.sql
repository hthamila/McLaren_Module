---------------------------------------------------------------------------------------
--Added COVID Test Indicator looking at both the soft coded CPTs (CPT Fact) and hard coded cpts (CPT Code in Charge Fact table)
---------------------------------------------------------------------------------------
DROP TABLE encntr_covid_test IF EXISTS;
create table encntr_covid_test as
select company_id, patient_id, cpt_code, covid_tst_ind from
(
select company_id,patient_id, cpt_code, 1 as covid_tst_ind  from patcpt_fct
where cpt_code in ('87635','86328','86769','U0002','U0001','G2023','G2024')
group by company_id,patient_id, cpt_code
union
select company_id,patient_id, cpt_code, covid_tst_ind from
(
select company_id,patient_id, cpt_code, row_number() over (partition by company_id,patient_id, cpt_code order by service_date desc) as covid_tst_ind  from cv_patbill
where cpt_code in ('87635','86328','86769','U0002','U0001','G2023','G2024')
)z where covid_tst_ind=1
)a;

DROP TABLE covid_patient IF EXISTS;
create table covid_patient as
SELECT company_id, patient_id,
max(case when icd_code='U07.1' then 1 end) as covid_ptnt_ind,
max(case when icd_code='Z20.828' then 1 end) as covid_ssp_ind
 FROM (
SELECT  company_id, patient_id, diagnosisseq,icd_code,
        row_number() over(partition by company_id, patient_id, icd_code Order by  diagnosisseq) as covid_ptnt_ind
FROM dgns_fct WHERE icd_code in ('U07.1','Z20.828')
        )a WHERE covid_ptnt_ind=1
group by patient_id, company_id
;