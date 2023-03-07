execute sp_dearchive_landing_tbl();

drop table cv_measureresults if exists;
CREATE TABLE cv_measureresults as
SELECT transaction_id
        ,provider_npi
        ,location_tin
        ,organization_npi
        ,ccn
        ,product_code
        ,batch_id
        ,measure_id
        ,measure_sub_id
        ,measure_period_start
        ,measure_period_end
        ,calculated_for_date
        ,patient_guid
        ,case when is_ipp::varchar(5)='TRUE' then 1 else 0 end  is_ipp
        ,case when is_denom_exclusion::varchar(5)='TRUE' then 1 else 0 end  is_denom_exclusion
        ,case when is_denom_exception::varchar(5)='TRUE' then 1 else 0 end  is_denom_exception
        ,case when is_numerator::varchar(5)='TRUE' then 1 else 0 end  is_numerator
        ,case when is_denominator::varchar(5)='TRUE' then 1 else 0 end  is_denominator
        ,case when is_notmet::varchar(5)='TRUE' then 1 else 0 end  is_notmet
        ,case when msrpopl::varchar(5)='TRUE' then 1 else 0 end  msrpopl
        ,observ
        ,client_id
        ,patient_id
        ,alt_location_id
        ,eligibility_date
        ,numerator_crit_date
        ,parent_premier_entity_clazz
        ,parent_premier_entity_code
        ,premier_entity_clazz
        ,premier_entity_code
	,rcrd_isrt_ts


  FROM pce_qe16_misc_prd_zoom..stg_measureresults
DISTRIBUTE ON (patient_guid, measure_id, measure_period_start, measure_period_end);

DROP TABLE cv_mclaren_carenow IF EXISTS;
CREATE TABLE cv_mclaren_carenow as
(
SELECT 
    rcrd_load_type,
    rcrd_isrt_pcs_nm,
    rcrd_isrt_ts,
    rcrd_udt_pcs_nm,
    rcrd_udt_ts,
    rcrd_src_isrt_id,
    rcrd_src_isrt_ts,
    rcrd_src_udt_id,
    rcrd_src_udt_ts,
    rcrd_src_file_nm,
    rcrd_btch_audt_id,
    rcrd_pce_cst_nm,
    rcrd_pce_cst_src_nm,
    visit_date,
    location_name,
    patient_first_name,
    patient_last_name,
    patient_date_of_birth,
    patient_home_phone_number,
    patient_cell_phone_number,
    patient_state_residence,
    carenow_ptnt_bk,
    carenow_ptnt_dk,
    carenow_encntr_bk,
    carenow_encntr_dk
FROM
    (
        SELECT 
            rcrd_load_type,
            rcrd_isrt_pcs_nm,
            rcrd_isrt_ts,
            rcrd_udt_pcs_nm,
            rcrd_udt_ts,
            rcrd_src_isrt_id,
            rcrd_src_isrt_ts,
            rcrd_src_udt_id,
            rcrd_src_udt_ts,
            rcrd_src_file_nm,
            rcrd_btch_audt_id,
            rcrd_pce_cst_nm,
            rcrd_pce_cst_src_nm,
            visit_date,
            location_name,
            patient_first_name,
            patient_last_name,
            patient_date_of_birth,
            patient_home_phone_number,
            patient_cell_phone_number,
            patient_state_residence,
            UPPER(COALESCE(patient_last_name,'') || '|' || SUBSTRING(COALESCE(patient_first_name,''),1,1) || '|' || COALESCE(patient_date_of_birth,'0001-01-01')) AS carenow_ptnt_bk,
            HASH8(carenow_ptnt_bk) AS carenow_ptnt_dk,
            UPPER(COALESCE(patient_last_name,'') || '|' || SUBSTRING(COALESCE(patient_first_name,''),1,1) || '|' || COALESCE(patient_date_of_birth,'0001-01-01') || '|' || COALESCE(visit_date,'0001-01-01')) AS carenow_encntr_bk,
            HASH8(carenow_encntr_bk) AS carenow_encntr_dk,
            ROW_NUMBER () OVER (PARTITION BY carenow_encntr_bk ORDER BY rcrd_isrt_ts DESC, visit_date DESC) AS rn
        FROM pce_qe16_misc_prd_zoom..stg_mclaren_carenow
    ) stg_carenow
WHERE
    rn = 1
)
DISTRIBUTE ON (patient_last_name, patient_first_name, patient_date_of_birth, visit_date);

