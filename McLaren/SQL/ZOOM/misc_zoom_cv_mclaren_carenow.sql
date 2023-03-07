
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
        FROM
            pce_qe16_misc_prd_lnd..mclaren_carenow
        WHERE
            patient_last_name IS NOT NULL AND 
            patient_first_name IS NOT NULL AND  
            patient_date_of_birth IS NOT NULL AND
            visit_date IS NOT NULL
    ) stg_carenow
WHERE
    rn = 1
)
DISTRIBUTE ON (patient_last_name, patient_first_name, patient_date_of_birth, visit_date);

