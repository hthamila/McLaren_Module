-----------------------------------------------------------------
-------------Step 5: Load the Target Perm Tables ----------------
-----------------------------------------------------------------

DROP TABLE pce_qe16_slp_prd_stg..target_services_rev_wgj_v2_full_data IF EXISTS;
CREATE TABLE target_services_rev_wgj_v2_full_data AS
(
    SELECT
        *
    FROM
        pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data
)
DISTRIBUTE ON (discharge_campus, patient_account_number);
--3618475


DROP TABLE pce_qe16_slp_prd_stg..target_services_rev_wgj_v2 IF EXISTS;
CREATE TABLE target_services_rev_wgj_v2 AS
(
    SELECT
        *
    FROM
        pce_qe16_slp_prd_stg..target_services_rev_wgj_v2_full_data
    WHERE
        discharge_date >= '2021-06-01'
)
DISTRIBUTE ON (discharge_campus, patient_account_number);
--663008
--4.6s



