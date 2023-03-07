
\set ON_ERROR_STOP ON;

--******************************** CQDOC Population ******************************************
SELECT 'pce_qe16_slp_prd_stg..cqdoc_population_hist_data' AS processing_table;
DROP TABLE pce_qe16_slp_prd_stg..cqdoc_population_hist_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg..cqdoc_population_hist_data AS
SELECT DISTINCT
    eaf.fcy_nm,
    eaf.encntr_num,
    eaf.medical_record_number,
    eaf.empi AS empi,
    eaf.in_or_out_patient_ind AS in_or_out_patient_ind,
    eaf.dschrg_ts AS dschrg_ts
FROM
    pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
    INNER JOIN pce_qe16_slp_prd_dm..prd_chrg_fct cf
    ON
        eaf.fcy_nm = cf.fcy_nm AND
        eaf.encntr_num = cf.encntr_num
    INNER JOIN pce_qe16_slp_prd_dm..prd_encntr_dgns_fct edf
    ON
        eaf.fcy_nm = edf.fcy_nm AND
        eaf.encntr_num = edf.encntr_num
    INNER JOIN pce_qe16_slp_prd_stg..cqdoc_cpt_code_set_ref ccsr
    ON
        cf.cpt_code = ccsr.cpt_code AND
        ccsr.actv_ind = 'Y'
WHERE
    (DATE(eaf.dschrg_dt) BETWEEN TO_DATE('2019-10-01','YYYY-MM-DD') AND CURRENT_DATE) AND
    eaf.dschrg_tot_chrg_amt > 0 AND
    eaf.medical_record_number IS NOT NULL AND
    eaf.tot_chrg_ind > 0 AND
    cf.src_prfssnl_chrg_ind = 1 AND
    UPPER(eaf.ptnt_tp_cd) NOT IN ('BSCH','BSCHO') AND
    UPPER(eaf.src_prim_pyr_cd) NOT IN ('SELECT','SELEC') AND
    UPPER(COALESCE(eaf.src_prim_payor_grp1,'')) NOT IN ('OTHER','SELF PAY','') AND
    eaf.pos_code IN ('02', '11', '19', '22', '72', '50')
DISTRIBUTE ON (fcy_nm, encntr_num)
;


--******************************** Encounter File ******************************************
SELECT 'pce_qe16_slp_prd_stg..cqdoc_encounter_hist_data' AS processing_table;
DROP TABLE pce_qe16_slp_prd_stg..cqdoc_encounter_hist_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg..cqdoc_encounter_hist_data AS
SELECT
    DISTINCT
    'MI2002' AS system_id,
    CASE WHEN eaf.fin IS NULL THEN eaf.encntr_num ELSE eaf.fin ||'-'||eaf.encntr_num END AS encounter_id,
    eaf.medical_record_number AS mrn,
    eaf.empi AS upi,
    TRIM(eaf.in_or_out_patient_ind) AS patient_type,
    eaf.src_prim_pyr_cd AS primary_insurance_plan_id,
    eaf.src_scdy_pyr_cd AS secondary_insurance_plan_id,
    TO_CHAR(eaf.dschrg_ts,'YYYY-MM-DD HH24:MI') AS date_of_service,
    CASE
        WHEN eaf.attnd_pract_npi IS NOT NULL THEN eaf.attnd_pract_npi
        WHEN epf.encntr_pract_npi IS NOT NULL THEN epf.encntr_pract_npi
        WHEN cf.performingphysician_npi IS NOT NULL THEN cf.performingphysician_npi
    END AS rendering_provider_id,
    eaf.sub_fcy AS rendering_provider_clinic_id,
    NULL AS primary_care_provider_id,
    CAST(eaf.dschrg_tot_chrg_amt AS DECIMAL (12,2)) AS total_charges,
    CAST(eaf.est_net_rev_amt AS DECIMAL (12,2)) AS total_payment,
    'Cerner' AS src_sys_nm,
    eaf.pos_code AS place_of_service
FROM
    pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
    INNER JOIN pce_qe16_slp_prd_stg..cqdoc_population_hist_data pop
    ON
        eaf.fcy_nm = pop.fcy_nm AND
        eaf.encntr_num = pop.encntr_num
    LEFT OUTER JOIN
    (
        SELECT
            fcy_nm,
            encntr_num,
            encntr_pract_npi,
            field_description,
            practitioner_group,
            ROW_NUMBER() OVER (PARTITION BY fcy_nm, encntr_num ORDER BY service_start_date DESC) AS rn_pract
        FROM
            pce_qe16_slp_prd_dm..prd_encntr_pract_fct
        WHERE
            practitioner_group IN ('Attending') AND
            encntr_pract_npi IS NOT NULL
    ) epf
    ON
        eaf.fcy_nm = epf.fcy_nm AND
        eaf.encntr_num = epf.encntr_num AND
        epf.rn_pract = 1
    LEFT OUTER JOIN
    (
        SELECT
            cf.fcy_nm,
            cf.encntr_num,
            cf.performingphysician,
            pnsd.npi as performingphysician_npi,
            ROW_NUMBER() OVER (PARTITION BY fcy_nm, encntr_num ORDER BY total_charge DESC) AS rn_chrg
        FROM
            pce_qe16_slp_prd_dm..prd_chrg_fct cf
            INNER JOIN pce_qe16_slp_prd_dm..phy_npi_spclty_dim pnsd
            ON
                cf.fcy_nm = pnsd.company_id AND
                cf.performingphysician = pnsd.practitioner_code
        WHERE
            src_prfssnl_chrg_ind = 1
    ) cf
    ON
        eaf.fcy_nm = cf.fcy_nm AND
        eaf.encntr_num = cf.encntr_num AND
        cf.rn_chrg = 1
DISTRIBUTE ON (encounter_id)
;



--******************************** Diagnosis File ******************************************
SELECT 'pce_qe16_slp_prd_stg..cqdoc_diagnosis_hist_data' AS processing_table;
DROP TABLE pce_qe16_slp_prd_stg..cqdoc_diagnosis_hist_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg..cqdoc_diagnosis_hist_data AS
SELECT
    DISTINCT
    'MI2002' AS system_id,
    CASE WHEN eaf.fin IS NULL THEN eaf.encntr_num ELSE eaf.fin ||'-'||eaf.encntr_num END AS encounter_id,
    eaf.medical_record_number AS mrn,
    TO_CHAR(eaf.dschrg_ts,'YYYY-MM-DD HH24:MI') AS date_of_service,
    edf.icd_code AS icd_dx_code,
    edf.diagnosisseq AS icd_dx_position,
    edf.diagnosis_code_present_on_admission_flag AS icd_dx_poa,
    edf.icd_version AS icd_dx_version,
    'Cerner' AS src_sys_nm
FROM
    pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
    INNER JOIN pce_qe16_slp_prd_dm..prd_encntr_dgns_fct edf
    ON
        eaf.fcy_nm = edf.fcy_nm AND
        eaf.encntr_num = edf.encntr_num
    INNER JOIN pce_qe16_slp_prd_stg..cqdoc_population_hist_data pop
    ON
        eaf.fcy_nm = pop.fcy_nm AND
        eaf.encntr_num = pop.encntr_num
DISTRIBUTE ON (encounter_id, icd_dx_code)
;


--******************************** CPT Procedure File ******************************************
SELECT 'pce_qe16_slp_prd_stg..intermediate_cqdoc_cpt_hist_data' AS processing_table;
DROP TABLE pce_qe16_slp_prd_stg..intermediate_cqdoc_cpt_hist_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg..intermediate_cqdoc_cpt_hist_data AS
SELECT
    cf.fcy_nm,
    cf.encntr_num,
    cf.cpt_code,
    cf.cpt_modifier_1,
    cf.cpt_modifier_2,
    cf.cpt_modifier_3,
    cf.cpt_modifier_4,
    cf.total_charge,
    cf.service_date,
    cf.charge_code,
    SUM(cf.total_charge) OVER (PARTITION BY cf.fcy_nm, cf.encntr_num, cf.service_date, cf.charge_code, cf.cpt_code, cf.cpt_modifier_1, cf.cpt_modifier_2, cf.cpt_modifier_3, cf.cpt_modifier_4) AS agg_total_charge,
    CASE WHEN cf.cpt_modifier_1 = '26' THEN 1 ELSE 0 END AS cpt_priority,
    ROW_NUMBER() OVER(PARTITION BY cf.fcy_nm, cf.encntr_num, cf.cpt_code ORDER BY cpt_priority DESC, cf.cpt_modifier_1 DESC, cf.cpt_modifier_2 DESC, cf.cpt_modifier_3 DESC, cf.cpt_modifier_4 DESC) AS rn_cf
FROM
    pce_qe16_slp_prd_dm..prd_chrg_fct cf
    INNER JOIN pce_qe16_slp_prd_stg..cqdoc_population_hist_data pop
    ON
        cf.fcy_nm = pop.fcy_nm AND
        cf.encntr_num = pop.encntr_num
    INNER JOIN pce_qe16_slp_prd_stg..cqdoc_cpt_code_set_ref ccsr
    ON
        cf.cpt_code = ccsr.cpt_code AND
        ccsr.actv_ind = 'Y'
WHERE
    cf.src_prfssnl_chrg_ind = 1
DISTRIBUTE ON (fcy_nm, encntr_num)
;


SELECT 'pce_qe16_slp_prd_stg..cqdoc_cpt_hist_data' AS processing_table;
DROP TABLE pce_qe16_slp_prd_stg..cqdoc_cpt_hist_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg..cqdoc_cpt_hist_data AS
SELECT
    DISTINCT
    'MI2002' AS system_id,
    CASE WHEN eaf.fin IS NULL THEN eaf.encntr_num ELSE eaf.fin ||'-'||eaf.encntr_num END AS encounter_id,
    eaf.medical_record_number AS mrn,
    TO_CHAR(eaf.dschrg_ts,'YYYY-MM-DD HH24:MI') AS date_of_service,
    cf.cpt_code AS cpt_code,
    DENSE_RANK() OVER (PARTITION BY cf.fcy_nm, cf.encntr_num ORDER BY cf.cpt_code) AS cpt_code_position,
    cf.cpt_modifier_1 AS cpt_modifier1,
    cf.cpt_modifier_2 AS cpt_modifier2,
    cf.cpt_modifier_3 AS cpt_modifier3,
    cf.cpt_modifier_4 AS cpt_modifier4,
    'Cerner' AS src_sys_nm
FROM
    pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
    INNER JOIN
    (
        SELECT
            DISTINCT
            fcy_nm,
            encntr_num,
            cpt_code,
            cpt_modifier_1,
            cpt_modifier_2,
            cpt_modifier_3,
            cpt_modifier_4
        FROM
            pce_qe16_slp_prd_stg..intermediate_cqdoc_cpt_hist_data
        WHERE
            agg_total_charge <> 0 AND
            rn_cf = 1
    ) cf
    ON
        eaf.fcy_nm = cf.fcy_nm AND
        eaf.encntr_num = cf.encntr_num
    INNER JOIN pce_qe16_slp_prd_stg..cqdoc_population_hist_data pop
    ON
        eaf.fcy_nm = pop.fcy_nm AND
        eaf.encntr_num = pop.encntr_num

DISTRIBUTE ON (encounter_id, cpt_code)
;

--******************************** Patient File ******************************************
SELECT 'pce_qe16_slp_prd_stg..cqdoc_patient_hist_data' AS processing_table;
DROP TABLE pce_qe16_slp_prd_stg..cqdoc_patient_hist_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg..cqdoc_patient_hist_data AS
SELECT
    system_id,
    mrn,
    upi,
    patient_first_name,
    patient_last_name,
    patient_middle_name,
    patient_suffix,
    patient_dob,
    patient_sex_code,
    patient_race_code,
    patient_marital_status_code,
    patient_ethnicity_code,
    patient_address_line_1,
    patient_address_line_2,
    patient_city,
    patient_county,
    patient_state,
    patient_postal_code,
    src_sys_nm,
    patient_country
FROM
    (
        SELECT
            'MI2002' AS system_id,
            CAST(eaf.medical_record_number AS varchar(25)) AS mrn,
            CAST(NULL AS VARCHAR(25)) AS upi,
            CAST(eaf.ptnt_frst_nm AS VARCHAR(50)) AS patient_first_name,
            CAST(eaf.ptnt_lst_nm AS VARCHAR(50)) AS patient_last_name,
            CAST(eaf.ptnt_mid_nm AS VARCHAR(50)) AS patient_middle_name,
            eaf.ptnt_nm_sfx AS patient_suffix,
            eaf.brth_dt AS patient_dob,
            CASE
                WHEN eaf.ptnt_gnd = 'Male' THEN 'M'
                WHEN eaf.ptnt_gnd = 'Female' THEN 'F'
                WHEN eaf.ptnt_gnd = 'Unknown' THEN 'U'
                ELSE 'U'
            END AS patient_sex_code,
            CASE
                WHEN eaf.race_cd = '1' AND eaf.ethcty_descr <> 'Hispanic or Latino' THEN '1'
                WHEN eaf.race_cd = '2' AND eaf.ethcty_descr <> 'Hispanic or Latino' THEN '2'
                WHEN eaf.race_cd = '4' AND eaf.ethcty_descr <> 'Hispanic or Latino' THEN '4'
                WHEN eaf.race_cd = '6' AND eaf.ethcty_descr <> 'Hispanic or Latino' THEN '5'
                WHEN eaf.race_cd = '7' AND eaf.ethcty_descr <> 'Hispanic or Latino' THEN '5'
                WHEN eaf.race_cd = '9' AND eaf.ethcty_descr <> 'Hispanic or Latino' THEN '9'
                WHEN eaf.race_cd = 'U' AND eaf.ethcty_descr <> 'Hispanic or Latino' THEN '9'
                WHEN eaf.ethcty_descr = 'Hispanic or Latino' THEN '3'
                WHEN eaf.ethcty_descr IN ('Unknown','Not Hispanic or Latino') THEN '9'
            END AS patient_race_code,
            CASE
                WHEN eaf.mar_status = 'Married' THEN '1'
                WHEN eaf.mar_status = 'Single' THEN '2'
                WHEN eaf.mar_status = 'Legally Separated' THEN '3'
                WHEN eaf.mar_status = 'Divorced' THEN '4'
                WHEN eaf.mar_status = 'Widowed' THEN '5'
                WHEN eaf.mar_status = 'Unknown' THEN '6'
                WHEN eaf.mar_status = 'Life Partner' THEN '7'
                WHEN eaf.mar_status IS NULL THEN '9'
            END AS patient_marital_status_code,
            eaf.ethncty_cd AS patient_ethnicity_code,
            CAST(COALESCE(eaf.adr1, 'OTHER')AS VARCHAR(250)) AS patient_address_line_1,
            eaf.adr2 AS patient_address_line_2,
            COALESCE(eaf.cty, 'OTHER') AS patient_city,
            eaf.fips_cnty_descr AS patient_county,
            COALESCE(eaf.fips_ste_descr,'MI') AS patient_state,
            CASE
                WHEN eaf.ptnt_zip_cd = '-100' THEN '48228'
                ELSE eaf.ptnt_zip_cd
            END AS patient_postal_code,
            'Cerner' AS src_sys_nm,
            'USA' AS patient_country,
            ROW_NUMBER() OVER (PARTITION BY eaf.medical_record_number ORDER BY eaf.dschrg_ts DESC, eaf.upd_dt DESC) AS rn_eaf
        FROM
            pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
            INNER JOIN pce_qe16_slp_prd_stg..cqdoc_population_hist_data pop
            ON
                eaf.fcy_nm = pop.fcy_nm AND
                eaf.encntr_num = pop.encntr_num
    )A
WHERE
    rn_eaf = 1
DISTRIBUTE ON (mrn)
;


--******************************** Provider File ******************************************
SELECT 'pce_qe16_slp_prd_stg..cqdoc_provider_hist_data' AS processing_table;
DROP TABLE pce_qe16_slp_prd_stg..cqdoc_provider_hist_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg..cqdoc_provider_hist_data AS
SELECT
    DISTINCT
    'MI2002' AS system_id,
    pnsd.npi AS provider_id,
    pnsd.npi AS provider_npi,
    CASE
        WHEN (COALESCE(pnsd.practitioner_name,'') <> '' AND INSTR(pnsd.practitioner_name,',') <> 0) THEN TRIM(SUBSTRING(pnsd.practitioner_name, INSTR(pnsd.practitioner_name,',')+1))
        ELSE TRIM(SUBSTRING(pd.practitioner_name, INSTR(pd.practitioner_name,',')+1))
    END AS provider_first_name,
    CASE
        WHEN (COALESCE(pnsd.practitioner_name,'') <> '' AND INSTR(pnsd.practitioner_name,',') <> 0) THEN TRIM(SUBSTRING(pnsd.practitioner_name, 1, INSTR(pnsd.practitioner_name,',')-1))
        ELSE TRIM(SUBSTRING(pd.practitioner_name, 1, INSTR(pd.practitioner_name,',')-1))
    END AS provider_last_name,
    CAST(NULL AS varchar(50)) AS provider_middle_name,
    COALESCE(pnsd.practitioner_spclty_description,'UNKNOWN') AS provider_primary_specialty_descr,
    CAST(NULL AS date) AS provider_start_date,
    CAST(NULL AS date) AS provider_end_date,
    CASE
        WHEN pnsd.npi_dactv_dt IS NULL THEN 'Y'
        ELSE 'N'
    END AS provider_active_ind,
    'Cerner' AS src_sys_nm
FROM
    pce_qe16_slp_prd_dm..phy_npi_spclty_dim pnsd
    INNER JOIN
    (
        SELECT
            eaf.fcy_nm,
            eaf.encntr_num,
            CASE
                WHEN eaf.attnd_pract_npi IS NOT NULL THEN eaf.attnd_pract_npi
                WHEN epf.encntr_pract_npi IS NOT NULL THEN epf.encntr_pract_npi
                WHEN cf.performingphysician_npi IS NOT NULL THEN cf.performingphysician_npi
            END AS attnd_pract_npi
        FROM pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
        LEFT OUTER JOIN
        (
            SELECT
                fcy_nm,
                encntr_num,
                encntr_pract_npi,
                field_description,
                practitioner_group,
                ROW_NUMBER() OVER (PARTITION BY fcy_nm, encntr_num ORDER BY service_start_date DESC) AS rn_pract
            FROM
                pce_qe16_slp_prd_dm..prd_encntr_pract_fct
            WHERE
                practitioner_group IN ('Attending') AND
                encntr_pract_npi IS NOT NULL
        ) epf
        ON
            eaf.fcy_nm = epf.fcy_nm AND
            eaf.encntr_num = epf.encntr_num AND
            epf.rn_pract = 1
        LEFT OUTER JOIN
        (
            SELECT
                cf.fcy_nm,
                cf.encntr_num,
                cf.performingphysician,
                pnsd.npi as performingphysician_npi,
                ROW_NUMBER() OVER (PARTITION BY fcy_nm, encntr_num ORDER BY total_charge DESC) AS rn_chrg
            FROM
                pce_qe16_slp_prd_dm..prd_chrg_fct cf
                INNER JOIN pce_qe16_slp_prd_dm..phy_npi_spclty_dim pnsd
                ON
                    cf.fcy_nm = pnsd.company_id AND
                    cf.performingphysician = pnsd.practitioner_code
            WHERE
                src_prfssnl_chrg_ind = 1
        ) cf
        ON
            eaf.fcy_nm = cf.fcy_nm AND
            eaf.encntr_num = cf.encntr_num AND
            cf.rn_chrg = 1
    ) ed
    ON
        pnsd.npi = ed.attnd_pract_npi AND
        pnsd.company_id = ed.fcy_nm
    INNER JOIN pce_qe16_slp_prd_stg..cqdoc_population_hist_data pop
    ON
        ed.fcy_nm = pop.fcy_nm AND
        ed.encntr_num = pop.encntr_num
    LEFT OUTER JOIN pce_qe16_slp_prd_dm..phys_dim pd
    ON
        pnsd.npi = pd.npi AND
        pnsd.company_id = pd.company_id
DISTRIBUTE ON (provider_id)
;

--******************************** Clinic File ******************************************
SELECT 'pce_qe16_slp_prd_stg..cqdoc_clinic_hist_data' AS processing_table;
DROP TABLE pce_qe16_slp_prd_stg..cqdoc_clinic_hist_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg..cqdoc_clinic_hist_data AS
SELECT
    DISTINCT
    'MI2002' AS system_id,
    eaf.sub_fcy AS clinic_id,
    CAST(NULL AS varchar(10)) AS clinic_tin,
    eaf.sub_fcy AS clinic_name,
    CAST(NULL AS varchar(50)) AS clinic_loc,
    CAST(NULL AS varchar(250)) AS clinic_address_line_1,
    CAST(NULL AS varchar(50)) AS clinic_address_line_2,
    CAST(NULL AS varchar(80)) AS clinic_city,
    CAST(NULL AS char(10)) AS clinic_state,
    CAST(NULL AS varchar(10)) AS clinic_postal_code,
    CAST(NULL AS varchar(250)) AS clinic_facility_region_id,
    CAST(NULL AS varchar(250)) AS clinic_facility_region_display_name,
    'Cerner' AS src_sys_nm
FROM
    pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
    INNER JOIN pce_qe16_slp_prd_stg..cqdoc_population_hist_data pop
    ON
        eaf.fcy_nm = pop.fcy_nm AND
        eaf.encntr_num = pop.encntr_num
DISTRIBUTE ON (clinic_id)
;



--******************************** Payor File ******************************************
SELECT 'pce_qe16_slp_prd_stg..cqdoc_payor_hist_data' AS processing_table;
DROP TABLE pce_qe16_slp_prd_stg..cqdoc_payor_hist_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg..cqdoc_payor_hist_data AS
SELECT
    DISTINCT
    'MI2002' AS system_id,
    pgd.payer_code AS insurance_plan_id,
    pgd.payor_group1 AS financial_class,
    pgd.payer_description AS insurance_plan,
    'Cerner' AS src_sys_nm
FROM
    pce_qe16_slp_prd_dm..payr_grp_dim pgd
    INNER JOIN
    (
        SELECT
            fcy_nm,
            encntr_num,
            src_prim_pyr_cd AS src_prim_sec_pyr_cd
        FROM
            pce_qe16_slp_prd_dm..prd_encntr_anl_fct
        UNION
        SELECT
            fcy_nm,
            encntr_num,
            src_scdy_pyr_cd AS src_prim_sec_pyr_cd
        FROM
            pce_qe16_slp_prd_dm..prd_encntr_anl_fct
    )eaf
    ON
        pgd.payer_code = eaf.src_prim_sec_pyr_cd AND
        pgd.company_id = eaf.fcy_nm
    INNER JOIN pce_qe16_slp_prd_stg..cqdoc_population_hist_data pop
    ON
        eaf.fcy_nm = pop.fcy_nm AND
        eaf.encntr_num = pop.encntr_num
DISTRIBUTE ON (insurance_plan_id)
;

\unset ON_ERROR_STOP

