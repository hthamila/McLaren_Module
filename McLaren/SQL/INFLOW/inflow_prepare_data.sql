\set ON_ERROR_STOP ON;

----INFLOW Prepare Extract Dates
DROP TABLE pce_qe16_slp_prd_dm..inflow_extract_date_info IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..inflow_extract_date_info AS
SELECT
    CASE
        WHEN DAY(CURRENT_DATE) < 14 THEN CURRENT_DATE - INTERVAL '1 MONTHS'
        ELSE CURRENT_DATE
    END AS proc_month,
    TO_DATE(TO_CHAR(proc_month - INTERVAL '1 MONTHS','YYYYMM') || '15','YYYYMMDD') AS bill_txn_start_dt,
    TO_DATE(TO_CHAR(proc_month,'YYYYMM') || '14','YYYYMMDD') AS bill_txn_end_dt,
    TO_DATE(TO_CHAR(proc_month - INTERVAL '1 MONTHS','YYYYMM') || '01','YYYYMMDD') AS sched_pyrl_start_dt,
    LAST_DAY(proc_month - INTERVAL '1 MONTHS') AS sched_pyrl_end_dt
;

----INFLOW Population
DROP TABLE pce_qe16_slp_prd_dm..inflow_population_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..inflow_population_data AS
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
WHERE
    eaf.dschrg_dt <= CURRENT_DATE AND
    eaf.tot_chrg_ind > 0 AND
    UPPER(eaf.ptnt_tp_cd) NOT IN ('BSCH','BSCHO') AND
    UPPER(eaf.src_prim_pyr_cd) NOT IN ('SELECT','SELEC') AND
    eaf.pos_code IS NOT NULL AND
    cf.src_prfssnl_chrg_ind = 1
DISTRIBUTE ON (fcy_nm, encntr_num)
;

----INFLOW Billing
DROP TABLE pce_qe16_slp_prd_dm..inflow_billing_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..inflow_billing_data AS
SELECT
    cf.service_date AS date_of_service,
    cf.postdate AS post_date,
    DATE(cf.actvty_dt_tm) AS last_modified_date,
    cf.cpt_code AS cpt4_code,
    cf.cpt_descr AS cpt_code_description,
    cf.cpt_modifier_1 AS cpt_code_modifier_1,
    cf.cpt_modifier_2 AS cpt_code_modifier_2,
    cf.cpt_modifier_3 AS cpt_code_modifier_3,
    cf.cpt_modifier_4 AS cpt_code_modifier_4,
    cf.quantity AS units,
    cf.total_charge AS charge_amount,
    cf.wrk_rvu AS work_rvus,
    eaf.pos_code AS cms_place_of_service_code,
    eaf.prim_dgns_cd AS primary_icd_code,
    eaf.prim_dgns_descr AS primary_icd_code_description,
    cf.dgns_2 AS icd_diagnosis_2,
    cf.dgns_3 AS icd_diagnosis_3,
    cf.dgns_4 AS icd_diagnosis_4,
    cf.dgns_5 AS icd_diagnosis_5,
    eaf.empi AS patient_mrn_identifier,
    eaf.ptnt_ssn AS patient_ssn,
    CASE
        WHEN COALESCE(eaf.ptnt_lst_nm,'') = '' AND COALESCE(eaf.ptnt_frst_nm,'') = '' THEN NULL
        WHEN COALESCE(eaf.ptnt_frst_nm,'') = '' THEN eaf.ptnt_lst_nm
        WHEN COALESCE(eaf.ptnt_lst_nm,'') = '' THEN eaf.ptnt_frst_nm
        ELSE eaf.ptnt_lst_nm || ', ' || eaf.ptnt_frst_nm
    END AS patient_name,
    eaf.brth_dt AS patient_dob,
    CASE
        WHEN eaf.ptnt_gnd = 'Male' THEN 'M'
        WHEN eaf.ptnt_gnd = 'Female' THEN 'F'
        WHEN eaf.ptnt_gnd = 'Unknown' THEN 'U'
        ELSE 'U'
    END AS patient_gender,
    eaf.race_descr AS patient_race_ethnicity,
    eaf.mar_status AS patient_marital_status,
    CASE
        WHEN COALESCE(eaf.adr1,'') = '' AND COALESCE(eaf.adr2,'') = '' THEN NULL
        WHEN COALESCE(eaf.adr2,'') = '' THEN eaf.adr1
        WHEN COALESCE(eaf.adr1,'') = '' THEN eaf.adr2
        ELSE eaf.adr1 || ' ' || eaf.adr1
    END AS patient_address,
    eaf.cty AS patient_city,
    eaf.ptnt_zip_cd AS patient_zip_code,
    eaf.std_ste_cd AS patient_state,
    pd.pvdr_frst_nm AS rendering_provider_first_name,
    pd.pvdr_mid_nm AS rendering_provider_middle_name_initial,
    pd.pvdr_lgl_last_nm AS rendering_provider_last_name,
    pnsd.practitioner_name AS rendering_provider_full_name,
    pnsd.npi AS rendering_provider_npi,
    pnsd.practitioner_spclty_description AS rendering_providers_primary_specialty,
    pd.pvdr_cred_txt AS rendering_provider_credentials,
    NULL AS rendering_provider_medicaid_id,
    ppd.pvdr_frst_nm AS billing_provider_first_name,
    ppd.pvdr_mid_nm AS billing_provider_middle_name_initial,
    ppd.pvdr_lgl_last_nm AS billing_provider_last_name,
    ppnsd.practitioner_name AS billing_provider_full_name,
    ppnsd.npi AS billing_provider_npi,
    NULL AS billing_provider_tax_id,
    ppnsd.practitioner_spclty_description AS billing_providers_primary_specialty,
    ppd.pvdr_cred_txt AS billing_provider_credentials,
    NULL AS billing_provider_medicaid_id,
    NULL AS referring_provider_first_name,
    NULL AS referring_provider_middle_name_initial,
    NULL AS referring_provider_last_name,
    NULL AS referring_provider_full_name,
    NULL AS referring_provider_npi,
    NULL AS referring_providers_primary_specialty,
    NULL AS referring_provider_credentials,
    eaf.sub_fcy AS practice_name,
    eaf.sub_fcy AS billing_location_name,
    cf.department_description AS department_name,
    cf.dept AS cost_center,
    eaf.src_prim_pyr_descr AS primary_payer_name,
    eaf.fnc_cls_descr AS primary_payer_plan,
    eaf.src_prim_payor_grp3 AS primary_payer_financial_class,
    cf.charge_code AS charge_id,
    cf.encntr_num AS invoice_number,
    cf.fcy_nm AS facility_name,
    eaf.src_prim_payor_grp3 AS primary_payor_group_2,
    eaf.src_prim_payor_grp1 AS primary_payor_group_1,
    eaf.fin
FROM
    pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
    ------------------------------------------------------------------
    INNER JOIN pce_qe16_slp_prd_dm..prd_chrg_fct cf
    ON
        eaf.fcy_nm = cf.fcy_nm AND
        eaf.encntr_num = cf.encntr_num
    ------------------------------------------------------------------
    INNER JOIN pce_qe16_slp_prd_dm..inflow_population_data pop
    ON
        eaf.fcy_nm = pop.fcy_nm AND
        eaf.encntr_num = pop.encntr_num
    ------------------------------------------------------------------
    LEFT OUTER JOIN pce_qe16_slp_prd_dm..phy_npi_spclty_dim pnsd
    ON
        cf.fcy_nm = pnsd.company_id AND
        cf.performingphysician = pnsd.practitioner_code
    ------------------------------------------------------------------
    LEFT OUTER JOIN pce_qe16_slp_prd_dm..pvdr_dim pd
    ON
        pnsd.npi = pd.npi
    ------------------------------------------------------------------
    LEFT OUTER JOIN
    (
        SELECT
            DISTINCT
            company_id,
            npi,
            practitioner_name,
            practitioner_spclty_description
        FROM
            pce_qe16_slp_prd_dm..phy_npi_spclty_dim
    ) ppnsd
    ON
        eaf.fcy_nm = ppnsd.company_id AND
        eaf.attnd_pract_npi = ppnsd.npi
    ------------------------------------------------------------------
    LEFT OUTER JOIN pce_qe16_slp_prd_dm..pvdr_dim ppd
    ON
        ppnsd.npi = ppd.npi
    ------------------------------------------------------------------
    FULL OUTER JOIN pce_qe16_slp_prd_dm..inflow_extract_date_info edi
    ON
        1 = 1
    ------------------------------------------------------------------
WHERE
    cf.src_prfssnl_chrg_ind = 1 AND
    (DATE(cf.postdate) BETWEEN edi.bill_txn_start_dt AND edi.bill_txn_end_dt)
DISTRIBUTE ON (fin)
;

-- Inflow Transactions
DROP TABLE pce_qe16_slp_prd_dm..inflow_transactions_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..inflow_transactions_data AS
SELECT
    NULL AS charge_id,
    pftf.transcode AS transaction_id,
    pftf.transcodedesc AS transaction_description,
    pftf.transtype AS transaction_type_description,
    pftf.postdate AS post_date,
    NULL AS last_modified_date,
    CASE WHEN pftf.fcy_pymt_ind = 1 THEN pftf.amount ELSE NULL END AS payment_amount,
    CASE WHEN pftf.fcy_adj_ind = 1 THEN pftf.amount ELSE NULL END AS adjustment_amount, --All NULLS
    CAST(NULL AS DECIMAL(15,2)) AS refund_amount,
    eaf.src_prim_pyr_descr AS payer_name,
    eaf.fnc_cls_descr AS payer_plan,
    eaf.src_prim_payor_grp3 AS payer_financial_class,
    NULL AS reason_category,
    NULL AS claim_adjudication_reason_code,
    NULL AS claim_adjudication_reason_description,
    NULL AS other_reason_detail,
    pftf.encntr_num AS invoice_number_encounter_id,
    pftf.amount AS transaction_amount,
    eaf.fin
FROM
    pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
    ------------------------------------------------------------------
    INNER JOIN pce_qe16_slp_prd_dm..prd_fnc_txn_fct pftf
    ON
        eaf.fcy_nm = pftf.fcy_nm AND
        eaf.encntr_num = pftf.encntr_num
    ------------------------------------------------------------------
    INNER JOIN pce_qe16_slp_prd_dm..inflow_population_data pop
    ON
        eaf.fcy_nm = pop.fcy_nm AND
        eaf.encntr_num = pop.encntr_num
    ------------------------------------------------------------------
    FULL OUTER JOIN pce_qe16_slp_prd_dm..inflow_extract_date_info edi
    ON
        1 = 1
    ------------------------------------------------------------------
WHERE
    LOWER(pftf.transtype) NOT IN ('non-posting') AND
    (DATE(pftf.postdate) BETWEEN edi.bill_txn_start_dt AND edi.bill_txn_end_dt)
DISTRIBUTE ON (fin)
;


---Inflow Scheduling
DROP TABLE pce_qe16_slp_prd_dm..inflow_scheduling_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..inflow_scheduling_data AS
SELECT DISTINCT
    sc.schapptid AS appt_id,
    sc.appointmentsubfacility AS location,
    sc.appointmentlocation AS practice_name,
    NULL AS department_name,
    NULL AS cost_center,
    NULL AS cost_center_description,
    sc.appointmentprovidernamefull AS appt_provider_full_name,
    NULL AS appt_provider_first_name,
    NULL AS appt_provider_middle_name,
    NULL AS appt_provider_last_name,
    sc.appointmentprovidercredential AS appt_provider_credentials,
    sc.appointmentprovidernpi AS appt_provider_npi,
    sc.appt_pvdr_pract_spclty_descr AS appt_provider_primary_specialty,
    sc.referringprovidernamefull AS referring_provider_full_name,
    NULL AS referring_provider_first_name,
    NULL AS referring_provider_middle_name,
    NULL AS referring_provider_last_name,
    sc.referringprovidercredential AS referring_provider_credentials,
    sc.referringprovidernpi AS referring_provider_npi,
    sc.referring_pvdr_pract_spclty_descr AS referring_provider_primary_specialty,
    sc.mrn AS patient_identifier,
    sc.appointmenttype AS appt_type,
    sc.appointmentcreatedate AS created_date,
    sc.appointmentdate AS appt_date,
    sc.cancelperformdate AS cancel_date,
    sc.cancelreason AS cancel_reason,
    sc.appointmenttime AS appt_time,
    sc.appointmentduration AS scheduled_length,
    sc.appointmentstatus AS appt_status,
    sc.checkinperformdate AS check_in_date,
    sc.checkinperformtime AS check_in_time,
    sc.checkoutperformdate AS check_out_date,
    sc.checkoutperformtime AS check_out_time,
    sc.reschedulecount AS reschedule_indicator,
    sc.rescheduleperformdate AS reschedule_date,
    sc.rescheduleperformtime AS reschedule_time,
    sc.reschedulereason AS reschedule_reason,
    sc.orderingprovidernamefull AS ordering_provider_name,
    sc.orderingprovidercredential AS ordering_provider_credentials,
    sc.orderingprovidernpi AS ordering_provider_npi,
    sc.fin AS fin,
    ap.apptnt_type AS inflow_new_est_appointment,
    re.bumped_cancelled AS reschedule_type,
    ca.bumped_cancelled AS cancel_type,
    sc.encountertype AS encounter_type,
    sc.encounterstatus AS encounter_status
FROM
    pce_qe16_slp_prd_dm..prd_phys_scheduling_fct sc
    ------------------------------------------------------------------
    LEFT OUTER JOIN pce_qe16_slp_prd_dm..apptnt_type_dim ap
    ON
        sc.appointmenttype = ap.apptnt
    ------------------------------------------------------------------
    LEFT OUTER JOIN pce_qe16_slp_prd_dm..cancel_reschedule_reason_dim re
    ON
        re.reschedule_cancel_reason = sc.reschedulereason
    ------------------------------------------------------------------
    LEFT OUTER JOIN pce_qe16_slp_prd_dm..cancel_reschedule_reason_dim ca
    ON
        ca.reschedule_cancel_reason = sc.cancelreason
    ------------------------------------------------------------------
    FULL OUTER JOIN pce_qe16_slp_prd_dm..inflow_extract_date_info edi
    ON
        1 = 1
    ------------------------------------------------------------------
WHERE
    sc.appointmentprovidercount > 0 AND
    sc.appointmentprovidernpi IS NOT NULL AND
    (COALESCE(sc.appointmentstatus,'') NOT IN ('Confirmed','Hold')) AND
    ((COALESCE(sc.cancelreason,'') NOT IN ('Hold','Other')) OR (COALESCE(sc.reschedulereason,'') NOT IN ('Hold','Other'))) AND
    sc.encountertype IN ('Clinic', 'Outpatient', 'Recurring') AND
    (DATE(appt_date) BETWEEN sched_pyrl_start_dt AND sched_pyrl_end_dt)
DISTRIBUTE ON (appt_id)
;

\unset ON_ERROR_STOP

