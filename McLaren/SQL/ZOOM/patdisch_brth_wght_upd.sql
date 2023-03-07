\set ON_ERROR_STOP ON;

UPDATE patdisch a
SET a.birth_weight_in_grams = b.birth_weight_in_grams,a.ccn_care_setting = b.ccn_care_setting
FROM (select distinct company_id, patient_id,birth_weight_in_grams, ccn_care_setting, rank from pulse_qa_processing_adhoc_updates) b where
a.company_id = b.company_id and
a.patient_id = b.patient_id and
b.rank = 1 and a.rcrd_btch_audt_id = (select max(batch_id) from pce_qe16_prd_utl..batch_header where status = 'COMPLETED');

--2022-03-23: MLH-967 : Updates for Mother_Id and Birth Weight in Grams
UPDATE patdisch a
SET 
    a.birth_weight_in_grams = b.birth_weight_in_grams,
    a.mothersaccount = b.mom_encounter_no,
    a.mothersname = b.mom_name
FROM
    (SELECT DISTINCT billing_number,birth_weight_in_grams, mom_encounter_no , mom_name FROM pulse_pce_processing) b 
WHERE
    a.company_id = 'Flint' and
    a.patient_id = b.billing_number and 
    (a.rcrd_pce_cst_src_nm, a.rcrd_btch_audt_id) IN (SELECT source, MAX(batch_id) FROM pce_qe16_prd_utl..batch_header WHERE status = 'COMPLETED' and source in ('INST_BILL','CERNER') GROUP BY source );

generate statistics on patdisch;

\unset ON_ERROR_STOP
