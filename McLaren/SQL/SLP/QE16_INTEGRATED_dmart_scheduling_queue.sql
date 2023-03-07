\set ON_ERROR_STOP ON;

SELECT 'processing table: intermediate_stage_sch_req_queue_fct' AS table_processing;
DROP TABLE intermediate_stage_sch_req_queue_fct IF EXISTS;
CREATE TABLE intermediate_stage_sch_req_queue_fct AS
(
SELECT
    request_queue,
    request_action,
    appt_type,
    request_made_date,
    request_made_time,
    mrn,
    patient_name,
    entry_id,
    remove,
    se_entry_state,
    se_update_date,
    se_update_time,
    sea_sch_state,
    sea_order_status,
    sea_update_date,
    sea_update_time,
    order_status,
    order_status_date,
    order_status_time,
    order_mnemonic,
    location_code_value_set,
    location,
    order_date,
    order_time,
    order_id,
    request_creator,
    creator_npi,
    creator_position,
    extract_date
FROM
(
    SELECT
        request_queue,
        request_action,
        appt_type,
        request_made_date,
        request_made_time,
        mrn,
        patient_name,
        entry_id,
        remove,
        se_entry_state,
        se_update_date,
        se_update_time,
        sea_sch_state,
        sea_order_status,
        sea_update_date,
        sea_update_time,
        order_status,
        order_status_date,
        order_status_time,
        order_mnemonic,
        location_code_value_set,
        location,
        order_date,
        order_time,
        order_id,
        request_creator,
        creator_npi,
        creator_position,
        extract_date,
        ROW_NUMBER() OVER (PARTITION BY entry_id, request_queue ORDER BY request_made_date DESC, request_made_time DESC, se_update_date DESC, se_update_time DESC, sea_update_date DESC, sea_update_time DESC, order_status_date DESC, order_status_time DESC, order_date DESC, order_time DESC, extract_date DESC) AS rn_sch_req_queue
    FROM
        pce_qe16_oper_prd_zoom..cv_sch_req_queue
) sch_req_queue
WHERE
    rn_sch_req_queue = 1
)
DISTRIBUTE ON (entry_id);


SELECT 'processing table: intermediate_stage_sch_req_queue_fct_prev' AS table_processing;
DROP TABLE intermediate_stage_sch_req_queue_fct_prev IF EXISTS;
ALTER TABLE intermediate_sch_req_queue_fct RENAME TO intermediate_stage_sch_req_queue_fct_prev;
ALTER TABLE intermediate_stage_sch_req_queue_fct RENAME TO intermediate_sch_req_queue_fct;


select 'processing table: prd_sch_req_queue_fct' as table_processing;
DROP TABLE prd_sch_req_queue_fct IF EXISTS;
CREATE TABLE prd_sch_req_queue_fct AS SELECT *,now() as rcrd_isrt_ts FROM intermediate_sch_req_queue_fct;

\unset ON_ERROR_STOP

