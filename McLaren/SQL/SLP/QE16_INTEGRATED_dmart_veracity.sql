\set ON_ERROR_STOP ON;


DROP TABLE pce_qe16_slp_prd_dm..prd_inventory_tasks IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_inventory_tasks AS (
SELECT 
case_id,
task_id, 
pcn, 
TO_DATE(task_due_date,'MMDDYYYY') AS task_due_date, 
task_name, 
TO_DATE(task_created_date, 'MMDDYYYY') AS task_created_date, 
CAST(task_created_time AS TIME) AS task_created_time,
TO_DATE(task_closed_date, 'MMDDYYYY') AS task_closed_date,
CAST(task_closed_time AS TIME) AS task_closed_time, 
task_outcome, 
task_owner, stage, 
task_level, 
primary_task, 
appeal_amount, 
denied_amount, 
recovered_amount, 
underpayment_amount
FROM pce_qe16_oper_prd_zoom..cv_inventory_tasks);


DROP TABLE pce_qe16_slp_prd_dm..prd_inventory_cases IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..prd_inventory_cases AS (
SELECT
patient,
case_id, 
pcn, 
TO_DATE(case_created,'MMDDYYYY') AS case_created,
TO_DATE(case_closed,'MMDDYYYY') AS case_closed,
closed_reason, 
TO_DATE(svc_from,'MMDDYYYY') AS svc_from, 
TO_DATE(svc_to,'MMDDYYYY') AS svc_to,
TO_DATE(initial_case_request_date,'MMDDYYYY') AS initial_case_request_date, 
facility, 
case_owner, 
case_type, 
case_category, 
payer, 
payer_plan, 
other_payer_plan_name, 
acct_num, 
initial_at_risk, 
charges, 
paid, 
contractual_adj, 
patient_rep, 
recoupment_amount, 
date_of_recoupment
FROM pce_qe16_oper_prd_zoom..cv_inventory_cases);


\unset ON_ERROR_STOP
