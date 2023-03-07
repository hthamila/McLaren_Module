\set ON_ERROR_STOP ON;

DROP TABLE pce_qe16_hr_prd_zoom..inflow_payroll_data IF EXISTS;
CREATE TABLE pce_qe16_hr_prd_zoom..inflow_payroll_data AS
SELECT
    pr.emplid AS employee_id,
    pr.firstname AS employee_first_name,
    pr.middlename AS employee_middle_name,
    pr.lastname AS employee_last_name,
    pr.employeename AS employee_full_name,
    NULL AS employee_npi,
    pr.jobcode AS job_code_id,
    pr.jobcodedescr AS job_code_description,
    pr.homedeptid AS department_id,
    pr.homedeptdescr AS department_name,
    pr.paybegindt AS pay_period_start_date,
    pr.payenddt AS pay_period_end_date,
    pr.checkdt AS check_date,
    pr.paycode AS earnings_code,
    pr.paycodedescr AS earnings_description,
    pr.hours AS hours,
    pr.amount AS amount,
    pr.company AS company,
    pr.paygroup AS paygroup,
    pc.include_exclude AS paycode_inclusion_indicator,
    pc.hours_category AS hours_category
FROM
    pce_qe16_hr_prd_zoom..cv_peoplesoft_payroll pr
    LEFT OUTER JOIN pce_qe16_slp_prd_dm..inflow_paycode pc
    ON
        pr.paycode = pc.pay_code
    FULL OUTER JOIN pce_qe16_slp_prd_dm..inflow_extract_date_info edi
    ON
        1 = 1
WHERE
   (DATE(pr.payenddt) BETWEEN sched_pyrl_start_dt AND sched_pyrl_end_dt)
DISTRIBUTE ON (employee_id,pay_period_end_date)
;

\unset ON_ERROR_STOP
