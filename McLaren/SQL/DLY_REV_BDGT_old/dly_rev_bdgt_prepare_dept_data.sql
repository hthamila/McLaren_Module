\set ON_ERROR_STOP ON;

--This table holds the start and end date info for revenue budget calculation
DROP TABLE pce_qe16_slp_prd_stg..dly_rev_bdgt_date_info IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg..dly_rev_bdgt_date_info AS
SELECT
    min(TO_DATE(date, 'MMDDYYYY')) AS rev_bdgt_strt_dt,
    max(TO_DATE(date, 'MMDDYYYY')) AS rev_bdgt_end_dt,
    (rev_bdgt_strt_dt - INTERVAL '12 MONTHS') AS actual_rev_strt_dt,
    (rev_bdgt_end_dt - INTERVAL '12 MONTHS') AS actual_rev_end_dt
FROM
    pce_qe16_inst_bill_prd_lnd..budget;

--This table holds the revenue budget at month level
DROP TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt AS
SELECT
    CASE
        WHEN combined_facility = 'St Lukes' THEN 'St. Lukes'
        ELSE combined_facility
    END AS fcy_nm,
    TO_DATE(date,'MMDDYYYY') AS budget_month, --column name needs to be changed as budget month
    CASE
        WHEN inpatient_outpatient_flag = 'Inpatient' THEN 'I'
        WHEN inpatient_outpatient_flag = 'Outpatient' THEN 'O'
        ELSE inpatient_outpatient_flag
    END AS in_or_out_patient_ind,
    department_group2,
    sum(CAST(budget AS decimal(24, 6))) AS budget_amt
FROM
    pce_qe16_inst_bill_prd_lnd..budget
WHERE
    inpatient_outpatient_flag IN ('Outpatient', 'Inpatient')
    AND fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
GROUP BY
    fcy_nm,
    budget_month,
    in_or_out_patient_ind,
    department_group2
DISTRIBUTE ON (fcy_nm,budget_month,in_or_out_patient_ind);


--This table has the number of days,weekdays,day name.
DROP TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_cdr_dim IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_cdr_dim AS
SELECT
    cd.cdr_dt,
    TO_CHAR(cd.cdr_dt,'YYYYMM') AS cdr_month,
    cd.day_of_wk_num,
    cd.day_of_wk_abbr,
    cd.num_of_days_in_mo,
    count(*) OVER(PARTITION BY cd.yr_num,cd.mo_of_yr_num,cd.day_of_wk_num ) AS day_of_wk_cnt,
    cd_prev.num_of_days_in_cur_mo,
    cd_prev.day_of_wk_cnt_cur_yr
FROM
    pce_qe16_slp_prd_dm..cdr_dim cd
INNER JOIN (
SELECT
    TO_CHAR((cdr_dt - INTERVAL '12 MONTHS'),'YYYYMM') AS prior_cdr_month,
    TO_CHAR(cdr_dt,'YYYYMM') AS cdr_month,
    day_of_wk_num,
    day_of_wk_abbr,
    num_of_days_in_mo AS num_of_days_in_cur_mo,
    count(*) AS day_of_wk_cnt_cur_yr
FROM
    pce_qe16_slp_prd_dm..cdr_dim
GROUP BY prior_cdr_month,cdr_month,day_of_wk_num,day_of_wk_abbr,num_of_days_in_cur_mo) cd_prev
    ON TO_CHAR(cd.cdr_dt,'YYYYMM') = cd_prev.prior_cdr_month
    AND cd.day_of_wk_num = cd_prev.day_of_wk_num
FULL OUTER JOIN pce_qe16_slp_prd_stg..dly_rev_bdgt_date_info
ON 1=1
WHERE
    DATE(cd.cdr_dt) BETWEEN actual_rev_strt_dt AND actual_rev_end_dt
DISTRIBUTE ON(cdr_dt);


--This table has the actual revenue at service date level
DROP TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_actual IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_actual AS
SELECT
    ef.fcy_nm,
    ef.in_or_out_patient_ind,
    cf.dept_grp_2,
    cf.service_date,
    TO_CHAR(cf.service_date,'YYYYMM') AS service_month,
    cd.day_of_wk_num,
    cd.day_of_wk_abbr,
    cd.num_of_days_in_mo,
    cd.day_of_wk_cnt,
    cd.num_of_days_in_cur_mo,
    cd.day_of_wk_cnt_cur_yr,
    sum(cf.total_charge) AS charge_amt
FROM
    pce_qe16_slp_prd_dm..prd_encntr_anl_fct ef
INNER JOIN pce_qe16_slp_prd_dm..prd_chrg_fct cf ON
    ef.fcy_nm = cf.fcy_nm
    AND ef.encntr_num = cf.encntr_num
INNER JOIN pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_cdr_dim cd
ON cd.cdr_dt = cf.service_date
FULL OUTER JOIN pce_qe16_slp_prd_stg..dly_rev_bdgt_date_info
ON 1=1
WHERE
    DATE(cf.service_date) BETWEEN actual_rev_strt_dt AND actual_rev_end_dt
    AND ef.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
    AND ef.excld_trnsfr_encntr_ind = '0'
    AND ef.tot_chrg_ind = '1'
GROUP BY
    ef.fcy_nm,
    ef.in_or_out_patient_ind,
    cf.dept_grp_2,
    cf.service_date,
    service_month,
    cd.day_of_wk_num,
    cd.day_of_wk_abbr,
    cd.num_of_days_in_mo,
    cd.day_of_wk_cnt,
    cd.num_of_days_in_cur_mo,
    cd.day_of_wk_cnt_cur_yr
DISTRIBUTE ON (fcy_nm,in_or_out_patient_ind,service_date);

--This table has the department group details which has does not have actual
DROP TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_actual_missing IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_actual_missing AS
    SELECT
    bd.fcy_nm,
    bd.in_or_out_patient_ind,
    bd.department_group2 AS dept_grp_2,
    cd.cdr_dt AS service_date,
    TO_CHAR((bd.budget_month - INTERVAL '12 MONTHS'),'YYYYMM') AS service_month,
    cd.day_of_wk_num,
    cd.day_of_wk_abbr,
    cd.num_of_days_in_mo,
    cd.day_of_wk_cnt,
    cd.num_of_days_in_cur_mo,
    cd.day_of_wk_cnt_cur_yr,
    bd.budget_amt / cd.num_of_days_in_mo AS charge_amt
FROM
    pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt bd
INNER JOIN pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_cdr_dim cd
ON
    TO_CHAR((bd.budget_month - INTERVAL '12 MONTHS'),'YYYYMM') = cd.cdr_month
LEFT OUTER JOIN (
    SELECT
        DISTINCT
        fcy_nm,
        in_or_out_patient_ind,
        dept_grp_2,
        service_month
        --day_of_wk_num_dr
    FROM
        pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_actual)ba
ON
    bd.fcy_nm = ba.fcy_nm
    AND bd.in_or_out_patient_ind = ba.in_or_out_patient_ind
    AND bd.department_group2 = ba.dept_grp_2
    AND TO_CHAR((bd.budget_month - INTERVAL '12 MONTHS'),'YYYYMM') = ba.service_month
    --cd.day_of_wk_num_dr = ba.day_of_wk_num_dr
WHERE
    bd.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
    AND ba.dept_grp_2 IS NULL
DISTRIBUTE  ON (fcy_nm,in_or_out_patient_ind,service_date);


--This table has the calculated revenue at day of week level for prior year
DROP TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_prior_yr IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_prior_yr AS
SELECT
    fcy_nm,
    in_or_out_patient_ind,
    service_month,
    dept_grp_2,
    day_of_wk_num,
    day_of_wk_abbr,
    num_of_days_in_mo,
    day_of_wk_cnt,
    num_of_days_in_cur_mo,
    day_of_wk_cnt_cur_yr,
    CAST(sum(charge_amt) AS DOUBLE) AS total_rev_prior_yr,
    CAST(total_rev_prior_yr/day_of_wk_cnt AS DOUBLE) AS per_day_rev_prior_yr,
    CAST(sum(per_day_rev_prior_yr) over(partition by fcy_nm,in_or_out_patient_ind, service_month,dept_grp_2) AS DOUBLE) AS per_wk_rev_prior_yr,
    CAST((CASE WHEN per_day_rev_prior_yr = 0 THEN 0 ELSE  per_day_rev_prior_yr/per_wk_rev_prior_yr END) AS DOUBLE) AS percent_per_day_rev_prior_yr
FROM
    ( 
   SELECT * FROM pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_actual
   UNION ALL 
   SELECT * FROM pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_actual_missing)A
GROUP BY
    fcy_nm,
    in_or_out_patient_ind,
    service_month,
    dept_grp_2,
    day_of_wk_num,
    day_of_wk_abbr,
    num_of_days_in_mo,
    day_of_wk_cnt,
    num_of_days_in_cur_mo,
    day_of_wk_cnt_cur_yr
DISTRIBUTE ON (fcy_nm,in_or_out_patient_ind,service_month);


--This table has the calculated revenue at day of week level for current year
DROP TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_cur_yr IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_cur_yr AS
SELECT
    py.fcy_nm,
    py.in_or_out_patient_ind,
    py.service_month,
    bd.budget_month,
    py.dept_grp_2,
    py.day_of_wk_num,
    py.day_of_wk_abbr,
    py.num_of_days_in_mo,
    py.day_of_wk_cnt,
    py.num_of_days_in_cur_mo,
    py.day_of_wk_cnt_cur_yr,
    py.total_rev_prior_yr,
    py.per_day_rev_prior_yr,
    py.per_wk_rev_prior_yr,
    py.percent_per_day_rev_prior_yr,
    CAST(percent_per_day_rev_prior_yr*day_of_wk_cnt_cur_yr AS DOUBLE) AS total_rvu_cur_yr,
    CAST(sum(total_rvu_cur_yr) over(partition by py.fcy_nm,py.in_or_out_patient_ind,py.service_month,py.dept_grp_2) AS DOUBLE) AS per_wk_rev_cur_yr,
    CAST((CASE WHEN total_rvu_cur_yr = 0 THEN 0 ELSE total_rvu_cur_yr/per_wk_rev_cur_yr END) AS DOUBLE) AS percent_per_day_cur_yr,
    bd.budget_amt,
    CAST((percent_per_day_cur_yr*budget_amt) AS DOUBLE) AS total_rev_cur_yr,
    CAST((total_rev_cur_yr/py.day_of_wk_cnt_cur_yr) AS DOUBLE) AS daily_budget_amount
FROM
    pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_prior_yr py
INNER JOIN (
SELECT
    fcy_nm,
    TO_CHAR((budget_month - INTERVAL '12 MONTHS'),'YYYYMM') as budget_month_prior_yr,
    budget_month,
    in_or_out_patient_ind,
    department_group2,
    sum(budget_amt) AS budget_amt
FROM
    pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt
GROUP BY
    fcy_nm,
    budget_month,
    in_or_out_patient_ind,
    department_group2
)bd
ON bd.fcy_nm = py.fcy_nm
AND bd.budget_month_prior_yr = py.service_month
AND bd.in_or_out_patient_ind = py.in_or_out_patient_ind
AND bd.department_group2 = py.dept_grp_2
DISTRIBUTE ON (fcy_nm,in_or_out_patient_ind,service_month);


--This table hold the forecasted daily revenue budget at service date level
DROP TABLE pce_qe16_slp_prd_stg..stg_dly_rev_bdgt_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_stg..stg_dly_rev_bdgt_data AS
SELECT
    cr.fcy_nm AS facility,
    cd.cdr_dt AS service_date,
    cr.in_or_out_patient_ind AS ptnt_type,
    cr.dept_grp_2,
    cr.daily_budget_amount
FROM
    pce_qe16_slp_prd_stg.prmradmp.intermediate_stg_rev_bdgt_cur_yr cr
INNER JOIN pce_qe16_slp_prd_dm..cdr_dim cd
ON
    cr.day_of_wk_num = cd.day_of_wk_num
    AND TO_CHAR(cr.budget_month,'YYYYMM') = TO_CHAR(cd.cdr_dt,'YYYYMM')
DISTRIBUTE ON (facility,service_date,ptnt_type);


\unset ON_ERROR_STOP
