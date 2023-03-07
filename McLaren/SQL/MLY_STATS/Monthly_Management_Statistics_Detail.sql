\set ON_ERROR_STOP ON;

DROP TABLE pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail (
    fcy_nm VARCHAR(250),
    in_or_out_patient_ind VARCHAR(3),
    report_date_name VARCHAR(250),
    report_date TIMESTAMP,
    report_month VARCHAR(10),
    peoplesoft_acct_num VARCHAR(25),
    peoplesoft_stat_name VARCHAR(250),
    measure_detail_unit VARCHAR(250),
    measure_detail_code VARCHAR(250),
    measure_detail_description VARCHAR(250),
    measure_unit VARCHAR(250),
    measure_value numeric(38,10),
    measure_numr numeric(38,10),
    measure_dnmr numeric(38,10)
)DISTRIBUTE ON (fcy_nm, peoplesoft_acct_num);


--- Observation Discharges
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail WHERE peoplesoft_acct_num = '850005';
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail
(fcy_nm,in_or_out_patient_ind, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_detail_unit, measure_detail_code, measure_detail_description, measure_unit, measure_value)
(
SELECT
    eaf.fcy_nm AS fcy_nm,
    NULL AS in_or_out_patient_ind,
    'Discharge Date' AS report_date_name,
    eaf.dschrg_dt AS report_date,
    TO_CHAR(eaf.dschrg_dt,'YYYYMM') AS report_month,
    '850005' AS peoplesoft_acct_num,
    'Observation Discharges' AS peoplesoft_stat_name,
    'Nursing Station' AS measure_detail_unit,
    NULL AS measure_detail_code,
    --eaf.nrg_stn AS measure_detail_description,
    eaf.last_nrsng_dept AS measure_detail_description,
    'Total - Discharges' AS measure_unit,
    SUM(eaf.obsrv_stay_ind) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_dschrg_stat_metrics_eaf eaf
WHERE
    eaf.in_or_out_patient_ind = 'O' AND
    eaf.obsrv_stay_ind = '1'
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
);


---Observation Patient Days
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail WHERE peoplesoft_acct_num IN ('850020');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail
(fcy_nm,in_or_out_patient_ind, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_detail_unit, measure_detail_code, measure_detail_description, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    eaf_cf.in_or_out_patient_ind,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    '850020' AS peoplesoft_acct_num,
    'Observation Day' AS peoplesoft_stat_name,
    'Department' AS measure_detail_unit,
    eaf_cf.dept AS measure_detail_code,
    eaf_cf.department_description AS measure_detail_description,
    'Quantity/24' AS measure_unit, --change
    SUM(eaf_cf.quantity)/24 AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
WHERE
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.service_date <= CURRENT_DATE AND
    eaf_cf.in_or_out_patient_ind = 'O' AND
    eaf_cf.revenue_code = '0762' AND
    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
);

---Observation Hours (minutes)
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail WHERE peoplesoft_acct_num IN ('850021');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail
(fcy_nm,in_or_out_patient_ind, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_detail_unit, measure_detail_code, measure_detail_description, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
eaf_cf.in_or_out_patient_ind,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    '850021' AS peoplesoft_acct_num,
    'Observation Minutes' AS peoplesoft_stat_name,
    'Department' AS measure_detail_unit,
    eaf_cf.dept AS measure_detail_code,
    eaf_cf.department_description AS measure_detail_description,
    'Quantity*60' AS measure_unit,--change
    SUM(eaf_cf.quantity)*60 AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
WHERE
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.service_date <= CURRENT_DATE AND
    eaf_cf.in_or_out_patient_ind = 'O' AND
    eaf_cf.revenue_code = '0762' AND
    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
);


---IP Encounters AND OP Encounters-By Dept
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail WHERE peoplesoft_acct_num IN ('810051','830051');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail
(fcy_nm,in_or_out_patient_ind, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_detail_unit, measure_detail_code, measure_detail_description, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm_grp AS fcy_nm,
    eaf_cf.in_or_out_patient_ind,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    CASE
        WHEN eaf_cf.in_or_out_patient_ind = 'I' THEN '810051'
        WHEN eaf_cf.in_or_out_patient_ind = 'O' THEN '830051'
    END AS peoplesoft_acct_num,
    CASE
        WHEN eaf_cf.in_or_out_patient_ind = 'I' THEN 'IP Encounters'
        WHEN eaf_cf.in_or_out_patient_ind = 'O' THEN 'OP Encounters'
    END AS peoplesoft_stat_name,
    'Department' AS measure_detail_unit,
    eaf_cf.dept_derived AS measure_detail_code,
    eaf_cf.department_description_derived AS measure_detail_description,
    'Encounters' AS measure_unit,
    SUM(fin_or_encntr_num_cnt) AS measure_value
FROM
    (
        SELECT
            REPLACE(fcy_nm,' Prof','') AS fcy_nm_grp,
            in_or_out_patient_ind,
            CASE WHEN REPLACE(fcy_nm,' Prof','') IN ('Bay','Central','Flint','Lansing','Lapeer','Macomb','Oakland','St. Lukes') THEN fin ELSE encntr_num END AS fin_or_encntr_num,
            service_date,
            CASE 
                 WHEN UPPER(department_group) = 'LAB' THEN 'Lab'
                 WHEN UPPER(department_group) = 'Lab Off Site' THEN 'Lab Off Site'
                 ELSE dept 
            END AS dept_derived,
            CASE 
                WHEN UPPER(department_group) = 'LAB' THEN 'Lab'
                WHEN UPPER(department_group) = 'Lab Off Site' THEN 'Lab Off Site'
                ELSE department_description 
            END AS department_description_derived,
            COUNT(DISTINCT fin_or_encntr_num) AS fin_or_encntr_num_cnt
        FROM
            pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf
        WHERE
            --COALESCE(revenue_code,'') NOT IN ('0762') AND --2023-01-18
            SUBSTRING(COALESCE(revenue_code,''),1,3) NOT IN ('010','011','012','013','014','015','016','017','020','021') AND
            COALESCE(total_charge,0) <> 0 AND
            (CASE WHEN LENGTH(dept) = 9 AND (SUBSTRING(dept,5,5) BETWEEN '50000' AND '58574') THEN 0 ELSE 1 END) = 1 AND
            COALESCE(department_group,'') NOT IN ('Nursing','Nursing - Critical Care') AND
            COALESCE(excld_trnsfr_encntr_ind,0) = 0 AND
            tot_chrg_ind = 1 
        GROUP BY
            fcy_nm_grp,
            in_or_out_patient_ind,
            fin_or_encntr_num,
            service_date,
            dept_derived,
            department_description_derived
    ) eaf_cf 
WHERE
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.service_date <= CURRENT_DATE --AND
    --eaf_cf.fcy_nm_grp IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
);


---Inpatient Encounter Types
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail WHERE peoplesoft_acct_num IN ('INF001','810008','810007','810016','810017','INF002','810005');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail
(fcy_nm,in_or_out_patient_ind, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_detail_unit, measure_detail_code, measure_detail_description, measure_unit, measure_value)
(
SELECT
    eaf.fcy_nm AS fcy_nm,
    NULL AS in_or_out_patient_ind,
    'Discharge Date' AS report_date_name,
    eaf.dschrg_dt AS report_date,
    TO_CHAR(eaf.dschrg_dt,'YYYYMM') AS report_month,
    CASE
        WHEN eaf.iptnt_encntr_type = 'Specialty Care' THEN 'INF001'
        WHEN eaf.iptnt_encntr_type = 'Rehab' THEN '810008'
        WHEN eaf.iptnt_encntr_type = 'Psych' THEN '810007'
        WHEN eaf.iptnt_encntr_type = 'Hospice' THEN '810016'
        WHEN eaf.iptnt_encntr_type = 'Newborn' THEN '810017'
        WHEN eaf.boarder_baby_ind = 1 THEN 'INF002'
        WHEN eaf.iptnt_encntr_type = 'Acute' THEN '810005'
    END AS peoplesoft_acct_num,
    CASE
        WHEN eaf.iptnt_encntr_type = 'Specialty Care' THEN 'IP Discharges-Special Care'
        WHEN eaf.iptnt_encntr_type = 'Rehab' THEN 'IP Discharges-Rehab'
        WHEN eaf.iptnt_encntr_type = 'Psych' THEN 'IP Discharges-Psych'
        WHEN eaf.iptnt_encntr_type = 'Hospice' THEN 'IP Discharges-Hospice'
        WHEN eaf.iptnt_encntr_type = 'Newborn' THEN 'IP Discharges-Newborn'
        WHEN eaf.boarder_baby_ind = 1 THEN 'IP Discharges-Boarder Baby'
        WHEN eaf.iptnt_encntr_type = 'Acute' THEN 'IP Discharges-Acute'
    END AS peoplesoft_stat_name,
    'Nursing Station' AS measure_detail_unit,
    NULL AS measure_detail_code,
    --eaf.nrg_stn AS measure_detail_description,
    eaf.last_nrsng_dept AS measure_detail_description,
    'Total - Discharges' AS measure_unit,
    SUM(eaf.iptnt_dschrg_ind) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_dschrg_stat_metrics_eaf eaf
WHERE
    eaf.in_or_out_patient_ind = 'I' AND
    eaf.dschrg_dt IS NOT NULL AND
    eaf.iptnt_dschrg_ind = 1
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
);




---Patient Days - Inpatient Encounter Types
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail WHERE peoplesoft_acct_num IN ('810028','810029', '810026', '810027', 'INF003', '810025','INF004');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail
(fcy_nm,in_or_out_patient_ind, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_detail_unit, measure_detail_code, measure_detail_description, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    NULL AS in_or_out_patient_ind,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    CASE
        WHEN eaf_cf.iptnt_encntr_type = 'Specialty Care' THEN 'INF003'
        WHEN eaf_cf.iptnt_encntr_type = 'Rehab' THEN '810029'
        WHEN eaf_cf.iptnt_encntr_type = 'Psych' THEN '810028'
        WHEN eaf_cf.iptnt_encntr_type = 'Hospice' THEN '810026'
        WHEN eaf_cf.iptnt_encntr_type = 'Newborn' THEN '810027'
        WHEN eaf_cf.boarder_baby_ind = 1 THEN 'INF004'
        WHEN eaf_cf.in_or_out_patient_ind = 'I' AND LOWER(eaf_cf.ptnt_tp_cd) NOT IN ('lip','mip') THEN '810025'
    END AS peoplesoft_acct_num,
    CASE
        WHEN eaf_cf.iptnt_encntr_type = 'Specialty Care' THEN 'IP Patient Days-Special Care'
        WHEN eaf_cf.iptnt_encntr_type = 'Rehab' THEN 'IP Patient Days-Rehab'
        WHEN eaf_cf.iptnt_encntr_type = 'Psych' THEN 'IP Patient Days-Psych'
        WHEN eaf_cf.iptnt_encntr_type = 'Hospice' THEN 'IP Patient Days-Hospice'
        WHEN eaf_cf.iptnt_encntr_type = 'Newborn' THEN 'IP Patient Days-Newborn'
        WHEN eaf_cf.boarder_baby_ind = 1 THEN 'IP Patient Days-Boarder Baby'
        WHEN eaf_cf.in_or_out_patient_ind = 'I' AND LOWER(eaf_cf.ptnt_tp_cd) NOT IN ('lip','mip') THEN 'IP Patient Days-Adult & Peds'
    END AS peoplesoft_stat_name,
        'Department' AS measure_detail_unit,
    eaf_cf.dept AS measure_detail_code,
    eaf_cf.department_description AS measure_detail_description,
    'Quantity' AS measure_unit,
    SUM(eaf_cf.quantity) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
WHERE
    eaf_cf.in_or_out_patient_ind = 'I' AND
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb') AND
    SUBSTRING(eaf_cf.revenue_code, 1, 3) IN ('010', '011', '012', '013', '014', '015', '016', '017', '020', '021')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
);


---Emergency Room
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail WHERE peoplesoft_acct_num IN ('850050');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail
(fcy_nm,in_or_out_patient_ind, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_detail_unit, measure_detail_code, measure_detail_description, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    eaf_cf.in_or_out_patient_ind,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    '850050' AS peoplesoft_acct_num,
    'ED Visits(IP and Outpatient)' AS peoplesoft_stat_name,
    'Department' AS measure_detail_unit,
    CASE 
         WHEN UPPER(department_group) = 'LAB' THEN 'Lab'
         WHEN UPPER(department_group) = 'Lab Off Site' THEN 'Lab Off Site'
         ELSE dept 
    END AS measure_detail_code,
    CASE 
         WHEN UPPER(department_group) = 'LAB' THEN 'Lab'
         WHEN UPPER(department_group) = 'Lab Off Site' THEN 'Lab Off Site'
         ELSE department_description 
    END AS measure_detail_description,
    'Quantity' AS measure_unit,
    SUM(eaf_cf.quantity) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
WHERE
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.service_date <= CURRENT_DATE AND
    eaf_cf.cpt_code IN ('99281','99282','99283','99284','99285','99291') AND
    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
);


---IP Surgical Cases and OP Surgical Cases
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail WHERE peoplesoft_acct_num IN ('810060','830060');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail
(fcy_nm,in_or_out_patient_ind, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_detail_unit, measure_detail_code, measure_detail_description, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    eaf_cf.in_or_out_patient_ind,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    CASE
        WHEN eaf_cf.in_or_out_patient_ind = 'I' THEN '810060'
        WHEN eaf_cf.in_or_out_patient_ind = 'O' THEN '830060'
    END AS peoplesoft_acct_num,
    CASE
        WHEN eaf_cf.in_or_out_patient_ind = 'I' THEN 'IP Surgical Cases'
        WHEN eaf_cf.in_or_out_patient_ind = 'O' THEN 'OP Surgical Cases'
    END AS peoplesoft_stat_name,
    'Department' AS measure_detail_unit,
    eaf_cf.dept AS measure_detail_code,
    eaf_cf.department_description AS measure_detail_description,
    'Surg Cases' AS measure_unit, 
    COUNT(DISTINCT eaf_cf.encntr_num) AS measure_value
FROM
    (
        SELECT
            fcy_nm,
            encntr_num,
            service_date,
            in_or_out_patient_ind,
            dept,
            department_description,
            SUM(quantity) AS agg_qty
        FROM
            pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf
        WHERE
            service_date IS NOT NULL AND
            (
                persp_clncl_smy_descr IN ('SURGERY TIME','AMBULATORY SURGERY SERVICES') OR
                raw_chargcode IN ('SN99042','65000002')
            ) AND
            fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
        GROUP BY
            fcy_nm,
            encntr_num,
            service_date,
            in_or_out_patient_ind,
            dept,
            department_description
    ) eaf_cf
WHERE
    agg_qty > 0
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
);


---IP OR Case Minutes AND OP OR Case Minutes
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail WHERE peoplesoft_acct_num IN ('810061','830061');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail
(fcy_nm,in_or_out_patient_ind, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_detail_unit, measure_detail_code, measure_detail_description, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    eaf_cf.in_or_out_patient_ind,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    CASE
        WHEN eaf_cf.in_or_out_patient_ind = 'I' THEN '810061'
        WHEN eaf_cf.in_or_out_patient_ind = 'O' THEN '830061'
    END AS peoplesoft_acct_num,
    CASE
        WHEN eaf_cf.in_or_out_patient_ind = 'I' THEN 'IP OR Case Minutes'
        WHEN eaf_cf.in_or_out_patient_ind = 'O' THEN 'OP OR Case Minutes'
    END AS peoplesoft_stat_name,
    'Department' AS measure_detail_unit,
    eaf_cf.dept AS measure_detail_code,
    eaf_cf.department_description AS measure_detail_description,
    'OR Case Minutes' AS measure_unit, 
    SUM(eaf_cf.calculated_or_hrs*60) AS measure_value
FROM
    (
        SELECT
            fcy_nm,
            encntr_num,
            service_date,
            in_or_out_patient_ind,
            dept,
            department_description,
            SUM(calculated_or_hrs) AS calculated_or_hrs,
            SUM(quantity) AS agg_qty
        FROM
            pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf
        WHERE
            service_date IS NOT NULL AND
            (
                persp_clncl_smy_descr IN ('SURGERY TIME','AMBULATORY SURGERY SERVICES') OR
                raw_chargcode IN ('SN99042','65000002')
            ) AND
            fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
        GROUP BY
            fcy_nm,
            encntr_num,
            service_date,
            in_or_out_patient_ind,
            dept,
            department_description
    ) eaf_cf
WHERE
    agg_qty > 0
GROUP BY 1,2,3,4,5,6,7,8,9,10,11
);

DROP TABLE pce_qe16_slp_prd_dm..stg_mly_mgmt_stats_detail IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..stg_mly_mgmt_stats_detail AS
(
SELECT
dsm.fcy_nm,
dsm.in_or_out_patient_ind,
dsm.report_date_name,
dsm.report_date,
dsm.report_month,
cdr.fyr_num AS fiscal_year,
dsm.peoplesoft_acct_num,
dsm.peoplesoft_stat_name AS measure_name,
measure_detail_unit,
measure_detail_code,
measure_detail_description,
dsm.measure_unit,
dsm.measure_value,
dsm.measure_numr,
dsm.measure_dnmr,
cdr.bsn_day_ind,
cdr.hol_ind
FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics_detail dsm
INNER JOIN pce_qe16_slp_prd_dm..cdr_dim cdr
ON
    dsm.report_date = cdr.cdr_dt
WHERE
    dsm.report_date <= LAST_DAY(ADD_MONTHS(DATE_TRUNC('month',CURRENT_DATE),-1))
) DISTRIBUTE ON (fcy_nm, report_date)
;


\unset ON_ERROR_STOP




