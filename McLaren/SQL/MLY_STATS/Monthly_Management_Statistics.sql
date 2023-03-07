\set ON_ERROR_STOP ON;
------------------ Discharge Level Metrics-----------------

DROP TABLE pce_qe16_slp_prd_dm..stage_dschrg_stat_metrics_eaf IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..stage_dschrg_stat_metrics_eaf AS
(
SELECT
    eaf.fcy_nm,
    eaf.encntr_num,
    eaf.dschrg_dt,
    eaf.excld_trnsfr_encntr_ind,
    eaf.tot_chrg_ind,
    eaf.in_or_out_patient_ind,
    eaf.ptnt_tp_cd,
    eaf.dschrg_svc,
    eaf.adm_svc,
    eaf.ms_drg_cd,
    msdrgdim.ms_drg_type_cd,
    eaf.src_prim_payor_grp1,
    eaf.src_prim_payor_grp3,
    eaf.src_prim_pyr_cd,
    eaf.sub_fcy,
    eaf.iptnt_encntr_type,
    eaf.iptnt_dschrg_ind,
    eaf.boarder_baby_ind,
    eaf.obsrv_stay_ind,
    eaf.case_mix_idnx_num,
    eaf.nrg_stn,
    SUBSTR(eaf.dschrg_nrsng_dept,INSTR(eaf.dschrg_nrsng_dept,'-')+2) AS last_nrsng_dept
FROM
    pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
    LEFT OUTER JOIN pce_qe16_slp_prd_dm..ms_drg_dim msdrgdim
    ON
        msdrgdim.ms_drg_cd = eaf.ms_drg_cd
WHERE
    COALESCE(eaf.excld_trnsfr_encntr_ind,0) = 0 AND
    eaf.tot_chrg_ind = 1 AND
    eaf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb') AND
    eaf.fy2018_filter = 1
)
DISTRIBUTE ON (fcy_nm, encntr_num);



DROP TABLE pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics (
    fcy_nm VARCHAR(250),
    report_date_name VARCHAR(250),
    report_date TIMESTAMP,
    report_month VARCHAR(10),
    peoplesoft_acct_num VARCHAR(25),
    peoplesoft_stat_name VARCHAR(250),
    measure_unit VARCHAR(250),
    measure_value numeric(38,10),
    measure_numr numeric(38,10),
    measure_dnmr numeric(38,10)
)DISTRIBUTE ON (fcy_nm, peoplesoft_acct_num);


---Inpatient Encounter Types
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('INF001','810008','810007','810016','810017','INF002','810005');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_value)
(
SELECT
    eaf.fcy_nm AS fcy_nm,
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
    'Total - Discharges' AS measure_unit,
    SUM(eaf.iptnt_dschrg_ind) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_dschrg_stat_metrics_eaf eaf
WHERE
    eaf.in_or_out_patient_ind = 'I' AND
    eaf.dschrg_dt IS NOT NULL AND
    eaf.iptnt_dschrg_ind = 1
GROUP BY 1,2,3,4,5,6,7
);


---IP Births
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num = '810040';
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_value)
(
SELECT
    eaf.fcy_nm AS fcy_nm,
    'Discharge Date' AS report_date_name,
    eaf.dschrg_dt AS report_date,
    TO_CHAR(eaf.dschrg_dt,'YYYYMM') AS report_month,
    '810040' AS peoplesoft_acct_num,
    'IP Births' AS peoplesoft_stat_name,
    'Total - Discharges' AS measure_unit,
    SUM(eaf.iptnt_dschrg_ind) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_dschrg_stat_metrics_eaf eaf
WHERE
    eaf.in_or_out_patient_ind = 'I' AND
    eaf.dschrg_dt IS NOT NULL AND
    (eaf.boarder_baby_ind = 1 OR eaf.iptnt_encntr_type = 'Newborn')
GROUP BY 1,2,3,4,5,6,7
);


--- IP Medical
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num = '810006';
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_numr, measure_dnmr, measure_value)
(
SELECT
    eaf.fcy_nm AS fcy_nm,
    'Discharge Date' AS report_date_name,
    eaf.dschrg_dt AS report_date,
    TO_CHAR(eaf.dschrg_dt,'YYYYMM') AS report_month,
    '810006' AS peoplesoft_acct_num,
    'IP Discharges-Medical' AS peoplesoft_stat_name,
    'Total - Discharges' AS measure_unit,
    SUM(CASE WHEN eaf.ms_drg_type_cd = 'MED' THEN eaf.iptnt_dschrg_ind ELSE 0 END) AS measure_numr,
    SUM(CASE WHEN eaf.ms_drg_type_cd IN ('MED','SURG') THEN eaf.iptnt_dschrg_ind ELSE 0 END) AS measure_dnmr,
    SUM(CASE WHEN eaf.ms_drg_type_cd IN ('MED','SURG','OTH','UNKNOWN') THEN eaf.iptnt_dschrg_ind ELSE 0 END) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_dschrg_stat_metrics_eaf eaf
WHERE
    eaf.in_or_out_patient_ind = 'I' AND
    eaf.dschrg_dt IS NOT NULL AND
    eaf.iptnt_encntr_type IN ('Acute','Psych','Rehab') AND --2023-01-18
    COALESCE(eaf.boarder_baby_ind,0) <> 1
GROUP BY 1,2,3,4,5,6,7
);


--- IP Surgical
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num = '810009';
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_numr, measure_dnmr, measure_value)
(
SELECT
    eaf.fcy_nm AS fcy_nm,
    'Discharge Date' AS report_date_name,
    eaf.dschrg_dt AS report_date,
    TO_CHAR(eaf.dschrg_dt,'YYYYMM') AS report_month,
    '810009' AS peoplesoft_acct_num,
    'IP Discharges-Surgical' AS peoplesoft_stat_name,
    'Total - Discharges' AS measure_unit,
    SUM(CASE WHEN eaf.ms_drg_type_cd = 'SURG' THEN eaf.iptnt_dschrg_ind ELSE 0 END) AS measure_numr,
    SUM(CASE WHEN eaf.ms_drg_type_cd IN ('MED','SURG') THEN eaf.iptnt_dschrg_ind ELSE 0 END) AS measure_dnmr,
    SUM(CASE WHEN eaf.ms_drg_type_cd IN ('MED','SURG','OTH','UNKNOWN') THEN eaf.iptnt_dschrg_ind ELSE 0 END) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_dschrg_stat_metrics_eaf eaf
WHERE
    eaf.in_or_out_patient_ind = 'I' AND
    eaf.dschrg_dt IS NOT NULL AND
    eaf.iptnt_encntr_type IN ('Acute','Psych','Rehab') AND --2023-01-18
    COALESCE(eaf.boarder_baby_ind,0) <> 1
GROUP BY 1,2,3,4,5,6,7
);


--- Observation Discharges
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num = '850005';
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_value)
(
SELECT
    eaf.fcy_nm AS fcy_nm,
    'Discharge Date' AS report_date_name,
    eaf.dschrg_dt AS report_date,
    TO_CHAR(eaf.dschrg_dt,'YYYYMM') AS report_month,
    '850005' AS peoplesoft_acct_num,
    'Observation Discharges' AS peoplesoft_stat_name,
    'Total - Discharges' AS measure_unit,
    SUM(eaf.obsrv_stay_ind) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_dschrg_stat_metrics_eaf eaf
WHERE
    eaf.in_or_out_patient_ind = 'O' AND
    eaf.obsrv_stay_ind = '1'
GROUP BY 1,2,3,4,5,6,7
);


---All Payor CMI MTD
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num = '860020';
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_numr, measure_dnmr)
(
SELECT
    eaf.fcy_nm AS fcy_nm,
    'Discharge Date' AS report_date_name,
    eaf.dschrg_dt AS report_date,
    TO_CHAR(eaf.dschrg_dt,'YYYYMM') AS report_month,
    '860020' AS peoplesoft_acct_num,
    'All Payor CMI MTD' AS peoplesoft_stat_name,
    'Case Mix Index' AS measure_unit,
    SUM(eaf.case_mix_idnx_num) AS measure_numr,
    SUM(CASE WHEN eaf.case_mix_idnx_num IS NULL THEN 0 ELSE 1 END) AS measure_dnmr
FROM
    pce_qe16_slp_prd_dm..stage_dschrg_stat_metrics_eaf eaf
WHERE
    eaf.in_or_out_patient_ind = 'I' AND
    eaf.dschrg_dt IS NOT NULL AND
    eaf.iptnt_encntr_type = 'Acute' AND
    COALESCE(eaf.boarder_baby_ind,0) <> 1
GROUP BY 1,2,3,4,5,6,7
);


---Medicare Case Mix Index MTD
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num = '860022';
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_numr, measure_dnmr)
(
SELECT
    eaf.fcy_nm AS fcy_nm,
    'Discharge Date' AS report_date_name,
    eaf.dschrg_dt AS report_date,
    TO_CHAR(eaf.dschrg_dt,'YYYYMM') AS report_month,
    '860022' AS peoplesoft_acct_num,
    'Medicare Case Mix Index MTD' AS peoplesoft_stat_name,
    'Case Mix Index' AS measure_unit,
    SUM(eaf.case_mix_idnx_num) AS measure_numr,
    SUM(CASE WHEN eaf.case_mix_idnx_num IS NULL THEN 0 ELSE 1 END) AS measure_dnmr
FROM
    pce_qe16_slp_prd_dm..stage_dschrg_stat_metrics_eaf eaf
WHERE
    eaf.in_or_out_patient_ind = 'I' AND
    eaf.dschrg_dt IS NOT NULL AND
    eaf.iptnt_encntr_type = 'Acute' AND
    COALESCE(eaf.boarder_baby_ind,0) <> 1 AND
    UPPER(eaf.src_prim_payor_grp1) = 'MEDICARE' AND
    eaf.case_mix_idnx_num IS NOT NULL
GROUP BY 1,2,3,4,5,6,7
);


------------------Service Level Metrics---------------

DROP TABLE pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf AS
(
SELECT
    eaf.fcy_nm,
    eaf.encntr_num,
    eaf.dschrg_dt,
    eaf.in_or_out_patient_ind,
    eaf.ptnt_tp_cd,
    eaf.dschrg_svc,
    eaf.adm_svc,
    eaf.ms_drg_cd,
    eaf.src_prim_payor_grp1,
    eaf.src_prim_payor_grp3,
    eaf.src_prim_pyr_cd,
    eaf.sub_fcy,
    eaf.excld_trnsfr_encntr_ind,
    eaf.tot_chrg_ind,
    eaf.boarder_baby_ind,
    eaf.obsrv_stay_ind,
    eaf.case_mix_idnx_num,
    eaf.iptnt_encntr_type,
    CASE
        WHEN eaf.in_or_out_patient_ind = 'I' AND iptnt_encntr_type IN ('Specialty Care','Rehab','Psych','Hospice','Newborn','Acute') THEN 1
        ELSE NULL
    END AS iptnt_dschrg_ind, --- Need to be directly used from eaf. check
    eaf.fcy_num,
    eaf.robotic_srgy_ind,
    eaf.fin,
    eaf.srgl_case_ind,
    cf.service_date,
    cf.quantity,
    cf.raw_chargcode,
    cf.charge_code,
    cf.chargecodedesc,
    cf.revenue_code,
    cf.persp_clncl_smy_descr,
    cf.persp_clncl_smy_cd,
    cf.total_charge,
    cf.dept,
    cf.department_group,
    cf.department_description,
    cf.calculated_or_hrs,
    cf.cpt_code,
    cf.cpt_descr,
    cf.cmb_fcy_nm_chrg
FROM
    pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
    INNER JOIN pce_qe16_slp_prd_dm..prd_chrg_fct cf
    ON
        eaf.fcy_nm = cf.fcy_nm AND
        eaf.encntr_num = cf.encntr_num
WHERE
    COALESCE(eaf.excld_trnsfr_encntr_ind,0) = 0 AND
    eaf.fy2018_filter = 1
)
DISTRIBUTE ON (fcy_nm, encntr_num);



---Patient Days - Inpatient Encounter Types
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('810028','810029', '810026', '810027', 'INF003', '810025','INF004');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
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
    'Quantity' AS measure_unit,
    SUM(eaf_cf.quantity) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
WHERE
    eaf_cf.in_or_out_patient_ind = 'I' AND
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb') AND
    SUBSTRING(eaf_cf.revenue_code, 1, 3) IN ('010', '011', '012', '013', '014', '015', '016', '017', '020', '021')
GROUP BY 1,2,3,4,5,6,7
);


---Normal Deliveries and C-Section Deliveries
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('810042','810043');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    CASE
        WHEN eaf_cf.raw_chargcode IN ('14000017','14000018', '14000019', '14000020') THEN '810042'
        WHEN eaf_cf.raw_chargcode IN ('14000034', '14000035', '14000036') THEN '810043'
    END AS peoplesoft_acct_num,
    CASE
        WHEN eaf_cf.raw_chargcode IN ('14000017','14000018', '14000019', '14000020') THEN 'Normal Deliveries'
        WHEN eaf_cf.raw_chargcode IN ('14000034', '14000035', '14000036') THEN 'C-Section Deliveries'
    END AS peoplesoft_stat_name,
    'Total Cases' AS measure_unit,
    COUNT(DISTINCT eaf_cf.encntr_num) AS measure_value
FROM
    (
        SELECT
            fcy_nm,
            encntr_num,
            service_date,
            raw_chargcode,
            SUM(quantity) AS agg_qty
        FROM
            pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf
        WHERE
            in_or_out_patient_ind = 'I' AND
            service_date IS NOT NULL AND
            raw_chargcode IN ('14000017','14000018', '14000019', '14000020', '14000034', '14000035', '14000036') AND
            fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
        GROUP BY
            fcy_nm,
            encntr_num,
            service_date,
            raw_chargcode
    ) eaf_cf
WHERE
    agg_qty > 0
GROUP BY 1,2,3,4,5,6,7
);



---IP Surgical Cases and OP Surgical Cases
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('810060','830060');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
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
    'Total Cases' AS measure_unit,
    COUNT(DISTINCT eaf_cf.encntr_num) AS measure_value
FROM
    (
        SELECT
            fcy_nm,
            encntr_num,
            service_date,
            in_or_out_patient_ind,
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
            in_or_out_patient_ind
    ) eaf_cf
WHERE
    agg_qty > 0
GROUP BY 1,2,3,4,5,6,7
);


---IP OR Case Minutes AND OP OR Case Minutes
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('810061','830061');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
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
    'OR Case Minutes' AS measure_unit,
    SUM(eaf_cf.calculated_or_hrs*60) AS measure_value
FROM
    (
        SELECT
            fcy_nm,
            encntr_num,
            service_date,
            in_or_out_patient_ind,
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
            in_or_out_patient_ind
    ) eaf_cf
WHERE
    agg_qty > 0
GROUP BY 1,2,3,4,5,6,7
);



---Observation Patient Days
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('850020');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    '850020' AS peoplesoft_acct_num,
    'Observation Patient Days' AS peoplesoft_stat_name,
    'Quantity' AS measure_unit,
    SUM(eaf_cf.quantity)/24 AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
WHERE
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.service_date <= CURRENT_DATE AND
    eaf_cf.in_or_out_patient_ind = 'O' AND
    eaf_cf.revenue_code = '0762' AND
    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
GROUP BY 1,2,3,4,5,6,7
);


---Observation Hours (minutes)
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('850021');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    '850021' AS peoplesoft_acct_num,
    'Observation Hours (minutes)' AS peoplesoft_stat_name,
    'Quantity' AS measure_unit,
    SUM(eaf_cf.quantity)*60 AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
WHERE
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.service_date <= CURRENT_DATE AND
    eaf_cf.in_or_out_patient_ind = 'O' AND
    eaf_cf.revenue_code = '0762' AND
    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
GROUP BY 1,2,3,4,5,6,7
);


---IP Encounters AND OP Encounters
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('810051','830051');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(
SELECT
    eaf_cf.fcy_nm_grp AS fcy_nm,
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
            dept_derived
    ) eaf_cf 
WHERE
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.service_date <= CURRENT_DATE --AND
    --eaf_cf.fcy_nm_grp IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
GROUP BY 1,2,3,4,5,6,7
);


---Inpatient Admission(IP Admits),ED OP Visits
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('850051','850052');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    CASE
        WHEN eaf_cf.in_or_out_patient_ind = 'I' THEN '850051'
        WHEN eaf_cf.in_or_out_patient_ind = 'O' THEN '850052'
    END AS peoplesoft_acct_num,
    CASE
        WHEN eaf_cf.in_or_out_patient_ind = 'I' THEN 'Inpatient Admission(IP Admits)'
        WHEN eaf_cf.in_or_out_patient_ind = 'O' THEN 'ED OP Visits'
    END AS peoplesoft_stat_name,
    'Quantity' AS measure_unit,
    SUM(eaf_cf.quantity) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
WHERE
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.service_date <= CURRENT_DATE AND
    eaf_cf.cpt_code IN ('99281','99282','99283','99284','99285','99291') AND
    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
GROUP BY 1,2,3,4,5,6,7
);

---ED Visits(IP and Outpatient)
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('850050');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    '850050' AS peoplesoft_acct_num,
    'ED Visits(IP and Outpatient)' AS peoplesoft_stat_name,
    'Quantity' AS measure_unit,
    SUM(eaf_cf.quantity) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
WHERE
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.service_date <= CURRENT_DATE AND
    eaf_cf.cpt_code IN ('99281','99282','99283','99284','99285','99291') AND
    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
GROUP BY 1,2,3,4,5,6,7
);


---OP Visits
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('830052');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    'Service Date' AS report_date_name,
    eaf_cf.service_date AS report_date,
    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
    '830052' AS peoplesoft_acct_num,
    'OP Visits' AS peoplesoft_stat_name,
    'Encounters' AS measure_unit,
    COUNT(DISTINCT eaf_cf.encntr_num) AS measure_value
	
FROM 
    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
WHERE
    --eaf_cf.cmb_fcy_nm_chrg NOT IN ('MMG') AND
    (CASE WHEN LENGTH(dept) = 9 AND (SUBSTRING(dept,5,5) BETWEEN '50000' AND '58574') THEN 0 ELSE 1 END) = 1 AND
    SUBSTRING(COALESCE(eaf_cf.revenue_code,''),1,3) NOT IN ('010','011','012','013','014','015','016','017','020','021') AND
    eaf_cf.in_or_out_patient_ind = 'O' AND
    eaf_cf.tot_chrg_ind = 1 AND
    COALESCE(eaf_cf.total_charge,0) <> 0 AND
    COALESCE(eaf_cf.department_group,'') NOT IN ('Nursing','Nursing - Critical Care') AND
    eaf_cf.service_date IS NOT NULL AND
    eaf_cf.service_date <= CURRENT_DATE AND
    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb')
GROUP BY 1,2,3,4,5,6,7
);



-----Robotic Surgery
--DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('Robotic Surgery');
--INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
--(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_value)
--(
--SELECT
--    eaf_cf.fcy_nm AS fcy_nm,
--    'Service Date' AS report_date_name,
--    eaf_cf.service_date AS report_date,
--    TO_CHAR(eaf_cf.service_date,'YYYYMM') AS report_month,
--    'Robotic Surgery' AS peoplesoft_acct_num,
--    'Robotic Surgery' AS peoplesoft_stat_name,
--    'Total Cases' AS measure_unit,
--    COUNT(DISTINCT eaf_cf.encntr_num) AS measure_value
--FROM
--    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
--    LEFT OUTER JOIN pce_qe16_slp_prd_dm..val_set_dim vsd_sc
--    ON
--        eaf_cf.persp_clncl_smy_cd = vsd_sc.cd AND
--        vsd_sc.cohrt_id = 'ROBO_SURG_SPL_SMY_CD'
--    LEFT OUTER JOIN pce_qe16_slp_prd_dm..val_set_dim vsd_cc
--    ON
--        eaf_cf.raw_chargcode = vsd_cc.cd AND
--        vsd_cc.cohrt_id = 'ROBO_SURG_CHRG_CD' AND
--        eaf_cf.fcy_num = vsd_cc.val_set_nm AND
--        UPPER(vsd_cc.cd_descr) = UPPER(eaf_cf.chargecodedesc)
--WHERE
--    eaf_cf.service_date IS NOT NULL AND
--    eaf_cf.service_date <= CURRENT_DATE AND
--    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb') AND
--    (
--        vsd_sc.cd IS NOT NULL OR
--        vsd_cc.cd IS NOT NULL
--    )
--GROUP BY 1,2,3,4,5,6,7
--);

---Robotic Surgery
DELETE FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics WHERE peoplesoft_acct_num IN ('Robotic Surgery');
INSERT INTO pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics
(fcy_nm, report_date_name, report_date, report_month, peoplesoft_acct_num, peoplesoft_stat_name, measure_unit, measure_value)
(
SELECT
    eaf_cf.fcy_nm AS fcy_nm,
    'Discharge Date' AS report_date_name,
    eaf_cf.dschrg_dt AS report_date,
    TO_CHAR(eaf_cf.dschrg_dt,'YYYYMM') AS report_month,
    'INF005' AS peoplesoft_acct_num,
    'Robotic Surgery' AS peoplesoft_stat_name,
    'Total Cases' AS measure_unit,
    COUNT(distinct eaf_cf.encntr_num) AS measure_value
FROM
    pce_qe16_slp_prd_dm..stage_svc_stat_metrics_eaf_cf eaf_cf
WHERE
    eaf_cf.dschrg_dt IS NOT NULL AND
    eaf_cf.dschrg_dt <= CURRENT_DATE AND
    eaf_cf.fcy_nm IN ('Bay', 'Caro', 'Central', 'Flint', 'Karmanos', 'Lansing', 'Lapeer', 'Macomb', 'Northern', 'Oakland', 'Port Huron', 'St. Lukes', 'Thumb') AND
    robotic_srgy_ind = '1' AND 
    tot_chrg_ind > 0 AND
    (
    persp_clncl_smy_descr IN ('SURGERY TIME','AMBULATORY SURGERY SERVICES') OR
    raw_chargcode IN ('SN99042','65000002')
   ) AND 
   srgl_case_ind = '1'
GROUP BY 1,2,3,4,5,6,7
);


--- Final Table loading
DROP TABLE pce_qe16_slp_prd_dm..stg_mly_mgmt_stats IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..stg_mly_mgmt_stats AS
(
SELECT
dsm.fcy_nm,
dsm.report_date_name,
dsm.report_date,
dsm.report_month,
cdr.fyr_num AS fiscal_year,
dsm.peoplesoft_acct_num,
dsm.peoplesoft_stat_name AS measure_name,
dsm.measure_unit,
dsm.measure_value,
dsm.measure_numr,
dsm.measure_dnmr,
cdr.bsn_day_ind,
cdr.hol_ind
FROM pce_qe16_slp_prd_dm..intermediate_stg_dly_stat_metrics dsm
INNER JOIN pce_qe16_slp_prd_dm..cdr_dim cdr
ON
    dsm.report_date = cdr.cdr_dt
WHERE
    dsm.report_date <= LAST_DAY(ADD_MONTHS(DATE_TRUNC('month',CURRENT_DATE),-1))
) DISTRIBUTE ON (fcy_nm, report_date)
;

\unset ON_ERROR_STOP





