-----------------------------------------------------------------
-------------Step 1: Identify the population --------------------
-----------------------------------------------------------------

DROP TABLE stage_resources_rev_bb IF EXISTS;
CREATE TABLE stage_resources_rev_bb AS
(
SELECT
    s.discharge_campus,
    s.patient_account_number,
    s.check_in_date,
    s.discharge_date,
    s.ip_op,
    s.ms_drg,
    s.ms_drg_desc,
    s.ms_drg_med_surg,
    s.ms_drg_bus_line,
    e.prim_pcd_cd, 
    e.prim_pcd_descr,
    e.prim_dgns_cd,
    e.prim_dgns_descr,
    s.primary_cpt_hcpcs,
    s.cpt_hcpcs_desc,
    s.service_family, 
    s.attrb_physcn_nm,
    s.attrb_physn_npi, 
    e.cal_svc_ln AS service_line,
    e.cal_sub_svc_ln AS sub_service_line,
    e.cal_svc_nm AS service,
    e.robotic_srgy_ind,
    e.ptnt_tp_descr,
    e.age_in_yr, 
    k.revenue_code,
    k.revenue_code_description,
    k.quantity*k.spl_unit_cnvr*k.persp_clncl_dtl_unit  AS adj_qty,
    k.chargecodedesc,
    k.persp_clncl_dtl_descr,
    k.persp_clncl_smy_descr,
    k.persp_clncl_std_dept_descr_v10, 
    k.persp_clncl_std_dept_v10_rollup_cgy_descr, 
    k.persp_clncl_dtl_spl_modfr_descr, 
    k.rcc_based_direct_cst_amt,
    s.est_net_rev_amt,
    s.src_prim_payor_grp1,
    s.src_prim_payor_grp2, 
    s.src_prim_payor_grp3 
FROM
    pce_qe16_slp_prd_stg.aallaway.stage_claim_gold s
    ---------------------------------------------------------------------
    LEFT JOIN pce_qe16_slp_prd_dm..prd_chrg_fct k
    ON
        s.patient_account_number = k.patient_id AND
        s.discharge_campus = k.company_id
    ---------------------------------------------------------------------
    LEFT JOIN pce_qe16_slp_prd_dm..prd_encntr_anl_fct e
    ON
        s.patient_account_number = e.encntr_num AND
        s.discharge_campus = e.fcy_nm
    ---------------------------------------------------------------------
WHERE
    e.cal_svc_nm IN (SELECT cd FROM pce_qe16_slp_prd_stg..val_set_dim WHERE cohrt_id = 'ADV_SVC_NM') AND
    (s.discharge_campus IN (SELECT cd FROM pce_qe16_slp_prd_stg..val_set_dim WHERE cohrt_id = 'ADV_FACILITY_CODES')) AND
    (s.discharge_date BETWEEN '2018-10-01' AND LAST_DAY(CURRENT_DATE - INTERVAL '1 MONTH')) --update date to current month where data is available
) 
DISTRIBUTE ON (discharge_campus, patient_account_number);
--22333151
--1m 23s


-----------------------------------------------------------------
-------------Step 2: Prepare the Inpatients Data ----------------
-----------------------------------------------------------------

DROP TABLE stage_ip_resource_rev_bb IF EXISTS;
CREATE TABLE stage_ip_resource_rev_bb as
(-- query 1 (o): to calculate per case values
    SELECT 
        slr.discharge_campus,
        slr.patient_account_number,
        slr.check_in_date,
        slr.discharge_date,
        slr.ip_op,
        slr.ms_drg,
        slr.ms_drg_desc,
        slr.ms_drg_med_surg,
        slr.ms_drg_bus_line,
        slr.prim_pcd_cd, 
        slr.prim_pcd_descr,
        slr.prim_dgns_cd,
        slr.prim_dgns_descr,
        slr.primary_cpt_hcpcs,
        slr.cpt_hcpcs_desc,
        slr.service_family, 
        slr.attrb_physcn_nm, 
        slr.attrb_physn_npi,
        slr.service_line,
        slr.sub_service_line,
        slr.service,
        slr.robotic_srgy_ind,
        slr.ptnt_tp_descr,
        slr.age_in_yr, 
        slr.revenue_code, 
        slr.revenue_code_description,
        SUM(slr.adj_qty) AS adj_qty,
        slr.chargecodedesc,
        slr.persp_clncl_dtl_descr, 
        slr.persp_clncl_smy_descr,
        slr.persp_clncl_std_dept_descr_v10, 
        slr.persp_clncl_std_dept_v10_rollup_cgy_descr, 
        slr.persp_clncl_dtl_spl_modfr_descr,
        slr.est_net_rev_amt,
        slr.src_prim_payor_grp1,
        slr.src_prim_payor_grp2, 
        slr.src_prim_payor_grp3, 
        SUM(slr.rcc_based_direct_cst_amt) AS rcc_based_direct_cst_amt,
        SUM(slr.adj_qty)/COUNT(DISTINCT slr.patient_account_number) AS quantity_per_case,
        SUM(slr.rcc_based_direct_cst_amt)/COUNT(DISTINCT slr.patient_account_number) AS cost_per_case,
        COUNT(DISTINCT slr.patient_account_number) as resource_cases,
        CAST(u.util_cases AS NUMERIC)/CAST(t.total_cases AS NUMERIC) as util_rate,
        t.total_cases,
        u.util_cases,
        q.compl_cnt,
        q.csa_cmp_scl_fctr,
        q.los_cnt,
        q.csa_expc_los_cnt,
        q.mrtly_cnt,
        q.csa_mort_scl_fctr,
        q.readm_cnt,
        q.csa_hwr4_expc_30day_readm_scl_fctr
    FROM
        pce_qe16_slp_prd_stg..stage_resources_rev_bb slr
        -----------------------------------------------------------------
        LEFT OUTER JOIN
        (
        -- fy19 calculate total cases
            SELECT 
                slt.discharge_campus,
                slt.ip_op,
                slt.ms_drg,
                slt.prim_pcd_cd,
                slt.attrb_physn_npi, 
                slt.service,
                COUNT(DISTINCT slt.patient_account_number) AS total_cases
            FROM
                pce_qe16_slp_prd_stg..stage_resources_rev_bb slt
            -- aggregate to calculate total_cases (t)
            WHERE
                (slt.discharge_date BETWEEN '2018-10-01' AND '2019-09-30') AND
                slt.ip_op = 'I'
            GROUP BY
                slt.discharge_campus,
                slt.ip_op,
                slt.ms_drg,
                slt.prim_pcd_cd,
                slt.attrb_physn_npi, 
                slt.service
        ) t
        ON 
            t.discharge_campus = slr.discharge_campus AND
            t.ip_op = slr.ip_op AND
            t.ms_drg = slr.ms_drg AND
            t.prim_pcd_cd = slr.prim_pcd_cd AND
            t.attrb_physn_npi = slr.attrb_physn_npi AND
            t.service = slr.service
        -----------------------------------------------------------------
        LEFT OUTER JOIN
        (-- calculate fy19 utilization cases, repeat from above, now including persp_clncl_dtl (u)
            SELECT 
                slu.discharge_campus,
                slu.ip_op,
                slu.ms_drg,
                slu.prim_pcd_cd,
                slu.attrb_physn_npi, 
                slu.service,
                slu.persp_clncl_dtl_descr,
                COUNT(DISTINCT slu.patient_account_number) AS util_cases
            FROM
                pce_qe16_slp_prd_stg..stage_resources_rev_bb slu
            -- aggregate to calculate util_cases (u)
            WHERE
                (slu.discharge_date BETWEEN '2018-10-01' AND '2019-09-30') AND
                slu.ip_op = 'I'
            GROUP BY
                slu.discharge_campus,
                slu.ip_op,
                slu.ms_drg,
                slu.prim_pcd_cd,
                slu.attrb_physn_npi, 
                slu.service,
                slu.persp_clncl_dtl_descr
        ) u
        ON 
            u.discharge_campus = slr.discharge_campus AND
            u.ip_op = slr.ip_op AND
            u.ms_drg = slr.ms_drg AND
            u.prim_pcd_cd = slr.prim_pcd_cd AND
            u.attrb_physn_npi = slr.attrb_physn_npi AND
            u.service = slr.service AND
            u.persp_clncl_dtl_descr = slr.persp_clncl_dtl_descr
        -----------------------------------------------------------------
        LEFT OUTER JOIN
        (
            SELECT
                fcy_nm,
                encntr_num,
                ptnt_cl_cd,
                compl_cnt,
                csa_cmp_scl_fctr,
                los_cnt,
                csa_expc_los_cnt,
                mrtly_cnt,
                csa_mort_scl_fctr,
                CASE WHEN re_adm_day_cnt = -1 THEN 0 ELSE 1 END AS readm_cnt,
                csa_hwr4_expc_30day_readm_scl_fctr
            FROM
                pce_qe16_slp_prd_dm..prd_encntr_qly_anl_fct
        ) q
        ON
            slr.patient_account_number = q.encntr_num
        -----------------------------------------------------------------
    WHERE
        slr.ip_op = 'I'
    GROUP BY
        slr.discharge_campus,
        slr.patient_account_number,
        slr.check_in_date,
        slr.discharge_date,
        slr.ip_op,
        slr.ms_drg,
        slr.ms_drg_desc,
        slr.ms_drg_med_surg,
        slr.ms_drg_bus_line,
        slr.primary_cpt_hcpcs,
        slr.cpt_hcpcs_desc,
        slr.prim_pcd_cd, 
        slr.prim_pcd_descr,
        slr.prim_dgns_cd,
        slr.prim_dgns_descr,
        slr.service_family, 
        slr.attrb_physcn_nm, 
        slr.attrb_physn_npi,
        slr.service_line,
        slr.sub_service_line,
        slr.service,
        slr.robotic_srgy_ind,
        slr.ptnt_tp_descr,
        slr.age_in_yr,
        slr.revenue_code, 
        slr.revenue_code_description,
        slr.chargecodedesc,
        slr.persp_clncl_dtl_descr, 
        slr.persp_clncl_smy_descr,
        slr.persp_clncl_std_dept_descr_v10, 
        slr.persp_clncl_std_dept_v10_rollup_cgy_descr, 
        slr.persp_clncl_dtl_spl_modfr_descr,
        slr.est_net_rev_amt,
        slr.src_prim_payor_grp1,
        slr.src_prim_payor_grp2, 
        slr.src_prim_payor_grp3, 
        t.total_cases,
        u.util_cases,
        q.compl_cnt,
        q.csa_cmp_scl_fctr,
        q.los_cnt,
        q.csa_expc_los_cnt,
        q.mrtly_cnt,
        q.csa_mort_scl_fctr,
        q.readm_cnt,
        q.csa_hwr4_expc_30day_readm_scl_fctr
)
DISTRIBUTE ON (discharge_campus, patient_account_number);
--2715493
--1m 34s



-----------------------------------------------------------------
-------------Step 3: Prepare the Outpatients Data ---------------
-----------------------------------------------------------------

DROP TABLE stage_op_resource_rev_bb IF EXISTS;
CREATE TABLE stage_op_resource_rev_bb AS
(-- query 1 (o): to calculate per case values
    SELECT 
        slr.discharge_campus,
        slr.patient_account_number,
        slr.check_in_date,
        slr.discharge_date,
        slr.ip_op,
        slr.ms_drg,
        slr.ms_drg_desc,
        slr.ms_drg_med_surg,
        slr.ms_drg_bus_line,
        slr.prim_pcd_cd, 
        slr.prim_pcd_descr,
        slr.prim_dgns_cd,
        slr.prim_dgns_descr,
        slr.primary_cpt_hcpcs,
        slr.cpt_hcpcs_desc,
        slr.service_family, 
        slr.attrb_physcn_nm, 
        slr.attrb_physn_npi,
        slr.service_line,
        slr.sub_service_line,
        slr.service,
        slr.robotic_srgy_ind,
        slr.ptnt_tp_descr,
        slr.age_in_yr, 
        slr.revenue_code, 
        slr.revenue_code_description,
        SUM(slr.adj_qty) AS adj_qty,
        slr.chargecodedesc,
        slr.persp_clncl_dtl_descr, 
        slr.persp_clncl_smy_descr,
        slr.persp_clncl_std_dept_descr_v10, 
        slr.persp_clncl_std_dept_v10_rollup_cgy_descr, 
        slr.persp_clncl_dtl_spl_modfr_descr,
        slr.est_net_rev_amt,
        slr.src_prim_payor_grp1,
        slr.src_prim_payor_grp2, 
        slr.src_prim_payor_grp3, 
        SUM(slr.rcc_based_direct_cst_amt) AS rcc_based_direct_cst_amt,
        SUM(slr.adj_qty)/COUNT(DISTINCT slr.patient_account_number) AS quantity_per_case,
        SUM(slr.rcc_based_direct_cst_amt)/COUNT(DISTINCT slr.patient_account_number) AS cost_per_case,
        count(distinct slr.patient_account_number) AS resource_cases,
        CAST(u.util_cases AS numeric)/CAST(t.total_cases AS numeric) AS util_rate,
        t.total_cases,
        u.util_cases,
        CAST(NULL AS NUMERIC(14,10)) AS compl_cnt,
        CAST(NULL AS NUMERIC(18,4)) AS csa_cmp_scl_fctr,
        CAST(NULL AS INTEGER) AS los_cnt,
        CAST(NULL AS NUMERIC(14,10)) AS csa_expc_los_cnt,
        CAST(NULL AS INTEGER) AS mrtly_cnt,
        CAST(NULL AS NUMERIC(18,4)) AS csa_mort_scl_fctr,
        CAST(NULL AS INTEGER) AS readm_cnt,
        CAST(NULL AS NUMERIC(18,10)) AS csa_hwr4_expc_30day_readm_scl_fctr
    FROM
        pce_qe16_slp_prd_stg..stage_resources_rev_bb slr
        -----------------------------------------------------------------
        LEFT OUTER JOIN
        (
        -- fy19 calculate total cases
            SELECT 
                slt.discharge_campus,
                slt.ip_op,
                slt.primary_cpt_hcpcs,
                slt.prim_dgns_cd,
                slt.attrb_physn_npi, 
                slt.service,
                COUNT(DISTINCT slt.patient_account_number) AS total_cases
            FROM
                pce_qe16_slp_prd_stg..stage_resources_rev_bb slt
            -- aggregate to calculate total_cases (t)
            WHERE
                (slt.discharge_date BETWEEN '2018-10-01' AND '2019-09-30') AND
                slt.ip_op = 'O'
            GROUP BY
                slt.discharge_campus,
                slt.ip_op,
                slt.primary_cpt_hcpcs,
                slt.prim_dgns_cd,
                slt.attrb_physn_npi, 
                slt.service
        ) t
        ON 
            t.discharge_campus = slr.discharge_campus AND
            t.ip_op = slr.ip_op AND
            t.primary_cpt_hcpcs = slr.primary_cpt_hcpcs AND
            t.prim_dgns_cd = slr.prim_dgns_cd AND
            t.attrb_physn_npi = slr.attrb_physn_npi AND
            t.service = slr.service
        -----------------------------------------------------------------
        LEFT OUTER JOIN
        (-- calculate fy19 utilization cases, repeat from above, now including persp_clncl_dtl (u)
            SELECT 
                slu.discharge_campus,
                slu.ip_op,
                slu.primary_cpt_hcpcs,
                slu.prim_dgns_cd,
                slu.attrb_physn_npi, 
                slu.service,
                slu.persp_clncl_dtl_descr,
                COUNT(DISTINCT slu.patient_account_number) AS util_cases
            FROM
                pce_qe16_slp_prd_stg..stage_resources_rev_bb slu
            -- aggregate to calculate util_cases (u)
            WHERE
                (slu.discharge_date BETWEEN '2018-10-01' AND '2019-09-30') AND
                slu.ip_op = 'O'
            GROUP BY
                slu.discharge_campus,
                slu.ip_op,
                slu.primary_cpt_hcpcs,
                slu.prim_dgns_cd,
                slu.attrb_physn_npi, 
                slu.service,
                slu.persp_clncl_dtl_descr
        ) u
        ON 
            u.discharge_campus = slr.discharge_campus AND
            u.ip_op = slr.ip_op AND
            u.primary_cpt_hcpcs = slr.primary_cpt_hcpcs AND
            u.prim_dgns_cd = slr.prim_dgns_cd AND
            u.attrb_physn_npi = slr.attrb_physn_npi AND
            u.service = slr.service AND
            u.persp_clncl_dtl_descr = slr.persp_clncl_dtl_descr
        -----------------------------------------------------------------
    WHERE
        slr.ip_op = 'O'
    GROUP BY
        slr.discharge_campus,
        slr.patient_account_number,
        slr.check_in_date,
        slr.discharge_date,
        slr.ip_op,
        slr.ms_drg,
        slr.ms_drg_desc,
        slr.ms_drg_med_surg,
        slr.ms_drg_bus_line,
        slr.primary_cpt_hcpcs,
        slr.cpt_hcpcs_desc,
        slr.prim_pcd_cd, 
        slr.prim_pcd_descr,
        slr.prim_dgns_cd,
        slr.prim_dgns_descr,
        slr.service_family, 
        slr.attrb_physcn_nm, 
        slr.attrb_physn_npi,
        slr.service_line,
        slr.sub_service_line,
        slr.service,
        slr.robotic_srgy_ind,
        slr.ptnt_tp_descr,
        slr.age_in_yr,
        slr.revenue_code, 
        slr.revenue_code_description,
        slr.chargecodedesc,
        slr.persp_clncl_dtl_descr, 
        slr.persp_clncl_smy_descr,
        slr.persp_clncl_std_dept_descr_v10, 
        slr.persp_clncl_std_dept_v10_rollup_cgy_descr, 
        slr.persp_clncl_dtl_spl_modfr_descr,
        slr.est_net_rev_amt,
        slr.src_prim_payor_grp1,
        slr.src_prim_payor_grp2, 
        slr.src_prim_payor_grp3, 
        t.total_cases,
        u.util_cases
)
DISTRIBUTE ON (discharge_campus, patient_account_number);
--902982
--12.2s


-----------------------------------------------------------------
-------------Step 4: Load the Target Staging Table --------------
-----------------------------------------------------------------

DROP TABLE stage_target_services_rev_wgj_v2_full_data IF EXISTS;
CREATE TABLE stage_target_services_rev_wgj_v2_full_data AS
(
    SELECT
        CAST(rev.discharge_campus AS VARCHAR(255)) AS discharge_campus,
        rev.patient_account_number,
        rev.check_in_date,
        rev.discharge_date,
        rev.ip_op,
        rev.ms_drg,
        rev.ms_drg_desc,
        rev.ms_drg_med_surg,
        rev.ms_drg_bus_line,
        rev.prim_pcd_cd,
        rev.prim_pcd_descr,
        rev.prim_dgns_cd,
        rev.prim_dgns_descr,
        rev.primary_cpt_hcpcs,
        rev.cpt_hcpcs_desc,
        rev.service_family,
        rev.attrb_physcn_nm,
        rev.attrb_physn_npi,
        rev.service_line,
        rev.sub_service_line,
        rev.service,
        rev.robotic_srgy_ind,
        rev.ptnt_tp_descr,
        rev.age_in_yr,
        rev.revenue_code,
        rev.revenue_code_description,
        rev.adj_qty,
        rev.chargecodedesc,
        rev.persp_clncl_dtl_descr,
        rev.persp_clncl_smy_descr,
        rev.persp_clncl_std_dept_descr_v10,
        rev.persp_clncl_std_dept_v10_rollup_cgy_descr,
        rev.persp_clncl_dtl_spl_modfr_descr,
        rev.rcc_based_direct_cst_amt,
        rev.est_net_rev_amt,
        rev.src_prim_payor_grp1,
        rev.src_prim_payor_grp2,
        rev.src_prim_payor_grp3,
        rev.quantity_per_case,
        rev.cost_per_case,
        rev.resource_cases,
        rev.util_rate,
        rev.total_cases,
        rev.util_cases,
        rev.compl_cnt,
        rev.csa_cmp_scl_fctr,
        rev.los_cnt,
        rev.csa_expc_los_cnt,
        rev.mrtly_cnt,
        rev.csa_mort_scl_fctr,
        rev.readm_cnt,
        rev.csa_hwr4_expc_30day_readm_scl_fctr,
        ibt.antibiotic_bone_cement_baseline_performance_period,
        ibt.antibiotic_bone_cement_baseline_period_begin,
        ibt.antibiotic_bone_cement_baseline_period_end,
        ibt.antibiotic_bone_cement_baseline_performance_rate,
        ibt.antibiotic_bone_cement_baseline_volume,
        ibt.antibiotic_bone_cement_baseline_avg_qty_case,
        ibt.antibiotic_bone_cement_initiative_flag,
        ibt.aquamantys_hip_baseline_performance_period,
        ibt.aquamantys_hip_baseline_period_begin,
        ibt.aquamantys_hip_baseline_period_end,
        ibt.aquamantys_hip_baseline_performance_rate,
        ibt.aquamantys_hip_baseline_volume,
        ibt.aquamantys_hip_baseline_avg_qty_case,
        ibt.aquamantys_hip_initiative_flag,
        ibt.aquamantys_knee_baseline_performance_period,
        ibt.aquamantys_knee_baseline_period_begin,
        ibt.aquamantys_knee_baseline_period_end,
        ibt.aquamantys_knee_baseline_performance_rate,
        ibt.aquamantys_knee_baseline_volume,
        ibt.aquamantys_knee_baseline_avg_qty_case,
        ibt.aquamantys_knee_initiative_flag,
        ibt.reduce_bivalirudin_use_baseline_performance_period,
        ibt.reduce_bivalirudin_use_baseline_period_begin,
        ibt.reduce_bivalirudin_use_baseline_period_end,
        ibt.reduce_bivalirudin_use_baseline_performance_rate,
        ibt.reduce_bivalirudin_use_baseline_volume,
        ibt.reduce_bivalirudin_use_baseline_avg_qty_case,
        ibt.reduce_bivalirudin_use_initiative_flag,
        ibt.reduce_echo_contrast_lumason_definity_use_baseline_performance_period,
        ibt.reduce_echo_contrast_lumason_definity_use_baseline_period_begin,
        ibt.reduce_echo_contrast_lumason_definity_use_baseline_period_end,
        ibt.reduce_echo_contrast_lumason_definity_use_baseline_performance_rate,
        ibt.reduce_echo_contrast_lumason_definity_use_baseline_volume,
        ibt.reduce_echo_contrast_lumason_definity_use_baseline_avg_qty_case,
        ibt.reduce_echo_contrast_lumason_definity_use_initiative_flag,
        ibt.custom_cutting_block_baseline_performance_period,
        ibt.custom_cutting_block_baseline_period_begin,
        ibt.custom_cutting_block_baseline_period_end,
        ibt.custom_cutting_block_baseline_performance_rate,
        ibt.custom_cutting_block_baseline_volume,
        ibt.custom_cutting_block_baseline_avg_qty_case,
        ibt.custom_cutting_block_initiative_flag,
        ibt.reduce_tirofiban_use_baseline_performance_period,
        ibt.reduce_tirofiban_use_baseline_period_begin,
        ibt.reduce_tirofiban_use_baseline_period_end,
        ibt.reduce_tirofiban_use_baseline_performance_rate,
        ibt.reduce_tirofiban_use_baseline_volume,
        ibt.reduce_tirofiban_use_baseline_avg_qty_case,
        ibt.reduce_tirofiban_use_initiative_flag,
        ibt.aquamantys_hip_opportunity_cases,
        ibt.src_file_load_dt_tm,
        ibt.src_file_loaded_by,
        ibt.src_file_id
    FROM 
        (
            SELECT * FROM pce_qe16_slp_prd_stg..stage_ip_resource_rev_bb
        UNION
            SELECT * FROM pce_qe16_slp_prd_stg..stage_op_resource_rev_bb
        )rev
    ---------------------------------------------------------------------
    LEFT JOIN pce_qe16_prd_ct.aallaway.stage_initiative_baseline ibt
    ON
        rev.discharge_campus = ibt.discharge_campus
    ---------------------------------------------------------------------
)
DISTRIBUTE ON (discharge_campus, patient_account_number);
--3618475
--1m 31s

insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '2 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '3 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '4 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '5 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '6 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '7 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '8 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '9 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '10 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '11 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '12 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
insert into pce_qe16_slp_prd_stg..stage_target_services_rev_wgj_v2_full_data (discharge_campus, discharge_date) (SELECT cd AS discharge_campus, (LAST_DAY(CURRENT_DATE - INTERVAL '13 MONTH') + INTERVAL '1 DAY') AS discharge_date FROM pce_qe16_slp_prd_stg..val_set_dim where cohrt_id = 'ADV_FACILITY_CODES');
