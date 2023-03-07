DROP VIEW  pce_qe16_slp_prd_dm..spl_dim; 

CREATE TABLE pce_qe16_slp_prd_dm..spl_dim AS 
(SELECT DISTINCT cf.company_id, vset_fcy.alt_cd AS fcy_num, cf.charge_code, spl.cdm_cd 
 FROM ((pce_qe16_oper_prd_zoom.prmradmp.cv_patbill cf
 LEFT JOIN prmradmp.val_set_dim vset_fcy 
 ON ((("nvarchar"(vset_fcy.cd) = cf.company_id) 
 AND (vset_fcy.cohrt_id = 'FACILITY_CODES'::"varchar")))) 
 LEFT JOIN pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl spl 
 ON ((((cf.charge_code = "nvarchar"(spl.cdm_cd)) 
 AND (vset_fcy.alt_cd = spl.fcy_num)) AND (cf.charge_code = "nvarchar"(spl.cdm_cd))))) 
 WHERE (spl.cdm_cd ISNULL)) ((SELECT (pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.fcy_num)::varchar(80) AS fcy_num, 
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_cd, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_strt_cdr_dk,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_strt_dt, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_end_cdr_dk,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_end_dt, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_descr,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_procedure_cd_v10 AS persp_clncl_dtl_pcd_cd_v10,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_procedure_descr_v10 AS persp_clncl_dtl_pcd_descr_v10, 
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.spl_unit_conv AS spl_unit_cnvr, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_cd AS persp_clncl_dtl_cd,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_descr AS persp_clncl_dtl_descr,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_unit AS persp_clncl_dtl_unit,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcs_cd AS persp_clncl_smy_cd,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcs_descr AS persp_clncl_smy_descr, 
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_cd_v10 AS persp_clncl_std_dept_cd_v10,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_descr_v10 AS persp_clncl_std_dept_descr_v10,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_v10_rollup_cat_cd AS persp_clncl_std_dept_v10_rollup_cgy_cd, 
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_v10_rollup_cat_descr AS persp_clncl_std_dept_v10_rollup_cgy_descr,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_spl_modfr_cd AS persp_clncl_dtl_spl_modfr_cd,
 pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_spl_modfr_descr AS persp_clncl_dtl_spl_modfr_descr FROM pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl) 
 UNION (SELECT ('-100'::"varchar")::varchar(80) AS fcy_num,
 ('-100'::"nvarchar")::nvarchar(45) AS cdm_cd, '19000101'::int8 AS cdm_strt_cdr_dk, '1900-01-01'::date AS cdm_strt_dt, '29000101'::int8 AS cdm_end_cdr_dk,
 '2900-01-01'::date AS cdm_end_dt, ('UNKNOWN'::"varchar")::varchar(60) AS cdm_descr, ('-100'::"varchar")::varchar(15) AS persp_clncl_dtl_pcd_cd_v10, 
 ('UNKNOWN'::"varchar")::varchar(50) AS persp_clncl_dtl_pcd_descr_v10, ('0'::"numeric")::numeric(18,6) AS spl_unit_cnvr,
 ('0'::"numeric")::numeric(15,0) AS persp_clncl_dtl_cd, ('UNKNOWN'::"varchar")::varchar(50) AS persp_clncl_dtl_descr,
 ('0'::"numeric")::numeric(18,6) AS persp_clncl_dtl_unit, ('-100'::"varchar")::varchar(15) AS persp_clncl_smy_cd, 
 ('UNKNOWN'::"varchar")::varchar(50) AS persp_clncl_smy_descr, -100 AS persp_clncl_std_dept_cd_v10, ('UNKNOWN'::"varchar")::varchar(40) AS persp_clncl_std_dept_descr_v10,
 ('-100'::"varchar")::varchar(15) AS persp_clncl_std_dept_v10_rollup_cgy_cd, ('UNKNOWN'::"varchar")::varchar(60) AS persp_clncl_std_dept_v10_rollup_cgy_descr,
 ('-100'::"varchar")::varchar(10) AS persp_clncl_dtl_spl_modfr_cd, ('UNKNOWN'::"varchar")::varchar(40) AS persp_clncl_dtl_spl_modfr_descr)) 
 UNION (SELECT zoom_uniq_chrg_codes.fcy_num, (zoom_uniq_chrg_codes.charge_code)::nvarchar(45) AS cdm_cd, '19000101'::int8 AS cdm_strt_cdr_dk, 
 '1900-01-01'::date AS cdm_strt_dt, '29000101'::int8 AS cdm_end_cdr_dk, '2900-01-01'::date AS cdm_end_dt, ('UNKNOWN'::"varchar")::varchar(60) AS cdm_descr, 
 ('-100'::"varchar")::varchar(15) AS persp_clncl_dtl_pcd_cd_v10, ('UNKNOWN'::"varchar")::varchar(50) AS persp_clncl_dtl_pcd_descr_v10, ('0'::"numeric")::numeric(18,6) AS spl_unit_cnvr,
 ('0'::"numeric")::numeric(15,0) AS persp_clncl_dtl_cd, ('UNKNOWN'::"varchar")::varchar(50) AS persp_clncl_dtl_descr, ('0'::"numeric")::numeric(18,6) AS persp_clncl_dtl_unit, 
 ('-100'::"varchar")::varchar(15) AS persp_clncl_smy_cd, ('UNKNOWN'::"varchar")::varchar(50) AS persp_clncl_smy_descr, -100 AS persp_clncl_std_dept_cd_v10,
 ('UNKNOWN'::"varchar")::varchar(40) AS persp_clncl_std_dept_descr_v10, ('-100'::"varchar")::varchar(15) AS persp_clncl_std_dept_v10_rollup_cgy_cd,
 ('UNKNOWN'::"varchar")::varchar(60) AS persp_clncl_std_dept_v10_rollup_cgy_descr, ('-100'::"varchar")::varchar(10) AS persp_clncl_dtl_spl_modfr_cd,
 ('UNKNOWN'::"varchar")::varchar(40) AS persp_clncl_dtl_spl_modfr_descr FROM  zoom_uniq_chrg_codes)
distribute on (fcy_num, charge_code);
