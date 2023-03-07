--QADV Views

CREATE
	OR REPLACE VIEW pce_qe16_slp_prd_dm..encntr_fct AS
SELECT   *
FROM pce_qe16_prd_qadv.prmradmp.encntr;

DROP VIEW pce_qe16_slp_prd_dm..encntr_fct;

CREATE
	OR REPLACE VIEW pce_qe16_slp_prd_dm..encntr_fct AS
SELECT   *
FROM pce_qe16_prd_qadv.prmradmp.encntr;

DROP VIEW pce_qe16_slp_prd_dm..stnd_dschrg_sts_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_dschrg_sts_dim
AS SELECT pce_qe16_prd_qadv.prmradmp.dschrg_sts_ref.dschrg_sts_cd, pce_qe16_prd_qadv.prmradmp.dschrg_sts_ref.dschrg_sts_descr, pce_qe16_prd_qadv.prmradmp.dschrg_sts_ref.audt_sk FROM pce_qe16_prd_qadv.prmradmp.dschrg_sts_ref
UNION
SELECT -100, 'UNKNOWN',-100;

--DROP VIEW pce_qe16_slp_prd_dm..stnd_fcy_pyr_dim;

--QADV Views

--DROP VIEW pce_qe16_slp_prd_dm..stnd_dschrg_sts_dim;

--CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_dschrg_sts_dim
--AS SELECT pce_qe16_prd_qadv.prmradmp.dschrg_sts_ref.dschrg_sts_cd,
--pce_qe16_prd_qadv.prmradmp.dschrg_sts_ref.dschrg_sts_descr, pce_qe16_prd_qadv.prmradmp.dschrg_sts_ref.audt_sk FROM pce_qe16_prd_qadv.prmradmp.dschrg_sts_ref
--UNION 
--select -100,
DROP VIEW pce_qe16_slp_prd_dm..stnd_fcy_pyr_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_fcy_pyr_dim 
AS SELECT distinct pce_qe16_prd_qadv.prmradmp.fcy_pyr_ref.stnd_pyr_cd AS std_pyr_cd, pce_qe16_prd_qadv.prmradmp.fcy_pyr_ref.stnd_pyr_descr AS std_pyr_descr 
FROM pce_qe16_prd_qadv.prmradmp.fcy_pyr_ref
UNION 
SELECT -100, 'UNKNOWN' ;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_fcy_pyr_dim 
AS SELECT pce_qe16_prd_qadv.prmradmp.fcy_pyr_ref.fcy_num, 
pce_qe16_prd_qadv.prmradmp.fcy_pyr_ref.fcy_pyr_cd,
pce_qe16_prd_qadv.prmradmp.fcy_pyr_ref.fcy_pyr_descr, 
pce_qe16_prd_qadv.prmradmp.fcy_pyr_ref.stnd_pyr_cd AS std_pyr_cd, 
pce_qe16_prd_qadv.prmradmp.fcy_pyr_ref.stnd_pyr_descr AS std_pyr_descr, 
pce_qe16_prd_qadv.prmradmp.fcy_pyr_ref.audt_sk FROM pce_qe16_prd_qadv.prmradmp.fcy_pyr_ref

DROP VIEW pce_qe16_slp_prd_dm..stnd_adm_src_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_adm_src_dim AS 
SELECT distinct CAST(pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.adm_src_cd as VARCHAR(75)) as adm_src_cd, pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.adm_src_descr,
pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.audt_sk 
FROM pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref
UNION 
SELECT '-100', 'UNKNOWN',100;

DROP VIEW pce_qe16_slp_prd_dm..stnd_adm_type_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_adm_type_dim AS 
SELECT distinct CAST(pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref.adm_type_cd as VARCHAR(75)) as adm_type_cd,
pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref.adm_type_descr, pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref.audt_sk 
FROM pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref
UNION 
SELECT '-100', 'UNKNOWN',100;

DROP VIEW pce_qe16_slp_prd_dm..stnd_ptnt_type_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_ptnt_type_dim AS
SELECT pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.stnd_ptnt_type_cd AS std_encntr_type_cd, 
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.stnd_ptnt_type_descr AS std_encntr_type_descr,
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.ptnt_type_smy_cd AS encntr_type_smy_cd,
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.skilled_nurse_cd,
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.skilled_nurse_descr,
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.ptnt_type_smy_descr AS encntr_type_smy_descr, 
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.audt_sk 
FROM pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref
UNION 
SELECT -100, 'UKNOWN',-100,-100,'UNKNOWN','UNKNOWN',-100;


DROP VIEW pce_qe16_slp_prd_dm..stnd_physcn_spcly_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_physcn_spcly_dim AS 
SELECT CAST(pce_qe16_prd_qadv.prmradmp.std_phys_speciality.code as VARCHAR(75)) AS cd, 
pce_qe16_prd_qadv.prmradmp.std_phys_speciality.descr 
FROM pce_qe16_prd_qadv.prmradmp.std_phys_speciality
UNION
select '-100','UNKNOWN';


DROP VIEW pce_qe16_slp_prd_dm..stnd_fcy_demog_dim; 
CREATE  OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_fcy_demog_dim AS SELECT * FROM pce_qe16_prd_qadv..fcy_demog_ref

DROP VIEW pce_qe16_slp_prd_dm..stnd_ptnt_zip_dim; 
CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_ptnt_zip_dim AS SELECT * FROM pce_qe16_prd_qadv..ptnt_zip_ref
UNION 
SELECT '-100','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN','-100','UNKNOWN','UNKNOWN','UNKNOWN',-100;


DROP VIEW pce_qe16_slp_prd_dm..stnd_pract_dim; 
CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_pract_dim AS SELECT * FROM pce_qe16_prd_qadv..pract_ref
UNION
SELECT '-100','-100','UNKNOWN','UNKNOWN','UNKNOWN',-100;

DROP VIEW pce_qe16_slp_prd_dm..stnd_pnt_of_orig_ref; 
CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_pnt_of_orig_ref AS SELECT * FROM pce_qe16_prd_qadv..pnt_of_orig_ref
union 
SELECT '-100','UNKNOWN',-100;

--ZOOM Tables / Views

DROP VIEW pce_qe16_slp_prd_dm..cdm_dim;

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..cdm_dim AS SELECT * FROM pce_qe16_oper_prd_zoom.qe16zmp.cv_cdmmstr;

DROP VIEW pce_qe16_slp_prd_dm..dept_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..dept_dim AS SELECT * FROM pce_qe16_oper_prd_zoom.qe16zmp.cv_dept;

DROP VIEW pce_qe16_slp_prd_dm..dgns_fct; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..dgns_fct AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patdgns;

DROP VIEW pce_qe16_slp_prd_dm..cv_patprac; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..cv_patprac AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patprac;

DROP VIEW pce_qe16_slp_prd_dm..proc_fct; 

DROP VIEW pce_qe16_slp_prd_dm..pattype_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..pattype_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_pattype;

DROP VIEW pce_qe16_slp_prd_dm..phys_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..phys_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_physmstr;

DROP VIEW pce_qe16_slp_prd_dm..pattype_map_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..pattype_map_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_pattypemap;

DROP VIEW pce_qe16_slp_prd_dm..paymstr_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..paymstr_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_paymstr;

DROP VIEW pce_qe16_slp_prd_dm..phys_spec_map_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..phys_spec_map_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_physmap;

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..cv_patbill AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patbill;

DROP VIEW pce_qe16_slp_prd_dm..pract_fct; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..pract_fct AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patprac;

DROP VIEW pce_qe16_slp_prd_dm..revuse_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..revuse_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_revuse;

DROP VIEW pce_qe16_slp_prd_dm..paymap_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..paymap_dim AS
SELECT rcrd_load_type
       , rcrd_isrt_pcs_nm
       , rcrd_isrt_ts
       , rcrd_udt_pcs_nm
       , rcrd_udt_ts
       , rcrd_src_isrt_id
       , rcrd_src_isrt_ts
       , rcrd_src_udt_id
       , rcrd_src_udt_ts
       , rcrd_src_file_nm
       , rcrd_btch_audt_id
       , rcrd_pce_cst_nm
       , rcrd_pce_cst_src_nm
       , company_id
       , payer_code
       , CASE WHEN standard_payer_code = '#N/A' THEN NULL ELSE standard_payer_code END 
  FROM pce_qe16_oper_prd_zoom.qe16zmp.cv_paymap;


DROP VIEW pce_qe16_slp_prd_dm..stnd_adm_src_dim;

--CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_adm_src_dim AS 
--SELECT pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.adm_src_cd,
--pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.adm_src_descr,
--pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.audt_sk FROM pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref;

--DROP VIEW pce_qe16_slp_prd_dm..stnd_adm_type_dim;
--
--CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_adm_type_dim AS SELECT pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref.adm_type_cd, pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref.adm_type_descr, pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref.audt_sk FROM pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref;

DROP VIEW pce_qe16_slp_prd_dm..stnd_ptnt_tp_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_ptnt_tp_dim AS SELECT pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.stnd_ptnt_type_cd AS std_encntr_type_cd, pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.stnd_ptnt_type_descr AS std_encntr_type_descr, pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.ptnt_type_smy_cd AS encntr_type_smy_cd, pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.skilled_nurse_cd, pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.skilled_nurse_descr, pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.ptnt_type_smy_descr AS encntr_type_smy_descr, pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.audt_sk FROM pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref;

DROP VIEW pce_qe16_slp_prd_dm..stnd_physcn_spcly_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_physcn_spcly_dim AS SELECT pce_qe16_prd_qadv.prmradmp.std_phys_speciality.code AS cd, pce_qe16_prd_qadv.prmradmp.std_phys_speciality.descr FROM pce_qe16_prd_qadv.prmradmp.std_phys_speciality;

--DROP VIEW pce_qe16_slp_prd_dm..spl_dim;
--
--CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..spl_dim AS
--SELECT pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.fcy_num, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_cd, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_strt_cdr_dk, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_strt_dt, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_end_cdr_dk, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_end_dt, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_descr, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_procedure_cd_v10 AS persp_clncl_dtl_pcd_cd_v10, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_procedure_descr_v10 AS persp_clncl_dtl_pcd_descr_v10, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.spl_unit_conv AS spl_unit_cnvr, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_cd AS persp_clncl_dtl_cd, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_descr AS persp_clncl_dtl_descr, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_unit AS persp_clncl_dtl_unit, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcs_cd AS persp_clncl_smy_cd, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcs_descr AS persp_clncl_smy_descr, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_cd_v10 AS persp_clncl_std_dept_cd_v10, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_descr_v10 AS persp_clncl_std_dept_descr_v10, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_v10_rollup_cat_cd AS persp_clncl_std_dept_v10_rollup_cgy_cd, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_v10_rollup_cat_descr AS persp_clncl_std_dept_v10_rollup_cgy_descr, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_spl_modfr_cd AS persp_clncl_dtl_spl_modfr_cd, pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_spl_modfr_descr AS persp_clncl_dtl_spl_modfr_descr FROM pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl;

DROP VIEW stnd_fcy_demog_dim; 
CREATE  OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_fcy_demog_dim AS SELECT * FROM pce_qe16_prd_qadv..fcy_demog_ref;

DROP VIEW pce_qe16_slp_prd_dm..stnd_ptnt_zip_dim; 
CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_ptnt_zip_dim AS SELECT * FROM pce_qe16_prd_qadv..ptnt_zip_ref; 

DROP VIEW pce_qe16_slp_prd_dm..stnd_pract_dim; 
CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_pract_dim AS SELECT * FROM pce_qe16_prd_qadv..pract_ref; 

DROP VIEW pce_qe16_slp_prd_dm..stnd_pnt_of_orig_ref; 
CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_pnt_of_orig_ref AS SELECT * FROM pce_qe16_prd_qadv..pnt_of_orig_ref; 

--ZOOM Tables / Views

DROP VIEW pce_qe16_slp_prd_dm..cdm_dim;

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..cdm_dim AS SELECT * FROM pce_qe16_oper_prd_zoom.qe16zmp.cv_cdmmstr;

DROP VIEW pce_qe16_slp_prd_dm..dept_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..dept_dim AS SELECT * FROM pce_qe16_oper_prd_zoom.qe16zmp.cv_dept;

DROP VIEW pce_qe16_slp_prd_dm..patcpt_fct; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..patcpt_fct AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patcpt;

DROP VIEW pce_qe16_slp_prd_dm..dgns_fct; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..dgns_fct AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patdgns;

DROP VIEW pce_qe16_slp_prd_dm..pract_fct; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..pract_fct AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patprac;

DROP VIEW pce_qe16_slp_prd_dm..proc_fct; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..proc_fct AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patproc;

DROP VIEW pce_qe16_slp_prd_dm..pattype_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..pattype_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_pattype;

DROP VIEW pce_qe16_slp_prd_dm..phys_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..phys_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_physmstr;

DROP VIEW pce_qe16_slp_prd_dm..pattype_map_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..pattype_map_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_pattypemap;

DROP VIEW pce_qe16_slp_prd_dm..paymstr_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..paymstr_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_paymstr;

DROP VIEW pce_qe16_slp_prd_dm..phys_spec_map_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..phys_spec_map_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_physmap;

DROP VIEW pce_qe16_slp_prd_dm..charge_fct; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..charge_fct AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patbill;

DROP VIEW pce_qe16_slp_prd_dm..pract_fct; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..pract_fct AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patprac;

DROP VIEW pce_qe16_slp_prd_dm..revuse_dim; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..revuse_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_revuse;

--DROP VIEW pce_qe16_slp_prd_dm..paymap_dim; 

--CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..paymap_dim AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_paymap;

DROP VIEW pce_qe16_slp_prd_dm..encntr_dgns_fct; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..dgns_fct AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patdgns;

DROP VIEW pce_qe16_slp_prd_dm..encntr_pcd_fct; 

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..pcd_fct AS SELECT * FROM pce_qe16_oper_prd_zoom..cv_patproc;

--ACO DB

--DROP VIEW pce_qe16_slp_prd_dm..hcpcs_ccs_dim; 
--
--CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..hcpcs_dim as select * from pce_ae00_aco_prd_cdr..hcpcs_dim
--union 
--select -100,'-100','UNKNOWN','UNKNOWN',-100;

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..fips_adr_dim as select * from pce_ae00_aco_prd_cdr..prv_fips_adr_dim;

--CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..ms_drg_dim as select * from pce_ae00_aco_prd_cdr..ms_drg_dim;

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..dgns_dim as 
SELECT dgns_sk
       , dgns_cd
       , dgns_descr
       , 'ICD' || dgns_icd_ver as dgns_icd_ver
       , dgns_descr_long
       , dgns_alt_cd
       , dgns_3_dgt_cd
       , dgns_3_dgt_descr
       , dgns_4_dgt_cd
       , dgns_4_dgt_descr
       , dgns_5_dgt_cd
       , dgns_5_dgt_descr
       , dgns_6_dgt_cd
       , dgns_6_dgt_descr

  FROM pce_ae00_aco_prd_cdr..dgns_dim;

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..pcd_dim as 
SELECT icd_pcd_sk
       , icd_pcd_cd
       , icd_pcd_descr
       ,'ICD' || icd_ver as  icd_ver
       , icd_pcd_3_dgt_cd
       , icd_pcd_3_dgt_descr
       , icd_pcd_4_dgt_cd
       , icd_pcd_4_dgt_descr
       , icd_pcd_5_dgt_cd
       , icd_pcd_5_dgt_descr
       , icd_pcd_6_dgt_cd
       , icd_pcd_6_dgt_descr
       , icd_pcd_alt_cd
  FROM pce_ae00_aco_prd_cdr..pcd_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_cpt_dim as select * from pce_qe16_prd_qadv.prmretlp.cpt_4th_edition_ref;

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..pvdr_dim as select * from pce_ae00_aco_prd_cdr..pvdr_dim;

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..rev_cl_dim as select * from pce_ae00_aco_prd_cdr..rev_cl_dim
UNION 
select -100,'-100','UNKNOWN','-100','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN';

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..ms_drg_dim as 
with zoom_uniq_ms_drg_cd as 
(select distinct msdrg_code from  pce_qe16_oper_prd_zoom..cv_patdisch WHERE msdrg_code NOT IN (select ms_drg_cd from pce_ae00_aco_prd_cdr..ms_drg_dim ))
select * from pce_ae00_aco_prd_cdr..ms_drg_dim
UNION 
select -100,
'-100', 'UNKNOWN',
'-100', 'UNKNOWN',
'-100', 'UNKNOWN',
'-100', 'UNKNOWN',
'-100', 'UNKNOWN',
'UNKNOWN',
-100,
'UNKNOWN',
'UNKNOWN',
0.0,
0.0,
'UNKNOWN',
'UNKNOWN',
'UNKNOWN',
'UNKNOWN',
'UNKNOWN'
UNION 
SELECT hash4(msdrg_code,0), 
msdrg_code, 'UNKNOWN',
'-100', 'UNKNOWN',
'-100', 'UNKNOWN',
'-100', 'UNKNOWN',
'-100', 'UNKNOWN',
'UNKNOWN',
-100,
'UNKNOWN',
'UNKNOWN',
0.0,
0.0,
'UNKNOWN',
'UNKNOWN',
'UNKNOWN',
'UNKNOWN',
'UNKNOWN'
From zoom_uniq_ms_drg_cd;

DROP VIEW pce_qe16_slp_prd_dm..apr_drg_dim;
CREATE
	OR REPLACE VIEW pce_qe16_slp_prd_dm..apr_drg_dim AS
with zoom_uniq_apr_drg_cd as 
(select distinct apr_code from  pce_qe16_oper_prd_zoom..cv_patdisch WHERE apr_code NOT IN (select apr_drg_id from pce_qe16_prd_qadv..apr_drg_ref ))
SELECT   *
FROM pce_qe16_prd_qadv.prmradmp.apr_drg_ref 
union
select -100,'UNKNOWN','','',100
union 
select CAST(apr_code as integer),'UNKNOWN','','',100
from zoom_uniq_apr_drg_cd;

CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..rev_cl_dim as 
with zoom_uniq_rev_cd as 
(select distinct revenue_code from  pce_qe16_oper_prd_zoom..cv_patbill WHERE revenue_code NOT IN (select rev_cd from pce_ae00_aco_prd_cdr..rev_cl_dim ))
select * from pce_ae00_aco_prd_cdr..rev_cl_dim
UNION 
select -100,'-100','UNKNOWN','-100','UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN'
UNION 
select hash4(revenue_code) ,'-100','UNKNOWN',revenue_code,'UNKNOWN','UNKNOWN','UNKNOWN','UNKNOWN'
from zoom_uniq_rev_cd;

--HCPCS Dim
CREATE OR REPLACE VIEW  pce_qe16_slp_prd_dm..hcpcs_dim as 
with zoom_uniq_hcpcs_cd as 
(select distinct cpt_code from  pce_qe16_oper_prd_zoom..cv_patbill WHERE cpt_code NOT IN (select hcpcs_cd from pce_ae00_aco_prd_cdr..hcpcs_dim ))
select * from pce_ae00_aco_prd_cdr..hcpcs_dim
union 
select -100,'-100','UNKNOWN','UNKNOWN',0
UNION 
select hash4(cpt_code), cpt_code, 'UNKNOWN','UNKNOWN',-1 from zoom_uniq_hcpcs_cd;


CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..spl_dim AS
with zoom_uniq_chrg_codes as 
(
  SELECT distinct cf.company_id, VSET_FCY.alt_cd as fcy_num, cf.charge_code, spl.cdm_cd
  FROM pce_qe16_oper_prd_zoom..cv_patbill cf
    LEFT JOIN pce_qe16_slp_prd_dm..val_set_dim VSET_FCY
  ON VSET_FCY.cd = cf.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN  pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl spl on cf.charge_code = spl.cdm_cd
  and VSET_FCY.alt_cd = spl.fcy_num and cf.charge_Code = spl.cdm_cd
  WHERE spl.cdm_cd is NULL
)
SELECT pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.fcy_num, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_cd, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_strt_cdr_dk, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_strt_dt, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_end_cdr_dk, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_end_dt, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cdm_descr, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_procedure_cd_v10 AS persp_clncl_dtl_pcd_cd_v10,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_procedure_descr_v10 AS persp_clncl_dtl_pcd_descr_v10,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.spl_unit_conv AS spl_unit_cnvr, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_cd AS persp_clncl_dtl_cd,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_descr AS persp_clncl_dtl_descr,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.cpm_unit AS persp_clncl_dtl_unit, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcs_cd AS persp_clncl_smy_cd,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcs_descr AS persp_clncl_smy_descr, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_cd_v10 AS persp_clncl_std_dept_cd_v10,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_descr_v10 AS persp_clncl_std_dept_descr_v10, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_v10_rollup_cat_cd AS persp_clncl_std_dept_v10_rollup_cgy_cd,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_dept_v10_rollup_cat_descr AS persp_clncl_std_dept_v10_rollup_cgy_descr,
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_spl_modfr_cd AS persp_clncl_dtl_spl_modfr_cd, 
pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl.pcd_spl_modfr_descr AS persp_clncl_dtl_spl_modfr_descr
FROM pce_qe16_prd_qadv.prmradmp.fcy_chrg_cd_ref_spl
UNION
select 
'-100',
'-100',
19000101,
'1900-01-01',
29000101,
'2900-01-01',
'UNKNOWN',
'-100',
'UNKNOWN',
0.0,
0.0,
'UNKNOWN',
0.0,
'-100',
'UNKNOWN',
-100,
'UNKNOWN',
'-100',
'UNKNOWN',
'-100',
'UNKNOWN'
UNION 
select 
fcy_num,
charge_code,
19000101,
'1900-01-01',
29000101,
'2900-01-01',
'UNKNOWN',
'-100',
'UNKNOWN',
0.0,
0.0,
'UNKNOWN',
0.0,
'-100',
'UNKNOWN',
-100,
'UNKNOWN',
'-100',
'UNKNOWN',
'-100',
'UNKNOWN'
FROM zoom_uniq_chrg_codes; 


