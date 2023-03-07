--###########################################################################################
--             Physician NPI Specialty
--###########################################################################################
--select 'processing table:  intermediate_stage_temp_physician_npi_spclty' as table_processing;
DROP TABLE intermediate_stage_temp_physician_npi_spclty

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_physician_npi_spclty AS (
		SELECT DISTINCT PT.company_id
		,PT.practitioner_code
		,NPIREG.npi
--Code Change : Source NPI's are tied with More than one Physician so using NPI
--Registry detaiils
 --		,PT.practitioner_name
--,coalesce(NPIREG.pvdr_lgl_last_nm ||  ', ' || NPIREG.pvdr_frst_nm || ' ' || NPIREG.pvdr_mid_nm, NPIREG.pvdr_lgl_org_nm)   as practitioner_name
         --       ,NPIREG.pvdr_lgl_last_nm ||  ', ' || NPIREG.pvdr_frst_nm  as practitioner_name
--,coalesce(coalesce(NPIREG.pvdr_lgl_last_nm,'') ||  ', ' || coalesce(NPIREG.pvdr_frst_nm,'') || ' ' || coalesce(NPIREG.pvdr_mid_nm,''), coalesce(NPIREG.pvdr_lgl_org_nm,''))   as practitioner_name
--,decode(coalesce(NPIREG.pvdr_lgl_last_nm,'') ||  ', ' || coalesce(NPIREG.pvdr_frst_nm,'') || ' ' || coalesce(NPIREG.pvdr_mid_nm,''), ',', coalesce(NPIREG.pvdr_lgl_org_nm,'')) as practitioner_name
,CASE WHEN trim(coalesce(NPIREG.pvdr_lgl_last_nm,'') ||  ', ' || coalesce(NPIREG.pvdr_frst_nm,'') || ' ' || coalesce(NPIREG.pvdr_mid_nm,'')) = ',' THEN
coalesce(NPIREG.pvdr_lgl_org_nm,'')
ELSE
   trim(coalesce(NPIREG.pvdr_lgl_last_nm,'') ||  ', ' || coalesce(NPIREG.pvdr_frst_nm,'') || ' ' || coalesce(NPIREG.pvdr_mid_nm,''))
END as practitioner_name
--After CHANGE
		--,coalesce(NPIREG.hcare_pvdr_txnmy_cl_nm , NPIREG.hcare_scdy_pvdr_txnmy_cl_nm)  AS practitioner_spclty_description
		,replace(coalesce(NPIREG.hcare_pvdr_txnmy_descr,NPIREG.hcare_scdy_pvdr_txnmy_descr),'-',' ') as practitioner_spclty_description
		,coalesce(NPIREG.hcare_pvdr_txnmy_cd, NPIREG.hcare_scdy_pvdr_txnmy_cd) as mcare_spcly_cd
--Before Change
--		,coalesce(coalesce(CWALK.pvdr_spclty_descr,'') , coalesce(NPIREG.mcare_spcly_descr,''))  AS practitioner_spclty_description
--		,NPIREG.mcare_spcly_cd
--		,coalesce(NPIREG.hcare_pvdr_txnmy_cd, NPIREG.hcare_scdy_pvdr_txnmy_cd) as hcare_pvdr_txnmy_cd
--		,coalesce(NPIREG.hcare_pvdr_txnmy_cl_nm , NPIREG.hcare_scdy_pvdr_txnmy_cl_nm) as hcare_pvdr_txnmy_cl_nm
		,NPIREG.npi_dactv_dt
		FROM phys_dim PT
		INNER JOIN pvdr_dim NPIREG
		ON PT.npi = NPIREG.npi
--                LEFT JOIN manual_txny_pvdr_spcl_dim CWALK
--		on trim(NPIREG.hcare_pvdr_txnmy_cd) = Trim(CWALK.pvdr_txny_cd)
--      WHERE initcap(PT.company_id) <> 'Lansing'
		);
--###########################################################################################
--            Cancer Diagnosis Related
--###########################################################################################

--Code Change : 05/10/2019 : Added a new temp table in support of Cancer Patient Identification
--Code Change : 08/11/2020 : Modified where clause to include the ccs_Dgns_cgy_cd in support MLH-555 Update Cancer Case codes
--select 'processing table:  intermediate_stage_temp_dgns_ccs_dim_cancer_only' as table_processing;
DROP TABLE intermediate_stage_temp_dgns_ccs_dim_cancer_only IF EXISTS;

--Code Change: 01/05/2021 Commenting the existing logic
--SELECT distinct dgns_cd, ccs_dgns_cgy_descr
--FROM pce_ae00_aco_prd_cdr..dgns_ccs_dim
----Code Change : 10/01/2020 : Updating the where clause to consider 997 as well as per MLH-591
----WHERE (ccs_dgns_cgy_cd BETWEEN 11 and 47 ) AND eff_to_Dt is NULL;
--WHERE (
--(CAST(ccs_dgns_cgy_cd as INT) BETWEEN 11 and 47 ) OR
--(CAST(ccs_dgns_cgy_cd as INT) = 58 AND dgns_cd like 'E85%')
 --) AND eff_to_Dt is NULL;
 --Code change : 01/05/2021 : Updating the cancer_dgns_codes as per Member's request

CREATE TABLE intermediate_stage_temp_dgns_ccs_dim_cancer_only AS
SELECT distinct aco.dgns_cd, aco.ccs_dgns_cgy_descr
FROM cncr_dgns_dim cncr
INNER JOIN pce_ae00_aco_prd_cdr..dgns_ccs_dim aco
on cncr.ccs_dgns_cgy_cd = aco.ccs_dgns_cgy_cd AND aco.dgns_cd = cncr.dgns_cd AND
aco.dgns_cd_ver = cncr.dgns_cd_ver AND aco.eff_to_dt is NULL;

--Code Change : 08/11/2020 : Modified where clause to comment the existing criteria
--WHERE lower(ccs_dgns_cgy_descr) in
--(
--'cancer of head and neck',
--'cancer of esophagus',
--'cancer of stomach',
--'cancer of colon',
--'cancer of rectum and anus',
--'cancer of liver and intrahepatic bile duct',
--'cancer of pancreas',
--'cancer of other GI organs; peritoneum',
--'cancer of bronchus; lung',
--'cancer; other respiratory and intrathoracic',
--'cancer of bone and connective tissue',
--'Other non-epithelial cancer of skin',
--'cancer of breast',
--'cancer of uterus',
--'cancer of cervix',
--'cancer of ovary',
--'cancer of other female genital organs',
--'cancer of prostate',
--'cancer of testis',
--'cancer of other male genital organs',
--'cancer of bladder',
--'cancer of kidney and renal pelvis',
--'cancer of other urinary organs',
--'cancer of brain and nervous system',
--'cancer of thyroid',
--'cancer; other and unspecified primary'
--)

--###########################################################################################
--             FIPS Adr Dim
--###########################################################################################

--select 'processing table:  intermediate_stage_temp_fips_adr_dim' as table_processing;
DROP TABLE intermediate_stage_temp_fips_adr_dim IF EXISTS;
CREATE TABLE intermediate_stage_temp_fips_adr_dim as
(
   select
Q.ptnt_zip_cd as fips_zip_cd,
F.fips_cnty_descr,
Q.ste_descr as ptnt_fips_ste_descr
from stnd_ptnt_zip_dim Q

LEFT JOIN fips_adr_dim F
on F.fips_cnty_cd = Q.cnty_fips_nm and Q.ste_cd = F.fips_ste_descr
);

--###########################################################################################
--            Patient Type Code Dimension
--###########################################################################################

--select 'processing table: intermediate_stage_temp_ptnt_type_fcy_std_cd ' as table_processing;
DROP TABLE intermediate_stage_temp_ptnt_type_fcy_std_cd

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_ptnt_type_fcy_std_cd AS (
		SELECT PATTYPE.company_id
		,PATTYPE.patient_type_code
		,PATTYPE.patient_type_description
		,MAP.standard_patient_type_code
		,STD.std_encntr_type_descr FROM pattype_dim PATTYPE LEFT JOIN pattype_map_dim MAP ON MAP.patient_type_code = PATTYPE.patient_type_code
		AND MAP.company_id = PATTYPE.company_id LEFT JOIN stnd_ptnt_tp_dim STD ON STD.std_encntr_type_cd = MAP.standard_patient_type_code
		);


--###########################################################################################
--           Payer Code Dimension
--###########################################################################################

-- 07/14/2021: Physicalize intermediate_stage_temp_payer_fcy_std_code Table
   DROP TABLE intermediate_stage_temp_payer_fcy_std_code IF EXISTS;
	CREATE TABLE intermediate_stage_temp_payer_fcy_std_code AS (
	with qadv_data AS (
	select distinct std_pyr_cd, std_pyr_descr from  stnd_fcy_pyr_dim )
		SELECT PMSTR.company_id
		,PMSTR.payer_code
		,PMSTR.payer_description
		,PMSTR.payer_code AS fcy_payer_code
		,PMSTR.payer_description AS fcy_payer_description
		,PMAP.standard_payer_code AS std_payer_code
		,QAPYR.std_pyr_descr AS std_payer_descr
		,PMSTR.payor_group1
		,PMSTR.payor_group2
		,PMSTR.payor_group3
	     from paymstr_dim PMSTR
	        INNER JOIN paymap_dim PMAP
	on PMSTR.payer_code = PMAP.payer_code and PMSTR.company_id = PMAP.company_id
	 INNER JOIN qadv_data QAPYR
     on QAPYR.std_pyr_cd = PMAP.standard_payer_code
		);--16559

--###########################################################################################
--            Physician Specialty Related
--###########################################################################################

--select 'processing table: intermediate_stage_temp_physician_fcy_std_spclty ' as table_processing;
DROP TABLE intermediate_stage_temp_physician_fcy_std_spclty

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_physician_fcy_std_spclty AS (
		SELECT DISTINCT PT.company_id
		,PT.practitioner_code
		,PT.practitioner_name
		,PT.practitioner_specialty_code AS practitioner_spclty_code
		,PM.standard_practitioner_specialty_code AS standard_practitioner_spclty_code
		,STD.descr AS practitioner_spclty_description FROM phys_spec_map_dim PM
		INNER JOIN phys_dim PT
		ON PM.practitioner_code = PT.practitioner_code
		AND PT.company_id = PM.company_id
		INNER JOIN stnd_physcn_spcly_dim STD
		ON STD.cd = PM.standard_practitioner_specialty_code
		);--154290

--###########################################################################################
--            Discharge Status Code Related
--###########################################################################################

--select 'processing table:  intermediate_stage_temp_discharge_fcy_std_status_code' as table_processing;
DROP TABLE intermediate_stage_temp_discharge_fcy_std_status_code

IF EXISTS;
	CREATE TABLE intermediate_stage_temp_discharge_fcy_std_status_code AS (
		SELECT DISTINCT ZOOM.discharge_status
		,DISSTATUS.dschrg_sts_cd
		,DISSTATUS.dschrg_sts_descr FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM INNER JOIN intermediate_stage_temp_eligible_encntr_data ENCNTR ON ZOOM.company_id = ENCNTR.company_id
		AND ZOOM.patient_id = ENCNTR.patient_id LEFT JOIN dschrg_sts_dim DISSTATUS ON CAST(DISSTATUS.dschrg_sts_cd AS INT) = CAST(ZOOM.discharge_status AS INT)
		);--37


--CODE Change : Adding intermediate_stage_intermediate_stage_spl_dim to fix the Lansing encounters with Charge Code but SPL Code is NULL issue
--select 'processing table:  intermediate_stage_spl_dim' as table_processing;
DROP TABLE intermediate_stage_spl_dim IF EXISTS;
CREATE TABLE intermediate_stage_spl_dim as
with zoom_uniq_chrg_codes as
         (
             SELECT distinct cf.company_id, VSET_FCY.alt_cd as fcy_num, cf.charge_code, spl.cdm_cd
             FROM pce_qe16_oper_prd_zoom..cv_patbill cf
                      LEFT JOIN val_set_dim VSET_FCY
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

