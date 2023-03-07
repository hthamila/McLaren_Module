\set ON_ERROR_STOP ON
BEGIN;
DELETE FROM clm_line_fct;

--Temp Table to apply derived DRG on Hospital Inpatient Professional Claims
create temp table fcy_case_id_drg as
with fcy_case_drg as
(
SELECT distinct o.facilitycaseid ,o.drg as der_drg
	FROM pce_qe16_aco_prd_lnd..outclaims o
		INNER JOIN cst_modl_dim cd ON o.prm_line = cd.cst_modl_line_cd
	WHERE cd.care_setting_sub_cgy_nm = 'Facility - Hospital Inpatient' and o.prm_admits=1
)

select distinct o.claimid,o.facilitycaseid, cd.care_setting_sub_cgy_nm, fcg.der_drg
	FROM :v_lnd_db..cv_outclaims o
			INNER JOIN cst_modl_dim cd ON o.prm_line = cd.cst_modl_line_cd
			INNER JOIN fcy_case_drg fcg on o.facilitycaseid = fcg.facilitycaseid
			WHERE cd.care_setting_cgy_nm = 'Hospital Inpatient (facility and professional)' and cd.care_setting_sub_cgy_nm='Professional - Hospital Inpatient'
			and o.drg is null and o.facilitycaseid is not null
;

INSERT INTO clm_line_fct
SELECT
  string_to_int(substr(RAWTOHEX(hash(o.claimid || '|' || linenum, 0)), 17), 16) AS clm_line_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16)                 AS pln_mbr_sk,
  member_id																	  AS mbr_id_num,
  o.claimid                                                                     AS clm_id,
  o.linenum                                                                     AS clm_line_num,
  caseadmitid                                                                 AS case_adm_id,
  sequencenumber                                                              AS clm_seq_num,
  o.facilitycaseid                                                              AS fcy_case_id,
  groupid                                                                     AS grp_id,
  contractid                                                                  AS ctr_id,
  prm_prv_id_tin as rndrg_pvdr_tin,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16)               AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(admittype, 0)), 17), 16)                 AS adm_type_sk,
  string_to_int(substr(RAWTOHEX(hash(prm_prv_id_attending, 0)), 17), 16)      AS attnd_pvdr_sk,
  string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16)                AS bill_pvdr_sk,
  string_to_int(substr(RAWTOHEX(hash(prm_prv_id_operating, 0)), 17), 16)      AS oprg_pvdr_sk,
  string_to_int(substr(RAWTOHEX(hash(prm_line, 0)), 17), 16)                  AS cst_modl_sk,
  string_to_int(substr(RAWTOHEX(hash(prm_prv_id_ccn, 0)), 17), 16) as ccn_sk,
  string_to_int(substr(RAWTOHEX(hash(
                                    CASE WHEN SUBSTR(prm_prv_id_ccn, 3, 1) IN ('M', 'R', 'S', 'T', 'U', 'W', 'Y', 'Z')
                                      THEN SUBSTR(prm_prv_id_ccn, 1, 2) || '0' || SUBSTR(prm_prv_id_ccn, 4, 3)
                                    ELSE prm_prv_id_ccn END
                                    , 0)), 17), 16) as ccn_alt_sk,
  CASE WHEN SUBSTR(prm_prv_id_ccn, 3, 1) IN ('M', 'R', 'S', 'T', 'U', 'W', 'Y', 'Z')
                                      THEN SUBSTR(prm_prv_id_ccn, 1, 2) || '0' || SUBSTR(prm_prv_id_ccn, 4, 3)
                                    ELSE prm_prv_id_ccn END as prm_prv_id_ccn,
  CASE WHEN SUBSTR(prm_prv_id_ccn, 3, 1) IN ('M', 'R', 'S', 'T', 'U', 'W', 'Y', 'Z') THEN SUBSTR(prm_prv_id_ccn, 3, 1) END AS fcy_unit_cd,
  prm_util_type as cst_modl_utlz_type_cd,
  NULL                                                                        AS cst_modl_utlz_type_descr,
  modifier                                                                       frst_pcd_modfr_cd,
  NULL                                                                        AS frst_pcd_modfr_descr,
  modifier2                                                                      sec_pcd_modfr_cd,
  NULL                                                                        AS sec_pcd_modfr_descr,
  memberstatus                                                                AS mbr_elegibility_sts_cd,
  NULL                                                                        AS mbr_elegibility_sts_descr,
  srcspecialty                                                                AS raw_src_spcly_cd,
  NULL                                                                        AS raw_src_spcly_descr,
  srcproduct                                                                  AS src_pd_nm,
  product                                                                     AS pd_nm,
  riskpool                                                                    AS rsk_pool_nm,
  lob                                                                         AS lob_nm,
  srclob                                                                      AS src_lob_nm,
  string_to_int(substr(RAWTOHEX(hash(specialty, 0)), 17), 16)                 AS aco_spcly_sk,
  string_to_int(substr(RAWTOHEX(hash(admitdiag||icdversion, 0)), 17), 16)     AS adm_dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(dischargestatus, 0)), 17), 16)           AS dschrg_sts_sk,
  string_to_int(substr(RAWTOHEX(hash(hcpcs, 0)), 17), 16)                     AS hcpcs_sk,
  string_to_int(substr(RAWTOHEX(hash(coalesce(drg,der_drg), 0)), 17), 16)     AS ms_drg_sk,
  coalesce(drg,der_drg)	                                                      AS ms_drg_cd,
  string_to_int(substr(RAWTOHEX(hash(icddiag1 || icdversion, 0)), 17), 16)    AS prim_dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(icdproc1 || icdversion, 0)), 17), 16)    AS prim_icd_pcd_sk,
  string_to_int(substr(RAWTOHEX(hash(pos, 0)), 17), 16)                       AS plc_of_svc_sk,
  string_to_int(substr(RAWTOHEX(hash(srcpos, 0)), 17), 16)                    AS sbmted_plc_of_svc_sk,
  string_to_int(substr(RAWTOHEX(hash(poa1, 0)), 17), 16)                      AS poa_sk,
  string_to_int(substr(RAWTOHEX(hash(revcode, 0)), 17), 16)                   AS rev_cl_sk,
--   src_ccn_id,
  NULL,
--   src_ccn_nm,
  NULL,
  allowed                                                                     AS alwd_amt,
  billed                                                                      AS bill_amt,
  paid                                                                        AS paid_amt,
  coinsurance                                                                 AS co_insr_amt,
  cob                                                                         AS cob_amt,
  copay                                                                       AS co_pay_amt,
  prm_costs as cst_modl_cst_amt,
  deductible                                                                  AS ddcb_amt,
  patientpay                                                                  AS ptnt_pay_amt,
  to_date(paiddate, 'YYYY/MM/DD')                                             AS paid_dt,
  to_date(prm_fromdate, 'YYYY/MM/DD')                                         AS svc_fm_dt,
  to_date(prm_todate, 'YYYY/MM/DD')                                           AS svc_to_dt,
  to_number(to_char(date(svc_fm_dt), 'YYYYMM'), '99999999')                   AS svc_fm_mo_yr_num,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'), '99999999')                   AS svc_to_mo_yr_num,
--   cvrd_clm_ind,
  NULL,
  CASE WHEN encounterflag = 'N' THEN 0 ELSE 1 END                             AS encntr_ind,
  CASE WHEN prm_oon_yn='Y' THEN 1 ELSE 0 END                                  AS out_of_ntw_ind,
  prm_admits as cst_modl_in_ptnt_clm_adm_ind,  --just not inpatient, it's for all admission type (not restricted to short term acute hospitalization)
  prm_days as cst_modl_day_cnt,
  prm_util as cst_modl_utlz_cnt,
  prm_line as cst_modl_line_cd,
  days                                                                        AS day_of_svc_cnt,
  units                                                                       AS svc_unit_cnt,
  1                                                                           AS clm_line_fct_cnt,
  NOW()                                                                       AS vld_fm_ts,
  'QE16'                                                                      AS pce_cst_cd,
  'MILLIMAN'                                                                  AS pce_cst_src_nm,
  string_to_int(substr(RAWTOHEX(hash(SUBSTR(billtype, 0, 3), 0)), 17), 16)    AS bill_type_sk,
  string_to_int(substr(RAWTOHEX(hash(prm_betos_code, 0)), 17), 16)            AS betos_sk,
  claimlinestatus                                                             AS clm_line_sts,
  userdefnum3                                                                 AS zero_snf_paid

FROM :v_lnd_db..cv_outclaims o
	left join fcy_case_id_drg using (claimid,facilitycaseid);
COMMIT;
\unset ON_ERROR_STOP
