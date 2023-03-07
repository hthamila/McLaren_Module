\set ON_ERROR_STOP ON;

CREATE TEMP TABLE clm_dgns_fct_tmpa AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '0', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(admitdiag || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash('1', 0)), 17), 16) AS poa_sk,
  0 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  1 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
from pce_qe16_aco_prd_lnd..cv_outclaims
);

CREATE TEMP TABLE clm_dgns_fct_tmp1 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '1', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag1 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa1, 0)), 17), 16) AS poa_sk,
  1 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  1 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp2 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '2', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag2 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa2, 0)), 17), 16) AS poa_sk,
  2 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp3 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '3', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag3 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa3, 0)), 17), 16) AS poa_sk,
  3 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp4 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '4', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag4 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa4, 0)), 17), 16) AS poa_sk,
  4 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp5 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '5', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag5 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa5, 0)), 17), 16) AS poa_sk,
  5 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp6 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '6', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag6 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa6, 0)), 17), 16) AS poa_sk,
  6 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp7 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '7', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag7 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa7, 0)), 17), 16) AS poa_sk,
  7 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp8 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '8', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag8 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa8, 0)), 17), 16) AS poa_sk,
  8 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp9 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '9', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag9 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa9, 0)), 17), 16) AS poa_sk,
  9 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp10 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '10', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag10 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa10, 0)), 17), 16) AS poa_sk,
  10 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp11 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '11', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag11 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa11, 0)), 17), 16) AS poa_sk,
  11 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp12 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '12', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag12 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa12, 0)), 17), 16) AS poa_sk,
  12 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp13 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '13', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag13 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa13, 0)), 17), 16) AS poa_sk,
  13 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp14 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '14', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag14 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa14, 0)), 17), 16) AS poa_sk,
  14 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

CREATE TEMP TABLE clm_dgns_fct_tmp15 AS
(SELECT
   string_to_int(substr(RAWTOHEX(hash(claimid || '|' || linenum || '|' || '15', 0)), 17), 16) AS clm_dgns_fct_sk,
  string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) as pln_mbr_sk,
   string_to_int(substr(RAWTOHEX(hash(providerid, 0)), 17), 16) AS pvdr_sk,
  claimid AS clm_id,
   linenum AS clm_line_num,
  string_to_int(substr(RAWTOHEX(hash(admitsource, 0)), 17), 16) AS adm_src_sk,
  string_to_int(substr(RAWTOHEX(hash(icddiag15 || icdversion, 0)), 17), 16) AS dgns_sk,
  string_to_int(substr(RAWTOHEX(hash(poa15, 0)), 17), 16) AS poa_sk,
  15 AS icd_pos_num,
  to_date(prm_fromdate,'YYYY/MM/DD') AS svc_fm_dt,
  to_date(prm_todate,'YYYY/MM/DD') AS svc_to_dt,
  to_number(to_char(date(svc_to_dt), 'YYYYMM'),'99999999') AS svc_to_mo_yr_num,
  0 AS adm_dgns_ind,
  0 AS prim_dgns_ind,
  1 AS clm_dgns_fct_cnt,
  riskpool as rsk_pool_nm,
  NOW() AS vld_fm_ts,
	'QE16' AS pce_cst_cd,
	'U' AS pce_cst_src_nm
FROM pce_qe16_aco_prd_lnd..cv_outclaims);

BEGIN;
DELETE FROM clm_dgns_fct;
INSERT INTO clm_dgns_fct
  SELECT *
  FROM
    (SELECT *
     FROM clm_dgns_fct_tmpa
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp1
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp2
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp3
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp4
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp5
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp6
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp7
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp8
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp9
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp10
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp11
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp12
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp13
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp14
     UNION ALL
     SELECT *
     FROM clm_dgns_fct_tmp15
    ) a;
COMMIT;

\unset ON_ERROR_STOP
