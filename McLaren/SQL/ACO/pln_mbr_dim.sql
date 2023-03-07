\set ON_ERROR_STOP ON;

DELETE FROM pln_mbr_dim;
INSERT INTO pln_mbr_dim (pln_mbr_sk, frst_nm, last_nm, full_nm, mbr_id_num, brth_dt, dth_dt, gnd_cd, gnd_descr, adr_line_1, adr_line_2, cty_nm, ste_cd, ste_descr, zip_cd, cms_hcc_scor_num, othr_hcc_scor_num, mbr_id_alt, 
                         care_mgn_sts_ind, benf_flg)
SELECT
  string_to_int(substr(RAWTOHEX(hash(mem.member_id, 0)), 17), 16) AS pln_mbr_sk,
  TRIM(GET_VALUE_VARCHAR(ARRAY_SPLIT(mem.mem_name, ','),2)) AS frst_nm,
  TRIM(GET_VALUE_VARCHAR(ARRAY_SPLIT(mem.mem_name, ','),1)) AS last_nm,
  mem.mem_name AS full_name,
  mem.member_id AS mbr_id_num,
  mem.dob AS brth_dt,
  mem.death_date AS dth_dt,
  mem.gender AS gnd_cd,
  CASE WHEN mem.gender = 'M' THEN 'Male' ELSE 'Female' END AS gnd_descr,
  mem.mem_address_line_1 AS adr_line_1,
  mem.mem_address_line_2 AS adr_line_2,
  TRIM(GET_VALUE_VARCHAR(ARRAY_SPLIT(mem.mem_city_state, ','),1)) AS cty_nm,
  mem.mem_state AS ste_cd,
  NULL AS ste_descr,
  mem.mem_zip5 AS zip_cd,
  hcc.hcc_scr,
  mem.risk_scr_primary,
  member_id_alt as mbr_id_alt,
  CASE WHEN cm_status = 'Care Managed'
	THEN 1 ELSE 0 END as care_mgn_sts_ind,
  hcc.benf_flg
FROM pce_qe16_aco_prd_lnd..cv_members mem
LEFT OUTER JOIN benf_hcc_vw hcc ON mem.member_id = hcc.mbi
LEFT OUTER JOIN care_managed on mem.member_id=care_managed.member_id;

\unset ON_ERROR_STOP
