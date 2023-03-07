\set ON_ERROR_STOP ON;

CREATE TEMP TABLE eu_members_fixed AS
SELECT
  trim(bene_mbi_id) mbi,
  trim(bene_hic_num) hicn,
  trim(bene_sex_cd) sex,
  trim(bene_brth_dt) bth_dt,
  trim(bene_death_dt) dth_dt,
  assignment_type clm_assgn_step_flg,
  assigned_before prev_assign_ben_flag,
  enrollflag12 mon_elig_flag_12,
  enrollflag11 mon_elig_flag_11,
  enrollflag10 mon_elig_flag_10,
  enrollflag9 mon_elig_flag_9,
  enrollflag8 mon_elig_flag_8,
  enrollflag7 mon_elig_flag_7,
  enrollflag6 mon_elig_flag_6,
  enrollflag5 mon_elig_flag_5,
  enrollflag4 mon_elig_flag_4,
  enrollflag3 mon_elig_flag_3,
  enrollflag2 mon_elig_flag_2,
  enrollflag1 mon_elig_flag_1,
  trim(esrd_score) hcc_esrd,
  trim(dis_score) hcc_disabled,
  trim(agdu_score) hcc_aged_dual,
  trim(agnd_score) hcc_aged_non_dual
FROM pce_qe16_aco_prd_lnd..eu_members
WHERE enrollflag12 != 0
  UNION ALL
SELECT
  trim(bene_mbi_id) mbi,
  trim(bene_hic_num) hicn,
  trim(bene_sex_cd) sex,
  trim(bene_brth_dt) bth_dt,
  trim(bene_death_dt) dth_dt,
  assignment_type clm_assgn_step_flg,
  assigned_before prev_assign_ben_flag,
  CASE WHEN enrollflag11 = '0' THEN
  CASE WHEN enrollflag10 = '0' THEN
  CASE WHEN enrollflag9 = '0' THEN
  CASE WHEN enrollflag8 = '0' THEN
  CASE WHEN enrollflag7 = '0' THEN
  CASE WHEN enrollflag6 = '0' THEN
  CASE WHEN enrollflag5 = '0' THEN
  CASE WHEN enrollflag4 = '0' THEN
  CASE WHEN enrollflag3 = '0' THEN
  CASE WHEN enrollflag2 = '0' THEN
  CASE WHEN enrollflag1 = '0' THEN
  '0'
          ELSE enrollflag1 END
          ELSE enrollflag2 END
          ELSE enrollflag3 END
          ELSE enrollflag4 END
          ELSE enrollflag5 END
          ELSE enrollflag6 END
          ELSE enrollflag7 END
          ELSE enrollflag8 END
          ELSE enrollflag9 END
          ELSE enrollflag10 END
          ELSE enrollflag11 END AS mon_elig_flag_lst,
  CASE WHEN enrollflag11 = '0' THEN mon_elig_flag_lst ELSE enrollflag11 END AS mon_elig_flag_11,
  CASE WHEN enrollflag10 = '0' THEN mon_elig_flag_lst ELSE enrollflag10 END AS mon_elig_flag_10,
  CASE WHEN enrollflag9 = '0' THEN mon_elig_flag_lst ELSE enrollflag9 END AS mon_elig_flag_9,
  CASE WHEN enrollflag8 = '0' THEN mon_elig_flag_lst ELSE enrollflag8 END AS mon_elig_flag_8,
  CASE WHEN enrollflag7 = '0' THEN mon_elig_flag_lst ELSE enrollflag7 END AS mon_elig_flag_7,
  CASE WHEN enrollflag6 = '0' THEN mon_elig_flag_lst ELSE enrollflag6 END AS mon_elig_flag_6,
  CASE WHEN enrollflag5 = '0' THEN mon_elig_flag_lst ELSE enrollflag5 END AS mon_elig_flag_5,
  CASE WHEN enrollflag4 = '0' THEN mon_elig_flag_lst ELSE enrollflag4 END AS mon_elig_flag_4,
  CASE WHEN enrollflag3 = '0' THEN mon_elig_flag_lst ELSE enrollflag3 END AS mon_elig_flag_3,
  CASE WHEN enrollflag2 = '0' THEN mon_elig_flag_lst ELSE enrollflag2 END AS mon_elig_flag_2,
  CASE WHEN enrollflag1 = '0' THEN mon_elig_flag_lst ELSE enrollflag1 END AS mon_elig_flag_1,
  trim(esrd_score) hcc_esrd,
  trim(dis_score) hcc_disabled,
  trim(agdu_score) hcc_aged_dual,
  trim(agnd_score) hcc_aged_non_dual
FROM pce_qe16_aco_prd_lnd..eu_members
WHERE enrollflag12 = 0;

DROP TABLE members_eligbility_dim IF EXISTS;
CREATE TEMP TABLE members_eligbility_dim AS
  SELECT
	mbi,
    hicn,
    val :: DATE - INTERVAL '11 month' AS elig_month,
    mon_elig_flag_1,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a
  UNION ALL
  SELECT
	mbi,
    hicn,
    val :: DATE - INTERVAL '10 month' AS elig_month,
    mon_elig_flag_2,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a
  UNION ALL
  SELECT
    mbi,
	hicn,
    val :: DATE - INTERVAL '9 month' AS elig_month,
    mon_elig_flag_3,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a
  UNION ALL
  SELECT
    mbi,
	hicn,
    val :: DATE - INTERVAL '8 month' AS elig_month,
    mon_elig_flag_4,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a
  UNION ALL
  SELECT
    mbi,
	hicn,
    val :: DATE - INTERVAL '7 month' AS elig_month,
    mon_elig_flag_5,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a
  UNION ALL
  SELECT
    mbi,
	hicn,
    val :: DATE - INTERVAL '6 month' AS elig_month,
    mon_elig_flag_6,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a
  UNION ALL
  SELECT
    mbi,
	hicn,
    val :: DATE - INTERVAL '5 month' AS elig_month,
    mon_elig_flag_7,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a
  UNION ALL
  SELECT
    mbi,
	hicn,
    val :: DATE - INTERVAL '4 month' AS elig_month,
    mon_elig_flag_8,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a
  UNION ALL
  SELECT
    mbi,
	hicn,
    val :: DATE - INTERVAL '3 month' AS elig_month,
    mon_elig_flag_9,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a
  UNION ALL
  SELECT
    mbi,
	hicn,
    val :: DATE - INTERVAL '2 month' AS elig_month,
    mon_elig_flag_10,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a
  UNION ALL
  SELECT
    mbi,
	hicn,
    val :: DATE - INTERVAL '1 month' AS elig_month,
    mon_elig_flag_11,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a
  UNION ALL
  SELECT
    mbi,
	hicn,
    val :: DATE AS elig_month,
    mon_elig_flag_12,
    CASE WHEN hcc_esrd = '' THEN CASE WHEN hcc_disabled = '' THEN CASE WHEN hcc_aged_dual = '' THEN hcc_aged_non_dual ELSE hcc_aged_dual END ELSE hcc_disabled END ELSE hcc_esrd END AS hcc_scr
  FROM eu_members_fixed
    CROSS JOIN (SELECT * FROM dt_meta WHERE descr = 'asgnt_dt') a;


DELETE FROM pln_mbr_asgnt_dim;
INSERT INTO pln_mbr_asgnt_dim (pln_mbr_asgnt_sk, pln_mbr_sk, mbr_id_num, asgnt_wndw_strt_dt, asgnt_wndw_end_dt, elig_sts, mdcl_mo_cnt, pharm_mo_cnt, dntl_mo_cnt, vsn_mo_cnt, asgnt_ind, vld_fm_ts, pce_cst_cd, pce_cst_src_nm)
SELECT
  string_to_int(substr(RAWTOHEX(hash(mtw.member_id || mtw.date_start || mtw.date_end, 0)), 17), 16) AS pln_mbr_asgnt_sk,
  string_to_int(substr(RAWTOHEX(hash(mtw.member_id, 0)), 17), 16) AS pln_mbr_sk,
  mtw.member_id as mbr_id_num,
  mtw.date_start,
  mtw.date_end,
  --CASE WHEN med.mon_elig_flag_1 IS NOT NULL THEN
   -- CASE WHEN mon_elig_flag_1 = 0 THEN 'Not Eligible' WHEN mon_elig_flag_1 = 1 THEN 'ESRD' WHEN mon_elig_flag_1 = 2 THEN 'Disabled' WHEN mon_elig_flag_1 = 3 THEN 'Aged Dual' WHEN mon_elig_flag_1 = 4 THEN 'Aged Non-Dual' END
  --ELSE 
  elig_status_1 AS elig_sts,
  memmos_medical,
  memmos_rx,
  memmos_dental,
  memmos_vision,
  CASE WHEN mtw.assignment_indicator = 'Y' THEN 1 WHEN mtw.assignment_indicator = 'N' THEN 0 ELSE NULL END,
  NOW(),
  'QE16',
  'MILLIMAN'
FROM pce_qe16_aco_prd_lnd..cv_member_time_windows mtw
  LEFT OUTER JOIN pce_qe16_aco_prd_lnd..cv_members mem ON mtw.member_id = mem.member_id
--  LEFT OUTER JOIN members_eligbility_dim med ON med.elig_month = mtw.elig_month AND med.mbi = mem.member_id;
;
DROP TABLE qtr_asgnt_dim IF EXISTS;
CREATE TABLE qtr_asgnt_dim AS
  SELECT
    pln_mbr_sk,
    DATE_PART('quarter', asgnt_wndw_strt_dt) AS asgnt_qtr,
    DATE_PART('year', asgnt_wndw_strt_dt)    AS asgnt_yr
  FROM pln_mbr_asgnt_dim
  WHERE asgnt_ind = 1
  GROUP BY pln_mbr_sk, asgnt_qtr, asgnt_yr
  ORDER BY pln_mbr_sk;

\unset ON_ERROR_STOP
