\set ON_ERROR_STOP ON;
BEGIN;
--DROP TABLE index_admissions if exists;
CREATE TEMP TABLE index_admissions AS
  SELECT
    cf.pln_mbr_sk,
    pmd.mbr_id_num,
    cf.clm_id,
    cf.fcy_case_id,
    cf.ccn_sk,
    ccnd.ccn_id,
    ccnd.fcy_nm,
    cf.dschrg_sts_sk,
    dsd.dschrg_sts_cd,
    cf.cst_modl_sk,
    cd.care_svc_sub_cgy_nm,
    cd.cst_modl_line_cgy_nm,
    cf.prim_dgns_sk,
    cf.prim_icd_pcd_sk,
    min(cf.svc_fm_dt)                       svc_fm_dt,
    max(cf.svc_to_dt)                       svc_to_dt,
    sum(cf.paid_amt)                        paid_amt,
    sum(cf.cst_modl_in_ptnt_clm_adm_ind) AS adm_ind,
    SUM(cst_modl_day_cnt) AS los
  FROM clm_line_fct cf
    INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
    LEFT OUTER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
    LEFT OUTER JOIN dgns_ccs_dim cdm ON dd.dgns_alt_cd = cdm.dgns_cd AND dd.dgns_icd_ver = cdm.dgns_cd_ver
    LEFT OUTER JOIN ccn_dim ccnd ON cf.ccn_sk = ccnd.ccn_sk
    LEFT OUTER JOIN dschrg_sts_dim dsd ON cf.dschrg_sts_sk = dsd.dschrg_sts_sk
    LEFT OUTER JOIN pln_mbr_dim pmd ON pmd.pln_mbr_sk = cf.pln_mbr_sk
    LEFT OUTER JOIN ahrq_val_set_dim ie1 ON cdm.ccs_dgns_cgy_cd=ie1.cd and ie1.cohrt_id = 'Readmission - D.3'
WHERE
   (ie1.cd is null or cdm.ccs_dgns_cgy_cd not in ('254')) 
  GROUP BY cf.pln_mbr_sk, pmd.mbr_id_num, cf.clm_id, cf.fcy_case_id, cf.ccn_sk, ccnd.ccn_id, ccnd.fcy_nm, cf.dschrg_sts_sk, dsd.dschrg_sts_cd, cf.cst_modl_sk,
    cd.care_svc_sub_cgy_nm, cd.cst_modl_line_cgy_nm, cf.prim_dgns_sk, cf.prim_icd_pcd_sk
  HAVING adm_ind > 0;

--DROP TABLE pac_admissions if exists;
CREATE TEMP TABLE pac_admissions AS
  SELECT
    ip.pln_mbr_sk,
    ip.mbr_id_num,
    pac.clm_id               AS clm_id,
    pac.ccn_sk,
    pac.fcy_case_id,
    pac.dschrg_sts_sk,
    pac.care_svc_sub_cgy_nm  AS care_svc_sub_cgy_nm,
    pac.cst_modl_line_cgy_nm AS cst_modl_line_cgy_nm,
    pac.cst_modl_sk,
    pac.svc_fm_dt            AS svc_fm_dt,
    pac.svc_to_dt            AS svc_to_dt,
    pac.paid_amt             AS paid_amt,
    pac.adm_ind              AS adm_ind,
    pac.los,
    pac.prim_dgns_sk,
    pac.prim_icd_pcd_sk
  FROM index_admissions pac
    INNER JOIN index_admissions ip ON pac.pln_mbr_sk = ip.pln_mbr_sk AND pac.clm_id != ip.clm_id
  WHERE
--     pac.care_svc_sub_cgy_nm IN ('Skilled Nursing Facility', 'Long-Term Acute Care', 'Rehabilitation', 'Home Health')
--       pac.care_svc_sub_cgy_nm = 'Long-Term Acute Care'
        pac.care_svc_sub_cgy_nm = 'Skilled Nursing Facility'
        AND ip.cst_modl_line_cgy_nm = 'FIP'
        AND pac.svc_fm_dt = ip.svc_to_dt
        AND pac.dschrg_sts_cd != '07';


-- Denominator
-- SELECT COUNT(DISTINCT clm_id)
-- FROM pac_admissions
-- WHERE svc_to_dt
-- BETWEEN (SELECT dt
--                FROM dt_meta
--                WHERE descr = 'roll_yr_strt') AND (SELECT dt
--                           FROM dt_meta
--                           WHERE descr = 'roll_yr_end');

--DROP TABLE pac_readm if exists;
CREATE TEMP TABLE pac_readm AS
SELECT
  string_to_int(substr(RAWTOHEX(hash(a.clm_id, 0)), 17), 16) AS clm_readm_anl_sk,
  a.pln_mbr_sk,
  a.clm_id,
  a.fcy_case_id,
  readm_clm_id,
  a.dschrg_sts_sk,
  a.prim_dgns_sk AS dgns_sk,
  a.ccn_sk,
  readm_dgns_sk,
  a.prim_icd_pcd_sk AS icd_pcd_sk,
  readm_icd_pcd_sk,
  readm_ccn_sk,
  a.cst_modl_sk,
  readm_cst_modl_sk,
  readm_svc_fm_dt,
  readm_svc_to_dt,
  'IP' AS readm_type_cd,
  'Inpatient' AS readm_type_descr,
  'SNF' AS idnx_adm_type_cd,
  'Skilled Nursing Facility' AS idnx_adm_type_descr,
  a.svc_fm_dt,
  a.svc_to_dt,
  readm_paid_amt,
  a.paid_amt,
  CASE WHEN ie1.cd_descr <> ''
      THEN 1
    ELSE NULL END               AS frst_pln_readm_ind,
    CASE WHEN ie2.cd_descr <> '' OR ie3.cd_descr <> ''
      THEN 1
    ELSE NULL END               AS sec_pln_readm_ind,
    CASE WHEN ie4.cd_descr <> '' OR ie5.cd_descr <> ''
      THEN 1
    ELSE NULL END               AS third_pln_readm_ind,
    CASE WHEN ie6.cd_descr <> '' OR ie7.cd_descr <> ''
      THEN 1
    ELSE NULL END               AS fourth_pln_readm_ind,
        CASE WHEN frst_pln_readm_ind = 1 OR sec_pln_readm_ind = 1 OR (third_pln_readm_ind = 1 AND fourth_pln_readm_ind IS NULL)
      THEN 1
    ELSE NULL END               AS pln_ind,
    CASE WHEN (readm_clm_id IS NOT NULL AND frst_pln_readm_ind IS NULL AND sec_pln_readm_ind IS NULL AND third_pln_readm_ind IS NULL) OR
              (readm_clm_id IS NOT NULL AND frst_pln_readm_ind IS NULL AND sec_pln_readm_ind IS NULL AND third_pln_readm_ind = 1 AND fourth_pln_readm_ind = 1)
      THEN 1
    ELSE NULL END               AS unpln_ind,
  NULL AS cohrt_nm,
  readm_svc_fm_dt - a.svc_to_dt AS days_to_readm,
  readm_los AS readm_los_cnt,
  a.los AS los_cnt,
    1                                                                           AS clm_readm_anl_fct_cnt,
  NOW()                                                                       AS vld_fm_ts,
  'QE16'                                                                      AS pce_cst_cd,
  'MILLIMAN'                                                                  AS pce_cst_src_nm
FROM (

  SELECT
    pac.pln_mbr_sk,
    pac.mbr_id_num,
    pac.clm_id,
    pac.fcy_case_id,
    pac.ccn_sk,
    pac.dschrg_sts_sk,
    pac.care_svc_sub_cgy_nm,
    pac.prim_dgns_sk,
    pac.prim_icd_pcd_sk,
    pac.paid_amt,
    pac.svc_fm_dt,
    pac.svc_to_dt,
    pac.los,
    pac.cst_modl_sk,
    ip2.clm_id    AS           readm_clm_id,
    ip2.svc_fm_dt AS           readm_svc_fm_dt,
    ip2.svc_to_dt AS           readm_svc_to_dt,
    ip2.los AS readm_los,
    ip2.ccn_sk AS readm_ccn_sk,
    ip2.prim_dgns_sk AS readm_dgns_sk,
    ip2.prim_icd_pcd_sk AS readm_icd_pcd_sk,
    ip2.paid_amt AS readm_paid_amt,
    ip2.dschrg_sts_sk AS readm_dschrg_sts_sk,
    ip2.cst_modl_sk AS readm_cst_modl_sk,
    RANK()
    OVER (
      PARTITION BY pac.clm_id
      ORDER BY ip2.svc_fm_dt, ip2.svc_to_dt ) rnk
  FROM pac_admissions pac
    INNER JOIN index_admissions ip2
      ON pac.pln_mbr_sk = ip2.pln_mbr_sk AND pac.clm_id != ip2.clm_id AND ip2.svc_fm_dt >= pac.svc_to_dt
  WHERE
    (ip2.cst_modl_line_cgy_nm = 'FIP')
--         AND pac.svc_to_dt
--         BETWEEN (SELECT dt
--                  FROM dt_meta
--                  WHERE descr = 'roll_yr_strt') AND (SELECT dt
--                                                     FROM dt_meta
--                                                     WHERE descr = 'roll_yr_end')
) a
  LEFT OUTER JOIN index_admissions readm_ip ON a.mbr_id_num = readm_ip.mbr_id_num AND a.readm_clm_id = readm_ip.clm_id
  LEFT OUTER JOIN dgns_dim dd ON readm_ip.prim_dgns_sk = dd.dgns_sk
  LEFT OUTER JOIN icd_pcd_dim ipd ON readm_ip.prim_icd_pcd_sk = ipd.icd_pcd_sk
  LEFT OUTER JOIN icd_pcd_ccs_dim cpm ON cpm.icd_pcd_cd = ipd.icd_pcd_alt_cd AND cpm.icd_pcd_cd_ver = ipd.icd_ver
  LEFT OUTER JOIN dgns_ccs_dim cdm ON cdm.dgns_cd = dd.dgns_alt_cd AND cdm.dgns_cd_ver = dd.dgns_icd_ver
  LEFT OUTER JOIN ahrq_val_set_dim ie1 ON ie1.cd = cpm.icd_pcd_ccs_cgy_cd AND ie1.cohrt_id = 'Readmission - PR.1'
  LEFT OUTER JOIN ahrq_val_set_dim ie2
    ON ie2.cd = cdm.ccs_dgns_cgy_cd AND ie2.cohrt_id = 'Readmission - PR.2' AND ie2.cd_dmn_nm = 'CCS Diagnosis'
  LEFT OUTER JOIN ahrq_val_set_dim ie3
    ON ie3.cd = cdm.dgns_cd AND ie3.cohrt_id = 'Readmission - PR.2' AND ie3.cd_dmn_nm = 'ICD 10 Diagnosis' AND
       cdm.dgns_cd_ver = '10'
  LEFT OUTER JOIN ahrq_val_set_dim ie4
    ON ie4.cd = cpm.icd_pcd_ccs_cgy_cd AND ie4.cohrt_id = 'Readmission - PR.3' AND
       ie4.cd_dmn_nm = 'CCS Procedure'
  LEFT OUTER JOIN ahrq_val_set_dim ie5
    ON ie5.cd = cpm.icd_pcd_cd AND ie5.cohrt_id = 'Readmission - PR.3' AND ie5.cd_dmn_nm = 'ICD 10 Procedure'
       AND cpm.icd_pcd_cd_ver = '10'
  LEFT OUTER JOIN ahrq_val_set_dim ie6
    ON ie6.cd = cdm.ccs_dgns_cgy_cd AND ie6.cohrt_id = 'Readmission - PR.4' AND ie6.cd_dmn_nm = 'CCS Diagnosis'
  LEFT OUTER JOIN ahrq_val_set_dim ie7
    ON ie7.cd = cdm.dgns_cd AND ie7.cohrt_id = 'Readmission - PR.4' AND ie7.cd_dmn_nm = 'ICD 10 Diagnosis' AND
       cdm.dgns_cd_ver = '10'
WHERE a.rnk = 1;

DELETE FROM clm_readm_anl_fct WHERE idnx_adm_type_cd = 'SNF';
INSERT INTO clm_readm_anl_fct

/*SELECT * FROM pac_readm
UNION ALL
SELECT
  string_to_int(substr(RAWTOHEX(hash(clm_id, 0)), 17), 16) AS clm_readm_anl_sk,
  pln_mbr_sk,
  clm_id,
  NULL AS readm_clm_id,
  dschrg_sts_sk,
  prim_dgns_sk AS dgns_sk,
  ccn_sk,
  NULL AS readm_dgns_sk,
  prim_icd_pcd_sk AS icd_pcd_sk,
  NULL AS readm_icd_pcd_sk,
  NULL AS readm_ccn_sk,
  cst_modl_sk,
  NULL AS readm_cst_modl_sk,
  NULL AS readm_svc_fm_dt,
  NULL AS readm_svc_to_dt,
  NULL AS readm_type_cd,
  NULL AS readm_type_descr,
  'SNF' AS idnx_adm_type_cd,
  'Skilled Nursing Facility' AS idnx_adm_type_descr,
  svc_fm_dt,
  svc_to_dt,
  NULL AS readm_paid_amt,
  paid_amt,
  NULL AS frst_pln_readm_ind,
  NULL AS sec_pln_readm_ind,
  NULL AS third_pln_readm_ind,
  NULL AS fourth_pln_readm_ind,
  NULL AS pln_ind,
  NULL AS unpln_ind,
  NULL AS cohrt_nm,
  NULL AS day_to_readm_cnt,
  NULL AS readm_los_cnt,
  los AS los_cnt,
    1                                                                         AS clm_readm_anl_fct_cnt,
  NOW()                                                                       AS vld_fm_ts,
  'QE16'                                                                      AS pce_cst_cd,
  'MILLIMAN'                                                                  AS pce_cst_src_nm
FROM pac_admissions WHERE clm_id NOT IN (SELECT DISTINCT clm_id FROM pac_readm);*/

----------------------------------------------------------------------
---newlogic to stamp readmission indicators on LAST claim of the SNF---
-----------------------------------------------------------------------
with dschrg_clm_id as
(
select * from 
(
	select clm_id as dschrg_clm_id, fcy_case_id, RANK()
    		OVER (
      	PARTITION BY fcy_case_id ORDER BY svc_to_dt,clm_id desc) rnk
	from clm_line_fct cf
		INNER JOIN dschrg_sts_dim dsd ON dsd.dschrg_sts_sk = cf.dschrg_sts_sk
		INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
where cd.care_svc_sub_cgy_nm = 'Skilled Nursing Facility' and cf.clm_line_num='001' 
	and dsd.snf_readm_excl_ind is null
)a
where a.rnk=1
)

SELECT string_to_int(substr(RAWTOHEX(hash(d.dschrg_clm_id, 0)), 17), 16) AS clm_readm_anl_sk
       , pln_mbr_sk
       , d.dschrg_clm_id as clm_id
       , readm_clm_id
       , dschrg_sts_sk
       , dgns_sk
       , ccn_sk
       , readm_dgns_sk
       , icd_pcd_sk
       , readm_icd_pcd_sk
       , readm_ccn_sk
       , cst_modl_sk
       , readm_cst_modl_sk
       , readm_svc_fm_dt
       , readm_svc_to_dt
       , readm_type_cd
       , readm_type_descr
       , idnx_adm_type_cd
       , idnx_adm_type_descr
       , svc_fm_dt
       , svc_to_dt
       , readm_paid_amt
       , paid_amt
       , frst_pln_readm_ind
       , sec_pln_readm_ind
       , third_pln_readm_ind
       , fourth_pln_readm_ind
       , pln_ind
       , unpln_ind
       , cohrt_nm
       , days_to_readm
       , readm_los_cnt
       , los_cnt
       , clm_readm_anl_fct_cnt
       , vld_fm_ts
       , pce_cst_cd
       , pce_cst_src_nm

  FROM pac_readm p
  	inner join dschrg_clm_id d on p.fcy_case_id=d.fcy_case_id

UNION 

SELECT
  string_to_int(substr(RAWTOHEX(hash(dcd.dschrg_clm_id, 0)), 17), 16) AS clm_readm_anl_sk,
  pa.pln_mbr_sk,
  dcd.dschrg_clm_id as clm_id,
  NULL AS readm_clm_id,
  pa.dschrg_sts_sk,
  pa.prim_dgns_sk AS dgns_sk,
  pa.ccn_sk,
  NULL AS readm_dgns_sk,
  pa.prim_icd_pcd_sk AS icd_pcd_sk,
  NULL AS readm_icd_pcd_sk,
  NULL AS readm_ccn_sk,
  pa.cst_modl_sk,
  NULL AS readm_cst_modl_sk,
  NULL AS readm_svc_fm_dt,
  NULL AS readm_svc_to_dt,
  NULL AS readm_type_cd,
  NULL AS readm_type_descr,
  'SNF' AS idnx_adm_type_cd,
  'Skilled Nursing Facility' AS idnx_adm_type_descr,
   pa.svc_fm_dt,
   pa.svc_to_dt,
  NULL AS readm_paid_amt,
  pa.paid_amt,
  NULL AS frst_pln_readm_ind,
  NULL AS sec_pln_readm_ind,
  NULL AS third_pln_readm_ind,
  NULL AS fourth_pln_readm_ind,
  NULL AS pln_ind,
  NULL AS unpln_ind,
  NULL AS cohrt_nm,
  NULL AS day_to_readm_cnt,
  NULL AS readm_los_cnt,
  pa.los AS los_cnt,
    1                                                                         AS clm_readm_anl_fct_cnt,
  NOW()                                                                       AS vld_fm_ts,
  'QE16'                                                                      AS pce_cst_cd,
  'MILLIMAN'                                                                  AS pce_cst_src_nm
FROM --pac_admissions pa
	index_admissions pa
	inner join dschrg_clm_id dcd on pa.fcy_case_id=dcd.fcy_case_id
	left join pac_readm pr on pa.clm_id=pr.clm_id 
WHERE pr.clm_id is null 
;

COMMIT;
\unset ON_ERROR_STOP
