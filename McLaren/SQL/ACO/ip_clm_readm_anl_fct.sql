\set ON_ERROR_STOP ON;

BEGIN;
-- This table lists all inpatient admissions
CREATE TEMP TABLE hwr_admissions AS
  SELECT
    cf.pln_mbr_sk,
    cf.clm_id,
    prim_dgns_sk,
    prim_icd_pcd_sk,
    dschrg_sts_sk,
    ms_drg_sk,
    cf.cst_modl_sk,
    ccn_sk,
    min(cf.svc_fm_dt)                       svc_fm_dt,
    max(cf.svc_to_dt)                       svc_to_dt,
    sum(cf.paid_amt)                        paid_amt,
    sum(cf.cst_modl_in_ptnt_clm_adm_ind) AS adm_ind,
    SUM(cf.cst_modl_day_cnt) AS los
  FROM clm_line_fct cf
    INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
  --  INNER JOIN assgn_vw av ON cf.pln_mbr_sk = av.pln_mbr_sk
  WHERE cd.cst_modl_line_cgy_nm = 'FIP'
  GROUP BY cf.pln_mbr_sk, cf.clm_id, prim_dgns_sk, prim_icd_pcd_sk, dschrg_sts_sk, ms_drg_sk, cf.cst_modl_sk, ccn_sk
  HAVING adm_ind > 0;

--0064470559588PTA

-- Finds all admissions that meet the inclusion/exclusion requirements to be an index admission in the HWR measure
CREATE TEMP TABLE hwr_index_admissions AS
  SELECT
    pln_mbr_sk,
    clm_id,
    prim_dgns_sk,
    prim_icd_pcd_sk,
    hwra.dschrg_sts_sk,
    ms_drg_sk,
    ccn_sk,
    cst_modl_sk,
    svc_fm_dt,
    svc_to_dt,
    paid_amt,
    los,
    CASE WHEN iedm2.cd_descr IS NOT NULL
      THEN 1
    ELSE NULL END AS surgy_gyn_cohort_ind,
    CASE WHEN iedm2.cd_descr IS NULL AND iedm4.cd_descr IS NOT NULL
      THEN 1
    ELSE NULL END AS cardiorespiratory_cohort_ind,
    CASE WHEN iedm2.cd_descr IS NULL AND iedm5.cd_descr IS NOT NULL
      THEN 1
    ELSE NULL END AS cardiovascular_cohort_ind,
    CASE WHEN iedm2.cd_descr IS NULL AND iedm6.cd_descr IS NOT NULL
      THEN 1
    ELSE NULL END AS neurology_cohort_ind,
    CASE WHEN iedm2.cd_descr IS NULL AND iedm7.cd_descr IS NOT NULL
      THEN 1
    ELSE NULL END AS medicine_cohort_ind
  FROM hwr_admissions hwra
    LEFT OUTER JOIN dgns_dim dd ON hwra.prim_dgns_sk = dd.dgns_sk
    LEFT OUTER JOIN dschrg_sts_dim dsd ON hwra.dschrg_sts_sk = dsd.dschrg_sts_sk
    LEFT OUTER JOIN icd_pcd_dim ipd ON hwra.prim_icd_pcd_sk = ipd.icd_pcd_sk
    LEFT OUTER JOIN icd_pcd_ccs_dim cpm ON cpm.icd_pcd_cd = ipd.icd_pcd_alt_cd AND cpm.icd_pcd_cd_ver = ipd.icd_ver
    LEFT OUTER JOIN dgns_ccs_dim cdm ON cdm.dgns_cd = dd.dgns_alt_cd AND cdm.dgns_cd_ver = dd.dgns_icd_ver
    LEFT OUTER JOIN ahrq_val_set_dim iedm2 ON iedm2.cd = cpm.icd_pcd_ccs_cgy_cd AND iedm2.cohrt_id = 'Readmission - D.2'
    LEFT OUTER JOIN ahrq_val_set_dim iedm3 ON iedm3.cd = cdm.ccs_dgns_cgy_cd AND iedm3.cohrt_id = 'Readmission - D.3'
    LEFT OUTER JOIN ahrq_val_set_dim iedm4 ON iedm4.cd = cdm.ccs_dgns_cgy_cd AND iedm4.cohrt_id = 'Readmission - D.4'
    LEFT OUTER JOIN ahrq_val_set_dim iedm5 ON iedm5.cd = cdm.ccs_dgns_cgy_cd AND iedm5.cohrt_id = 'Readmission - D.5'
    LEFT OUTER JOIN ahrq_val_set_dim iedm6 ON iedm6.cd = cdm.ccs_dgns_cgy_cd AND iedm6.cohrt_id = 'Readmission - D.6'
    LEFT OUTER JOIN ahrq_val_set_dim iedm7 ON iedm7.cd = cdm.ccs_dgns_cgy_cd AND iedm7.cohrt_id = 'Readmission - D.7'
  WHERE (iedm2.cd_descr IS NOT NULL OR iedm2.cd_descr IS NULL AND iedm3.cd_descr IS NULL)
        AND cdm.ccs_dgns_cgy_cd NOT IN (SELECT ie1.cd
                                        FROM ahrq_val_set_dim ie1
                                        WHERE ie1.cohrt_id = 'Readmission - D.1')
        AND ccs_dgns_cgy_cd != '254'
        AND dsd.dschrg_sts_cd NOT IN (SELECT iedm4.cd
                                      FROM ahrq_val_set_dim iedm4
                                      WHERE iedm4.cohrt_id = 'Readmission Exclude');

DELETE FROM clm_readm_anl_fct WHERE idnx_adm_type_cd = 'IP';
INSERT INTO clm_readm_anl_fct
  SELECT
    string_to_int(substr(RAWTOHEX(hash(clm_id, 0)), 17), 16) AS clm_readm_anl_sk,
    pmd.pln_mbr_sk,
    clm_id,
    readm_clm_id,
    idsd.dschrg_sts_sk,
    idd.dgns_sk,
    a.ccn_sk,
    readm_prim_dgns_sk AS readm_dgns_sk,
    iipd.icd_pcd_sk,
    readm_prim_icd_pcd_sk AS readm_icd_pcd_sk,
    readm_ccn_sk,
    a.cst_modl_sk,
    readm_cst_modl_sk,
    readm_svc_fm_dt,
    readm_svc_to_dt,
    'IP' AS readm_type_cd,
    'Inpatient' AS readm_type_descr,
    'IP' AS idnx_adm_type_cd,
    'Inpatient' AS idnx_adm_type_descr,
    svc_fm_dt,
    svc_to_dt,
    readm_paid_amt,
    paid_amt,
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
    CASE WHEN cardiorespiratory_cohort_ind = 1 THEN 'Cardiorespiratory'
      WHEN cardiovascular_cohort_ind = 1 THEN 'Cardiovascular'
        WHEN medicine_cohort_ind = 1 THEN 'Medicine'
          WHEN neurology_cohort_ind = 1 THEN 'Neurology'
    WHEN surgy_gyn_cohort_ind = 1 THEN 'Surgery/Gynecology' END AS cohrt_nm,
    readm_svc_fm_dt - svc_to_dt AS day_to_readm_cnt,
    readm_los AS readm_los_cnt,
    los AS los_cnt,
    1                                                                         AS clm_readm_anl_fct_cnt,
  NOW()                                                                       AS vld_fm_ts,
  'QE16'                                                                      AS pce_cst_cd,
  'MILLIMAN'                                                                  AS pce_cst_src_nm
  FROM (
         -- Subquery that finds all eligible index admissions and readmission pairs and ranks the readmissions to only take the first one
         SELECT
           ia.pln_mbr_sk,
           ia.clm_id,
           ia.svc_fm_dt,
           ia.svc_to_dt,
           ia.prim_dgns_sk,
           ia.prim_icd_pcd_sk,
           ia.dschrg_sts_sk,
           ia.ms_drg_sk,
           ia.ccn_sk,
           ia.cst_modl_sk,
           ia.surgy_gyn_cohort_ind,
           ia.cardiorespiratory_cohort_ind,
           ia.cardiovascular_cohort_ind,
           ia.neurology_cohort_ind,
           ia.medicine_cohort_ind,
           ia.paid_amt,
           ia.los,
           readm.clm_id                 AS readm_clm_id,
           readm.svc_fm_dt              AS readm_svc_fm_dt,
           readm.svc_to_dt              AS readm_svc_to_dt,
           readm.prim_dgns_sk           AS readm_prim_dgns_sk,
           readm.prim_icd_pcd_sk        AS readm_prim_icd_pcd_sk,
           readm.dschrg_sts_sk          AS readm_dschrg_sts_sk,
           readm.ms_drg_sk              AS readm_ms_drg_sk,
           readm.ccn_sk                 AS readm_ccn_sk,
           readm.cst_modl_sk            AS readm_cst_modl_sk,
           readm.paid_amt               AS readm_paid_amt,
           readm.los                    AS readm_los,
           RANK()
           OVER (
             PARTITION BY ia.clm_id
             ORDER BY readm.svc_fm_dt , readm.svc_to_dt ) AS rnk
         FROM hwr_index_admissions ia
           LEFT OUTER JOIN hwr_admissions readm ON ia.clm_id != readm.clm_id AND ia.pln_mbr_sk = readm.pln_mbr_sk AND readm.svc_fm_dt > ia.svc_to_dt
--                AND ia.svc_to_dt
--          BETWEEN (SELECT dt
--                FROM dt_meta
--                WHERE descr = 'roll_yr_strt') AND (SELECT dt
--                                    FROM dt_meta
--                                    WHERE descr = 'roll_yr_end')
       ) a
    INNER JOIN pln_mbr_dim pmd ON a.pln_mbr_sk = pmd.pln_mbr_sk
    INNER JOIN pln_mbr_asgnt_dim pmad ON pmad.pln_mbr_sk = a.pln_mbr_sk AND a.svc_to_dt BETWEEN pmad.asgnt_wndw_strt_dt AND pmad.asgnt_wndw_end_dt
    LEFT OUTER JOIN ccn_dim iccnd ON iccnd.ccn_sk = a.ccn_sk
    LEFT OUTER JOIN dschrg_sts_dim idsd ON idsd.dschrg_sts_sk = a.dschrg_sts_sk
    LEFT OUTER JOIN ms_drg_dim imdd ON imdd.ms_drg_sk = a.ms_drg_sk
    LEFT OUTER JOIN dgns_dim idd ON a.prim_dgns_sk = idd.dgns_sk
    LEFT OUTER JOIN icd_pcd_dim iipd ON a.prim_icd_pcd_sk = iipd.icd_pcd_sk
    LEFT OUTER JOIN icd_pcd_ccs_dim icpm ON icpm.icd_pcd_cd = iipd.icd_pcd_alt_cd AND icpm.icd_pcd_cd_ver = iipd.icd_ver
    LEFT OUTER JOIN dgns_ccs_dim icdm ON icdm.dgns_cd = idd.dgns_alt_cd AND icdm.dgns_cd_ver = idd.dgns_icd_ver
    LEFT OUTER JOIN dgns_dim rdd ON a.readm_prim_dgns_sk = rdd.dgns_sk
    LEFT OUTER JOIN icd_pcd_dim ripd ON a.readm_prim_icd_pcd_sk = ripd.icd_pcd_sk
    LEFT OUTER JOIN icd_pcd_ccs_dim rcpm ON rcpm.icd_pcd_cd = ripd.icd_pcd_alt_cd AND rcpm.icd_pcd_cd_ver = ripd.icd_ver
    LEFT OUTER JOIN dgns_ccs_dim rcdm ON rcdm.dgns_cd = rdd.dgns_alt_cd AND rcdm.dgns_cd_ver = rdd.dgns_icd_ver
    LEFT OUTER JOIN ahrq_val_set_dim ie1 ON ie1.cd = rcpm.icd_pcd_ccs_cgy_cd AND ie1.cohrt_id = 'Readmission - PR.1'
    LEFT OUTER JOIN ahrq_val_set_dim ie2 ON ie2.cd = rcdm.ccs_dgns_cgy_cd AND ie2.cohrt_id = 'Readmission - PR.2' AND ie2.cd_dmn_nm = 'CCS Diagnosis'
    LEFT OUTER JOIN ahrq_val_set_dim ie3 ON ie3.cd = rcdm.dgns_cd AND ie3.cohrt_id = 'Readmission - PR.2' AND ie3.cd_dmn_nm = 'ICD 10 Diagnosis' AND rcdm.dgns_cd_ver = '10'
    LEFT OUTER JOIN ahrq_val_set_dim ie4 ON ie4.cd = rcpm.icd_pcd_ccs_cgy_cd AND ie4.cohrt_id = 'Readmission - PR.3' AND ie4.cd_dmn_nm = 'CCS Procedure'
    LEFT OUTER JOIN ahrq_val_set_dim ie5
      ON ie5.cd = rcpm.icd_pcd_cd AND ie5.cohrt_id = 'Readmission - PR.3' AND ie5.cd_dmn_nm = 'ICD 10 Procedure' AND rcpm.icd_pcd_cd_ver = '10'
    LEFT OUTER JOIN ahrq_val_set_dim ie6 ON ie6.cd = rcdm.ccs_dgns_cgy_cd AND ie6.cohrt_id = 'Readmission - PR.4' AND ie6.cd_dmn_nm = 'CCS Diagnosis'
    LEFT OUTER JOIN ahrq_val_set_dim ie7 ON ie7.cd = rcdm.dgns_cd AND ie7.cohrt_id = 'Readmission - PR.4' AND ie7.cd_dmn_nm = 'ICD 10 Diagnosis' AND rcdm.dgns_cd_ver = '10'
  WHERE a.rnk = 1;
COMMIT;
\unset ON_ERROR_STOP
