\set ON_ERROR_STOP ON;

CREATE TEMP TABLE end_of_life_smy_fct_tmp1 AS
(
  SELECT
    cf.pln_mbr_sk,
    min(cf.svc_to_dt) AS mn_dt,
    dsd.dschrg_sts_descr,
    1                 AS fIP_Decedent_ind
  FROM clm_line_fct cf
    INNER JOIN dschrg_sts_dim dsd ON cf.dschrg_sts_sk = dsd.dschrg_sts_sk
    INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
    INNER JOIN rev_cl_dim rcd ON cf.rev_cl_sk = rcd.rev_cl_sk
    RIGHT OUTER JOIN pln_mbr_asgnt_dim pmad
      ON cf.pln_mbr_sk = pmad.pln_mbr_sk AND cf.svc_to_dt BETWEEN pmad.asgnt_wndw_strt_dt AND pmad.asgnt_wndw_end_dt
    INNER JOIN assgn_vw asgn ON pmad.pln_mbr_sk = asgn.pln_mbr_sk
  WHERE
    dsd.dschrg_sts_cd IN ('20', '40', '41', '42')
    AND rcd.rev_cd NOT IN ('0813', '0816', '0819', '0810', '0817', '0815', '0812', '0814')
    AND asgn.q2_assign = 1
    AND cd.cst_modl_line_cgy_nm = 'FIP'
    AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
    AND cf.pln_mbr_sk NOT IN (SELECT cf2.pln_mbr_sk
                              FROM clm_line_fct cf2
                                INNER JOIN cst_modl_dim cd2 ON cf2.cst_modl_sk = cd2.cst_modl_sk
                                INNER JOIN dschrg_sts_dim dsd2 ON cf2.dschrg_sts_sk = dsd2.dschrg_sts_sk
                              WHERE (cd2.cst_modl_line_cgy_nm = 'SNF' OR
                                     cd2.cst_modl_line_descr IN ('OTH Hospice', 'OTH Home Health'))
                                    AND dsd2.dschrg_sts_cd IN ('20', '40', '41', '42')
                              GROUP BY cf2.pln_mbr_sk)
  GROUP BY cf.pln_mbr_sk, dsd.dschrg_sts_descr
);

--SNF, Homehealth Decedents
CREATE TEMP TABLE end_of_life_smy_fct_tmp2 AS
(
  SELECT
    cf.pln_mbr_sk,
    min(cf.svc_to_dt) AS mn_dt,
    dsd.dschrg_sts_descr,
    max(CASE WHEN (cd.cst_modl_line_cgy_nm = 'SNF')
      THEN 1
        ELSE 0 END)   AS SNF_DECEDENT_IND,
    max(CASE WHEN (cd.cst_modl_line_descr = 'OTH Home Health')
      THEN 1
        ELSE 0 END)   AS HM_HLT_DECEDENT_IND
  FROM clm_line_fct cf
    INNER JOIN dschrg_sts_dim dsd ON cf.dschrg_sts_sk = dsd.dschrg_sts_sk
    INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
    INNER JOIN rev_cl_dim rcd ON cf.rev_cl_sk = rcd.rev_cl_sk
    RIGHT OUTER JOIN pln_mbr_asgnt_dim pmad
      ON cf.pln_mbr_sk = pmad.pln_mbr_sk AND cf.svc_to_dt BETWEEN pmad.asgnt_wndw_strt_dt AND pmad.asgnt_wndw_end_dt
    INNER JOIN assgn_vw asgn ON pmad.pln_mbr_sk = asgn.pln_mbr_sk
  WHERE dsd.dschrg_sts_cd IN ('20', '40', '41', '42')
        AND rcd.rev_cd NOT IN ('0813', '0816', '0819', '0810', '0817', '0815', '0812', '0814')
        AND asgn.q2_assign = 1
  GROUP BY cf.pln_mbr_sk, dsd.dschrg_sts_descr
);

create temp table end_of_life_smy_fct_tmp3 as
(
  SELECT
    q.pln_mbr_sk,
    q.mn_dt,
    sum(cf2.cst_modl_utlz_cnt) AS util,
    cf2.cst_modl_utlz_type_cd,
    (CASE WHEN (cst_modl_utlz_type_cd = 'Visits' AND util < 3)
      THEN 1
     ELSE 0 END)               AS hspcL3_decedent_IND,
    (CASE WHEN (cst_modl_utlz_type_cd = 'Visits' AND util >= 3)
      THEN 1
     ELSE 0 END)               AS hspcG3_decedent_IND
  FROM
    (SELECT
       pmad.pln_mbr_sk,
       min(cf.svc_to_dt)                          AS mn_dt,
       DATE_PART('month', pmad.asgnt_wndw_end_dt) AS elig_mn,
       DATE_PART('year', pmad.asgnt_wndw_end_dt)  AS elig_yr,
       dsd.dschrg_sts_descr
     FROM clm_line_fct cf
       INNER JOIN dschrg_sts_dim dsd ON cf.dschrg_sts_sk = dsd.dschrg_sts_sk
       INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
       INNER JOIN rev_cl_dim rcd ON cf.rev_cl_sk = rcd.rev_cl_sk
       RIGHT OUTER JOIN pln_mbr_asgnt_dim pmad
         ON cf.pln_mbr_sk = pmad.pln_mbr_sk AND cf.svc_to_dt BETWEEN pmad.asgnt_wndw_strt_dt AND pmad.asgnt_wndw_end_dt
       INNER JOIN assgn_vw asgn ON pmad.pln_mbr_sk = asgn.pln_mbr_sk
     WHERE dsd.dschrg_sts_cd IN ('20', '40', '41', '42')
           AND rcd.rev_cd NOT IN ('0813', '0816', '0819', '0810', '0817', '0815', '0812', '0814')
           AND asgn.q2_assign = 1 AND cd.cst_modl_line_descr = 'OTH Hospice'
     GROUP BY pmad.pln_mbr_sk, dsd.dschrg_sts_descr, elig_mn, elig_yr) AS q
    INNER JOIN clm_line_fct cf2 ON q.pln_mbr_sk = cf2.pln_mbr_sk AND q.elig_mn = DATE_PART('month', cf2.svc_to_dt) AND
                                   q.elig_yr = DATE_PART('year', cf2.svc_to_dt)
    INNER JOIN cst_modl_dim cd2 ON cf2.cst_modl_sk = cd2.cst_modl_sk
    INNER JOIN assgn_vw asgn ON cf2.pln_mbr_sk = asgn.pln_mbr_sk

  WHERE asgn.q2_assign = 1 AND cd2.cst_modl_line_descr = 'OTH Hospice'
  GROUP BY q.pln_mbr_sk, q.mn_dt, cf2.cst_modl_utlz_type_cd
);

--Total Population
CREATE TEMP TABLE end_of_life_smy_fct_tmp4 AS
(
  SELECT count(DISTINCT pmad.pln_mbr_sk) AS tot_ppn
  FROM pln_mbr_asgnt_dim pmad
    INNER JOIN assgn_vw asgn ON asgn.pln_mbr_sk = pmad.pln_mbr_sk
  WHERE asgn.q2_assign = 1
);

DELETE FROM end_of_life_utlz_fct;
--Creating the End of Life Summary Table based on the Temp Tables Created Above
	INSERT INTO end_of_life_utlz_fct
SELECT
  string_to_int(substr(RAWTOHEX(hash(mbr_id_num, 0)), 17), 16) AS end_of_life_utlz_fct_sk,
  pln_mbr_sk,
  elig_sts,
  NULL,
  t.last_svc_dt,
    SUM(CASE WHEN (t.SVC_DAYS <= 29 AND t.SVC_DAYS >= 0)
    THEN t.hm_hlt_PAID_AMT
      ELSE 0 END)            AS hm_hlt_paid_amt,
    SUM(CASE WHEN (t.SVC_DAYS <= 29 AND t.SVC_DAYS >= 0)
    THEN t.hspc_PAID_AMT
      ELSE 0 END)            AS hspc_paid_amt,
  SUM(CASE WHEN (t.SVC_DAYS <= 29 AND t.SVC_DAYS >= 0)
    THEN t.IP_PAID_AMT
      ELSE 0 END)            AS ip_paid_amt,
  SUM(CASE WHEN (t.SVC_DAYS <= 29 AND t.SVC_DAYS >= 0)
    THEN t.offc_PAID_AMT
      ELSE 0 END)            AS offc_paid_amt,
  SUM(CASE WHEN (t.SVC_DAYS <= 29 AND t.SVC_DAYS >= 0)
    THEN t.op_PAID_AMT
      ELSE 0 END)            AS op_paid_amt,
    SUM(CASE WHEN (t.SVC_DAYS <= 29 AND t.SVC_DAYS >= 0)
    THEN t.othr_PAID_AMT
      ELSE 0 END)            AS othr_paid_amt,
    SUM(CASE WHEN (t.SVC_DAYS <= 29 AND t.SVC_DAYS >= 0)
    THEN t.tot_paid_amt
      ELSE 0 END)            AS tot_paid_amt,
  SUM(CASE WHEN (t.SVC_DAYS <= 29 AND t.SVC_DAYS >= 0)
    THEN t.snf_PAID_AMT
      ELSE 0 END)            AS snf_paid_amt,
    max(t.fip_decedent_ind)    AS fip_decedent_ind,
  max(t.hm_hlt_decedent_ind) AS hm_hlt_decedent_ind,
  max(t.hspcg3_decedent_ind) AS hspcg3_decedent_ind,
  max(t.hspcl3_decedent_ind) AS hspcl3_decedent_ind,
  max(t.snf_decedent_ind)    AS snf_decedent_ind,
  max(t.tot_ppn)             AS tot_ppn,
  SUM(CASE WHEN (t.SVC_DAYS <= 29 AND t.SVC_DAYS >= 0)
    THEN t.ed_vsts
      ELSE NULL END)         AS ed_vst_cnt,
  1                                                                           AS end_of_life_smy_fct_cnt,
  NOW()                                                                       AS vld_fm_ts,
  'QE16'                                                                      AS pce_cst_cd,
  'MILLIMAN'                                                                  AS pce_cst_src_nm

FROM
  (SELECT
     pmd.mbr_id_num,
      pmad.elig_sts,
      pmd.brth_dt,
    pmd.gnd_descr,
    cf.pln_mbr_sk,
     cf.svc_to_dt,
     eolst2.mn_dt                AS last_svc_dt,
     eolst2.mn_dt - CF.SVC_TO_DT AS SVC_Days,
     eolst2.SNF_DECEDENT_IND,
     eolst2.HM_HLT_DECEDENT_IND,
     eolst1.FIP_Decedent_ind,
     eolst3.hspcL3_decedent_ind,
     eolst3.hspcg3_decedent_ind,
     eolst4.tot_ppn,
     max(CASE WHEN cd.cst_modl_line_descr IN
                   ('FOP Emergency room – hospital', 'FOP Emergency room – Urgent care', 'PROF ER Visits and Observation Care')
       THEN cf.cst_modl_utlz_cnt
         ELSE NULL END)          AS ed_vsts,
     sum(cf.paid_amt)            AS tot_paid_amt,
     sum(CASE WHEN (cd.care_setting_cgy_nm = 'Hospital Inpatient (facility and professional)')
       THEN cf.paid_amt
         ELSE NULL END)          AS ip_paid_amt,
     sum(CASE WHEN (cd.care_setting_cgy_nm = 'Professional Office/Other')
       THEN cf.paid_amt
         ELSE NULL END)          AS Offc_Paid_Amt,
     sum(CASE WHEN (cd.care_setting_cgy_nm = 'Outpatient (facility and professional)')
       THEN cf.paid_amt
         ELSE NULL END)          AS op_Paid_Amt,
     sum(CASE WHEN (cd.care_setting_cgy_nm = 'Skilled Nursing Facility')
       THEN cf.paid_amt
         ELSE NULL END)          AS snf_Paid_Amt,
     sum(CASE WHEN (cd.care_setting_cgy_nm = 'Home Health')
       THEN cf.paid_amt
         ELSE NULL END)          AS hm_hlt_Paid_Amt,
     sum(CASE WHEN (cd.care_setting_cgy_nm = 'Hospice')
       THEN cf.paid_amt
         ELSE NULL END)          AS hspc_Paid_Amt,
     sum(CASE WHEN (cd.care_setting_cgy_nm = 'Other')
       THEN cf.paid_amt
         ELSE NULL END)          AS othr_Paid_Amt

   FROM clm_line_fct cf
     INNER JOIN dschrg_sts_dim dsd ON cf.dschrg_sts_sk = dsd.dschrg_sts_sk
  INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
  INNER JOIN pln_mbr_dim pmd ON cf.pln_mbr_sk = pmd.pln_mbr_sk
  INNER JOIN pln_mbr_asgnt_dim pmad ON cf.pln_mbr_sk = pmad.pln_mbr_sk AND cf.svc_to_dt BETWEEN pmad.asgnt_wndw_strt_dt AND pmad.asgnt_wndw_end_dt
  INNER JOIN assgn_vw asgn ON cf.pln_mbr_sk = asgn.pln_mbr_sk
  INNER JOIN end_of_life_smy_fct_tmp2 eolst2 ON cf.pln_mbr_sk = eolst2.pln_mbr_sk
  LEFT JOIN end_of_life_smy_fct_tmp1 eolst1 ON cf.pln_mbr_sk = eolst1.pln_mbr_sk AND cf.svc_to_dt = eolst1.mn_dt
  LEFT JOIN end_of_life_smy_fct_tmp3 eolst3 ON cf.pln_mbr_sk = eolst3.pln_mbr_sk AND cf.svc_to_dt = eolst3.mn_dt
  CROSS JOIN end_of_life_smy_fct_tmp4 eolst4
  WHERE asgn.q2_assign = 1
  GROUP BY pmd.mbr_id_num, cf.pln_mbr_sk, pmad.elig_sts, pmd.brth_dt, pmd.gnd_descr, cf.svc_to_dt, eolst2.mn_dt, SVC_Days,eolst2.SNF_DECEDENT_IND,
  eolst2.HM_HLT_DECEDENT_IND,eolst1.FIP_Decedent_ind,eolst3.hspcL3_decedent_ind,eolst3.hspcg3_decedent_ind,eolst4.tot_ppn
  ) AS t

GROUP BY t.mbr_id_num,
  pln_mbr_sk,
  t.elig_sts,
  t.last_svc_dt,
  t.brth_dt,
  t.gnd_descr;

\unset ON_ERROR_STOP
