\set ON_ERROR_STOP ON;

--Admits for Asthma in Younger Adults
CREATE TEMP TABLE ambul_snsv_cdtn_fct_tmp1 AS
  (SELECT
     cf.pln_mbr_sk,
     cf.clm_id,
     cf.svc_to_dt,
     1 AS asthma_adm_ind
   FROM clm_line_fct cf
     INNER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id = 'ACSASTD'
     LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
	 LEFT OUTER JOIN (
       SELECT cdf.clm_id, 'x' Excl_ind
       FROM clm_dgns_fct cdf
         INNER JOIN dgns_dim dd2 ON dd2.dgns_sk = cdf.dgns_sk
         INNER JOIN ahrq_val_set_dim vsd2 ON dd2.dgns_alt_cd = vsd2.cd
       WHERE vsd2.cohrt_id = 'RESPAN'
       GROUP BY cdf.clm_id)x
	  on cf.clm_id=x.clm_id
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
     AND x.excl_ind IS NULL
   GROUP BY cf.pln_mbr_sk, cf.clm_id, cf.svc_to_dt);


-- Admits for Bacterial Pneumonia
CREATE TEMP TABLE ambul_snsv_cdtn_fct_tmp2 AS
  (SELECT
     cf.pln_mbr_sk,
     cf.clm_id,
     cf.svc_to_dt,
     1 AS bacterial_pneu_adm_ind
   FROM clm_line_fct cf
     INNER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id = 'ACSBACD'
     LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
	 LEFT OUTER JOIN 
	 (SELECT cdf.clm_id, 'X' excl_ind
       FROM clm_dgns_fct cdf
         INNER JOIN dgns_dim dd2 ON dd2.dgns_sk = cdf.dgns_sk
         INNER JOIN ahrq_val_set_dim vsd2 ON dd2.dgns_alt_cd = vsd2.cd AND vsd2.cohrt_id in ('ACSBA2D','IMMUNID')
       GROUP BY cdf.clm_id)X on cf.clm_id=X.clm_id
	 LEFT OUTER JOIN
	 (SELECT cpf.clm_id, 'Y' proc_excl_ind
       FROM clm_pcd_fct cpf
         INNER JOIN icd_pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
         INNER JOIN ahrq_val_set_dim vsd3 ON ipd.icd_pcd_alt_cd = vsd3.cd AND vsd3.cohrt_id in ('IMMUNIP')
       GROUP BY cpf.clm_id)Y on cf.clm_id=Y.clm_id
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
	 AND X.excl_ind is null
     AND Y.proc_excl_ind is null
     GROUP BY cf.pln_mbr_sk, cf.clm_id, cf.svc_to_dt);

--Admits for COPD
CREATE TEMP TABLE ambul_snsv_cdtn_fct_tmp3 AS
  (SELECT
	 cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 'COPD_adm_ind' as msr_nm,
     1 AS COPD_adm_ind
   FROM clm_line_fct cf
     INNER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id in ('ACCOPDD','ACSASTD')
     LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
	 LEFT OUTER JOIN 
	 (SELECT cdf.clm_id, 'x' excl_ind
       FROM clm_dgns_fct cdf
         INNER JOIN dgns_dim dd2 ON dd2.dgns_sk = cdf.dgns_sk
         INNER JOIN ahrq_val_set_dim vsd2 ON dd2.dgns_alt_cd = vsd2.cd AND vsd2.cohrt_id='RESPAN'
       GROUP BY cdf.clm_id)x
	 on cf.clm_id=x.clm_id
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
     AND x.excl_ind is null
   GROUP BY cf.pln_mbr_sk,cf.mbr_id_num, cf.clm_id, cf.svc_to_dt);

-- Admits for Dehydration
CREATE TEMP TABLE ambul_snsv_cdtn_fct_tmp4 AS
  (SELECT
	 cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 'dehydration_adm_ind' as msr_nm,
     1 AS dehydration_adm_ind
   FROM clm_line_fct cf
     INNER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id in ('ACSDEHD')
	 LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
	 LEFT OUTER JOIN (SELECT distinct cdf.clm_id, 'x'excl_ind
       FROM clm_dgns_fct cdf
         INNER JOIN dgns_dim dd2 ON dd2.dgns_sk = cdf.dgns_sk
         INNER JOIN ahrq_val_set_dim vsd2 ON dd2.dgns_alt_cd = vsd2.cd AND vsd2.cohrt_id='CRENLFD'
	 )x on cf.clm_id=x.clm_id
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
     AND x.excl_ind is null
   GROUP BY cf.pln_mbr_sk,cf.mbr_id_num, cf.clm_id, cf.svc_to_dt);

--Admits for Diabetes Long-term Complications, Diabetes Short-term Complications and Uncontrolled Diabetes
CREATE TEMP TABLE ambul_snsv_cdtn_fct_tmp5 AS
  (SELECT
	 cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 CASE WHEN vsd.cohrt_id = 'ACDIALD' then 'DLTC_adm_ind'
		  WHEN vsd.cohrt_id = 'ACDIASD' then 'DSTC_adm_ind' 
		  WHEN vsd.cohrt_id = 'ACDIAUD' then 'ud_adm_ind' end
		as msr_nm,
	 CASE WHEN vsd.cohrt_id = 'ACDIALD' then 1 else 0 end as DLTC_adm_ind,
	 CASE WHEN vsd.cohrt_id = 'ACDIASD' then 1 else 0 end as DSTC_adm_ind,
	 CASE WHEN vsd.cohrt_id = 'ACDIAUD' then 1 else 0 end as ud_adm_ind

   FROM clm_line_fct cf
     INNER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id in ('ACDIASD','ACDIALD','ACDIAUD')
     LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
   GROUP BY cf.pln_mbr_sk,cf.mbr_id_num, cf.clm_id, cf.svc_to_dt, vsd.cohrt_id);

--Admits for Heart Failure
CREATE TEMP TABLE ambul_snsv_cdtn_fct_tmp6 AS
  (SELECT
	 cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 'hf_adm_ind' as msr_nm,
     1 AS hf_adm_ind
   FROM clm_line_fct cf
     INNER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id = 'MRTCHFD'
     LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
	 LEFT OUTER JOIN
	 (SELECT cpf.clm_id, 'x' excl_ind
       FROM clm_pcd_fct cpf
         INNER JOIN icd_pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
         INNER JOIN ahrq_val_set_dim vsd3 ON ipd.icd_pcd_alt_cd = vsd3.cd and vsd3.cohrt_id = 'ACSCARP'
       GROUP BY cpf.clm_id)X
	 ON cf.clm_id=X.clm_id
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
     AND excl_ind is null
   GROUP BY cf.pln_mbr_sk,cf.mbr_id_num, cf.clm_id, cf.svc_to_dt);

--Admits for Hypertension
CREATE TEMP TABLE ambul_snsv_cdtn_fct_tmp7 AS
  (SELECT
	 cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 'htn_adm_ind' as msr_nm,
     1 AS htn_adm_ind
   FROM clm_line_fct cf
     INNER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id='ACSHYPD'
     LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
	 LEFT OUTER JOIN (
       SELECT distinct cdf.clm_id, 'x' excl_ind
       FROM clm_dgns_fct cdf
         INNER JOIN dgns_dim dd2 ON dd2.dgns_sk = cdf.dgns_sk
         INNER JOIN ahrq_val_set_dim vsd2 ON dd2.dgns_alt_cd = vsd2.cd AND vsd2.cohrt_id='ACSHY2D'
         INNER JOIN clm_pcd_fct cpf ON cdf.clm_id = cpf.clm_id
         INNER JOIN icd_pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
         INNER JOIN ahrq_val_set_dim vsd3 ON ipd.icd_pcd_alt_cd = vsd3.cd AND vsd3.cohrt_id='DIALY2P'
	   UNION
	   SELECT distinct cpf.clm_id, 'x' excl_ind
       FROM clm_pcd_fct cpf
         INNER JOIN icd_pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
         INNER JOIN ahrq_val_set_dim vsd2 ON ipd.icd_pcd_alt_cd = vsd2.cd AND vsd2.cohrt_id='ACSCARP'
   )x on cf.clm_id=x.clm_id
	 
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
	 AND x.excl_ind is null
   GROUP BY cf.pln_mbr_sk,cf.mbr_id_num, cf.clm_id, cf.svc_to_dt);

--Admits for Lower-Extremity Amputation among Diabetes Patients
CREATE TEMP TABLE ambul_snsv_cdtn_fct_tmp8 AS
  (SELECT
     cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
 'LEADP_ADM_IND' as msr_nm,
     1 AS LEADP_ADM_IND
   FROM clm_line_fct cf
   	 INNER JOIN clm_dgns_fct cdf on cf.clm_id=cdf.clm_id
	 INNER JOIN clm_pcd_fct cpf on cf.clm_id=cpf.clm_id
     INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id='ACSLEAD'
     INNER JOIN icd_pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
     INNER JOIN ahrq_val_set_dim vsd2 ON ipd.icd_pcd_alt_cd = vsd2.cd AND vsd2.cohrt_id='ACSLEAP'
     LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
	 LEFT OUTER JOIN (SELECT cdf.clm_id, 'x' excl_ind
       FROM clm_dgns_fct cdf
         INNER JOIN dgns_dim dd2 ON dd2.dgns_sk = cdf.dgns_sk
         INNER JOIN ahrq_val_set_dim vsd3 ON dd2.dgns_alt_cd = vsd3.cd AND vsd3.cohrt_id='ACLEA2D'
	 )x on cf.clm_id=x.clm_id
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
	 AND x.excl_ind is null
   GROUP BY cf.pln_mbr_sk, cf.mbr_id_num, cf.clm_id, cf.svc_to_dt);

--Admits for Perforated Appendix Admission Rate
CREATE TEMP TABLE ambul_snsv_cdtn_fct_tmp9 AS
  (SELECT
	 cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 'PAAR_ADM_IND' as msr_nm,
     1 AS PAAR_ADM_IND
   FROM clm_line_fct cf
   	 INNER JOIN clm_dgns_fct cdf on cf.clm_id=cdf.clm_id
     INNER JOIN dgns_dim dd ON cdf.dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id in ('ACSAPPD','ACSAP2D')
     LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
	 LEFT OUTER JOIN ms_drg_dim md ON cf.ms_drg_sk = md.ms_drg_sk
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
	 AND md.ms_drg_mdc_cd <> '14'
   GROUP BY cf.pln_mbr_sk,cf.mbr_id_num, cf.clm_id, cf.svc_to_dt);

--Admits for Urinary Tract Infection
CREATE TEMP TABLE ambul_snsv_cdtn_fct_tmp10 AS
  (SELECT
	 cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 'uti_adm_ind' as msr_nm,
     1 AS uti_adm_ind
   FROM clm_line_fct cf
     INNER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id='ACSUTID'
     LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
	 LEFT OUTER JOIN (SELECT distinct cdf.clm_id, 'x' excl_ind
       FROM clm_dgns_fct cdf
         INNER JOIN dgns_dim dd2 ON dd2.dgns_sk = cdf.dgns_sk
         INNER JOIN ahrq_val_set_dim vsd2 ON dd2.dgns_alt_cd = vsd2.cd AND vsd2.cohrt_id in ('KIDNEY','IMMUNID')
		 UNION
		 SELECT distinct cpf.clm_id, 'x' excl_ind
       FROM clm_pcd_fct cpf
         INNER JOIN icd_pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
         INNER JOIN ahrq_val_set_dim vsd3 ON ipd.icd_pcd_alt_cd = vsd3.cd AND vsd3.cohrt_id='IMMUNIP'
	 )x on cf.clm_id=x.clm_id
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
	 AND x.excl_ind is null
   GROUP BY cf.pln_mbr_sk, cf.mbr_id_num, cf.clm_id, cf.svc_to_dt);

--Inpatient Admissions
CREATE TEMP TABLE ambul_snsv_cdtn_fct_tmp11 AS
  (SELECT
     cf.pln_mbr_sk,
     cf.clm_id,
     cf.svc_to_dt,
     1 AS ip_adm_ind
   FROM clm_line_fct cf
     INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
   WHERE
     cf.cst_modl_in_ptnt_clm_adm_ind = 1
     AND cd.care_setting_cgy_nm = 'Hospital Inpatient (facility and professional)'
   GROUP BY cf.pln_mbr_sk, cf.clm_id, cf.svc_to_dt);

-- Creating Table based on the population determined in above temp table
DROP TABLE ambul_snsv_cdtn_fct IF EXISTS;
CREATE TABLE ambul_snsv_cdtn_fct AS
  SELECT
    pmd.pln_mbr_sk,
    cf.clm_id,
    sum(cf.paid_amt)                     AS paid_amt,
    sum(cf.cst_modl_in_ptnt_clm_adm_ind) AS cst_modl_adm_ind,
    tmp1.asthma_adm_ind AS asthma_adm_ind,
    tmp2.bacterial_pneu_adm_ind AS bacterial_pneumonia_adm_ind,
    tmp3.copd_adm_ind AS copd_adm_ind,
    tmp4.dehydration_adm_ind AS dehydrton_adm_ind,
    tmp5.dltc_adm_ind AS dibts_long_term_cdtn_adm_ind,
    tmp5.dstc_adm_ind AS dibts_shrt_term_cdtn_adm_ind,
    tmp5.ud_adm_ind AS uncntld_dibts_adm_ind,
    tmp6.hf_adm_ind AS heart_failr_adm_ind,
    tmp7.htn_adm_ind AS htn_adm_ind,
    tmp8.LEADP_ADM_IND AS lwr_extremity_amputation_among_dibts_ptnts_adm_ind,
    tmp9.paar_adm_ind AS perforated_apndix_adm_ind,
    tmp10.uti_adm_ind AS urinary_tract_adm_ind,
    tmp11.ip_adm_ind AS hsptl_iptnt_adm_ind,
    MAX(COALESCE(tmp5.DSTC_adm_ind, 0), COALESCE(tmp5.DLTC_adm_ind, 0), COALESCE(tmp3.COPD_adm_ind, 0), COALESCE(tmp7.htn_adm_ind, 0), COALESCE(tmp6.hf_adm_ind, 0), COALESCE(tmp4.dehydration_adm_ind, 0), COALESCE(tmp2.bacterial_pneu_adm_ind, 0), COALESCE(tmp10.uti_adm_ind, 0), COALESCE(tmp5.ud_adm_ind, 0), COALESCE(tmp1.asthma_adm_ind, 0), COALESCE(tmp8.LEADP_ADM_IND, 0)) AS ovrl_cmpos_ind,
    MAX(COALESCE(tmp4.dehydration_adm_ind, 0), COALESCE(tmp2.bacterial_pneu_adm_ind, 0), COALESCE(tmp10.uti_adm_ind, 0)) AS acute_cmpos_ind,
    MAX(COALESCE(tmp5.DSTC_adm_ind, 0), COALESCE(tmp5.DLTC_adm_ind, 0), COALESCE(tmp3.COPD_adm_ind, 0), COALESCE(tmp7.htn_adm_ind, 0), COALESCE(tmp6.hf_adm_ind, 0), COALESCE(tmp5.ud_adm_ind, 0), COALESCE(tmp1.asthma_adm_ind, 0), COALESCE(tmp8.LEADP_ADM_IND, 0)) AS chronic_cmpos_ind,
    MAX(COALESCE(tmp5.DSTC_adm_ind, 0), COALESCE(tmp5.DLTC_adm_ind, 0), COALESCE(tmp5.ud_adm_ind, 0), COALESCE(tmp8.LEADP_ADM_IND, 0)) AS dibts_cmpos_ind,
    1                                                                           AS ambul_snsv_cdtn_fct_cnt,
  NOW()                                                                       AS vld_fm_ts,
  'QE16'                                                                      AS pce_cst_cd,
  'MILLIMAN'                                                                  AS pce_cst_src_nm
  FROM
    clm_line_fct cf
    INNER JOIN pln_mbr_dim pmd ON cf.pln_mbr_sk = pmd.pln_mbr_sk
   -- INNER JOIN qtr_asgnt_dim qad ON cf.pln_mbr_sk = qad.pln_mbr_sk
    LEFT OUTER JOIN ambul_snsv_cdtn_fct_tmp1 tmp1 ON cf.pln_mbr_sk = tmp1.pln_mbr_sk AND cf.clm_id = tmp1.clm_id AND cf.svc_to_dt = tmp1.svc_to_dt
    LEFT OUTER JOIN ambul_snsv_cdtn_fct_tmp2 tmp2 ON cf.pln_mbr_sk = tmp2.pln_mbr_sk AND cf.clm_id = tmp2.clm_id AND cf.svc_to_dt = tmp2.svc_to_dt
    LEFT OUTER JOIN ambul_snsv_cdtn_fct_tmp3 tmp3 ON cf.pln_mbr_sk = tmp3.pln_mbr_sk AND cf.clm_id = tmp3.clm_id AND cf.svc_to_dt = tmp3.svc_to_dt
    LEFT OUTER JOIN ambul_snsv_cdtn_fct_tmp4 tmp4 ON cf.pln_mbr_sk = tmp4.pln_mbr_sk AND cf.clm_id = tmp4.clm_id AND cf.svc_to_dt = tmp4.svc_to_dt
    LEFT OUTER JOIN ambul_snsv_cdtn_fct_tmp5 tmp5 ON cf.pln_mbr_sk = tmp5.pln_mbr_sk AND cf.clm_id = tmp5.clm_id AND cf.svc_to_dt = tmp5.svc_to_dt
    LEFT OUTER JOIN ambul_snsv_cdtn_fct_tmp6 tmp6 ON cf.pln_mbr_sk = tmp6.pln_mbr_sk AND cf.clm_id = tmp6.clm_id AND cf.svc_to_dt = tmp6.svc_to_dt
    LEFT OUTER JOIN ambul_snsv_cdtn_fct_tmp7 tmp7 ON cf.pln_mbr_sk = tmp7.pln_mbr_sk AND cf.clm_id = tmp7.clm_id AND cf.svc_to_dt = tmp7.svc_to_dt
    LEFT OUTER JOIN ambul_snsv_cdtn_fct_tmp8 tmp8 ON cf.pln_mbr_sk = tmp8.pln_mbr_sk AND cf.clm_id = tmp8.clm_id AND cf.svc_to_dt = tmp8.svc_to_dt
    LEFT OUTER JOIN ambul_snsv_cdtn_fct_tmp9 tmp9 ON cf.pln_mbr_sk = tmp9.pln_mbr_sk AND cf.clm_id = tmp9.clm_id AND cf.svc_to_dt = tmp9.svc_to_dt
    LEFT OUTER JOIN ambul_snsv_cdtn_fct_tmp10 tmp10 ON cf.pln_mbr_sk = tmp10.pln_mbr_sk AND cf.clm_id = tmp10.clm_id AND cf.svc_to_dt = tmp10.svc_to_dt
    LEFT OUTER JOIN ambul_snsv_cdtn_fct_tmp11 tmp11 ON cf.pln_mbr_sk = tmp11.pln_mbr_sk AND cf.clm_id = tmp11.clm_id AND cf.svc_to_dt = tmp11.svc_to_dt
  WHERE cf.cst_modl_in_ptnt_clm_adm_ind = 1 and cf.svc_to_dt >='2017-10-01' 
       --qad.asgnt_yr = (SELECT val FROM dt_meta WHERE descr = 'asgnt_yr')
       --AND qad.asgnt_qtr = (SELECT val FROM dt_meta WHERE descr = 'asgnt_qtr')
  GROUP BY cf.clm_id, pmd.pln_mbr_sk, tmp1.asthma_adm_ind, tmp2.bacterial_pneu_adm_ind, tmp3.copd_adm_ind
    , tmp4.dehydration_adm_ind, tmp5.dltc_adm_ind, tmp5.dstc_adm_ind, tmp5.ud_adm_ind, tmp6.hf_adm_ind, tmp7.htn_adm_ind, tmp8.LEADP_ADM_IND
    , tmp9.paar_adm_ind, tmp10.uti_adm_ind, tmp11.ip_adm_ind
  ORDER BY pmd.pln_mbr_sk, cf.clm_id
  Distribute ON (clm_id);

\unset ON_ERROR_STOP

