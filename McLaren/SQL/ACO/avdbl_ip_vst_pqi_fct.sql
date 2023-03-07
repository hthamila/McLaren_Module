\set ON_ERROR_STOP ON;

truncate avdbl_ip_vst_pqi_fct;

--Admits for Asthma in Younger Adults
insert into avdbl_ip_vst_pqi_fct
  (SELECT
	 cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 'asthma_adm_ind' as msr_nm,
     1 AS asthma_adm_ind
   FROM clm_line_fct cf
     INNER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id = 'ACSASTD'
     LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
	 LEFT OUTER JOIN 
	 (
       SELECT cdf.clm_id, 'X' Excl_ind
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
 	GROUP BY cf.pln_mbr_sk, cf.mbr_id_num, cf.clm_id, cf.svc_to_dt, X.EXCL_IND);


-- Admits for Bacterial Pneumonia
insert into avdbl_ip_vst_pqi_fct
  (SELECT
	 cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 'bacterial_pneu_adm_ind' as msr_nm,
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
       GROUP BY cdf.clm_id	 
	 )X on cf.clm_id=X.clm_id
	 LEFT OUTER JOIN
	 (SELECT cpf.clm_id, 'Y' proc_excl_ind
       FROM clm_pcd_fct cpf
         INNER JOIN icd_pcd_dim ipd ON cpf.icd_pcd_sk = ipd.icd_pcd_sk
         INNER JOIN ahrq_val_set_dim vsd3 ON ipd.icd_pcd_alt_cd = vsd3.cd AND vsd3.cohrt_id in ('IMMUNIP')
       GROUP BY cpf.clm_id
	 )Y on cf.clm_id=Y.clm_id
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
	 AND X.excl_ind is null
     AND Y.proc_excl_ind is null
   GROUP BY cf.pln_mbr_sk, cf.mbr_id_num, cf.clm_id, cf.svc_to_dt);

--Admits for COPD
insert into avdbl_ip_vst_pqi_fct
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
insert into avdbl_ip_vst_pqi_fct
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
 
insert into avdbl_ip_vst_pqi_fct 
 (SELECT
	 cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 'dehydration_adm_ind' as msr_nm,
     1 AS dehydration_adm_ind
   FROM clm_line_fct cf
     INNER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id in ('HYPERID','ACPGASD','PHYSIDB')
	 INNER JOIN clm_dgns_fct cdf on cf.clm_id=cdf.clm_id
	 INNER JOIN dgns_dim dd2 ON dd2.dgns_sk = cdf.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd2 ON dd2.dgns_alt_cd = vsd2.cd AND vsd2.cohrt_id='ACSDEHD'
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
insert into avdbl_ip_vst_pqi_fct
  (SELECT
	 cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 CASE WHEN vsd.cohrt_id = 'ACDIALD' then 'DLTC_adm_ind'
		  WHEN vsd.cohrt_id = 'ACDIASD' then 'DSTC_adm_ind' 
		  WHEN vsd.cohrt_id = 'ACDIAUD' then 'ud_adm_ind' end
		as msr_nm,
	 CASE WHEN vsd.cohrt_id = 'ACDIALD' then 1
		  WHEN vsd.cohrt_id = 'ACDIASD' then 1 
		  WHEN vsd.cohrt_id = 'ACDIAUD' then 1 else 0 end
		as msr_ind
   FROM clm_line_fct cf
     INNER JOIN dgns_dim dd ON cf.prim_dgns_sk = dd.dgns_sk
     INNER JOIN ahrq_val_set_dim vsd ON dd.dgns_alt_cd = vsd.cd AND vsd.cohrt_id in ('ACDIASD','ACDIALD','ACDIAUD')
     LEFT OUTER JOIN adm_src_dim asd ON cf.adm_src_sk = asd.adm_src_sk
   WHERE
     asd.adm_src_cd NOT IN ('4', '5', '6')
     AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
   GROUP BY cf.pln_mbr_sk,cf.mbr_id_num, cf.clm_id, cf.svc_to_dt, vsd.cohrt_id);

--Admits for Heart Failure
insert into avdbl_ip_vst_pqi_fct
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
insert into avdbl_ip_vst_pqi_fct
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
insert into avdbl_ip_vst_pqi_fct
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
insert into avdbl_ip_vst_pqi_fct
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
insert into avdbl_ip_vst_pqi_fct
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
insert into avdbl_ip_vst_pqi_fct
  (SELECT
     cf.pln_mbr_sk,
     cf.mbr_id_num,
     cf.clm_id,
     cf.svc_to_dt,
	 'ip_adm_ind' as msr_nm,
     1 AS ip_adm_ind
   FROM clm_line_fct cf
     INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
   WHERE
     cf.cst_modl_in_ptnt_clm_adm_ind = 1
     AND cd.care_setting_cgy_nm = 'Hospital Inpatient (facility and professional)'
   GROUP BY cf.pln_mbr_sk, cf.mbr_id_num, cf.clm_id, cf.svc_to_dt);


\unset ON_ERROR_STOP

