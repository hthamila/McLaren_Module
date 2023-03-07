--#################################################################--
--                Fix for Service Line                             --
--#################################################################--

--select 'processing table:  temp_eligible_svc_ln_anl_fct' as table_processing;
-- DROP TABLE temp_eligible_svc_ln_anl_fct IF EXISTS;
-- CREATE TABLE  temp_eligible_svc_ln_anl_fct AS
-- (
-- -- CPT HCPCS ---
-- SELECT distinct p.company_id, p.patient_id,p.cpt_code as code ,'CPT' as criteria,
-- rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln , rnk.services as svc_nm, rnk.cd, rnk.cd_type,rnk.descr as cd_descr,
-- rnk.svc_cgy_rnk, rnk.svc_ln_rnk, rnk.sub_svc_ln_rnk, rnk.svc_rnk, tempe.inpatient_outpatient_flag
-- FROM  intermediate_stage_chrg_fct p
-- INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on p.company_id = tempe.company_id and p.patient_id = tempe.patient_id
-- INNER JOIN svc_hier_dim rnk
-- on p.cpt_code = rnk.cd and rnk.cd_type in ('HCPCS','CPT') and lower(rnk.svc_cgy) in ('surgical','medical')
--
-- UNION
--
-- SELECT distinct p.company_id, p.patient_id,p.cpt_code as code,'CPT' as criteria,
-- rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln , rnk.services as svc_nm, rnk.cd, rnk.cd_type,rnk.descr as cd_descr,
-- rnk.svc_cgy_rnk, rnk.svc_ln_rnk, rnk.sub_svc_ln_rnk, rnk.svc_rnk, tempe.inpatient_outpatient_flag
-- FROM  intermediate_stage_cpt_fct p
-- INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on p.company_id = tempe.company_id and p.patient_id = tempe.patient_id
-- INNER JOIN svc_hier_dim rnk
-- on p.cpt_code = rnk.cd and rnk.cd_type in ('HCPCS','CPT') and lower(rnk.svc_cgy) in ('surgical','medical')
--
-- UNION
--
-- -- PCD ICD 10 ICD 9 --
-- SELECT distinct pf.company_id, pf.patient_id,pf.icd_code as code,'ICD 10/9 PCS' as criteria,
-- rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln , rnk.services as svc_nm, rnk.cd, rnk.cd_type,rnk.descr as cd_descr,
-- rnk.svc_cgy_rnk, rnk.svc_ln_rnk, rnk.sub_svc_ln_rnk, rnk.svc_rnk, tempe.inpatient_outpatient_flag
-- FROM  intermediate_stage_encntr_pcd_fct pf
-- INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on pf.company_id = tempe.company_id and pf.patient_id = tempe.patient_id
-- INNER JOIN svc_hier_dim rnk
-- on pf.icd_code = rnk.cd and rnk.cd_type in ('ICD 10 PCS','ICD 9 PCS') and lower(rnk.svc_cgy) in ('surgical','medical')
-- WHERE pf.icd_type = 'P'
--
-- UNION
--
-- --ICD DGNS --
-- SELECT distinct df.company_id, df.patient_id,df.icd_code as code,'ICD 10 DGNS' as criteria,
-- rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln , rnk.services as svc_nm, rnk.cd, rnk.cd_type,rnk.descr as cd_descr,
-- rnk.svc_cgy_rnk, rnk.svc_ln_rnk, rnk.sub_svc_ln_rnk, rnk.svc_rnk, tempe.inpatient_outpatient_flag
-- FROM  intermediate_stage_encntr_dgns_fct df
-- INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on df.company_id = tempe.company_id and df.patient_id = tempe.patient_id
-- INNER JOIN svc_hier_dim rnk
-- on df.icd_code = rnk.cd and lower(rnk.svc_cgy) in ('surgical','medical') and rnk.cd_type in ('ICD 10 DGNS')
--
-- --MS DRG -----
-- UNION
--
-- SELECT distinct
-- patd.company_id, patd.patient_id, patd.msdrg_code as code,'MSDRG' as criteria,
-- rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln,rnk.services as svc_nm,rnk.cd,rnk.cd_type,rnk.descr as cd_descr,
-- rnk.svc_cgy_rnk,rnk.svc_ln_rnk,rnk.sub_svc_ln_rnk,rnk.svc_rnk, tempe.inpatient_outpatient_flag
-- FROM pce_qe16_oper_prd_zoom..cv_patdisch patd
-- INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on patd.company_id = tempe.company_id and patd.patient_id = tempe.patient_id
-- LEFT JOIN svc_hier_dim rnk
-- on patd.msdrg_code = rnk.cd and rnk.cd_type in ('MS-DRG') and lower(rnk.svc_cgy) in ('surgical','medical')
-- where patd.inpatient_outpatient_flag = 'I ');
--
-- ----********************************************

--CODE CHANGE: JULY 2021
DROP TABLE temp_eligible_inpatient_svc_ln_anl_fct IF EXISTS;
CREATE TABLE temp_eligible_inpatient_svc_ln_anl_fct AS
    (
-- -- CPT ---
-- select Z.fcy_nm as company_id, z.encntr_num as patient_id, Z.cpt_code as code, 'CPT' as criteria,
-- null as svc_cgy,
-- Z.svc_line as svc_ln,
-- Z.sub_svc_line as sub_svc_ln ,
-- Z.svc_nm as svc_nm,
--  Z.cpt_code as cd,
--  null as cd_type,
--  null as cd_descr,
-- null as svc_cgy_rnk, null as svc_ln_rnk, null as sub_svc_ln_rnk, null as svc_rnk, tempe.inpatient_outpatient_flag
--  from intermediate_stage_encntr_prim_cpt_fct Z
--  INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on Z.fcy_nm = tempe.company_id and z.encntr_num = tempe.patient_id
--  WHERE tempe.inpatient_outpatient_flag = 'I' and lower(Z.cpt_type) in ('surgical','non-surgical')
--
--
-- UNION

-- PCD ICD 10 ICD 9 --
        SELECT distinct pf.company_id, pf.patient_id,pf.icd_code as code,'ICD 10/9 PCS' as criteria,
                        rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln , rnk.services as svc_nm, rnk.cd, rnk.cd_type,rnk.descr as cd_descr,
                        rnk.svc_cgy_rnk, rnk.svc_ln_rnk, rnk.sub_svc_ln_rnk, rnk.svc_rnk, tempe.inpatient_outpatient_flag
        FROM  intermediate_stage_encntr_pcd_fct pf
                  INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on pf.company_id = tempe.company_id and pf.patient_id = tempe.patient_id
                  INNER JOIN svc_hier_dim rnk
                             on pf.icd_code = rnk.cd and rnk.cd_type in ('ICD 10 PCS','ICD 9 PCS') and lower(rnk.svc_cgy) in ('surgical','medical')
        WHERE pf.icd_type = 'P' and tempe.inpatient_outpatient_flag = 'I'

        UNION

--ICD DGNS --
        SELECT distinct df.company_id, df.patient_id,df.icd_code as code,'ICD 10 DGNS' as criteria,
                        rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln , rnk.services as svc_nm, rnk.cd, rnk.cd_type,rnk.descr as cd_descr,
                        rnk.svc_cgy_rnk, rnk.svc_ln_rnk, rnk.sub_svc_ln_rnk, rnk.svc_rnk, tempe.inpatient_outpatient_flag
        FROM  intermediate_stage_encntr_dgns_fct df
                  INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on df.company_id = tempe.company_id and df.patient_id = tempe.patient_id
                  INNER JOIN svc_hier_dim rnk
                             on df.icd_code = rnk.cd and lower(rnk.svc_cgy) in ('surgical','medical') and rnk.cd_type in ('ICD 10 DGNS')
        WHERE  tempe.inpatient_outpatient_flag = 'I'

--MS DRG -----
        UNION

        SELECT distinct
            patd.company_id, patd.patient_id, patd.msdrg_code as code,'MSDRG' as criteria,
            rnk.svc_cgy,rnk.svc_ln,rnk.sub_svc_ln,rnk.services as svc_nm,rnk.cd,rnk.cd_type,rnk.descr as cd_descr,
            rnk.svc_cgy_rnk,rnk.svc_ln_rnk,rnk.sub_svc_ln_rnk,rnk.svc_rnk, tempe.inpatient_outpatient_flag
        FROM pce_qe16_oper_prd_zoom..cv_patdisch patd
                 INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on patd.company_id = tempe.company_id and patd.patient_id = tempe.patient_id
                 LEFT JOIN svc_hier_dim rnk
                           on patd.msdrg_code = rnk.cd and rnk.cd_type in ('MS-DRG') and lower(rnk.svc_cgy) in ('surgical','medical')
        where patd.inpatient_outpatient_flag = 'I ');

DROP TABLE temp_eligible_outpatient_svc_ln_anl_fct IF EXISTS;
CREATE  TABLE temp_eligible_outpatient_svc_ln_anl_fct AS
    (
-- CPT ---
        select Z.fcy_nm as company_id, z.encntr_num as patient_id, Z.cpt_code as code, 'CPT' as criteria,
               null as svc_cgy,
               Z.svc_line as svc_ln,
               Z.sub_svc_line as sub_svc_ln ,
               Z.svc_nm as svc_nm,
               Z.cpt_code as cd,
               null as cd_type,
               null as cd_descr,
               null as svc_cgy_rnk, null as svc_ln_rnk, null as sub_svc_ln_rnk, null as svc_rnk, tempe.inpatient_outpatient_flag
        from intermediate_stage_encntr_prim_cpt_fct Z
                 INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on Z.fcy_nm = tempe.company_id and z.encntr_num = tempe.patient_id
        WHERE tempe.inpatient_outpatient_flag = 'O' and lower(Z.cpt_type) in ('surgical','non-surgical')
        --TODO Need to apply surgical, medical filter on the table : stg_encntr_prim_cpt

    );

DROP TABLE temp_eligible_outpatient_svc_ln_anl_fct IF EXISTS;
CREATE TABLE  temp_eligible_outpatient_svc_ln_anl_fct AS
    (
-- CPT ---
        select Z.fcy_nm as company_id, z.encntr_num as patient_id, Z.cpt_code as code, 'CPT' as criteria,
               null as svc_cgy,
               Z.svc_line as svc_ln,
               Z.sub_svc_line as sub_svc_ln ,
               Z.svc_nm as svc_nm,
               Z.cpt_code as cd,
               null as cd_type,
               null as cd_descr,
               null as svc_cgy_rnk, null as svc_ln_rnk, null as sub_svc_ln_rnk, null as svc_rnk, tempe.inpatient_outpatient_flag
        from stg_encntr_prim_cpt Z
                 INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on Z.fcy_nm = tempe.company_id and z.encntr_num = tempe.patient_id
        WHERE tempe.inpatient_outpatient_flag = 'O' and lower(Z.cpt_type) in ('surgical','non-surgical')
        --TODO Need to apply surgical, medical filter on the table : stg_encntr_prim_cpt

    );

DROP TABLE temp_eligible_outpatient_prim_cpt_svc_ln_anl_fct IF EXISTS;
CREATE  TABLE temp_eligible_outpatient_prim_cpt_svc_ln_anl_fct AS
    (
-- CPT ---
        select Z.fcy_nm as company_id, z.encntr_num as patient_id, Z.cpt_code as code, 'CPT' as criteria,
               null as svc_cgy,
               Z.svc_line as svc_ln,
               Z.sub_svc_line as sub_svc_ln ,
               Z.svc_nm as svc_nm,
               Z.cpt_code as cd,
               null as cd_type,
               null as cd_descr,
               null as svc_cgy_rnk, null as svc_ln_rnk, null as sub_svc_ln_rnk, null as svc_rnk, tempe.inpatient_outpatient_flag
        from intermediate_stage_encntr_prim_cpt_fct Z
                 INNER JOIN intermediate_stage_temp_eligible_encntrs tempe on Z.fcy_nm = tempe.company_id and z.encntr_num = tempe.patient_id
        WHERE tempe.inpatient_outpatient_flag = 'O' and lower(Z.cpt_type) in ('surgical','non-surgical')
        --TODO Need to apply surgical, medical filter on the table : stg_encntr_prim_cpt
    );



------
-- --select 'processing table:  temp_svc_ln_anl_fct' as table_processing;
-- DROP TABLE temp_svc_ln_anl_fct IF EXISTS;
-- CREATE TABLE  temp_svc_ln_anl_fct AS
-- SELECT sv.* , patd.msdrg_code,
-- cg.mclaren_major_slp_grouping,
-- row_number() over(partition by sv.company_id, sv.patient_id
-- Order by sv.svc_ln_rnk,sv.sub_svc_ln_rnk,sv.svc_rnk) as org_rec_num,
-- case when ((cg.mclaren_major_slp_grouping = sv.svc_ln) and sv.criteria = 'MS-DRG') then 999
-- when cg.mclaren_major_slp_grouping = sv.svc_ln then 99 else org_rec_num end as temp_rec_num
--
-- FROM temp_eligible_svc_ln_anl_fct sv
-- INNER JOIN pce_qe16_oper_prd_zoom..cv_patdisch patd on sv.patient_id = patd.patient_id and sv.company_id = patd.company_id
-- LEFT JOIN pce_qe16_oper_prd_zoom..cv_drgmap cg on patd.msdrg_code = cg.ms_drg_code
-- ORDER BY svc_cgy_rnk, svc_ln_rnk, sub_svc_ln_rnk, svc_rnk;

DROP TABLE temp_eligible_svc_ln_anl_fct IF EXISTS;
CREATE TABLE temp_eligible_svc_ln_anl_fct AS
SELECT * FROM temp_eligible_inpatient_svc_ln_anl_fct UNION
SELECT * FROM temp_eligible_outpatient_svc_ln_anl_fct;

------
-- --select 'processing table:  temp_svc_ln_anl_fct' as table_processing;
DROP TABLE temp_ip_svc_ln_anl_fct IF EXISTS;
CREATE TABLE  temp_ip_svc_ln_anl_fct AS
    (
        SELECT sv.* , patd.msdrg_code,
               cg.mclaren_major_slp_grouping,
               row_number() over(partition by sv.company_id, sv.patient_id
Order by sv.svc_ln_rnk,sv.sub_svc_ln_rnk,sv.svc_rnk) as org_rec_num,
                case when ((cg.mclaren_major_slp_grouping = sv.svc_ln) and sv.criteria = 'MS-DRG') then 999
                     when cg.mclaren_major_slp_grouping = sv.svc_ln then 99 else org_rec_num end as temp_rec_num
        FROM temp_eligible_svc_ln_anl_fct sv
                 INNER JOIN pce_qe16_oper_prd_zoom..cv_patdisch patd on sv.patient_id = patd.patient_id and sv.company_id = patd.company_id
                 LEFT JOIN pce_qe16_oper_prd_zoom..cv_drgmap cg on patd.msdrg_code = cg.ms_drg_code
        WHERE sv.inpatient_outpatient_flag = 'I'
        ORDER BY svc_cgy_rnk, svc_ln_rnk, sub_svc_ln_rnk, svc_rnk
    );

DROP TABLE temp_op_svc_ln_anl_fct IF EXISTS;
CREATE TABLE  temp_op_svc_ln_anl_fct AS
    (
        SELECT sv.* , patd.msdrg_code,
               cast(null  as varchar(50)) as mclaren_major_slp_grouping,
--cast(1 as bigint) as org_rec_num,
--cast(org_rec_num as bigint) as temp_rec_num
               CASE WHEN op_prim_cpt.patient_id IS NOT NULL THEN cast(1 as bigint) ELSE cast(0 as bigint) END as org_rec_num,
               cast(org_rec_num as bigint) as temp_rec_num
        FROM temp_eligible_svc_ln_anl_fct sv
                 LEFT JOIN temp_eligible_outpatient_prim_cpt_svc_ln_anl_fct op_prim_cpt
                           on sv.company_id = op_prim_cpt.company_id AND op_prim_cpt.patient_id = sv.patient_id
--ADDED on 09/14/2021
                               AND sv.code = op_prim_cpt.code
                 INNER JOIN pce_qe16_oper_prd_zoom..cv_patdisch patd on sv.patient_id = patd.patient_id and sv.company_id = patd.company_id
        WHERE sv.inpatient_outpatient_flag = 'O'
    )
;

DROP TABLE temp_svc_ln_anl_fct IF EXISTS;
CREATE  TABLE temp_svc_ln_anl_fct AS
select * from temp_ip_svc_ln_anl_fct UNION
select * from temp_op_svc_ln_anl_fct ;
---************************

--select 'processing table:  temp_rnk_svc_ln_anl_fct' as table_processing;
DROP TABLE temp_rnk_svc_ln_anl_fct IF EXISTS;
CREATE TABLE  temp_rnk_svc_ln_anl_fct AS
SELECT
    tsv.company_id
     ,tsv.patient_id
     ,tsv.inpatient_outpatient_flag
     ,tsv.code
     ,tsv.criteria as based_on
     ,tsv.svc_cgy
     ,tsv.svc_ln
     ,tsv.mclaren_major_slp_grouping
     ,tsv.sub_svc_ln
     ,tsv.svc_nm
     ,tsv.cd
     ,tsv.cd_type
     ,tsv.cd_descr
     ,tsv.svc_cgy_rnk
     ,tsv.svc_ln_rnk
     ,tsv.sub_svc_ln_rnk
     ,tsv.svc_rnk
     ,tsv.msdrg_code
     ,row_number() over(partition by tsv.company_id, tsv.patient_id
		Order by tsv.temp_rec_num desc, tsv.svc_ln_rnk, tsv.sub_svc_ln_rnk, tsv.svc_rnk) as ip_rec_num
	   ,tsv.org_rec_num as op_rec_num
     ,case when tsv.inpatient_outpatient_flag = 'I' then ip_rec_num else op_rec_num end as rec_num
FROM temp_svc_ln_anl_fct tsv;

---- svc_ln_anl_fct *************************************************************************************************
--select 'processing table:  intermediate_stage_svc_ln_anl_fct' as table_processing;
DROP TABLE intermediate_stage_svc_ln_anl_fct IF EXISTS;
CREATE TABLE intermediate_stage_svc_ln_anl_fct AS
SELECT
    te.company_id as fcy_nm
     ,te.patient_id as encntr_num
     ,sv.company_id
     ,sv.patient_id
     ,sv.inpatient_outpatient_flag
     ,sv.code
     ,sv.based_on
     ,sv.svc_cgy
     ,sv.svc_ln
     ,sv.mclaren_major_slp_grouping
     ,sv.sub_svc_ln
     ,sv.svc_nm
     ,sv.cd
     ,sv.cd_type
     ,sv.cd_descr
     ,sv.rec_num
     ,sv.svc_cgy_rnk
     ,sv.svc_ln_rnk
     ,sv.sub_svc_ln_rnk
     ,sv.svc_rnk
     ,sv.msdrg_code
     ,te.admission_ts as adm_dt
     ,te.discharge_ts as dschrg_dt
     ,now() rcrd_insrt_dt

FROM intermediate_stage_temp_eligible_encntrs te
         LEFT JOIN temp_rnk_svc_ln_anl_fct sv on te.company_id = sv.company_id and te.patient_id = sv.patient_id;

--select 'processing table:  intermediate_stage_temp_encntr_svc_hier' as table_processing;
DROP TABLE intermediate_stage_temp_encntr_svc_hier IF EXISTS;
CREATE TABLE  intermediate_stage_temp_encntr_svc_hier AS
select * from  intermediate_stage_svc_ln_anl_fct where rec_num=1;

--###############################################################################--
--                     End of Service Line Ranking                               --
--###############################################################################--
