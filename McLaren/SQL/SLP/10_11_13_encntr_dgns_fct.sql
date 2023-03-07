--intermediate_stage_encntr_dgns_fct Table  creation based on Net 3 years Of patient Account Number
--select 'processing table:  intermediate_stage_encntr_dgns_fct' as table_processing;
---------------------------------------------------------------------------------------------------------------------------------------
--CODE CHANGE : MLH-591:
----Commented the old code-----------------------------------------------------------------------------------------------------------------------------------
--DROP TABLE intermediate_stage_encntr_dgns_fct IF EXISTS ;
--CREATE TABLE intermediate_stage_encntr_dgns_fct AS
--
--SELECT
--	 hash8(Z.company_id) as fcy_nm_hash
--   	,hash8(Z.patient_id) as encntr_num_hash
--   	,Z.company_id as fcy_nm
--   	,z.patient_id AS encntr_num
--       	,VSET_FCY.alt_cd as fcy_num
--       	,DF.company_id
--       	,DF.patient_id
--       	,nvl(DF.icd_code, '-100') as icd_code
--       	,DF.icd_type
--       	,DF.diagnosis_code_present_on_admission_flag
--       	,DF.icd_version
--       	,DF.diagnosisseq
--	,case when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='final' and DF.diagnosisseq=1 then 'Primary'
--	   		  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='final' and DF.diagnosisseq>1 then 'Secondary'
--			  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='discharge' and DF.diagnosisseq=1 then 'Primary'
--			  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='discharge' and DF.diagnosisseq>1 then 'Secondary'
--			  when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq=1 then 'Primary'
--			  when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq>1 then 'Secondary'
--	   		else DF.diagnosistype end as diagnosistype
--	,case when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='PARAGON' then 0
--	   		  when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='3M CODING AND REIMBURSEMENT' then 0
--			  when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='POWERCHART' then 1
--			  ELSE 0 end as sourcesystemdiag_rnk
--	,DF.sourcesystemdiag
--    	,CASE WHEN CANCER.dgns_cd is NOT NULL THEN DF.icd_code ELSE '-100' END as cancer_dgns_cd
--       	,CASE WHEN CANCER.dgns_cd is NOT NULL THEN 1 ELSE NULL END as cancer_case_ind
--	,CANCER.ccs_dgns_cgy_descr as cancer_case_code_descr
--	,row_number() over(partition by Z.company_id, Z.patient_id Order by  DF.diagnosisseq) as rec_num
--	   -----****
--	,nvl(DGNSD.ccs_dgns_cgy_cd,'-100') AS ccs_dgns_cgy_cd
--	,nvl(DGNSD.ccs_dgns_cgy_descr,'UNKNOWN') AS ccs_dgns_cgy_descr
--	,NVL(DGNSD.ccs_dgns_lvl_1_cd,'-100') AS ccs_dgns_lvl_1_cd
--	,NVL(DGNSD.ccs_dgns_lvl_1_descr,'UNKNOWN') as ccs_dgns_lvl_1_descr
--	,nvl(DGNSD.ccs_dgns_lvl_2_cd,'-100') as ccs_dgns_lvl_2_cd
--	,nvl(DGNSD.ccs_dgns_lvl_2_descr,'UNKNOWN') AS ccs_dgns_lvl_2_descr
--  FROM intermediate_stage_temp_eligible_encntr_data Z
--  LEFT JOIN dgns_fct DF on Z.company_id = DF.company_id and Z.patient_id = DF.patient_id
--  LEFT JOIN dgns_dim DGNSD ON REPLACE(DF.icd_code,'.','')=REPLACE(DGNSD.dgns_cd,'.','') AND DF.icd_version = DGNSD.dgns_icd_ver
--  LEFT JOIN val_set_dim VSET_FCY ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
--  LEFT JOIN  intermediate_stage_temp_dgns_ccs_dim_cancer_only CANCER on CANCER.dgns_cd = replace(DF.icd_code, '.','')
--  --DISTRIBUTE ON (fcy_nm_hash, encntr_num_hash);
--  DISTRIBUTE ON (fcy_nm, encntr_num);

DROP TABLE intermediate_stage_encntr_dgns_fct IF EXISTS ;
CREATE TABLE intermediate_stage_encntr_dgns_fct AS
---03/23/2020 --New Algorithm Logic
select
        fcy_nm
        , encntr_num
        , fcy_num
        , company_id
        , patient_id
        , icd_type
        , icd_version
        , case when sourcesystemdiag<>'3M CODING AND REIMBURSEMENT' and zdiagnosistype='Admitting' then coalesce(admitdiagnosiscode,icd_code)
                when sourcesystemdiag<>'3M CODING AND REIMBURSEMENT' and zdiagnosistype='Primary' then coalesce(primaryicd10diagnosiscode,icd_code)
                else icd_code end as icd_code
        , diagnosisseq
        , zdiagnosistype as diagnosistype
        , diagnosis_code_present_on_admission_flag
        , sourcesystemdiag_rnk
        , sourcesystemdiag
        , rcrd_pce_cst_src_nm customersource
        , case when REPLACE(icd_code,'.','') in ('Z5111','Z510','Z5112') THEN 1 ELSE 0 END as non_cancer_case_dgns_ind
        , case when non_cancer_case_dgns_ind =0 AND CANCER.dgns_cd is NOT NULL THEN 1 ELSE 0 END cancer_case_dgns_ind

        ,case when zdiagnosistype ='Primary'
        --and DF.diagnosisseq =1
        and  non_cancer_case_dgns_ind=1 then non_cancer_case_dgns_ind else 0 end  as prim_dgns_non_cancer_case_ind
        ,case when zdiagnosistype ='Secondary'
        --and DF.diagnosisseq =2
        and cancer_case_dgns_ind = 1 then cancer_case_dgns_ind else 0 end sec_dgns_cancer_case_ind
        ,case when zdiagnosistype ='Primary'
        --and DF.diagnosisseq =1
        and cancer_case_dgns_ind = 1 then cancer_case_dgns_ind else 0 end prim_dgns_cancer_case_ind
        , CASE WHEN CANCER.dgns_cd is NOT NULL THEN icd_code ELSE '-100' END as cancer_dgns_cd
	, nvl(CANCER.ccs_dgns_cgy_descr,'UNKNOWN') as cancer_case_code_descr

        , nvl(DGNSD.ccs_dgns_cgy_cd,'-100') AS ccs_dgns_cgy_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--, nvl(DGNSD.ccs_dgns_cgy_descr,'UNKNOWN') AS ccs_dgns_cgy_descr
        , NVL(DGNSD.ccs_dgns_lvl_1_cd,'-100') AS ccs_dgns_lvl_1_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--, NVL(DGNSD.ccs_dgns_lvl_1_descr,'UNKNOWN') as ccs_dgns_lvl_1_descr
        , nvl(DGNSD.ccs_dgns_lvl_2_cd,'-100') as ccs_dgns_lvl_2_cd
-- 07/21/21: Removed descr column from encntr_anl_Fct since it can be derived from dimension table
--, nvl(DGNSD.ccs_dgns_lvl_2_descr,'UNKNOWN') AS ccs_dgns_lvl_2_descr

 from
(
	select a.*
                ,row_number() OVER (PARTITION BY a.company_id,a.patient_id,zdiagnosistype,diagnosisseq ORDER BY sourcesystemdiag_rnk) AS row_num
from
(select
         Z.company_id as fcy_nm
        ,Z.patient_id AS encntr_num
        ,VSET_FCY.alt_cd as fcy_num
        ,DF.company_id
        ,DF.patient_id
        ,DF.icd_type
        ,nvl(DF.icd_code, '-100') as icd_code
        ,PD.admitdiagnosiscode
        ,PD.primaryicd10diagnosiscode
        ,DF.diagnosis_code_present_on_admission_flag
        ,DF.icd_version
      --CODE CHANGE : 09/28/2021: As per Member request included admission diagnosis type to handle Port Huron records
        ,case when lower(DF.diagnosistype) in ('admitting','admission diagnosis','admit','admission') then 0 else DF.diagnosisseq end as diagnosisseq
        ,case when lower(DF.diagnosistype) in ('admitting','admit','admission') then 'Admitting'
              when lower(DF.diagnosistype)='final' and DF.diagnosisseq=1 then 'Primary'
              when lower(DF.diagnosistype)='final' and DF.diagnosisseq>1 then 'Secondary'
              when lower(DF.diagnosistype)='discharge' and DF.diagnosisseq=0 then 'Admitting'
              when lower(DF.diagnosistype)='discharge' and DF.diagnosisseq=1 then 'Primary'
              when lower(DF.diagnosistype)='discharge' and DF.diagnosisseq>1 then 'Secondary'
              when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='admission diagnosis' then 'Admitting'
              when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq=1 then 'Primary'
              when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq>1 then 'Secondary'
        --CODE CHANGE : 09/28/2021: As per Member request included principal diagnosis type also
              when initcap(DF.diagnosistype) = 'Principal' then 'Primary'
              ELSE initcap(DF.diagnosistype) end as zdiagnosistype
        ,case when DF.rcrd_pce_cst_src_nm='INST_BILL' and upper(DF.sourcesystemdiag)='PARAGON' then 0
                when DF.rcrd_pce_cst_src_nm='CERNER' and upper(DF.sourcesystemdiag)='3M CODING AND REIMBURSEMENT' then 0
                when upper(DF.sourcesystemdiag)='POWERCHART' then 1
                  ELSE 0 end as sourcesystemdiag_rnk
        ,DF.sourcesystemdiag
        ,DF.rcrd_pce_cst_src_nm
from intermediate_stage_temp_eligible_encntr_data Z
        LEFT JOIN dgns_fct DF on Z.company_id = DF.company_id and Z.patient_id = DF.patient_id
        LEFT JOIN pce_qe16_oper_prd_zoom..cv_patdisch PD on DF.company_id=PD.company_id and DF.patient_id=PD.patient_id
        LEFT JOIN val_set_dim VSET_FCY ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
)a
where zdiagnosistype in ('Admitting','Primary','Secondary')
)b
LEFT JOIN dgns_dim DGNSD ON REPLACE(b.icd_code,'.','')=REPLACE(DGNSD.dgns_cd,'.','') AND b.icd_version = DGNSD.dgns_icd_ver
LEFT JOIN intermediate_stage_temp_dgns_ccs_dim_cancer_only CANCER on replace(CANCER.dgns_cd,'.','') = replace(b.icd_code, '.','')
where b.row_num=1
distribute on (fcy_nm, encntr_num)
;
--Removing duplicates wherever primary is repeated
delete from intermediate_stage_encntr_dgns_fct where (fcy_nm, encntr_num, diagnosisseq) in (
select fcy_nm, encntr_num, max(diagnosisseq) diagnosisseq from intermediate_stage_encntr_dgns_fct where diagnosistype='Primary'
group by fcy_nm, encntr_num having count(1)>1)
;

--with encntr_dgns_data as (
--SELECT
--         Z.company_id as fcy_nm
--     	,z.patient_id AS encntr_num
--       	,VSET_FCY.alt_cd as fcy_num
--       	,DF.company_id
--       	,DF.patient_id
--       	,nvl(DF.icd_code, '-100') as icd_code
--       	,DF.icd_type
--       	,DF.diagnosis_code_present_on_admission_flag
--       	,DF.icd_version
--       	,DF.diagnosisseq
--	,case when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='final' and DF.diagnosisseq=1 then 'Primary'
--	   		  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='final' and DF.diagnosisseq>1 then 'Secondary'
--			  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='discharge' and DF.diagnosisseq=1 then 'Primary'
--			  when lower(DF.company_id) in ('lansing','mmg') and lower(DF.diagnosistype)='discharge' and DF.diagnosisseq>1 then 'Secondary'
--			  when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq=1 then 'Primary'
--			  when lower(DF.company_id)='karmanos' and lower(DF.diagnosistype)='final diagnosis' and  DF.diagnosisseq>1 then 'Secondary'
--	   		else DF.diagnosistype end as zdiagnosistype
--	,case when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='PARAGON' then 0
--	   		  when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='3M CODING AND REIMBURSEMENT' then 0
--			  when lower(DF.company_id) in ('lansing','mmg') and upper(DF.sourcesystemdiag)='POWERCHART' then 1
--			  ELSE 0 end as sourcesystemdiag_rnk
--	,DF.sourcesystemdiag
	--CODE CHANGE: MLH-591 : Added two new indicators irrespective of diagnosistype
--	,case when REPLACE(DF.icd_code,'.','') in ('Z5111','Z510','Z5112') THEN 1 ELSE 0 END as non_cancer_case_dgns_ind
--	,case when non_cancer_case_dgns_ind =0 AND CANCER.dgns_cd is NOT NULL THEN 1 ELSE 0 END cancer_case_dgns_ind
--	,case when zdiagnosistype ='Primary'
	--and DF.diagnosisseq =1
--	and  non_cancer_case_dgns_ind=1 then non_cancer_case_dgns_ind else 0 end  as prim_dgns_non_cancer_case_ind
--	,case when zdiagnosistype ='Secondary'
	--and DF.diagnosisseq =2
--	and cancer_case_dgns_ind = 1 then cancer_case_dgns_ind else 0 end sec_dgns_cancer_case_ind
--	,case when zdiagnosistype ='Primary'
	--and DF.diagnosisseq =1
--	and cancer_case_dgns_ind = 1 then cancer_case_dgns_ind else 0 end prim_dgns_cancer_case_ind
--    ,CASE WHEN CANCER.dgns_cd is NOT NULL THEN DF.icd_code ELSE '-100' END as cancer_dgns_cd
--	,nvl(CANCER.ccs_dgns_cgy_descr,'UNKNOWN') as cancer_case_code_descr
--	,row_number() over(partition by Z.company_id, Z.patient_id Order by  DF.diagnosisseq) as rec_num
--	   -----****
--	,nvl(DGNSD.ccs_dgns_cgy_cd,'-100') AS ccs_dgns_cgy_cd
--	,nvl(DGNSD.ccs_dgns_cgy_descr,'UNKNOWN') AS ccs_dgns_cgy_descr
--	,NVL(DGNSD.ccs_dgns_lvl_1_cd,'-100') AS ccs_dgns_lvl_1_cd
--	,NVL(DGNSD.ccs_dgns_lvl_1_descr,'UNKNOWN') as ccs_dgns_lvl_1_descr
--	,nvl(DGNSD.ccs_dgns_lvl_2_cd,'-100') as ccs_dgns_lvl_2_cd
--	,nvl(DGNSD.ccs_dgns_lvl_2_descr,'UNKNOWN') AS ccs_dgns_lvl_2_descr
--  FROM intermediate_stage_temp_eligible_encntr_data Z
--  LEFT JOIN dgns_fct DF on Z.company_id = DF.company_id and Z.patient_id = DF.patient_id
--  LEFT JOIN dgns_dim DGNSD ON REPLACE(DF.icd_code,'.','')=REPLACE(DGNSD.dgns_cd,'.','') AND DF.icd_version = DGNSD.dgns_icd_ver
--  LEFT JOIN val_set_dim VSET_FCY ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
--  LEFT JOIN  intermediate_stage_temp_dgns_ccs_dim_cancer_only CANCER on replace(CANCER.dgns_cd,'.','') = replace(DF.icd_code, '.','')
--  )
--SELECT  fcy_nm
--       , encntr_num
--       , fcy_num
--       , company_id
--       , patient_id
--       , icd_code
--       , icd_type
--       , diagnosis_code_present_on_admission_flag
--       , icd_version
--       , diagnosisseq
--       , zdiagnosistype as diagnosistype
--       , sourcesystemdiag_rnk
--       , sourcesystemdiag
--       , non_cancer_case_dgns_ind
--       , cancer_case_dgns_ind
--       , prim_dgns_non_cancer_case_ind
--       , sec_dgns_cancer_case_ind
--       , prim_dgns_cancer_case_ind
--       , cancer_dgns_cd
--       , cancer_case_code_descr
--       , rec_num
--       , ccs_dgns_cgy_cd
--       , ccs_dgns_cgy_descr
--       , ccs_dgns_lvl_1_cd
--       , ccs_dgns_lvl_1_descr
--       , ccs_dgns_lvl_2_cd
--       , ccs_dgns_lvl_2_descr
--  FROM encntr_dgns_data
--    DISTRIBUTE ON (fcy_nm, encntr_num);
