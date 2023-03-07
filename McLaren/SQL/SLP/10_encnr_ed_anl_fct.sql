--Adding ED Encounter Analysis Fact --------------------

--select 'processing table:  intermediate_stage_encntr_ed_anl_fct' as table_processing;
DROP TABLE intermediate_stage_encntr_ed_anl_fct if exists;
create table intermediate_stage_encntr_ed_anl_fct as
select distinct
		ZOOM.company_id AS fcy_nm
       ,ZOOM.inpatient_outpatient_flag AS in_or_out_patient_ind
       ,ZOOM.medical_record_number
       ,ZOOM.patient_id AS encntr_num
       ,nvl(DGNSDIM.dgns_cd,'-100') AS prim_dgns_cd
       ,nvl(DGNSDIM.dgns_descr,'UNKNOWN') AS prim_dgns_descr
       ,DGNSDIM.alc_rel_pct
       ,DGNSDIM.drug_rel_pct
       ,DGNSDIM.ed_care_needed_not_prvntable_pct
       ,DGNSDIM.ed_care_needed_prvntable_avoidable_pct
       ,DGNSDIM.injry_rel_pct
       ,DGNSDIM.non_emrgnt_rel_pct
       ,DGNSDIM.psychology_rel_pct
       ,DGNSDIM.treatable_emrgnt_ptnt_care_pct
       ,DGNSDIM.unclsfd_pct
FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM
INNER JOIN  intermediate_stage_temp_eligible_encntr_data ZOOM3YRS
on ZOOM.company_id = ZOOM3YRS.company_id and ZOOM.patient_id = ZOOM3YRS.patient_id
LEFT JOIN dgns_dim DGNSDIM ON DGNSDIM.dgns_alt_cd = replace(ZOOM.primaryicd10diagnosiscode,'.','')
       and DGNSDIM.dgns_icd_ver ='ICD10'
--      DISTRIBUTE ON (fcy_nm_hash, encntr_num_hash);
DISTRIBUTE ON (fcy_nm, encntr_num);
----ED Encounter Analysis Fact --------------------------