--New
--select 'processing table:  intermediate_stage_encntr_cnslt_pract_fct' as table_processing;
DROP TABLE intermediate_stage_encntr_cnslt_pract_fct IF EXISTS;
CREATE TABLE intermediate_stage_encntr_cnslt_pract_fct as
with cnslt_pract_1 as
(
select C1.company_id, C1.patient_id,
C1.practitioner_code as cnslt_pract_1_cd,
SPCL.npi as cnslt_pract_1_npi,
SPCL.practitioner_name as cnslt_pract_1_nm,
SPCL.practitioner_spclty_description as cnslt_pract_1_spclty,
SPCL.mcare_spcly_cd as cnslt_pract_1_mcare_spcly_cd
FROM intermediate_stage_temp_eligible_encntr_data Z
INNER JOIN  intermediate_stage_encntr_pract_fct C1
on C1.company_id = Z.company_id and Z.patient_id = C1.patient_id
LEFT JOIN intermediate_stage_temp_physician_npi_spclty SPCL
on SPCL.company_id = C1.company_id and SPCL.practitioner_code = C1.practitioner_code
WHERE lower(C1.raw_role) = 'consulting 1'),
cnslt_pract_2 as
(
select C1.company_id, C1.patient_id, C1.practitioner_code as cnslt_pract_2_cd,
SPCL.npi as cnslt_pract_2_npi,
SPCL.practitioner_name as cnslt_pract_2_nm,
SPCL.practitioner_spclty_description as cnslt_pract_2_spclty,
SPCL.mcare_spcly_cd as cnslt_pract_2_mcare_spcly_cd
FROM intermediate_stage_temp_eligible_encntr_data Z
INNER JOIN  intermediate_stage_encntr_pract_fct C1
on C1.company_id = Z.company_id and Z.patient_id = C1.patient_id
INNER JOIN phys_dim P
on P.practitioner_code = C1.practitioner_code and C1.company_id = P.company_id
LEFT JOIN intermediate_stage_temp_physician_npi_spclty SPCL
on SPCL.company_id = C1.company_id and SPCL.practitioner_code = C1.practitioner_code
WHERE lower(C1.raw_role) = 'consulting 2'),
cnslt_pract_3 as
(
select C1.company_id, C1.patient_id, C1.practitioner_code as cnslt_pract_3_cd,
SPCL.npi as cnslt_pract_3_npi,
SPCL.practitioner_name as cnslt_pract_3_nm,
SPCL.practitioner_spclty_description as cnslt_pract_3_spclty,
SPCL.mcare_spcly_cd as cnslt_pract_3_mcare_spcly_cd
FROM intermediate_stage_temp_eligible_encntr_data Z
INNER JOIN  intermediate_stage_encntr_pract_fct C1
on C1.company_id = Z.company_id and Z.patient_id = C1.patient_id
INNER JOIN phys_dim P
on P.practitioner_code = C1.practitioner_code and C1.company_id = P.company_id
LEFT JOIN intermediate_stage_temp_physician_npi_spclty SPCL
on SPCL.company_id = C1.company_id and SPCL.practitioner_code = C1.practitioner_code
WHERE lower(C1.raw_role) = 'consulting 3')
select T1.company_id as fcy_nm, T1.patient_id as encntr_num,
C1.cnslt_pract_1_cd, cnslt_pract_1_nm,  C1.cnslt_pract_1_npi, C1.cnslt_pract_1_spclty, C1.cnslt_pract_1_mcare_spcly_cd,
C2.cnslt_pract_2_cd, cnslt_pract_2_nm,  C2.cnslt_pract_2_npi, C2.cnslt_pract_2_spclty, C2.cnslt_pract_2_mcare_spcly_cd,
C3.cnslt_pract_3_cd, cnslt_pract_3_nm,  C3.cnslt_pract_3_npi, C3.cnslt_pract_3_spclty, C3.cnslt_pract_3_mcare_spcly_cd
FROM intermediate_stage_temp_eligible_encntr_data T1
LEFT JOIN cnslt_pract_1 C1
on C1.company_id = T1.company_id and T1.patient_id = C1.patient_id
LEFT JOIN cnslt_pract_2 C2
on C2.company_id = T1.company_id and T1.patient_id = C2.patient_id
LEFT JOIN cnslt_pract_3 C3
on C3.company_id = T1.company_id and T1.patient_id = C3.patient_id;

--select 'processing table: intermediate_stage_temp_surgeon_pract ' as table_processing;
DROP TABLE intermediate_stage_temp_surgeon_pract IF exists;
CREATE TABLE intermediate_stage_temp_surgeon_pract AS
(
  select Z.company_id, Z.patient_id, P.surgeon_code as prim_srgn_cd,
  PHY.npi as prim_srgn_npi,
  PHY.practitioner_name as prim_srgn_nm,
  PHY.practitioner_spclty_description as prim_srgn_spclty,
  PHY.mcare_spcly_cd as prim_srgn_mcare_spcly_cd
  from intermediate_stage_temp_eligible_encntr_data Z
  LEFT JOIN  intermediate_stage_encntr_pcd_fct P
  on P.company_id = Z.company_id and Z.patient_id = P.patient_id
  LEFT JOIN  intermediate_stage_temp_physician_npi_spclty PHY
  on PHY.practitioner_code = P.surgeon_code and P.company_id = PHY.company_id
  WHERE P.proceduretype='Primary' AND surgeon_code is NOT NULL
);

