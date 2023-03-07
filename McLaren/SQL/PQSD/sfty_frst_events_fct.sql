CREATE OR REPLACE VIEW  pce_qe16_prd..sfty_frst_events_fct
AS WITH harm_events_msr AS (
SELECT c.site, c.person_type, c.incident_id, c.file_id, c.file_state, c.file_status_name, c.general_event_type, c.specific_event_type, c."location", c.department_number, c.event_department,
c.event_specific_location, c.severity, c.ndnqi_severity, c.event_date, c.entered_date, 
CASE WHEN ((((((c.file_state = 'New'::"varchar") OR (c.file_state = 'In-Progress'::"varchar")) OR (c.file_state = 'Closed'::"varchar")) OR (c.file_state ISNULL)) 
AND (c.general_event_type = 'Lab/Specimen'::"varchar")) AND (upper(c.site) ~~ like_escape('%HOMECARE%'::"varchar", '\'::"varchar"))) THEN 1 ELSE NULL::int4 END AS mml_events,
CASE WHEN (((((((((c.file_state = 'New'::"varchar") OR (c.file_state = 'In-Progress'::"varchar")) OR (c.file_state = 'Closed'::"varchar")) 
OR (c.file_state ISNULL)) AND ((c.file_status_name <> 'Duplicate'::"varchar") OR (c.file_status_name ISNULL))) AND (c.general_event_type <> 'Employee'::"varchar")) AND ((c.site <> 'MHC'::"varchar") 
OR (c.site <> NULL::"varchar"))) AND (((btrim(c."location") <> 'McLaren Bay Special Care'::"varchar") AND (btrim(c."location") <> 'Marwood Nursing & Rehabilitation'::"varchar")) 
AND (btrim(c."location") <> 'VitalCare Inc.'::"varchar"))) AND ((((((c.general_event_type = 'Adverse Drug Reaction'::"varchar") OR (c.general_event_type = 'Airway Management'::"varchar"))
OR ((c.general_event_type = 'Blood Product'::"varchar") OR (c.general_event_type = 'Diagnosis/Treatment'::"varchar"))) OR (((c.general_event_type = 'Equipment/Medical Device'::"varchar") 
OR (c.general_event_type = 'Facilities'::"varchar")) OR ((c.general_event_type = 'Fall'::"varchar") OR (c.general_event_type = 'Healthcare IT'::"varchar")))) 
OR ((((c.general_event_type = 'Infection'::"varchar") OR (c.general_event_type = 'IV/Vascular Access Device'::"varchar")) OR ((c.general_event_type = 'Line/Tube'::"varchar") 
OR (c.general_event_type = 'Maternal/Childbirth'::"varchar"))) OR (((c.general_event_type = 'Medication/Fluid'::"varchar") OR (c.general_event_type = 'Patient ID/Documentation/Consent'::"varchar")) 
OR ((c.general_event_type = 'Professional Conduct'::"varchar") OR (c.general_event_type = 'Provision of Care'::"varchar"))))) 
OR (((c.general_event_type = 'Restraints'::"varchar") OR (c.general_event_type = 'Safety/Security'::"varchar")) OR ((c.general_event_type = 'Skin/Tissue'::"varchar") 
OR (c.general_event_type = 'Surgery/Procedure'::"varchar"))))) THEN 1 ELSE NULL::int4 END AS tot_rep_events, CASE WHEN (((((((((c.file_state = 'New'::"varchar") 
OR (c.file_state = 'In-Progress'::"varchar")) OR (c.file_state = 'Closed'::"varchar")) OR (c.file_state ISNULL)) AND (c.general_event_type = 'Fall'::"varchar")) 
AND ((c.file_status_name <> 'Duplicate'::"varchar") OR (c.file_status_name ISNULL))) AND (c.person_type = 'In-Patient'::"varchar")) 
AND (((c.ndnqi_severity ~~ like_escape('2%'::"varchar", '\'::"varchar")) OR (c.ndnqi_severity ~~ like_escape('3%'::"varchar", '\'::"varchar"))) 
OR (c.ndnqi_severity ~~ like_escape('4%'::"varchar", '\'::"varchar")))) AND (((((((((((c.site = 'Bay'::"varchar") AND ((((m.deptnumber = '24060'::"varchar") 
OR (m.deptnumber = '24070'::"varchar")) OR ((m.deptnumber = '24071'::"varchar") OR (m.deptnumber = '24050'::"varchar"))) OR (((m.deptnumber = '24040'::"varchar") 
OR (m.deptnumber = '24020'::"varchar")) OR (m.deptnumber = '20020'::"varchar")))) OR ((c.site = 'Central Michigan'::"varchar") AND (((m.deptnumber = '24430'::"varchar") 
OR (m.deptnumber = '24420'::"varchar")) OR ((m.deptnumber = '24410'::"varchar") OR (m.deptnumber = '20010'::"varchar"))))) OR ((c.site = 'Flint'::"varchar") 
AND (((((m.deptnumber = '23040'::"varchar") OR (m.deptnumber = '23050'::"varchar")) OR ((m.deptnumber = '23060'::"varchar") OR (m.deptnumber = '23012'::"varchar"))) 
OR (((m.deptnumber = '23010'::"varchar") OR (m.deptnumber = '23020'::"varchar")) OR ((m.deptnumber = '23030'::"varchar") OR (m.deptnumber = '20010'::"varchar")))) 
OR ((((m.deptnumber = '23090'::"varchar") OR (m.deptnumber = '20210'::"varchar")) OR ((m.deptnumber = '23070'::"varchar") OR (m.deptnumber = '23080'::"varchar"))) 
OR (((m.deptnumber = '20410'::"varchar") OR (m.deptnumber = '20210'::"varchar")) OR (m.deptnumber = '20010'::"varchar")))))) OR ((c.site = 'Karmanos'::"varchar") 
AND ((((m.deptnumber = '401014'::"varchar") OR (m.deptnumber = '401008'::"varchar")) OR ((m.deptnumber = '401026'::"varchar") OR (m.deptnumber = '401019'::"varchar"))) 
OR (m.deptnumber = '401900'::"varchar")))) OR ((c.site = 'Lansing'::"varchar") AND (((((m.deptnumber = '30250'::"varchar") OR (m.deptnumber = '30255'::"varchar")) 
OR ((m.deptnumber = '30280'::"varchar") OR (m.deptnumber = '30285'::"varchar"))) OR (((m.deptnumber = '30290'::"varchar") OR (m.deptnumber = '30295'::"varchar")) 
OR ((m.deptnumber = '30300'::"varchar") OR (m.deptnumber = '30305'::"varchar")))) OR ((((m.deptnumber = '30225'::"varchar") OR (m.deptnumber = '30245'::"varchar"))
OR ((m.deptnumber = '30250'::"varchar") OR (m.deptnumber = '30255'::"varchar"))) OR (m.deptnumber = '30270'::"varchar"))))) OR ((c.site = 'Lapeer Region'::"varchar") 
AND ((((m.deptnumber = '20020'::"varchar") OR (m.deptnumber = '23330'::"varchar")) OR ((m.deptnumber = '20410'::"varchar") OR (m.deptnumber = '23340'::"varchar"))) 
OR (m.deptnumber = '23310'::"varchar")))) OR ((c.site = 'Macomb'::"varchar") AND ((((m.deptnumber = '24280'::"varchar") OR (m.deptnumber = '24260'::"varchar"))
OR ((m.deptnumber = '24210'::"varchar") OR (m.deptnumber = '24220'::"varchar"))) OR (((m.deptnumber = '24240'::"varchar") OR (m.deptnumber = '24230'::"varchar")) 
OR ((m.deptnumber = '24270'::"varchar") OR (m.deptnumber = '20010'::"varchar")))))) OR ((c.site = 'Northern'::"varchar") AND ((((m.deptnumber = '3616'::"varchar") OR (m.deptnumber = '3612'::"varchar")) 
OR ((m.deptnumber = '3642'::"varchar") OR (m.deptnumber = '3640'::"varchar"))) OR (m.deptnumber = '3614'::"varchar")))) OR ((c.site = 'Oakland'::"varchar") AND ((((m.deptnumber = '0329'::"varchar") 
OR (m.deptnumber = '0332'::"varchar")) OR ((m.deptnumber = '0333'::"varchar") OR (m.deptnumber = '0340'::"varchar"))) OR ((m.deptnumber = '0341'::"varchar") OR (m.deptnumber = '0335'::"varchar"))))) 
OR ((c.site = 'Port Huron'::"varchar") AND ((((m.deptnumber = '01.6055'::"varchar") OR (m.deptnumber = '01.6025'::"varchar")) OR ((m.deptnumber = '01.6050'::"varchar") 
OR (m.deptnumber = '01.6030'::"varchar"))) OR (m.deptnumber = '01.6035'::"varchar"))))) THEN 1 ELSE NULL::int4 END AS tot_fall_smry, CASE WHEN (((((((c.file_state = 'New'::"varchar") 
OR (c.file_state = 'In-Progress'::"varchar")) OR (c.file_state = 'Closed'::"varchar")) AND ((c.file_status_name <> 'Duplicate'::"varchar") OR (c.file_status_name ISNULL))) 
AND (c.general_event_type = 'Medication/Fluid'::"varchar")) AND ((c.severity >= 'F. Harm - Temporary, Hospitalization Needed'::"varchar") AND (c.severity <> 'Unknown'::"varchar"))) 
AND (((btrim(c."location") <> 'McLaren Bay Special Care'::"varchar") AND (btrim(c."location") <> 'Marwood Nursing & Rehabilitation'::"varchar"))
AND (btrim(c."location") <> 'VitalCare Inc.'::"varchar"))) THEN 1 ELSE NULL::int4 END AS tot_med_errors
FROM (pce_qe16_misc_prd_lnd.prmradmp.safety_detail c 
LEFT JOIN pce_qe16_misc_prd_lnd.prmradmp.safety_master m 
ON (((c.site = m.mhcfacility) AND (c.event_department = m.deptname))))) 
(SELECT harm_events_msr.site, harm_events_msr.person_type, harm_events_msr.incident_id, harm_events_msr.file_id, 
harm_events_msr.file_state, harm_events_msr.file_status_name, harm_events_msr.general_event_type,
harm_events_msr.specific_event_type, harm_events_msr."location", harm_events_msr.department_number,
harm_events_msr.event_department, harm_events_msr.event_specific_location, harm_events_msr.severity,
harm_events_msr.ndnqi_severity, harm_events_msr.event_date, harm_events_msr.entered_date, harm_events_msr.mml_events,
harm_events_msr.tot_rep_events, harm_events_msr.tot_fall_smry, harm_events_msr.tot_med_errors, harm_events_msr.site AS fcy_nm
FROM  harm_events_msr) 
UNION
(SELECT ('MHC'::"varchar")::varchar(20) as site,
harm_events_msr.person_type,
harm_events_msr.incident_id,
harm_events_msr.file_id, 
harm_events_msr.file_state, 
harm_events_msr.file_status_name,
harm_events_msr.general_event_type, harm_events_msr.specific_event_type, harm_events_msr."location", harm_events_msr.department_number, harm_events_msr.event_department,
harm_events_msr.event_specific_location, harm_events_msr.severity, harm_events_msr.ndnqi_severity, harm_events_msr.event_date, harm_events_msr.entered_date, 
harm_events_msr.mml_events, harm_events_msr.tot_rep_events, harm_events_msr.tot_fall_smry, harm_events_msr.tot_med_errors, ('MHC'::"varchar")::varchar(20) AS fcy_nm 
FROM  harm_events_msr WHERE (harm_events_msr.site IN (('Bay'::"varchar")::varchar(20), 
('Central'::"varchar")::varchar(20),
('Flint'::"varchar")::varchar(20), 
('Lansing'::"varchar")::varchar(20), 
('Lapeer'::"varchar")::varchar(20),
('Macomb'::"varchar")::varchar(20),
('Northern'::"varchar")::varchar(20), 
('Oakland'::"varchar")::varchar(20),
('Port Huron'::"varchar")::varchar(20))));
