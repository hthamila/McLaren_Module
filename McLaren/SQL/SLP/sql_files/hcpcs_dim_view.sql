CREATE OR REPLACE VIEW hcpcs_dim AS 
WITH zoom_uniq_hcpcs_cd 
AS (SELECT DISTINCT pce_qe16_oper_prd_zoom.prmradmp.cv_patbill.cpt_code FROM pce_qe16_oper_prd_zoom.prmradmp.cv_patbill
WHERE (pce_qe16_oper_prd_zoom.prmradmp.cv_patbill.cpt_code <> ALL (SELECT pce_ae00_aco_prd_cdr.prmradmp.hcpcs_dim.hcpcs_cd FROM pce_ae00_aco_prd_cdr.prmradmp.hcpcs_dim))) ((SELECT pce_ae00_aco_prd_cdr.prmradmp.hcpcs_dim.hcpcs_sk, pce_ae00_aco_prd_cdr.prmradmp.hcpcs_dim.hcpcs_cd, pce_ae00_aco_prd_cdr.prmradmp.hcpcs_dim.hcpcs_descr, pce_ae00_aco_prd_cdr.prmradmp.hcpcs_dim.hcpcs_descr_long, pce_ae00_aco_prd_cdr.prmradmp.hcpcs_dim.hcpcs_ind FROM pce_ae00_aco_prd_cdr.prmradmp.hcpcs_dim) UNION (SELECT '-100'::int8 AS hcpcs_sk, ('-100'::"varchar")::varchar(80) AS hcpcs_cd, ('UNKNOWN'::"varchar")::varchar(250) AS hcpcs_descr, ('UNKNOWN'::"varchar")::varchar(2048) AS hcpcs_descr_long, 0 AS hcpcs_ind)) UNION (SELECT int8(sqltoolkit.admin.hash4((zoom_uniq_hcpcs_cd.cpt_code)::varchar(5))) AS hcpcs_sk, (zoom_uniq_hcpcs_cd.cpt_code)::varchar(80) AS hcpcs_cd, ('UNKNOWN'::"varchar")::varchar(250) AS hcpcs_descr, ('UNKNOWN'::"varchar")::varchar(2048) AS hcpcs_descr_long, -1 AS hcpcs_ind FROM  zoom_uniq_hcpcs_cd);


