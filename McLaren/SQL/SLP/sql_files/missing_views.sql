DROP VIEW pce_qe16_slp_prd_dm..stnd_adm_src_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_adm_src_dim AS
SELECT distinct CAST(pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.adm_src_cd as VARCHAR(75)) as adm_src_cd, pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.adm_src_descr,
pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.audt_sk
FROM pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref
UNION
SELECT '-100', 'UNKNOWN',100;

DROP VIEW pce_qe16_slp_prd_dm..stnd_adm_type_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_adm_type_dim AS
SELECT distinct CAST(pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref.adm_type_cd as VARCHAR(75)) as adm_type_cd,
pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref.adm_type_descr, pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref.audt_sk
FROM pce_qe16_prd_qadv.prmradmp.stnd_adm_type_ref
UNION
SELECT '-100', 'UNKNOWN',100;

DROP VIEW pce_qe16_slp_prd_dm..stnd_ptnt_type_dim;

CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_ptnt_type_dim AS
SELECT pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.stnd_ptnt_type_cd AS std_encntr_type_cd,
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.stnd_ptnt_type_descr AS std_encntr_type_descr,
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.ptnt_type_smy_cd AS encntr_type_smy_cd,
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.skilled_nurse_cd,
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.skilled_nurse_descr,
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.ptnt_type_smy_descr AS encntr_type_smy_descr,
pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref.audt_sk
FROM pce_qe16_prd_qadv.prmradmp.stnd_ptnt_type_ref
UNION
SELECT -100, 'UKNOWN',-100,-100,'UNKNOWN','UNKNOWN',-100;
