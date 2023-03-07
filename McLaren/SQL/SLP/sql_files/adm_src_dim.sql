CREATE OR REPLACE VIEW pce_qe16_slp_prd_dm..stnd_adm_src_dim AS
SELECT distinct CAST(pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.adm_src_cd as VARCHAR(75)) as adm_src_cd, pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.adm_src_descr,
pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref.audt_sk
FROM pce_qe16_prd_qadv.prmradmp.stnd_adm_src_ref
UNION
SELECT '-100', 'UNKNOWN',100;

