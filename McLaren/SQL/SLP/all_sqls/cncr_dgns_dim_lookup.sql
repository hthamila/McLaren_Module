SELECT distinct aco.dgns_cd, aco.ccs_dgns_cgy_descr 
FROM cncr_dgns_dim cncr
INNER JOIN pce_ae00_aco_prd_cdr..dgns_ccs_dim aco
on cncr.ccs_dgns_cgy_cd = aco.ccs_dgns_cgy_cd AND aco.dgns_cd = cncr.dgns_cd AND 
aco.dgns_cd_ver = cncr.dgns_cd_ver AND aco.eff_to_dt is NULL; 