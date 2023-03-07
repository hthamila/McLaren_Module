--execute sp_dearchive_landing_tbl();

drop table cv_ebcrmember if exists;
CREATE TABLE cv_ebcrmember AS SELECT * FROM pce_qe16_bcbs_prd_lnd..ebcrmember A JOIN (SELECT DISTINCT permemberid,rcrd_btch_audt_id FROM (SELECT *, ROW_NUMBER() OVER(PARTITION BY permemberid ORDER BY rcrd_isrt_ts DESC, rcrd_btch_audt_id DESC) AS rn FROM pce_qe16_bcbs_prd_lnd..ebcrmember) src WHERE src.rn = 1)B USING (permemberid,rcrd_btch_audt_id) DISTRIBUTE ON (permemberid);
