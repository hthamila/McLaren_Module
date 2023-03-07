\set ON_ERROR_STOP ON;

DROP TABLE pce_qe16_slp_prd_dm..mly_mgmt_stats IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..mly_mgmt_stats AS SELECT * FROM pce_qe16_slp_prd_dm..stg_mly_mgmt_stats;

\unset ON_ERROR_STOP
