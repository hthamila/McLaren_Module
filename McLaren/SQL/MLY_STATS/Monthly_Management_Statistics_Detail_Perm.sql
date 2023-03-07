\set ON_ERROR_STOP ON;

DROP TABLE pce_qe16_slp_prd_dm..mly_mgmt_stats_detail IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..mly_mgmt_stats_detail AS SELECT * FROM pce_qe16_slp_prd_dm..stg_mly_mgmt_stats_detail;

\unset ON_ERROR_STOP
