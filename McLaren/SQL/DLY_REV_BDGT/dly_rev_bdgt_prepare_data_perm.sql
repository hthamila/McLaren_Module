\set ON_ERROR_STOP ON;

DROP TABLE pce_qe16_slp_prd_dm..dly_rev_bdgt_data IF EXISTS;

CREATE TABLE pce_qe16_slp_prd_dm..dly_rev_bdgt_data AS
SELECT *, CURRENT_TIMESTAMP AS rcrd_isrt_ts from pce_qe16_slp_prd_dm..stg_dly_rev_bdgt_data;

\unset ON_ERROR_STOP
