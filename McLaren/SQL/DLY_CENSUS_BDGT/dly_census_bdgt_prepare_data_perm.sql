\set ON_ERROR_STOP ON;

DROP TABLE pce_qe16_slp_prd_dm..dailystats_wkday_budget_tr IF EXISTS;

CREATE TABLE pce_qe16_slp_prd_dm..dailystats_wkday_budget_tr AS
SELECT *, CURRENT_TIMESTAMP AS rcrd_isrt_ts from pce_qe16_slp_prd_dm..stg_dailystats_wkday_budget_tr;

\unset ON_ERROR_STOP

