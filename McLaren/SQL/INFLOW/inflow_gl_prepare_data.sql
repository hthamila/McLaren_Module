\set ON_ERROR_STOP ON;

--Inflow GL
DROP TABLE pce_qe16_slp_prd_dm..inflow_gl_data IF EXISTS;
CREATE TABLE pce_qe16_slp_prd_dm..inflow_gl_data AS
SELECT DISTINCT
    cd.yr_num AS year,
    cd.mo_of_yr_num AS period,
    gl.entity_code,
    gl.entity_desc,
    gl.acct_unit,
    gl.unit_desc,
    gl.category,
    gl.acct_no,
    gl.acct_desc,
    gl.end_bal,
    gl.site,
    gl.site_desc
FROM
    pce_qe16_slp_prd_dm..gl_data gl
    INNER JOIN
    (
        SELECT
            fyr_num,
            fsc_mo_num,
            yr_num,
            mo_of_yr_num,
            frst_day_of_mo
        FROM
            pce_qe16_slp_prd_dm..cdr_dim
        GROUP BY
            fyr_num,
            fsc_mo_num,
            yr_num,
            mo_of_yr_num,
            frst_day_of_mo
    ) cd
    ON
        gl.year = cd.fyr_num AND
        gl.month = cd.fsc_mo_num
WHERE
     TO_CHAR(frst_day_of_mo,'YYYYMM') = TO_CHAR((CURRENT_DATE - INTERVAL '1 MONTHS'),'YYYYMM')
DISTRIBUTE ON (entity_code)
;

\unset ON_ERROR_STOP
