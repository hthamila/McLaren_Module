\set ON_ERROR_STOP ON

DELETE FROM dt_meta;
INSERT INTO dt_meta (descr, val)
VALUES ('roll_yr_strt', '2019-12-01');

INSERT INTO dt_meta (descr, val)
VALUES ('roll_yr_end', '2020-11-30');

INSERT INTO dt_meta (descr,val)
VALUES ('paid_date_mnth', '2020-12-31');

INSERT INTO dt_meta (descr, val)
VALUES ('multi_yr_strt', '2017-10-01');

INSERT INTO dt_meta (descr, val)
VALUES ('multi_yr_end', '2020-11-30');

-- These values are for the CMS assignment file. Set this to the most recent file. For example, for Q3 2016, these values will be '3', '2016', and '2016-09-15' respectively.
INSERT INTO dt_meta (descr, val)
VALUES ('asgnt_qtr', '3');

INSERT INTO dt_meta (descr, val)
VALUEs ('asgnt_yr', '2020');

INSERT INTO dt_meta (descr, val)
VALUEs ('asgnt_dt', '2020-09-15');

--update this view every quater 

DROP TABLE assgn_vw IF EXISTS;
CREATE TABLE assgn_vw AS
SELECT
    string_to_int(substr(RAWTOHEX(hash(member_id, 0)), 17), 16) AS pln_mbr_sk,
    mtw2.member_id,
    MAX(CASE WHEN elig_month BETWEEN '12-01-2019' AND '12-31-2019'
      THEN 1
        ELSE 0 END) AS q1_assign,
    MAX(CASE WHEN elig_month BETWEEN '03-01-2020' AND '03-31-2020'
      THEN 1
        ELSE 0 END) AS q2_assign,
    MAX(CASE WHEN elig_month BETWEEN '06-01-2020' AND '06-30-2020'
      THEN 1
        ELSE 0 END) AS q3_assign,
    MAX(CASE WHEN elig_month BETWEEN '09-01-2020' AND '09-30-2020'
      THEN 1
        ELSE 0 END) AS q4_assign
  FROM pce_qe16_aco_prd_lnd..cv_member_time_windows mtw2
  WHERE mtw2.assignment_indicator = 'Y'
  GROUP BY mtw2.member_id;


\unset ON_ERROR_STOP
