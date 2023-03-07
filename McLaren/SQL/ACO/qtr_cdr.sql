\set ON_ERROR_STOP ON;

delete from qtr_cdr where qtr_end_dt=(
select last_day(val) from pce_qe16_aco_prd_dm..dt_meta where descr='asgnt_dt')

insert into qtr_cdr (qtr_sk,qtr_beg_dt,qtr_end_dt,yr_num, qtr_of_yr_num,qtr_and_yr_abbr)
select qtr_sk+1 as qtr_sk, add_months(qtr_beg_dt,3) qtr_beg_dt, add_months(qtr_end_dt,3) qtr_end_dt, extract(year from add_months(qtr_beg_dt,3)) yr_num, 
extract(quarter from add_months(qtr_beg_dt,3)) qtr_of_yr_num,
extract(year from add_months(qtr_beg_dt,3))||' Q'||extract(quarter from add_months(qtr_beg_dt,3)) qtr_and_yr_abbr

from qtr_cdr where qtr_end_dt in(
select max(qtr_end_dt) from qtr_cdr);

\unset ON_ERROR_STOP
