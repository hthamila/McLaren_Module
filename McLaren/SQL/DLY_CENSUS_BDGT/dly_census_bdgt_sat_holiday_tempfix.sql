\set ON_ERROR_STOP ON;

--Script Location : /ds_data1/DI_PCE_PROD/sql/QE16/DLY_CENSUS_BDGT/dly_census_bdgt_sat_holiday_tempfix.sql

CREATE OR REPLACE VIEW cdr_dim_dailystats AS
SELECT 
	cdr_dk, cdr_dt, cdr_dt_full_nm, cdr_dt_abbr, yr_num, mo_of_yr_num, mo_of_yr_abbr, mo_of_yr_nm, mo_and_yr_num, 
	mo_and_yr_abbr, mo_and_yr_nm, qtr_of_yr_num, qtr_of_yr_abbr, qtr_and_yr_num, qtr_and_yr_abbr, day_of_yr_num, 
	day_of_mo_num, day_of_wk_num, day_of_wk_abbr, day_of_wk_nm, wk_of_yr_num, wk_of_yr_abbr, wk_and_yr_abbr, 
	prior_yr_num, prior_mo_of_yr_num, prior_mo_of_yr_abbr, prior_mo_of_yr_nm, prior_mo_and_yr_num, prior_mo_and_yr_abbr, 
	prior_mo_and_yr_nm, prior_qtr_of_yr_num, prior_qtr_of_yr_abbr, prior_qtr_and_yr_num, prior_qtr_and_yr_abbr, 
	last_day_of_mo, last_day_of_qtr, last_day_of_yr, last_day_of_wk, frst_day_of_mo, frst_day_of_qtr, frst_day_of_yr, 
	frst_day_of_wk, num_of_days_in_mo, num_of_days_in_qtr, fyr_num, fyr_beg_dt, fyr_end_dt, fsc_qtr_num, fsc_qtr_abbr, 
	fsc_qtr_and_yr_num, fsc_qtr_and_yr_abbr, fsc_qtr_beg_dt, fsc_qtr_end_dt, fsc_mo_num, fsc_mo_abbr, fsc_mo_nm, 
	fsc_mo_beg_dt, fsc_mo_end_dt, prior_fyr_num, prior_fyr_beg_dt, prior_fyr_end_dt, prior_fsc_qtr_num, prior_fsc_qtr_abbr, 
	prior_fsc_qtr_beg_dt, prior_fsc_qtr_end_dt, prior_fsc_mo_num, prior_fsc_mo_abbr, prior_fsc_mo_nm, prior_fsc_mo_beg_dt, 
	prior_fsc_mo_end_dt, fsc_wk_of_yr_num, fsc_wk_of_yr_abbr, fsc_wk_and_yr_abbr, bsn_day_ind, wkend_ind, 
	case 
		when (cdr_dt in ('2021-12-25', '2022-01-01')) then 1
		else case when (day_of_wk_abbr='Sat') then 0  else hol_ind end
	end as hol_ind,
	dt_err_ind, dt_err_descr, adj_prd_ind 
FROM pce_ae00_aco_prd_cdr.prmradmp.cdr_dim;

\unset ON_ERROR_STOP

