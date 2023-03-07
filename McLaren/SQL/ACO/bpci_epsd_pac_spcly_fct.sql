\set ON_ERROR_STOP ON;

drop table bpci_anchor_temp if exists;
CREATE TABLE bpci_anchor_temp as
	SELECT 
	b.mbr_id_num,
	b.elig_sts,
	b.ccn_id,
	b.fcy_nm,
	b.ms_drg_cd,
	b.ms_drg_descr,
	b.epsd_nm,
	b.svc_fm_dt,
	b.fcy_case_id,
	b.oprg_pvdr_spcly,
	b.oprg_pvdr_nm,
	b.oprg_pvdr_npi,
	b.dschrg_sts_cd,
	b.care_setting_cgy_nm,
	b.frst_clm_id,
	b.epsd_svc_to_dt,
	case when SUM(clfd.paid_amt) is null then b.epsd_paid_amt 
		else b.epsd_paid_amt + SUM(clfd.paid_amt) end AS epsd_paid_amt,
	b.epsd_readm_ind,
	b.nxt_epsd_drg,
	b.nxt_epsd_fm_dt,
	b.nxt_epsd_nm,
	b.prev_epsd_drg,
	b.prev_epsd_to_dt,
	b.prev_epsd_nm
	from (
	SELECT * FROM (
			  SELECT
					 clf.mbr_id_num,
					 clf.elig_sts,
					 clf.ccn_id,
					 clf.fcy_nm,
					 clf.ms_drg_cd,
					 clf.ms_drg_descr,
					 bpci.epsd_nm,
					 clf.svc_fm_dt,
					 clf.fcy_case_id,
					 clf.oprg_pvdr_spcly,
					 clf.oprg_pvdr_nm,
					 clf.oprg_pvdr_npi,
					 clf.dschrg_sts_cd,
					 clf.care_setting_cgy_nm,
					 MIN(clm_id) AS                                 frst_clm_id,
					 MAX(clf.svc_to_dt)                             epsd_svc_to_dt,
					 sum(clf.paid_amt)                              epsd_paid_amt,
					 MAX(readm_unplnd_30_dy_ind)                    epsd_readm_ind,
					 lead(clf.ms_drg_cd)
					 OVER (
						 PARTITION BY clf.mbr_id_num
						 ORDER BY clf.svc_fm_dt, MAX(clf.svc_to_dt) ) nxt_epsd_drg,
					 lead(clf.svc_fm_dt)
					 OVER (
						 PARTITION BY clf.mbr_id_num
						 ORDER BY clf.svc_fm_dt, MAX(clf.svc_to_dt) ) nxt_epsd_fm_dt,
					 lead(bpci.epsd_nm)
					 OVER (
						 PARTITION BY clf.mbr_id_num
						 ORDER BY clf.svc_fm_dt, MAX(clf.svc_to_dt) ) nxt_epsd_nm,
					 lag(clf.ms_drg_cd)
					 OVER (
						 PARTITION BY clf.mbr_id_num
						 ORDER BY clf.svc_fm_dt, MAX(clf.svc_to_dt) ) prev_epsd_drg,
					 lag(MAX(clf.svc_to_dt))
					 OVER (
						 PARTITION BY clf.mbr_id_num
						 ORDER BY clf.svc_fm_dt, MAX(clf.svc_to_dt) ) prev_epsd_to_dt,
					 lag(bpci.epsd_nm)
					 OVER (
						 PARTITION BY clf.mbr_id_num
						 ORDER BY clf.svc_fm_dt, MAX(clf.svc_to_dt) ) prev_epsd_nm

				 FROM
					 clm_line_fct_ds clf
					 INNER JOIN (select cd, cohrt_nm as epsd_nm from val_set_dim where val_set_nm='BPCI') bpci ON clf.ms_drg_cd = bpci.cd
				 WHERE
					 cst_modl_in_ptnt_clm_adm_ind = 1 
				 GROUP BY
					 clf.mbr_id_num,
					 clf.elig_sts,
					 clf.ccn_id,
					 clf.fcy_nm,
					 clf.ms_drg_cd,
					 clf.ms_drg_descr,
					 bpci.epsd_nm,
					 clf.svc_fm_dt,
					 clf.fcy_case_id,
					 clf.oprg_pvdr_spcly,
					 clf.oprg_pvdr_nm,
					 clf.oprg_pvdr_npi,
					 clf.dschrg_sts_cd,
					 clf.care_setting_cgy_nm 
					) a
	WHERE (a.prev_epsd_drg != ms_drg_cd OR days_between(prev_epsd_to_dt, svc_fm_dt) > 90 OR a.prev_epsd_drg IS NULL) 
	) b LEFT OUTER JOIN (select * from clm_line_fct_ds where cst_modl_in_ptnt_clm_adm_ind <> 1) clfd ON b.fcy_case_id = clfd.fcy_case_id  
	    GROUP BY
		b.mbr_id_num,
		b.elig_sts,
		b.ccn_id,
		b.fcy_nm,
		b.ms_drg_cd,
		b.ms_drg_descr,
		b.epsd_nm,
		b.svc_fm_dt,
		b.fcy_case_id,
		b.oprg_pvdr_spcly,
		b.oprg_pvdr_nm,
		b.oprg_pvdr_npi,
		b.dschrg_sts_cd,
		b.care_setting_cgy_nm,
		b.frst_clm_id,
		b.epsd_svc_to_dt,
		b.epsd_paid_amt,
		b.epsd_readm_ind,
		b.nxt_epsd_drg,
		b.nxt_epsd_fm_dt,
		b.nxt_epsd_nm,
		b.prev_epsd_drg,
		b.prev_epsd_to_dt,
		b.prev_epsd_nm;
--------------------------------------------------------	
DROP TABLE bpci_epsd_pac_spcly_fct IF EXISTS;
CREATE Table bpci_epsd_pac_spcly_fct AS
SELECT
	a.mbr_id_num,
	a.elig_sts,
	a.ccn_id,
	a.fcy_nm,
	a.ms_drg_cd,
	a.ms_drg_descr,
	a.epsd_nm,
	a.oprg_pvdr_spcly,
	a.oprg_pvdr_nm,
	a.oprg_pvdr_npi,
	case when pvdr.npi is null then 0 ELSE 1 END as ntw_ind,
	a.dschrg_sts_cd,
	a.svc_fm_dt,
	a.epsd_svc_to_dt,
	a.frst_clm_id,
	a.epsd_paid_amt AS anchr_paid_amt,
	a.epsd_readm_ind,
	
	--(CASE WHEN days_between(b.svc_fm_dt, a.epsd_svc_to_dt) <= 30
	 -- THEN '0 - 30 Days' 
	 -- WHEN days_between(b.svc_fm_dt, a.epsd_svc_to_dt) BETWEEN 31 AND 60
	 -- then '31 - 60 Days'
     -- WHEN days_between(b.svc_fm_dt, a.epsd_svc_to_dt) BETWEEN 61 AND 90
     -- then '61 - 90 Days'	  
	--	END)as post_anch_days
		
	nvl(SUM(CASE WHEN days_between(b.svc_fm_dt, a.epsd_svc_to_dt) <= 30
		THEN paid_amt END), 0) paid_amt_30_day_cnt,
	nvl(SUM(CASE WHEN days_between(b.svc_fm_dt, a.epsd_svc_to_dt) BETWEEN 31 AND 60
		THEN paid_amt END), 0) paid_amt_60_day_cnt,
	nvl(SUM(CASE WHEN days_between(b.svc_fm_dt, a.epsd_svc_to_dt) BETWEEN 61 AND 90
		THEN paid_amt END), 0) paid_amt_90_day_cnt,
	nvl(SUM(CASE WHEN (care_setting_sub_cgy_nm IN ('Home Health', 'Hospice', 'Skilled Nursing Facility')
						OR care_svc_sub_cgy_nm IN ('Long-Term Acute Care', 'Rehabilitation')) AND days_between(b.svc_fm_dt, a.epsd_svc_to_dt) <= 30 THEN paid_amt END), 0) pac_paid_amt_30_day_cnt,
	nvl(SUM(CASE WHEN (care_setting_sub_cgy_nm IN ('Home Health', 'Hospice', 'Skilled Nursing Facility')
						OR care_svc_sub_cgy_nm IN ('Long-Term Acute Care', 'Rehabilitation')) AND days_between(b.svc_fm_dt, a.epsd_svc_to_dt) BETWEEN 31 AND 60 THEN paid_amt END), 0) pac_paid_amt_60_day_cnt,
	nvl(SUM(CASE WHEN (care_setting_sub_cgy_nm IN ('Home Health', 'Hospice', 'Skilled Nursing Facility')
						OR care_svc_sub_cgy_nm IN ('Long-Term Acute Care', 'Rehabilitation')) AND days_between(b.svc_fm_dt, a.epsd_svc_to_dt) BETWEEN 61 AND 90 THEN paid_amt END), 0) pac_paid_amt_90_day_cnt,
	nvl(SUM(CASE WHEN b.care_setting_cgy_nm = 'Skilled Nursing Facility' AND days_between(b.svc_fm_dt, a.epsd_svc_to_dt) <= 90 THEN paid_amt END), 0) paid_amt_snf_total_cnt,
	nvl(SUM(CASE WHEN b.care_setting_cgy_nm = 'Home Health' AND days_between(b.svc_fm_dt, a.epsd_svc_to_dt) <= 90 THEN paid_amt END), 0) paid_amt_hh_total_cnt,
	nvl(SUM(CASE WHEN b.care_setting_cgy_nm = 'Hospital Inpatient (facility and professional)' AND days_between(b.svc_fm_dt, a.epsd_svc_to_dt) <= 90 THEN paid_amt END), 0) paid_amt_ip_total_cnt,
	nvl(SUM(CASE WHEN b.care_setting_cgy_nm = 'Outpatient (facility and professional)' AND days_between(b.svc_fm_dt, a.epsd_svc_to_dt) <= 90 THEN paid_amt END), 0) paid_amt_op_total_cnt,
	nvl(SUM(CASE WHEN b.care_setting_cgy_nm = 'Professional Office/Other' and b.care_svc_sub_cgy_nm='Professional - Office/Other' AND days_between(b.svc_fm_dt, a.epsd_svc_to_dt) <= 90 THEN paid_amt END), 0) paid_amt_prof_total_cnt,
	nvl(SUM(CASE WHEN SUBSTR(b.ccn_id, 3, 1) = '2' AND SUBSTR(b.ccn_id, 4) <= 299 AND days_between(b.svc_fm_dt, a.epsd_svc_to_dt) <= 90 THEN paid_amt_90_day_cnt END), 0) paid_amt_ltch_total_cnt
FROM bpci_anchor_temp a
	LEFT JOIN mcl_pvdr_dim pvdr on a.oprg_pvdr_npi=pvdr.npi
	LEFT JOIN clm_line_fct_ds b ON a.mbr_id_num = b.mbr_id_num AND b.svc_fm_dt > a.epsd_svc_to_dt
	and b.svc_fm_dt < nvl(nxt_epsd_fm_dt,current_date+600)
GROUP BY
		a.mbr_id_num,
		a.elig_sts,
		a.ccn_id,
		a.fcy_nm,
		a.ms_drg_cd,
		a.ms_drg_descr,
		a.oprg_pvdr_spcly,
		a.oprg_pvdr_nm,
		a.oprg_pvdr_npi,
		pvdr.npi,
		a.dschrg_sts_cd,
		a.epsd_nm,
		a.svc_fm_dt,
		a.epsd_svc_to_dt,
		a.frst_clm_id,
		a.nxt_epsd_fm_dt,
		a.epsd_readm_ind,
		a.epsd_paid_amt;

\unset ON_ERROR_STOP
