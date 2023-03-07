\set ON_ERROR_STOP ON;
--Creates temp table with all eligibile encounters with min & max service from/to date 
--Includes the encounters with ACUTE CARE and POST ACUTE with care settings of Hospice, Home Health, Skilled Nursing Facility
CREATE TEMP TABLE all_clm as
SELECT cf.pln_mbr_sk
				,cf.cst_modl_sk
				,cf.fcy_case_id as enc_id
				,cf.svc_fm_dt
				,cf.svc_to_dt
				,'' care_svc_cgy_nm
				,'fcy_case_id-'||cf.fcy_case_id as dmy_encntr_id
				,mdd.ms_drg_cd
				,mdd.ms_drg_descr
				,mdd.ms_drg_mdc_descr
				,mdd.svc_cgy_descr
				,mdd.drg_fam_nm
				,mdd.ms_drg_bsn_line_descr
			FROM clm_line_fct cf
			INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
			INNER JOIN assgn_vw av ON cf.pln_mbr_sk = av.pln_mbr_sk
			LEFT OUTER JOIN ms_drg_dim mdd ON cf.ms_drg_sk = mdd.ms_drg_sk
			WHERE cd.cst_modl_line_cgy_nm = 'FIP' 
				AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
union
select *, 	'' ms_drg_cd,'' ms_drg_descr
				,'' ms_drg_mdc_descr
				,'' svc_cgy_descr
				,'' drg_fam_nm
				,'' ms_drg_bsn_line_descr from 
(select pln_mbr_sk
        ,cst_modl_sk
        ,enc_id
        ,svc_fm_dt
        ,svc_to_dt
        ,care_svc_cgy_nm
        ,dmy_encntr_id from
(select *, row_number() over (partition by pln_mbr_sk,svc_fm_dt order by enc_id desc, svc_to_dt desc) as row from (
select cf.pln_mbr_sk, min(cf.svc_fm_dt) as svc_fm_dt, max(svc_to_dt) svc_to_dt, cf.clm_id as enc_id, cf.cst_modl_sk, cd.care_svc_cgy_nm, 'clm_id-'||cf.clm_id as dmy_encntr_id, 
sum(paid_amt) paid_amt, sum(cf.cst_modl_utlz_cnt) cst_modl_utlz_cnt
FROM clm_line_fct cf
INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
INNER JOIN assgn_vw av ON cf.pln_mbr_sk = av.pln_mbr_sk
where cd.care_svc_cgy_nm ='Home Health'
group by cf.pln_mbr_sk, cf.clm_id, cd.care_svc_cgy_nm, cf.cst_modl_sk
having sum(paid_amt)>0 and sum(cf.cst_modl_utlz_cnt)>0)a)b where row=1
union 
select pln_mbr_sk
        ,cst_modl_sk
        ,enc_id
        ,svc_fm_dt
        ,svc_to_dt
        ,care_svc_cgy_nm
        ,dmy_encntr_id from
(select *, row_number() over (partition by pln_mbr_sk,svc_fm_dt order by enc_id desc, svc_to_dt desc) as row  from 
(select cf.pln_mbr_sk, min(cf.svc_fm_dt) as svc_fm_dt, max(svc_to_dt) svc_to_dt, cf.clm_id as enc_id, cf.cst_modl_sk, cd.care_svc_cgy_nm, 'clm_id-'||cf.clm_id as dmy_encntr_id, 
sum(paid_amt) paid_amt, sum(cf.cst_modl_utlz_cnt) cst_modl_utlz_cnt
FROM clm_line_fct cf
INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
INNER JOIN assgn_vw av ON cf.pln_mbr_sk = av.pln_mbr_sk
where cd.care_svc_cgy_nm ='Hospice' and cf.cst_modl_utlz_cnt>0
group by cf.pln_mbr_sk, cf.clm_id, cd.care_svc_cgy_nm, cf.cst_modl_sk
having sum(paid_amt)>0 and sum(cf.cst_modl_utlz_cnt)>0)a)b where row=1
union 
select distinct  cf.pln_mbr_sk
				,cf.cst_modl_sk
				,cf.fcy_case_id as enc_id
				,min(cf.svc_fm_dt) as svc_fm_dt
				,max(cf.svc_to_dt) as svc_to_dt
				,cd.care_svc_cgy_nm
				,'fcy_case_id-'||cf.fcy_case_id as dmy_encntr_id
FROM clm_line_fct cf
INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
INNER JOIN assgn_vw av ON cf.pln_mbr_sk = av.pln_mbr_sk
INNER JOIN dschrg_sts_dim dsd on cf.dschrg_sts_sk=dsd.dschrg_sts_sk
where cd.care_svc_cgy_nm ='Skilled Nursing Facility' and dsd.dschrg_sts_cd=30
group by cf.pln_mbr_sk, cf.fcy_case_id,cd.care_svc_cgy_nm,cf.cst_modl_sk
having sum(paid_amt)>0)a;

--Cross Join of Encounters with ACUTE CARE & POST ACUTE CARE

CREATE TEMP TABLE indx_inp AS
WITH indx_inp AS (
SELECT cf.pln_mbr_sk
				,cf.mbr_id_num
				,cf.fcy_case_id as enc_id
				,cf.svc_fm_dt
				,cf.svc_to_dt
				,'fcy_case_id-'||cf.fcy_case_id as dmy_encntr_id
				,mdd.ms_drg_cd
				,ms_drg_descr
				,ms_drg_mdc_descr
				,svc_cgy_descr
				,drg_fam_nm
				,ms_drg_bsn_line_descr
			FROM clm_line_fct cf
			INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
			INNER JOIN assgn_vw av ON cf.pln_mbr_sk = av.pln_mbr_sk
			LEFT OUTER JOIN ms_drg_dim mdd ON cf.ms_drg_sk = mdd.ms_drg_sk
			WHERE cd.cst_modl_line_cgy_nm = 'FIP' 
				AND cf.cst_modl_in_ptnt_clm_adm_ind = 1
				)

SELECT distinct ac.pln_mbr_sk,
	   ac.enc_id,
	   ip.mbr_id_num,
	   ac.dmy_encntr_id,
	   ac.care_svc_cgy_nm,
	   ac.svc_fm_dt,
	   ac.svc_to_dt,
	   ac.ms_drg_cd,
	   ac.ms_drg_descr,
	   ac.ms_drg_mdc_descr,
	   ac.svc_cgy_descr,
	   ac.drg_fam_nm,
	   ac.ms_drg_bsn_line_descr
	FROM all_clm ac
INNER JOIN indx_inp ip ON ac.pln_mbr_sk = ip.pln_mbr_sk
--where ip.mbr_id_num='1AA9E45JQ21' ac.enc_id='0054262220224PTA'
;

--Next care settings for Inpatient Settings
CREATE TEMP TABLE nxt_care AS
SELECT DISTINCT pln_mbr_sk
	,mbr_id_num
	,enc_id
	,svc_fm_dt
	,svc_to_dt
	,care_svc_cgy_nm
	,ms_drg_cd
	,ms_drg_descr
	,ms_drg_mdc_descr
	,svc_cgy_descr
	,drg_fam_nm
	,ms_drg_bsn_line_descr
	,lead(enc_id) OVER (
		PARTITION BY pln_mbr_sk ORDER BY svc_to_dt 
		) AS nxt_enc_id
	,lead(dmy_encntr_id) OVER (
		PARTITION BY pln_mbr_sk ORDER BY svc_to_dt 
		) AS nxt_case_adm_id
	,lead(care_svc_cgy_nm) OVER (
		PARTITION BY pln_mbr_sk ORDER BY svc_to_dt
		) AS nxt_care_svc_cgy_nm
	,lead(svc_fm_dt) OVER (
		PARTITION BY pln_mbr_sk ORDER BY svc_to_dt
		) AS nxt_svc_fm_dt
	,lead(svc_to_dt) OVER (
		PARTITION BY pln_mbr_sk ORDER BY svc_to_dt
		) AS nxt_svc_to_dt
FROM indx_inp;

DROP TABLE post_drg_care_svc IF EXISTS;
CREATE TEMP TABLE post_drg_care_svc as SELECT *, days_between(svc_to_dt,nxt_svc_fm_dt) as days_fm_ip FROM nxt_care where care_svc_cgy_nm='' 
and nxt_care_svc_cgy_nm in ('Skilled Nursing Facility','Hospice','Home Health') and days_between(svc_to_dt,nxt_svc_fm_dt) >=0 ;


DROP TABLE post_drg_care_svc_fct IF EXISTS;
CREATE TABLE post_drg_care_svc_fct as
select *  from 
(select 	cf.clm_line_fct_sk
				,cf.pln_mbr_sk
				,cf.cst_modl_sk
				,cf.clm_id
				,cf.fcy_case_id
				,cd.care_svc_cgy_nm
				,cf.svc_fm_dt
				,cf.svc_to_dt
				,pdf.ms_drg_cd as pst_ms_drg_cd
				,pdf.ms_drg_descr as pst_ms_drg_descr
				,pdf.ms_drg_mdc_descr as pst_ms_drg_mdc_descr
				,pdf.svc_cgy_descr as pst_svc_cgy_descr
				,pdf.drg_fam_nm as pst_drg_fam_nm
				,pdf.ms_drg_bsn_line_descr as pst_ms_drg_bsn_line_descr
                                ,mclaren_major_slp_grouping pst_mcl_mjr_slp_grp
                                ,mclaren_service_line pst_mcl_svc_ln
                                ,mclaren_sub_service_line pst_mcl_sub_svc_ln
				,pdf.days_fm_ip
		
FROM clm_line_fct cf
INNER JOIN post_drg_care_svc pdf on cf.pln_mbr_sk=pdf.pln_mbr_sk and cf.clm_id=pdf.nxt_enc_id
INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
INNER JOIN assgn_vw av ON cf.pln_mbr_sk = av.pln_mbr_sk
INNER JOIN ms_drg_dim m on pdf.ms_drg_cd=m.ms_drg_cd
where cd.care_svc_cgy_nm  in ('Hospice') --20143
union 
select 	cf.clm_line_fct_sk
				,cf.pln_mbr_sk
				,cf.cst_modl_sk
				,cf.clm_id
				,cf.fcy_case_id
				,cd.care_svc_cgy_nm
				,cf.svc_fm_dt
				,cf.svc_to_dt
				,pdf.ms_drg_cd as pst_ms_drg_cd
				,pdf.ms_drg_descr as pst_ms_drg_descr
				,pdf.ms_drg_mdc_descr as pst_ms_drg_mdc_descr
				,pdf.svc_cgy_descr as pst_svc_cgy_descr
				,pdf.drg_fam_nm as pst_drg_fam_nm
				,pdf.ms_drg_bsn_line_descr as pst_ms_drg_bsn_line_descr
                                ,mclaren_major_slp_grouping pst_mcl_mjr_slp_grp
                                ,mclaren_service_line pst_mcl_svc_ln
                                ,mclaren_sub_service_line pst_mcl_sub_svc_ln
				,pdf.days_fm_ip
		
FROM clm_line_fct cf
INNER JOIN post_drg_care_svc pdf on cf.pln_mbr_sk=pdf.pln_mbr_sk and cf.clm_id=pdf.nxt_enc_id
INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
INNER JOIN assgn_vw av ON cf.pln_mbr_sk = av.pln_mbr_sk
INNER JOIN ms_drg_dim m on pdf.ms_drg_cd=m.ms_drg_cd
where cd.care_svc_cgy_nm  in ('Home Health')
union
select 	cf.clm_line_fct_sk
				,cf.pln_mbr_sk
				,cf.cst_modl_sk
				,cf.clm_id
				,cf.fcy_case_id
				,cd.care_svc_cgy_nm
				,cf.svc_fm_dt
				,cf.svc_to_dt
				,pdf.ms_drg_cd as pst_ms_drg_cd
				,pdf.ms_drg_descr as pst_ms_drg_descr
				,pdf.ms_drg_mdc_descr as pst_ms_drg_mdc_descr
				,pdf.svc_cgy_descr as pst_svc_cgy_descr
				,pdf.drg_fam_nm as pst_drg_fam_nm
				,pdf.ms_drg_bsn_line_descr as pst_ms_drg_bsn_line_descr
				,mclaren_major_slp_grouping pst_mcl_mjr_slp_grp
				,mclaren_service_line pst_mcl_svc_ln
				,mclaren_sub_service_line pst_mcl_sub_svc_ln
				,pdf.days_fm_ip
		
FROM clm_line_fct cf
INNER JOIN post_drg_care_svc pdf on cf.pln_mbr_sk=pdf.pln_mbr_sk and cf.fcy_case_id=pdf.nxt_enc_id
INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
INNER JOIN assgn_vw av ON cf.pln_mbr_sk = av.pln_mbr_sk
INNER JOIN ms_drg_dim m on pdf.ms_drg_cd=m.ms_drg_cd
where cd.care_svc_cgy_nm  in ('Skilled Nursing Facility')
)a


\unset ON_ERROR_STOP
