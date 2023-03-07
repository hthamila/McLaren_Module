\set ON_ERROR_STOP ON;

------------------------------------------------------------------------------------------
--             Adding ED per Case Cost based on Professional ER Visit Claims            --
------------------------------------------------------------------------------------------

DROP TABLE ed_cst_anl_fct if exists;
CREATE TABLE ed_cst_anl_fct AS
with fcy_paid as
(
select fcy_case_id, sum(paid_amt) ed_fcy_paid_amt from clm_line_fct cf
group by fcy_case_id
)

select cf.fcy_case_id,
        ed_fcy_paid_amt,
        case when max(cf.cst_modl_utlz_cnt)=0 then min(cf.cst_modl_utlz_cnt) else max(cf.cst_modl_utlz_cnt) end as ed_dschrg_ind,
        max(clm_id) clm_id
   from clm_line_fct cf
   join fcy_paid using (fcy_case_id)
   join cst_modl_dim c using (cst_modl_sk)
        where cf.cst_modl_line_cd in ('P51b') and fcy_case_id is not null and cst_modl_utlz_type_cd='Visits'
        group by 1,2
        order by fcy_case_id;

------------------------------------------------------------------------------------------------------------------------------------
--             SNF Discharges (Inludes Logic for Discharges 0$ + Deaths whos discharges still shows as Still a Patient            --
------------------------------------------------------------------------------------------------------------------------------------

drop table snf_dschrg_fct if exists;
create table snf_dschrg_fct as
with dt_meta as
(
select val::date as rpt_mnth  from dt_meta where descr='roll_yr_end'
),

dth_over90d_dschrg as
(
select facilitycaseid, member_id, claimid, prm_todate, dischargestatus, snf_cst_modl_day_cnt,
             case when dischargestatus='30' then days_between(prm_todate,rpt_mnth) else null end as days_no_clms,
             case when dischargestatus='30' and death_date<=rpt_mnth then 1
                else null end as dth_no_dschrg_ind,
             case when dischargestatus='30' and days_between(prm_todate,rpt_mnth) >90  then 1
                else null end as ovr90d_no_dschrg_ind
        from (
        select *, row_number() over (partition by facilitycaseid order by prm_fromdate desc, prm_todate desc, claimlinestatus asc) as row_num
   from
        pce_qe16_aco_prd_lnd..snf_outclaims
        join dt_meta on 1=1
)a where row_num=1
),

snf_fcy_paid_amt as
(
select facilitycaseid, sum(paid_amt) paid_amt, sum(snf_cst_modl_day_cnt) snf_cst_modl_day_cnt
--, min(claim_row_num) min_claim_row_num, max(claim_row_num) max_claim_row_num
        from pce_qe16_aco_prd_lnd..snf_outclaims
         group by facilitycaseid
)

select a.facilitycaseid,a.member_id, a.death_date, a.claimid,a.prm_fromdate, a.prm_todate, a.dischargestatus, a.claimlinestatus, a.paid_amt, a.prm_admits, a.claim_row_num,
        case when a.prm_admits in (-1,1) then s.snf_cst_modl_day_cnt else null end as snf_adm_cst_modl_day_cnt,
        case when claimlinestatus='P' and (a.dischargestatus<>'30' or dth_no_dschrg_ind=1) then 1
             when claimlinestatus='R' and (a.dischargestatus<>'30'or dth_no_dschrg_ind=1) then -1
                        else null end as snf_dschrg_ind,
        case when snf_dschrg_ind=1 then s.paid_amt
             when snf_dschrg_ind=-1 then (-1)*s.paid_amt
                else null end as snf_fcy_paid_amt,
        case when snf_dschrg_ind=1 then s.snf_cst_modl_day_cnt
                 --when snf_dschrg_ind=-1 then (-1)*s.snf_cst_modl_day_cnt
                else null end as snf_dschrg_cst_modl_day_cnt
from pce_qe16_aco_prd_lnd..snf_outclaims a
        --join mbi m using (member_id)
        join snf_fcy_paid_amt s using (facilitycaseid)
        left join dth_over90d_dschrg d using (facilitycaseid,claimid)
distribute on (facilitycaseid, claimid);

----------------------------------------------------
--             SNF to FIP Readmission Fact        --
----------------------------------------------------
--All Admissions related to SNF and InPatient
create temp table all_snf_ip_clm as 
(
select 
	cf.pln_mbr_sk,
    	cf.mbr_id_num,
	cf.clm_id,
	cf.fcy_case_id,
	ccnd.ccn_id,
    	ccnd.fcy_nm,
    	dsd.dschrg_sts_cd,
	dsd.dschrg_sts_descr,
    	cd.care_svc_sub_cgy_nm,
    	cd.cst_modl_line_cgy_nm,
	min(cf.svc_fm_dt)                       svc_fm_dt,
   	min(cf.svc_to_dt)                       svc_to_dt,
	sum(cf.cst_modl_in_ptnt_clm_adm_ind)    adm_ind
  FROM clm_line_fct cf
    INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
	LEFT OUTER JOIN ccn_dim ccnd ON ccnd.ccn_sk = cf.ccn_sk
	LEFT OUTER JOIN dschrg_sts_dim dsd ON dsd.dschrg_sts_sk = cf.dschrg_sts_sk
  where cd.cst_modl_line_cgy_nm in ('SNF','FIP')
	group by cf.pln_mbr_sk, cf.mbr_id_num, cf.clm_id, cf.fcy_case_id, ccnd.ccn_id, ccnd.fcy_nm, dsd.dschrg_sts_cd, dsd.dschrg_sts_descr, cd.care_svc_sub_cgy_nm,
	cd.cst_modl_line_cgy_nm
	having adm_ind>0
);

--Only Matched Admissions of SNF
create temp table pac_adm as
(
select distinct ip.pln_mbr_sk,
		pac.clm_id,
		pac.svc_fm_dt,
		pac.svc_to_dt,
		pac.cst_modl_line_cgy_nm

FROM all_snf_ip_clm pac
		inner join all_snf_ip_clm ip 
			on pac.pln_mbr_sk=ip.pln_mbr_sk and 
			   pac.clm_id <> ip.clm_id
	where pac.cst_modl_line_cgy_nm='SNF' AND ip.cst_modl_line_cgy_nm='FIP'
);

drop table snf_ip_readm_fct if exists;

create table snf_ip_readm_fct as
select pln_mbr_sk,
		clm_id,
		svc_fm_dt,
		svc_to_dt,
		cst_modl_line_cgy_nm, 
		readm_clm_id, 
		readm_svc_fm_dt, 
		readm_svc_to_dt, 
		readm_cst_modl_line_cgy_nm, 
		rnk as readm_cnt,
		readm_svc_fm_dt - a.svc_to_dt AS days_to_readm
		from 
(SELECT pc.pln_mbr_sk,
		pc.clm_id,
		pc.svc_fm_dt,
		pc.svc_to_dt,
		pc.cst_modl_line_cgy_nm,
		ip2.clm_id 					AS readm_clm_id,
		ip2.svc_fm_dt 				AS readm_svc_fm_dt,
    	ip2.svc_to_dt 				AS readm_svc_to_dt,
		ip2.cst_modl_line_cgy_nm 	AS readm_cst_modl_line_cgy_nm,
		RANK()
    		OVER ( PARTITION BY pc.clm_id
      				ORDER BY ip2.svc_fm_dt, ip2.svc_to_dt ) rnk
FROM pac_adm pc
		inner join all_snf_ip_clm ip2
		on pc.pln_mbr_sk=ip2.pln_mbr_sk and pc.clm_id <> ip2.clm_id and ip2.svc_fm_dt >=pc.svc_to_dt
	where (ip2.cst_modl_line_cgy_nm = 'FIP'))a WHERE rnk = 1;

----------------------------------------------------------------------
--  Identifying IP Discharge to Preferred SNF vs Non Preferred SNF  --
----------------------------------------------------------------------

drop table ip_dschrg_snf_adm_fct if exists;

create table ip_dschrg_snf_adm_fct as

select b.* 
	,1 ip_dschrg_snf_adm_ind
	,snf_svc_fm_dt - svc_to_dt days_to_snf_adm
	,CASE WHEN (asnf.algn = 'yes' and (snf_svc_fm_dt between asnf.eff_fm_dt and coalesce(asnf.eff_to_dt,'2999-12-31')))
        	THEN 1
        ELSE 0 end as pref_snf_algn_ind
from 
(select mbr_id_num, clm_id, ccn_id ip_ccn_id, fcy_nm ip_fcy_nm, care_svc_sub_cgy_nm ip_care_svc_sub_cgy_nm, cst_modl_line_cgy_nm ip_cst_modl_line_cgy_nm , svc_fm_dt, svc_to_dt, dschrg_sts_descr ip_dschrg_sts_descr, 
	lead(clm_id) over (partition by mbr_id_num order by svc_fm_dt) as snf_clm_id,
	lead(fcy_case_id) over (partition by mbr_id_num order by svc_fm_dt) as snf_fcy_case_id,
	lead(ccn_id) over (partition by mbr_id_num order by svc_fm_dt) as snf_ccn_id,
	lead(fcy_nm) over (partition by mbr_id_num order by svc_fm_dt) as snf_fcy_nm,
	lead(care_svc_sub_cgy_nm) over (partition by mbr_id_num order by svc_fm_dt) as snf_care_svc_sub_cgy_nm,
	lead(cst_modl_line_cgy_nm) over (partition by mbr_id_num order by svc_fm_dt) as snf_cst_modl_line_cgy_nm,
	lead(svc_fm_dt) over (partition by mbr_id_num order by svc_fm_dt) as snf_svc_fm_dt,
	lead(svc_to_dt) over (partition by mbr_id_num order by svc_fm_dt) as snf_svc_to_dt
	from
(select
        cf.mbr_id_num,
        cf.clm_id,
        cf.fcy_case_id,
        ccnd.ccn_id,
        ccnd.fcy_nm,
        dsd.dschrg_sts_cd,
        dsd.dschrg_sts_descr,
        cd.care_svc_sub_cgy_nm,
        cd.cst_modl_line_cgy_nm,
        min(cf.svc_fm_dt)                       svc_fm_dt,
        min(cf.svc_to_dt)                       svc_to_dt,
        sum(cf.cst_modl_in_ptnt_clm_adm_ind)    adm_ind
  FROM clm_line_fct cf
    INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
        LEFT OUTER JOIN ccn_dim ccnd ON ccnd.ccn_sk = cf.ccn_sk
        LEFT OUTER JOIN dschrg_sts_dim dsd ON dsd.dschrg_sts_sk = cf.dschrg_sts_sk
  where cd.cst_modl_line_cgy_nm in ('SNF','FIP')
        group by cf.mbr_id_num, cf.clm_id, cf.fcy_case_id, ccnd.ccn_id, ccnd.fcy_nm, dsd.dschrg_sts_cd, dsd.dschrg_sts_descr, cd.care_svc_sub_cgy_nm,
        cd.cst_modl_line_cgy_nm
        having adm_ind>0)a)b
		left join algn_snf asnf on b.snf_ccn_id=asnf.ccn_id
		where b.ip_cst_modl_line_cgy_nm='FIP' and b.snf_cst_modl_line_cgy_nm='SNF'
			and (snf_svc_fm_dt - svc_to_dt) < 31;


----------------------------------------------------------------------
--  Identifying SNF Discharge to ED Visit within 30 Days            --
----------------------------------------------------------------------

drop table snf_dschrg_ed_adm_fct if exists;

create table snf_dschrg_ed_adm_fct as

select b.*
        ,1 snf_ed_vst_ind
        ,ed_svc_fm_dt - svc_to_dt days_fm_snf_to_ed_vst
from
(select mbr_id_num, clm_id, ccn_id snf_ccn_id, fcy_nm snf_fcy_nm, care_svc_sub_cgy_nm snf_care_svc_sub_cgy_nm, cst_modl_line_cgy_nm snf_cst_modl_line_cgy_nm , svc_fm_dt, svc_to_dt, dschrg_sts_descr snf_dschrg_sts_descr,
        lead(clm_id) over (partition by mbr_id_num order by svc_fm_dt) as ed_clm_id,
        lead(fcy_case_id) over (partition by mbr_id_num order by svc_fm_dt) as ed_fcy_case_id,
        lead(ccn_id) over (partition by mbr_id_num order by svc_fm_dt) as ed_ccn_id,
        lead(fcy_nm) over (partition by mbr_id_num order by svc_fm_dt) as ed_fcy_nm,
        lead(care_svc_sub_cgy_nm) over (partition by mbr_id_num order by svc_fm_dt) as ed_care_svc_sub_cgy_nm,
        lead(cst_modl_line_cgy_nm) over (partition by mbr_id_num order by svc_fm_dt) as ed_cst_modl_line_cgy_nm,
        lead(svc_fm_dt) over (partition by mbr_id_num order by svc_fm_dt) as ed_svc_fm_dt,
        lead(svc_to_dt) over (partition by mbr_id_num order by svc_fm_dt) as ed_svc_to_dt
        from
(select
        cf.mbr_id_num,
        cf.clm_id,
        cf.fcy_case_id,
        ccnd.ccn_id,
        ccnd.fcy_nm,
        dsd.dschrg_sts_cd,
        dsd.dschrg_sts_descr,
        cd.care_svc_sub_cgy_nm,
        cd.cst_modl_line_cgy_nm,
        min(cf.svc_fm_dt)                       svc_fm_dt,
        max(cf.svc_to_dt)                       svc_to_dt,
		sum(paid_amt)							paid_amt
 	FROM clm_line_fct cf
    INNER JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
        LEFT OUTER JOIN ccn_dim ccnd ON ccnd.ccn_sk = cf.ccn_sk
        LEFT OUTER JOIN dschrg_sts_dim dsd ON dsd.dschrg_sts_sk = cf.dschrg_sts_sk
  	where cd.cst_modl_line_cd in ('I31','O11') and dschrg_sts_cd<>'30'
        group by cf.mbr_id_num, cf.clm_id, cf.fcy_case_id, ccnd.ccn_id, ccnd.fcy_nm, dsd.dschrg_sts_cd, dsd.dschrg_sts_descr, cd.care_svc_sub_cgy_nm,
        cd.cst_modl_line_cgy_nm
		having sum(paid_amt)>=0)a)b
    where b.snf_cst_modl_line_cgy_nm='SNF' and b.ed_cst_modl_line_cgy_nm='FOP'
                        and (ed_svc_fm_dt - svc_to_dt) between 0 and 31;

----------------------------------------------------------------------
--  Identifying SNF admission with Inpatient Fracture Discharge     --
----------------------------------------------------------------------

--Extract only Fracture Claims--
CREATE TEMP TABLE fracture_clm AS
SELECT * FROM
(       SELECT clm_id
                ,cd
                ,svc_ln
                ,icd_pcd_4_dgt_cd
                ,icd_pcd_4_dgt_descr
                ,row_number() over (partition by clm_id order by icd_pos_num) as rnk
        FROM    clm_pcd_fct cpf
                JOIN icd_pcd_dim pcd ON cpf.icd_pcd_sk=pcd.icd_pcd_sk
                JOIN svc_hier_dim s ON pcd.icd_pcd_cd=s.cd and svc_ln='Orthopedics' and cd_type='ICD 10 PCS'
)a
WHERE rnk=1
;

--Combine all Inpatient & SNF Claims--
CREATE TEMP TABLE inp_snf_frct_clm AS
SELECT  mbr_id_num
                ,clm_id
                ,min(cf.svc_fm_dt) svc_fm_dt
                ,max(cf.svc_to_dt) svc_to_dt
                ,max(cf.cst_modl_in_ptnt_clm_adm_ind) cst_modl_in_ptnt_clm_adm_ind
                ,icd_pcd_4_dgt_cd
                ,icd_pcd_4_dgt_descr
                ,cst_modl_line_cgy_nm
                ,1 frct_ind
        FROM clm_line_fct cf
                JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
                JOIN fracture_clm cpf USING (clm_id)
        where cd.cst_modl_line_cgy_nm = 'FIP'
GROUP BY mbr_id_num, clm_id, icd_pcd_4_dgt_cd, icd_pcd_4_dgt_descr, cst_modl_line_cgy_nm
HAVING max(cf.cst_modl_in_ptnt_clm_adm_ind)=1

union

SELECT  mbr_id_num
                ,clm_id
                ,min(cf.svc_fm_dt) svc_fm_dt
                ,max(cf.svc_to_dt) svc_to_dt
                ,max(cf.cst_modl_in_ptnt_clm_adm_ind) cst_modl_in_ptnt_clm_adm_ind
                ,''icd_pcd_4_dgt_cd
                ,''icd_pcd_4_dgt_descr
                ,cst_modl_line_cgy_nm
                ,1 frct_ind
    FROM clm_line_fct cf
                JOIN cst_modl_dim cd ON cf.cst_modl_sk = cd.cst_modl_sk
                JOIN dschrg_sts_dim dsd ON cf.dschrg_sts_sk = dsd.dschrg_sts_sk
    WHERE cd.cst_modl_line_cgy_nm = 'SNF' and dsd.dschrg_sts_cd='30'
GROUP BY mbr_id_num,clm_id, icd_pcd_4_dgt_cd, icd_pcd_4_dgt_descr, cst_modl_line_cgy_nm
HAVING max(cf.cst_modl_in_ptnt_clm_adm_ind)=1

;

DROP TABLE ip_dschrg_snf_frct_adm_fct IF EXISTS;
CREATE TABLE ip_dschrg_snf_frct_adm_fct as
SELECT * FROM
(SELECT clm_id, cst_modl_line_cgy_nm ip_cst_modl_line_cgy_nm , svc_fm_dt, svc_to_dt, frct_ind, icd_pcd_4_dgt_cd frct_4dgt_cd, icd_pcd_4_dgt_descr frct_4dgt_descr,
        lead(clm_id) over (partition by mbr_id_num order by svc_fm_dt) as snf_clm_id,
        lead(cst_modl_line_cgy_nm) over (partition by mbr_id_num order by svc_fm_dt) as snf_cst_modl_line_cgy_nm,
        lead(svc_fm_dt) over (partition by mbr_id_num order by svc_fm_dt) as snf_svc_fm_dt,
        lead(svc_to_dt) over (partition by mbr_id_num order by svc_fm_dt) as snf_svc_to_dt
        from inp_snf_frct_clm
)a
        WHERE ip_cst_modl_line_cgy_nm='FIP' and snf_cst_modl_line_cgy_nm='SNF'
        and (snf_svc_fm_dt - svc_to_dt)< 31
;


\unset ON_ERROR_STOP
