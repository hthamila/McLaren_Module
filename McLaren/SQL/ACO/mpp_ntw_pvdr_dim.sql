\set ON_ERROR_STOP ON

DROP TABLE mpp_ntw_pvdr_dim_prev IF EXISTS;
CREATE TABLE mpp_ntw_pvdr_dim_prev AS SELECT * FROM mpp_ntw_pvdr_dim;

--Drop the records which has updates
DELETE from mpp_ntw_pvdr_dim where npi in 
	(select npi from pce_qe16_aco_prd_lnd..mclaren_providers where rcrd_src_file_nm like 'MCLAREN_ACO_MPP_PVDR_%'
		and rcrd_btch_audt_id in (select max(rcrd_btch_audt_id) from pce_qe16_aco_prd_lnd..mclaren_providers)) ;

--update the records which are inactive(not received in current month file) to previous month last date
update mpp_ntw_pvdr_dim set actv_sts=0, end_mn=last_day(now()) - date_part('day',last_day(now())) where end_mn is null or end_mn > now();
 

--insert new & changed records 
insert into mpp_ntw_pvdr_dim
(
 		pvdr_sk, npi, frst_nm, last_nm, pvdr_ttl, stff_sts_nm, grp_tin, grp, txnmy_cd, txnmy_desc, affl_cd, rgon, indpnd_ind, strt_mn, end_mn, actv_sts, rcrd_btch_audt_id
)

select 
	string_to_int(substr(RAWTOHEX(hash(trim(mpl.npi), 0)), 17), 16) as pvdr_sk,
	trim(mpl.npi) as npi,
 	mpl.first_name,
   	mpl.last_name,
	mpl.title,
	mpl.staff_status,
	mpl.tin,
	mpl.grp,
	mpl.txnmy_cd, 
	mpl.txnmy_desc, 
	mpl.emp_sts,
	mpl.region_name, 
	CASE WHEN mpl.emp_sts='IND' then 1 ELSE 0 end as indpnd_ind,
	mpl.strt_date, 
        mpl.end_date end_mn,
        case when (mpl.actv_sts='1' or (actv_sts is null and end_mn >=now()) or end_mn is null) then 1 else 0 end as actv_sts,
	mpl.rcrd_btch_audt_id

from pce_qe16_aco_prd_lnd..mclaren_providers mpl where npi is not null and rcrd_src_file_nm like 'MCLAREN_ACO_MPP_PVDR_%' and
	rcrd_btch_audt_id in (select max(rcrd_btch_audt_id) from pce_qe16_aco_prd_lnd..mclaren_providers);

generate statistics on mpp_ntw_pvdr_dim;

--Insert final McLaren Table for Combined MHPN & MPP

truncate mcl_pvdr_dim;
insert into mcl_pvdr_dim
(
                pvdr_sk,in_ntw_pvdr_sk,mpp_pvdr_sk,mhpn_indicator,mpp_indicator, npi, frst_nm, last_nm, pvdr_ttl, stff_sts_nm, grp_tin, grp, txnmy_cd, txnmy_desc, affl_cd, rgon, indpnd_ind, strt_mn, end_mn, actv_sts, rcrd_btch_audt_id
)

select coalesce(a.in_ntw_pvdr_sk,b.pvdr_sk)  as pvdr_sk
	,a.in_ntw_pvdr_sk
	,b.pvdr_sk as mpp_pvdr_sk
	,case when a.in_ntw_pvdr_sk is not null then 1
		when (a.in_ntw_pvdr_sk is not null and b.pvdr_sk is not null) then 1
		else 0 end as mhpn_indicator
	,case when (a.in_ntw_pvdr_sk is not null and b.pvdr_sk is not null) then 1
		when b.pvdr_sk is not null then 1
		else 0 end as mpp_indicator        
	,coalesce(a.npi,b.npi) as npi
	,coalesce(a.frst_nm, b.frst_nm) as frst_nm
	,coalesce(a.last_nm, b.last_nm) as last_nm
    	,coalesce(a.pvdr_ttl,b.pvdr_ttl) as pvdr_ttl
    	,coalesce(a.stff_sts_nm,b.stff_sts_nm) as stff_sts_nm
    	,coalesce(a.grp_tin,b.grp_tin) as grp_tin
    	,coalesce(a.grp, b.grp) as grp
    	,coalesce(a.txnmy_cd, b.txnmy_cd) as txnmy_cd
    	,coalesce(a.txnmy_desc, b.txnmy_desc) as txnmy_desc
    	,coalesce(a.affl_cd, b.affl_cd) as affl_cd
    	,coalesce(a.rgon, b.rgon) as rgon
	,coalesce(a.indpnd_ind, b.indpnd_ind) as indpnd_ind
	,coalesce(a.strt_mn,b.strt_mn) as strt_mn
	,coalesce(a.end_mn,b.end_mn) as end_mn
    	,coalesce(a.actv_sts,b.actv_sts) as actv_sts
	,coalesce(a.rcrd_btch_audt_id, b.rcrd_btch_audt_id) as rcrd_btch_audt_id

from in_ntw_pvdr_dim a full join mpp_ntw_pvdr_dim b on a.in_ntw_pvdr_sk=b.pvdr_sk;

generate statistics on mcl_pvdr_dim;


\unset ON_ERROR_STOP

