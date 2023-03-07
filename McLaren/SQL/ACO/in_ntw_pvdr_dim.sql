\set ON_ERROR_STOP ON

DROP TABLE in_ntw_pvdr_dim_prev IF EXISTS;
CREATE TABLE in_ntw_pvdr_dim_prev AS SELECT * FROM in_ntw_pvdr_dim;

--Drop the records which has updates
DELETE from in_ntw_pvdr_dim where npi in (
	select npi from pce_qe16_aco_prd_lnd..mclaren_providers 
		where rcrd_src_file_nm like 'MCLAREN_ACO_MHPN_PVDR_%' 
			and rcrd_btch_audt_id in (select max(rcrd_btch_audt_id) from pce_qe16_aco_prd_lnd..mclaren_providers));

--update the records which are inactive(not received in current month file) to previous month last date
update in_ntw_pvdr_dim set actv_sts=0, end_mn=last_day(now()) - date_part('day',last_day(now())) where end_mn is null;
 

--insert new & changed records 
insert into in_ntw_pvdr_dim
(
 		in_ntw_pvdr_sk, npi, frst_nm, last_nm, pvdr_ttl, stff_sts_nm, grp_tin, grp, txnmy_cd, txnmy_desc, affl_cd, rgon, indpnd_ind, strt_mn, end_mn, actv_sts, rcrd_btch_audt_id
)

select 
	string_to_int(substr(RAWTOHEX(hash(trim(mpl.npi), 0)), 17), 16) as in_ntw_pvdr_sk,
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
	case when mpl.actv_sts='0' then mpl.end_date else null end as end_mn, 
	mpl.actv_sts,
	mpl.rcrd_btch_audt_id

from pce_qe16_aco_prd_lnd..mclaren_providers mpl where rcrd_src_file_nm like 'MCLAREN_ACO_MHPN_PVDR_%' and rcrd_btch_audt_id in
 (select max(rcrd_btch_audt_id) from pce_qe16_aco_prd_lnd..mclaren_providers);


update in_ntw_pvdr_dim i set i.rgon=t.region_name from 
(
select * from (select  npi, region_name, row_number() over (partition by npi order by file_mn_dt desc) as rnk from pce_qe16_aco_prd_dm..in_ntw_pvdr_dim intw
        left join pce_qe16_aco_prd_lnd..mhpn_pvdr_lnd m using (npi)
        where intw.rgon is null and m.region_name is not null)a
		where rnk=1
)t
where i.npi=t.npi and i.rgon is null;

generate statistics on in_ntw_pvdr_dim;

\unset ON_ERROR_STOP

