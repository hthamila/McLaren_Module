drop table temp_bene_pcp_services if exists;
create table temp_bene_pcp_services as select * from bene_pcp_vw;

delete from bene_pcp_attr;			  
insert into bene_pcp_attr
--Rank 1 Physician who are assigned to MHPN
select distinct a.*,
case when a.npi is not null then 1 else 0 end as in_ntw_ind, 'MHPN Physician' as attr
from temp_bene_pcp_services a join pce_qe16_aco_uat_lnd..stg_q_assign b on a.pln_mbr_sk=b.pln_mbr_sk and a.bill_pvdr_sk=b.q_assign_pvdr_sk
where a.rnk=1 and a.npi is not null and a.last_date < coalesce(a.end_mn,to_date('12/31/2099','MM/DD/YYYY')) 
and b.q_assign_pvdr_sk is not null;
--Rank 2 Physician who are assigned to MHPN
insert into bene_pcp_attr
select distinct a.*,
case when a.npi is not null then 1 else 0 end as in_ntw_ind, 'MHPN Physician' as attr
from temp_bene_pcp_services a join pce_qe16_aco_uat_lnd..stg_q_assign b on a.pln_mbr_sk=b.pln_mbr_sk and a.bill_pvdr_sk=b.q_assign_pvdr_sk
where a.rnk=2 and a.npi is not null and a.last_date < coalesce(a.end_mn,to_date('12/31/2099','MM/DD/YYYY')) and a.pln_mbr_sk not in (select pln_mbr_sk from bene_pcp_attr)
and b.q_assign_pvdr_sk is not null ;
--Rank 3 Physician who are assigned to MHPN
insert into bene_pcp_attr
select distinct a.*,
case when a.npi is not null then 1 else 0 end as in_ntw_ind, 'MHPN Physician' as attr
from temp_bene_pcp_services a join pce_qe16_aco_uat_lnd..stg_q_assign b on a.pln_mbr_sk=b.pln_mbr_sk and a.bill_pvdr_sk=b.q_assign_pvdr_sk
where a.rnk=3 and a.npi is not null and a.last_date < coalesce(a.end_mn,to_date('12/31/2099','MM/DD/YYYY')) and a.pln_mbr_sk not in (select pln_mbr_sk from bene_pcp_attr)
and b.q_assign_pvdr_sk is not null ;
--Rank 4 Physician who are assigned to MHPN
insert into bene_pcp_attr
select distinct a.*,
case when a.npi is not null then 1 else 0 end as in_ntw_ind, 'MHPN Physician' as attr
from temp_bene_pcp_services a join pce_qe16_aco_uat_lnd..stg_q_assign b on a.pln_mbr_sk=b.pln_mbr_sk and a.bill_pvdr_sk=b.q_assign_pvdr_sk
where a.rnk=4 and a.npi is not null and a.last_date < coalesce(a.end_mn,to_date('12/31/2099','MM/DD/YYYY')) and a.pln_mbr_sk not in (select pln_mbr_sk from bene_pcp_attr)
and b.q_assign_pvdr_sk is not null ;
--Rank 5 Physician who are assigned to MHPN
insert into bene_pcp_attr
select distinct a.*,
case when a.npi is not null then 1 else 0 end as in_ntw_ind, 'MHPN Physician' as attr
from temp_bene_pcp_services a join pce_qe16_aco_uat_lnd..stg_q_assign b on a.pln_mbr_sk=b.pln_mbr_sk and a.bill_pvdr_sk=b.q_assign_pvdr_sk
where a.rnk=5 and a.npi is not null and a.last_date < coalesce(a.end_mn,to_date('12/31/2099','MM/DD/YYYY')) and a.pln_mbr_sk not in (select pln_mbr_sk from bene_pcp_attr)
and b.q_assign_pvdr_sk is not null ;


