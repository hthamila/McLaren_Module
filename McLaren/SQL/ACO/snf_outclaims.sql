drop table snf_outclaims if exists;
create table snf_outclaims as

with mbi as
(
select member_id, death_date from cv_member_time_windows mtw
        join cv_members using (member_id)
--where mtw.assignment_indicator='Y' --and elig_month='2020-03-15'
group by member_id, death_date
)

select facilitycaseid, member_id, death_date, claimid,prm_fromdate, prm_todate, dischargestatus ,claimlinestatus, sum(paid) paid_amt, sum(prm_admits) prm_admits,
        sum(prm_days) as snf_cst_modl_day_cnt,
        max(userdefnum3) as userdefnum3,
        row_number() over (partition by facilitycaseid order by prm_fromdate,prm_todate, claimlinestatus asc) claim_row_num
from cv_outclaims
join mbi using (member_id)
where prm_line='I31'
group by facilitycaseid, member_id, death_date, claimid,prm_fromdate, prm_todate, claimlinestatus, dischargestatus
order by facilitycaseid, member_id, death_date, claimid,prm_fromdate, prm_todate, claimlinestatus, dischargestatus
distribute on (facilitycaseid);

