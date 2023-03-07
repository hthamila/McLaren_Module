\set ON_ERROR_STOP ON;

--Script Location : /ds_data1/DI_PCE_PROD/sql/QE16/DLY_CENSUS_BDGT/dly_census_bdgt_prepare_data.sql

drop table intermediate_dailystats_casevolume_wkday_actual if exists;
drop table intermediate_dailystats_casevolume_wkday_ratio if exists;
drop table intermediate_dailystats_bdgt_wkday_rvu if exists;
drop table intermediate_dailystats_bdgt_wkday_ratio if exists;
drop table dailystats_wkday_budget if exists;

--select * from intermediate_dailystats_casevolume_actual
drop table intermediate_dailystats_casevolume_actual if exists;
create table intermediate_dailystats_casevolume_actual(
        fcy_nm varchar(25), fcy_num varchar(25), yyyymmdd varchar(8), mnth date, dt date, wkday varchar(3), measure varchar(100), qnty double
);

--Discharge Count
insert into intermediate_dailystats_casevolume_actual (fcy_nm, fcy_num, dt, measure, qnty)
        select fcy_nm, fcy_num, dschrg_dt, 'Discharges', count(dschrg_ind)
        from pce_qe16_slp_prd_dm..prd_encntr_anl_fct
        where  in_or_out_patient_ind = 'I'
        and tot_chrg_ind = 1
        and excld_trnsfr_encntr_ind <>1
        and iptnt_encntr_type IN ('Acute', 'Psych', 'Rehab')
        group by fcy_nm, fcy_num, dschrg_dt;

--OP Visits Count
insert into intermediate_dailystats_casevolume_actual (fcy_nm, fcy_num, dt, measure, qnty)
        select eaf.fcy_nm, eaf.fcy_num, cf.service_date, 'OP Visits', count( distinct (eaf.fcy_nm || '-' || eaf.encntr_num) )
        from pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
        inner join pce_qe16_slp_prd_dm..prd_chrg_fct cf on eaf.encntr_num=cf.encntr_num and eaf.fcy_nm=cf.fcy_nm
        where  eaf.in_or_out_patient_ind = 'O' --O for Outpatient
        and eaf.tot_chrg_ind = 1
        and eaf.excld_trnsfr_encntr_ind <>1
        group by eaf.fcy_nm, eaf.fcy_num, cf.service_date;

--Patient Days Budget
insert into intermediate_dailystats_casevolume_actual (fcy_nm, fcy_num, dt, measure, qnty)
        select eaf.fcy_nm, eaf.fcy_num, cf.service_date, 'Patient Days', sum(cf.quantity)
        from pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
        inner join pce_qe16_slp_prd_dm..prd_chrg_fct cf on eaf.encntr_num=cf.encntr_num and eaf.fcy_nm=cf.fcy_nm
        where  substring(cf.revenue_code,1, 3) in ('010','011','012','013','014','015','016','017','020','021')
        and eaf.excld_trnsfr_encntr_ind <>1
        and eaf.iptnt_encntr_type IN ('Acute','Psych','Rehab')
        group by eaf.fcy_nm, eaf.fcy_num, cf.service_date;


--OP Observation Discharges Budget
insert into intermediate_dailystats_casevolume_actual (fcy_nm, fcy_num, dt, measure, qnty)
        select eaf.fcy_nm, eaf.fcy_num, eaf.dschrg_dt , 'OP Observation Discharges', count( distinct (eaf.fcy_num || '-' || eaf.encntr_num) )
        from pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
        where  eaf.in_or_out_patient_ind = 'O'
        and eaf.excld_trnsfr_encntr_ind <>1
        and eaf.tot_chrg_ind = 1
        group by eaf.fcy_nm, eaf.fcy_num, eaf.dschrg_dt order by 1, 2, 3;

--Surgical Cases Budget
insert into intermediate_dailystats_casevolume_actual (fcy_nm, fcy_num, dt, measure, qnty)
        select eaf.fcy_nm, eaf.fcy_num, cf.service_date, 'Surgical Cases', count( distinct (eaf.fcy_nm || '-' || eaf.encntr_num) )
        from pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
        inner join pce_qe16_slp_prd_dm..prd_chrg_fct cf on eaf.encntr_num=cf.encntr_num and eaf.fcy_nm=cf.fcy_nm
        where eaf.excld_trnsfr_encntr_ind <>1
        and (   cf.persp_clncl_smy_descr in ('SURGERY TIME','AMBULATORY SURGERY SERVICES')
                        or cf.raw_chargcode in ('60000001','60000002','60000003','60000004','60000005','60000006','60000007','60000008','60000009','60000010')
                )
        and NOT substring(cf.revenue_code,1, 3) in ('096','097','098')
        group by eaf.fcy_nm, eaf.fcy_num, cf.service_date;

--Emergency Dept(ED) Visits Budget
insert into intermediate_dailystats_casevolume_actual (fcy_nm, fcy_num, dt, measure, qnty)
        select eaf.fcy_nm, eaf.fcy_num, cf.service_date, 'ED Visits', sum( cf.quantity  )
        from pce_qe16_slp_prd_dm..prd_encntr_anl_fct eaf
        inner join pce_qe16_slp_prd_dm..prd_chrg_fct cf on eaf.encntr_num=cf.encntr_num and eaf.fcy_nm=cf.fcy_nm
        where eaf.excld_trnsfr_encntr_ind <>1
        and cf.cpt_code in ('99281','99282','99283','99284','99285','99291')
        and NOT substring(cf.revenue_code,1, 3) in ('096','097','098')
        group by eaf.fcy_nm, eaf.fcy_num, cf.service_date;

--Identify First Day of Month
--select * from intermediate_dailystats_casevolume_daily_actual order by 1, 2, 3;
drop table intermediate_dailystats_casevolume_daily_actual;
create table intermediate_dailystats_casevolume_daily_actual as
        select cvda.fcy_nm, cvda.fcy_num, cdr.frst_day_of_mo "mnth", cvda.dt, cvda.measure, isnull(cvda.qnty, 0) "qnty"
        From intermediate_dailystats_casevolume_actual cvda
        join pce_qe16_slp_prd_dm..cdr_dim_dailystats cdr on cvda.dt=cdr.cdr_dt ;

--Collect Monthly WeekDay Actual Qnantity for Previous Year
drop table intermediate_dailystats_casevolume_wkday_actual if exists;
create table intermediate_dailystats_casevolume_wkday_actual as
        select
                cvp.fcy_nm, cvp.measure, cdr.frst_day_of_mo "mnth",
                CASE WHEN cdr.hol_ind = 1 THEN 'Sun' ELSE day_of_wk_abbr END AS wkday,

                sum(case when isnull(cvma.qnty,0)=0 then 0.01 else cvda.qnty end) wkday_qnty,
                count(cdr.cdr_dt) wkday_cnt,
                sum(case when isnull(cvma.qnty,0)=0 then 0.01 else cvda.qnty end)/count(*) wkday_avg
        from ( select mb.fcy_nm, mb.measure, cdr.mnth "mnth"
                                from (select distinct fcy_nm, measure, mnth from dailystats_monthly_volume_budget) mb
                                join (select distinct frst_day_of_mo "mnth" from pce_qe16_slp_prd_dm..cdr_dim_dailystats) cdr on (mb.mnth - interval '1 year') = cdr.mnth
                ) cvp
        join cdr_dim_dailystats cdr on cdr.frst_day_of_mo = cvp.mnth
        left join intermediate_dailystats_casevolume_daily_actual cvda on cvda.fcy_nm=cvp.fcy_nm and cvda.measure=cvp.measure and cvda.dt=cdr.cdr_dt
        left join (
                select fcy_nm, measure, mnth, sum(qnty) qnty
                from intermediate_dailystats_casevolume_daily_actual
                group by fcy_nm, measure, mnth
        ) cvma on cvma.fcy_nm=cvp.fcy_nm and cvma.measure=cvp.measure and cvma.mnth=cvp.mnth
        group by cvp.fcy_nm, cvp.measure, cdr.frst_day_of_mo, CASE WHEN cdr.hol_ind = 1 THEN 'Sun' ELSE day_of_wk_abbr END
        order by fcy_nm, measure, cdr.frst_day_of_mo, CASE WHEN cdr.hol_ind = 1 THEN 'Sun' ELSE day_of_wk_abbr end;

--Calculate Previous Year WeekDay ratio and average
--Select * from intermediate_dailystats_casevolume_wkday_ratio order by 1, 2, 3, 4;
drop table intermediate_dailystats_casevolume_wkday_ratio if exists;
create table intermediate_dailystats_casevolume_wkday_ratio as
        select cvwda.fcy_nm, cvwda.measure, cvwda.mnth, cvwda.wkday, cvwda.wkday_qnty, cvwda.wkday_cnt, cvwda.wkday_avg,
                case when cvma.wkly_avg>0 then cvwda.wkday_avg/cvma.wkly_avg else 0 end wkday_ratio
        from intermediate_dailystats_casevolume_wkday_actual cvwda
        join (  select fcy_nm, measure, mnth, sum(wkday_avg) wkly_avg
                        from intermediate_dailystats_casevolume_wkday_actual group by fcy_nm, measure, mnth
                ) cvma on cvma.fcy_nm=cvwda.fcy_nm and cvma.measure=cvwda.measure and cvma.mnth=cvwda.mnth
        order by cvwda.fcy_nm, cvwda.measure, cvwda.mnth, cvwda.wkday   desc;

--Calculate WeekDay Budget in rvu units
drop table intermediate_dailystats_bdgt_wkday_rvu if exists;
create table intermediate_dailystats_bdgt_wkday_rvu as
        select wkr.fcy_nm, wkr.measure, bdgt_cdr.mnth, wkr.wkday, bdgt_cdr.wkday_cnt, (bdgt_cdr.wkday_cnt*wkr.wkday_ratio) wkday_rvu
        from  (
                select
                        mb.mnth, cdr.num_of_days_in_mo, cdr.yr_num, cdr.mo_of_yr_nm, cdr.mo_of_yr_num, cdr.frst_day_of_mo,
                        CASE WHEN cdr.hol_ind = 1 THEN 'Sun' ELSE day_of_wk_abbr end as day_of_wk_abbr,
                        count(*) wkday_cnt
                from pce_qe16_slp_prd_dm..cdr_dim_dailystats cdr
                inner join (select distinct mnth from dailystats_monthly_volume_budget) mb on cdr.frst_day_of_mo = mb.mnth
                group by mb.mnth, cdr.num_of_days_in_mo, cdr.yr_num, cdr.mo_of_yr_nm, cdr.mo_of_yr_num, cdr.frst_day_of_mo,
                        CASE WHEN cdr.hol_ind = 1 THEN 'Sun' ELSE day_of_wk_abbr END
                order by
                        cdr.yr_num, cdr.mo_of_yr_nm, cdr.mo_of_yr_num,
                        CASE WHEN cdr.hol_ind = 1 THEN 'Sun' ELSE day_of_wk_abbr END
        ) as bdgt_cdr
        join intermediate_dailystats_casevolume_wkday_ratio wkr on (bdgt_cdr.mnth - interval '1 year') = wkr.mnth and bdgt_cdr.day_of_wk_abbr=wkr.wkday
        order by wkr.fcy_nm, wkr.measure, bdgt_cdr.mnth, wkr.wkday;

--Calculate WeekDay Budget and Weekly Average
drop table intermediate_dailystats_bdgt_wkday_ratio if exists;
create table intermediate_dailystats_bdgt_wkday_ratio as
        select rvu.fcy_nm, rvu.measure, rvu.mnth, rvu.wkday, rvu.wkday_cnt, bdgt.qnty mnth_qnty,
                case when wka.wkly_avg>0 then (rvu.wkday_rvu/wka.wkly_avg) else 0 end bdgt_wkday_ratio,
                case when wka.wkly_avg>0 then (bdgt.qnty*rvu.wkday_rvu/wka.wkly_avg) else 0 end bdgt_wkday_qnty,                                --Ex : qnty for all Sundays of the month
                case when wka.wkly_avg>0 then (bdgt.qnty*rvu.wkday_rvu/(wka.wkly_avg*rvu.wkday_cnt)) else 0 end bdgt_wkday_avg  --Ex : avg qnty for One Sunday in the month
        from intermediate_dailystats_bdgt_wkday_rvu rvu
        join (
                select fcy_nm, measure, mnth, sum(wkday_rvu) wkly_avg from intermediate_dailystats_bdgt_wkday_rvu group by fcy_nm, measure, mnth
        ) as wka on wka.fcy_nm=rvu.fcy_nm and wka.measure=rvu.measure and wka.mnth=rvu.mnth
        join dailystats_monthly_volume_budget bdgt on  bdgt.fcy_nm=rvu.fcy_nm and bdgt.measure=rvu.measure and bdgt.mnth=rvu.mnth;


--Structure/Table to store Daily Budget
drop table dailystats_wkday_budget if exists;
create table dailystats_wkday_budget (
    fcy_nm varchar(25), rpt_dt date, measure varchar(100),
    wkday varchar(3), wkday_cnt int, wkday_budget double, mnth_budget double,
    pyr_wkday_cnt int, pyr_wkday_actual double, pyr_mnth_actual double
);

--Prepare Daily Budget for Each Day of The Month
insert into dailystats_wkday_budget(fcy_nm, rpt_dt, measure, wkday, wkday_cnt, wkday_budget, mnth_budget, pyr_wkday_cnt, pyr_wkday_actual, pyr_mnth_actual)
        select wkr.fcy_nm, bdgt_cdr.cdr_dt "rpt_dt", wkr.measure, wkr.wkday, wkr.wkday_cnt,
                wkr.bdgt_wkday_avg  "wkday_budget", wkr.mnth_qnty  "mnth_budget",
                pyr.wkday_cnt "pyr_wkday_cnt", pyr.wkday_qnty "pyr_wkday_actual", pyrm.mly_qnty "pyr_mnth_actual"
        from intermediate_dailystats_bdgt_wkday_ratio wkr
        join (
                        select bdgt.mnth, cdr.cdr_dt, CASE WHEN cdr.hol_ind = 1 THEN 'Sun' ELSE day_of_wk_abbr end as wkday
                        from pce_qe16_slp_prd_dm..cdr_dim_dailystats cdr
                        inner join (select distinct mnth from dailystats_monthly_volume_budget) bdgt on cdr.frst_day_of_mo = bdgt.mnth
                        order by cdr.cdr_dt
                ) as bdgt_cdr on wkr.mnth=bdgt_cdr.mnth and  wkr.wkday=bdgt_cdr.wkday
        join intermediate_dailystats_casevolume_wkday_actual pyr on
                wkr.fcy_nm=pyr.fcy_nm and  wkr.measure=pyr.measure and wkr.mnth=(pyr.mnth + interval '1 year') and  wkr.wkday=pyr.wkday
        join (select fcy_nm,  measure, mnth, sum(wkday_qnty) mly_qnty from intermediate_dailystats_casevolume_wkday_actual group by fcy_nm,  measure, mnth) pyrm on
                pyr.fcy_nm = pyrm.fcy_nm and pyr.measure=pyrm.measure and pyr.mnth=pyrm.mnth
        order by wkr.fcy_nm, bdgt_cdr.cdr_dt, wkr.measure;

--Transpose Budget By Rows to Columns
--Select * from dailystats_wkday_budget_tr order by 1, 2;
drop table dailystats_wkday_budget_tr if exists;
create table dailystats_wkday_budget_tr as
        select
                fcy_nm,
                rpt_dt,
                sum(case when measure = 'Discharges' then cast(wkday_budget as double) end) as Discharges_Budget,
                sum(case when measure = 'ED Visits' then cast(wkday_budget as double)  end) as ED_Visits_Budget,
                sum(case when measure = 'OP Visits' then cast(wkday_budget as double)  end) as OP_Visits_Budget,
                sum(case when measure = 'OP Observation Discharges' then cast(wkday_budget as double)  end) as Observation_Days_Budget,
                sum(case when measure = 'OP Observation Discharges' then cast(wkday_budget as double)  end) as OP_Observation_Discharges_Budget,
                sum(case when measure = 'Patient Days' then cast(wkday_budget as double)  end) as Patient_Days_Budget,
                sum(case when measure = 'Surgical Cases' then cast(wkday_budget as double)  end) as Surgical_Cases_Budget
        from dailystats_wkday_budget
        group by 1,2
        order by 1,2
        DISTRIBUTE ON (fcy_nm,rpt_dt);

\unset ON_ERROR_STOP

