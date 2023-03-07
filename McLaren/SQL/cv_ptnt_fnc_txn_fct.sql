\set ON_ERROR_STOP ON;

execute sp_dearchive_landing_tbl();

create temp table batch_id as select coalesce(max(rcrd_btch_audt_id),0) batch_id from pce_qe16_prof_bill_prd_zoom..ptnt_fnc_txn_fct;

create temp table stg_archarge as select * from cv_archarge where rcrd_btch_audt_id > (select batch_id from batch_id);
create temp table stg_arpayment as select * from cv_arpayment where rcrd_btch_audt_id > (select batch_id from batch_id);
create temp table stg_aradjustment as select * from cv_aradjustment where rcrd_btch_audt_id > (select batch_id from batch_id);
--create temp table charge_rule as select chargeid, case when sum(amount)>0 then 1 else null end chrg_gt_zero_ind from cv_archarge group by chargeid;

--Inserting Charge Records

insert into ptnt_fnc_txn_fct (rectype, chargeid, proccd, servicedate, postdate, extractdate, yrmon, trantype, trancd, units, txn_amt, diag, agc, site, sitedesc, location, locationdesc
       , guarnbr, patnbr, ordphysnbr, ordphysname, perfphysnbr, perfphysname, applsource, appliedtype, authnbr, procmodifier, chargestat, copaychargeid, origfc, origfcdesc, currfc
       , currfcdesc, priins, priinscert, secins, secinscert, guarname, patname, patfirst, patmid, patlast, sex, birthdate, finalstatementdate, agedate, msrnbr, chgapplsource
       , resptype, chgsourcetbl, cpt4, chargeproccd, lst_pst_date, chrg_amt, --chrg_gt_zero_ind, 
	rcrd_btch_audt_id)
SELECT  c.rectype
       , c.chargeid
       , c.proccd
       , to_date(c.servicedate,'YYYYMMDD') servicedate
       , to_date(c.postdate,'YYYYMMDD') postdate
       , to_date(c.extractdate,'YYYYMMDD') extractdate
       , to_date(c.yrmon||'01','YYYYMMDD') yrmon
       , c.trantype
       , c.trancd
       , c.units
       , c.amount txn_amt
       , c.diag
       , c.agc
       , c.site
       , c.sitedesc
       , c.location
       , c.locationdesc
       , c.guarnbr
       , c.patnbr
       , c.ordphysnbr
       , c.ordphysname
       , c.perfphysnbr
       , c.perfphysname
       , c.applsource
       , c.appliedtype
       , c.authnbr
       , c.procmodifier
       , c.chargestat
       , c.copaychargeid
       , c.origfc
       , c.origfcdesc
       , c.currfc
       , c.currfcdesc
       , c.priins
       , c.priinscert
       , c.secins
       , c.secinscert
       , c.guarname
       , c.patname
       , c.patfirst
       , c.patmid
       , c.patlast
       , c.sex
       , to_date(case when c.birthdate=0 then null else c.birthdate end,'YYYYMMDD') birthdate
       , to_date(case when c.finalstatementdate=0 then null else c.finalstatementdate end,'YYYYMMDD') finalstatementdate
       , to_date(case when c.agedate=999999999 then null else c.agedate end,'YYYYMMDD') agedate
       , c.msrnbr
       , c.chgapplsource
       , c.resptype
       , c.chgsourcetbl
       , d.cpt4
       , c.proccd as chargeproccd
       , pvdr.lst_pst_date
       , c.amount chrg_amt
       , c.rcrd_btch_audt_id
       
  FROM stg_archarge c
	left join cv_chrg_ptnt_pvdr_fct pvdr on c.chargeid=pvdr.chargeid and c.ServiceDate=pvdr.ServiceDate and c.proccd=pvdr.proccd
        left join prmretlp.cv_cpt4 d on c.proccd=d.proccd
--	left join charge_rule cr on c.chargeid=cr.chargeid
 where c.rcrd_btch_audt_id > (select batch_id from batch_id);

--Inserting Adjustment Records
insert into ptnt_fnc_txn_fct (rectype, chargeid, proccd, servicedate, postdate, receivedate, extractdate, yrmon, trantype, trancd, currfc, currfcdesc, txn_amt, ptnt_adj, insr_adj, agc, site, 
		sitedesc, location, locationdesc, chargeproccd, guarnbr, patnbr, claimnbr, carriernbr, carriername, carriercert, claimtot, applsource, prepaid, prepayunapplied, appliedtype, appliedunapplied, 
		inttabletype, rcrd_btch_audt_id, cpt4, guarname,patname, patfirst, patmid, patlast, sex, birthdate,OrdPhysNbr,PerfPhysNbr,ordphysname,perfphysname,diag,resptype, copaychargeid, chargestat, procmodifier,
		origfc, origfcdesc, lst_pst_date--, chrg_gt_zero_ind
)

SELECT rectype
       , c.chargeid
       , c.proccd
       , to_date(case when c.servicedate=0 then null else c.servicedate end,'YYYYMMDD') servicedate
       , to_date(case when postdate=0 then null else postdate end,'YYYYMMDD') postdate
       , to_date(case when receivedate=0 then null else receivedate end,'YYYYMMDD') receivedate
       , to_date(case when extractdate=0 then null else extractdate end,'YYYYMMDD') extractdate
       , to_date(yrmon||'01','YYYYMMDD') yrmon
       , trantype
       , trancd
       , pvdr.currfc
       , pvdr.currfcdesc
       , amount txn_amt
       , case when c.CurrFc in ('SP', 'ZC', 'ZD', 'ZE') and c.ProcCd not in ('ZAXFRFRM','ZAXFRTO') then amount else null end as ptnt_adj
       , case when c.CurrFc not in ('SP', 'ZC', 'ZD', 'ZE') and c.ProcCd not in ('ZAXFRFRM','ZAXFRTO') then amount else null end as insr_adj
       , agc
       , site
       , sitedesc
       , location
       , locationdesc
       , c.chargeproccd
       , c.guarnbr
       , c.patnbr
       , claimnbr
       , carriernbr
       , carriername
       , carriercert
       , claimtot
       , applsource
       , prepaid
       , prepayunapplied
       , appliedtype
       , appliedunapplied
       , inttabletype
       , rcrd_btch_audt_id
       , d.cpt4
       , guarname
       , patname
       , patfirst
       , patmid
       , patlast
       , sex
       , to_date(case when birthdate=0 then null else birthdate end,'YYYYMMDD') birthdate
       , pvdr.OrdPhysNbr
       , pvdr.PerfPhysNbr
       , pvdr.ordphysname
       , pvdr.perfphysname
       , pvdr.diag
       , pvdr.resptype
       , pvdr.copaychargeid
       , pvdr.chargestat
       , pvdr.procmodifier
       , pvdr.origfc
       , pvdr.origfcdesc
       , pvdr.lst_pst_date
     --  , cr.chrg_gt_zero_ind


  FROM stg_aradjustment c
   left join pce_qe16_prof_bill_prd_zoom..cv_chrg_ptnt_pvdr_fct pvdr on c.chargeid=pvdr.chargeid and c.ServiceDate=pvdr.ServiceDate and c.ChargeProcCd=pvdr.proccd
   left join pce_qe16_prof_bill_prd_zoom..cv_adj_chg_cpt4 d on c.chargeproccd=d.chargeproccd
   --left join charge_rule cr on c.chargeid=cr.chargeid
where c.rcrd_btch_audt_id > (select batch_id from batch_id);

--Inserting Payment Records
insert into ptnt_fnc_txn_fct (rectype, chargeid, proccd, servicedate, postdate, receivedate, extractdate, yrmon, trantype, trancd, currfc, currfcdesc, txn_amt, ptnt_pymt, insr_pymt, ovpmt, agc, site,
        sitedesc, location, locationdesc, chargeproccd, guarnbr, patnbr, claimnbr, carriernbr, carriername, carriercert, claimtot, cob, applsource, prepaid, prepayunapplied, appliedtype,
        appliedunapplied, inttabletype, cpt4, rcrd_btch_audt_id, guarname,patname, patfirst, patmid, patlast, sex, birthdate,OrdPhysNbr,PerfPhysNbr,ordphysname,perfphysname,diag,resptype, copaychargeid, chargestat, procmodifier,
	origfc, origfcdesc, lst_pst_date--, chrg_gt_zero_ind 
	)

SELECT rectype
       , c.chargeid
       , c.proccd
       , to_date(case when c.servicedate=0 then null else c.servicedate end,'YYYYMMDD') servicedate
       , to_date(case when postdate=0 then null else postdate end,'YYYYMMDD') postdate
       , to_date(case when receivedate=0 then null else receivedate end,'YYYYMMDD') receivedate
       , to_date(case when extractdate=0 then null else extractdate end,'YYYYMMDD') extractdate
       , to_date(yrmon||'01','YYYYMMDD') yrmon
       , trantype
       , trancd
       , pvdr.currfc
       , pvdr.currfcdesc
       , amount txn_amt
       , case when c.CurrFc in ('SP', 'ZC', 'ZD', 'ZE') then amount else null end as ptnt_pymt
       , case when c.CurrFc not in ('SP', 'XX', 'ZC', 'ZD', 'ZE') then amount else null end as insr_pymt
       , case when c.CurrFc='XX' then amount else null end as ovpmt
       , agc
       , site
       , sitedesc
       , location
       , locationdesc
       , c.chargeproccd
       , c.guarnbr
       , c.patnbr
       , claimnbr
       , carriernbr
       , carriername
       , carriercert
       , claimtot
       , cob
       , applsource
       , prepaid
       , prepayunapplied
       , appliedtype
       , appliedunapplied
       , inttabletype
       , cpt4
       , rcrd_btch_audt_id
       , guarname
       , patname
       , patfirst
       , patmid
       , patlast
       , sex
       , to_date(case when birthdate=0 then null else birthdate end,'YYYYMMDD') birthdate
       , pvdr.OrdPhysNbr
       , pvdr.PerfPhysNbr
       , pvdr.ordphysname
       , pvdr.perfphysname
       , pvdr.diag
	, pvdr.resptype
	, pvdr.copaychargeid
	, pvdr.chargestat
	, pvdr.procmodifier
       , pvdr.origfc
       , pvdr.origfcdesc
       , pvdr.lst_pst_date
  --     , cr.chrg_gt_zero_ind

  FROM stg_arpayment c
    left join pce_qe16_prof_bill_prd_zoom..cv_chrg_ptnt_pvdr_fct pvdr on c.chargeid=pvdr.chargeid and c.ServiceDate=pvdr.ServiceDate and c.ChargeProcCd=pvdr.proccd
    left join pce_qe16_prof_bill_prd_zoom..cv_adj_chg_cpt4 d on c.chargeproccd=d.chargeproccd
--    left join charge_rule cr on c.chargeid=cr.chargeid
where c.rcrd_btch_audt_id > (select batch_id from batch_id)
;

drop table aggr_chrg_fct if exists;
create table aggr_chrg_fct as
select chargeid 
,case when sum(coalesce(chrg_amt,0))+sum(coalesce(insr_pymt,0))+sum(coalesce(insr_adj,0))+sum(coalesce(ptnt_pymt,0))+sum(coalesce(ptnt_adj,0)) <> 0 then 1 else 0 end as bal_amt_ind
,case when sum(coalesce(chrg_amt,0)) > 0 then 1 else 0 end as chrg_amt_ind
from 
ptnt_fnc_txn_fct
group by chargeid;


\unset ON_ERROR_STOP

