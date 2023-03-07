--NET Reveneue ----------------------------------------NET Revenue Model
--Inpatient

----Qualifiers
----select 'processing table: intermediate_stage_temp_eligible_encntr_data ' as table_processing;
--DROP TABLE intermediate_stage_temp_eligible_encntr_data IF EXISTS;
--	CREATE TABLE intermediate_stage_temp_eligible_encntr_data AS (
--		SELECT DISTINCT ZOOM.company_id
--		,ZOOM.patient_id , ZOOM.inpatient_outpatient_flag ,
--		ZOOM.admission_ts, ZOOM.discharge_ts FROM pce_qe16_oper_prd_zoom..cv_patdisch ZOOM WHERE
----CODE change: Commented discharge_total_charges > 0
----		ZOOM.discharge_total_charges > 0 AND
----CODE Change: Added Discharge ts in the filter based on McLaren's request
--		(cast(ZOOM.admission_ts AS DATE) >= DATE ('2015-10-01') OR cast(ZOOM.discharge_ts AS DATE) >= DATE ('2015-10-01'))
--		);
--SELECT count(*)
--FROM intermediate_stage_temp_eligible_encntr_data;

--select 'processing table:  intermediate_stage_temp_table_all_ip_rows' as table_processing;
DROP TABLE intermediate_stage_temp_table_all_ip_rows IF Exists;
CREATE TABLE intermediate_stage_temp_table_all_ip_rows  As
with all_ip_recs as
(
  select
  X.company_id as fcy_nm,
  X.inpatient_outpatient_flag as in_or_out_patient_ind,
  X.patient_id as encntr_num,
  date(Z.admission_ts) as adm_dt,
  date(Z.discharge_ts) as dschrg_dt,
  X.msdrg_code as ms_drg_cd,
  X.patient_type as ptnt_tp_Cd,
 --CODE CHANGE: 08/31/2020 MLH-581 commenting the following
 X.reimbursement_amount as tot_pymt_amt,
 X.discharge_total_charges as tot_chrg_amt,
  -- FCYPYMT.fcy_pymt_amt  as tot_pymt_amt,
  -- FCYCHRG.fcy_chrg_amt  as tot_chrg_amt,
  X.accountbalance as acct_bal_amt,
    --case when ROUND((X.accountbalance/X.discharge_total_charges * 100),2) <= 10 THEN
    case when abs(X.accountbalance)/X.discharge_total_charges * 100 <= 10 THEN
    --  case when abs(X.accountbalance)/FCYCHRG.fcy_chrg_amt * 100 <= 10 THEN
      'Y'
	  ELSE
	   'N' END as est_acct_paid_ind,
   --  ROUND((X.accountbalance/X.discharge_total_charges),2) as acct_bal_pcnt,
   X.accountbalance/X.discharge_total_charges as acct_bal_pcnt,
   -- X.accountbalance/FCYCHRG.fcy_chrg_amt  as acct_bal_pcnt,
  Y.payor_group1 as src_prim_payor_grp1,
  1 as cnt,
  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
 CASE WHEN ( (date(now()) BETWEEN DATE(year(now())||'-10-01') AND DATE(year(now())||'-12-31')) AND
             (date(Z.discharge_ts) BETWEEN DATE(year(now())||'-10-01') AND DATE(year(now())||'-12-31'))
           )
    THEN
			'FY' || year(now())-1
	ELSE
			Z.fiscal_yr
	END as fiscal_yr
from pce_qe16_oper_prd_zoom..cv_patdisch X
 INNER JOIN pce_qe16_oper_prd_zoom..cv_paymstr Y
 ON Y.company_id = X.company_id and X.primary_payer_code = Y.payer_code
 INNER JOIN intermediate_stage_temp_eligible_encntr_data Z
 on Z.company_id = X.company_id and Z.patient_id = X.patient_id
 --CODE CHANGE : 08/31/2020 MLH-581
 LEFT JOIN  intermediate_stage_chrg_agg_fct FCYCHRG
 on FCYCHRG.encntr_num = X.patient_id AND FCYCHRG.fcy_nm = X.company_id
  --CODE CHANGE : 08/31/2020 MLH-581
 LEFT JOIN  intermediate_stage_ptnt_fnc_txn_agg_fct FCYPYMT
 on FCYPYMT.encntr_num = X.patient_id AND FCYPYMT.fcy_nm = X.company_id
  WHERE
  X.inpatient_outpatient_flag = 'I' and X.discharge_total_charges > 0
 -- AND round(FCYCHRG.fcy_chrg_amt) > 0 -- AND X.company_id !='Lansing'
  --Added Ptnt_tp_Cd Exclusions based on "Derived Net Revenue Reference Documents"
  --Code Change: COmmented as per reqiest from Lisa on 02/06
--  and upper(X.patient_type) NOT in ('LIP','MIP','BSCH','BSCHO','8','C','F','GCLK','LLOV','MCIV','OFCE','OFFICE','OFFICE SERIES','POV','PRO','Z','ZWH')
)
SELECT * FROM all_ip_recs;

--###################################################################
-- CASE A - 'BlueCross','Medicare','Medicaid'
--###################################################################
--Inpatient Payment Ratio only for 'BlueCross','Medicare','Medicaid' (PAID) irrespective of DRG is NULL OR NOT
--select 'processing table:  ip_hist_pymt_ratio_case_a' as table_processing;
DROP TABLE ip_hist_pymt_ratio_case_a IF EXISTS;
CREATE TABLE ip_hist_pymt_ratio_case_a AS
select
 --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       fiscal_yr,
	   fcy_nm,
in_or_out_patient_ind,
X.src_prim_payor_grp1,
'Oct 2016 thru till date' as algorithm_duration,
paid_cases as paid_cases,
tot_pymt_amt as payment,
tot_chrg_amt as charges,
--ROUND(tot_pymt_amt/tot_chrg_amt,2) as pymt_ratio
tot_pymt_amt/tot_chrg_amt as pymt_ratio
FROM
(
	select
	  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       fiscal_yr,
	   fcy_nm,
	   in_or_out_patient_ind ,
	   src_prim_payor_grp1,
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt ) as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--	   ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--       ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_ip_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')
--Code Change : Commente the next line
--AND  cast(dschrg_dt AS DATE) >= '2016-10-01'
--Code Change : UnCommented the next line
and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and  upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
Group by 1,2,3,4) X;

--Inpatient Payment Ratio only for 'BlueCross','Medicare','Medicaid' (PAID) of a DRG and Payor



--select 'processing table:  ip_hist_pymt_ratio_drg_case_a' as table_processing;
DROP TABLE ip_hist_pymt_ratio_drg_case_a IF EXISTS;
CREATE TABLE ip_hist_pymt_ratio_drg_case_a AS
select
--CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
X.fiscal_yr,
X.fcy_nm,
X.src_prim_payor_grp1 ,
'Oct 2016 thru till date' as algorithm_duration,
--CODE change: Commented the next Line
--X.ms_drg_cd,
X.paid_cases as total_paid_cases,
X.sum_drg_wghts,
X.tot_chrg_amt as total_charges,
X.tot_pymt_amt as paid_amount,
X.tot_pymt_amt/X.tot_chrg_amt as pymt_ratio,
--ROUND(X.tot_pymt_amt/X.tot_chrg_amt,2) as pymt_ratio,
X.tot_pymt_amt /X.sum_drg_wghts  as drg_weighted_pmnt_per_case
FROM
(
	select
    --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       fiscal_yr,
	   fcy_nm,
	   in_or_out_patient_ind ,
	   src_prim_payor_grp1,
--CODE change: Commented the next Line
--	   Z.ms_drg_cd,
	   sum(cnt)  as paid_cases ,
	   sum(MSDRG.drg_wght) as sum_drg_wghts,
	   sum(tot_pymt_amt) as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--	   ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--     ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_ip_rows Z
INNER JOIN intermediate_stage_temp_ms_drg_dim_hist MSDRG
on Z.ms_drg_cd = MSDRG.ms_drg_cd
WHERE Z.est_acct_paid_ind ='Y' and Z.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--CODE change: Uncommented the next Line
and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
and MSDRG.drg_wght > 0.000
and Z.ms_drg_cd NOT IN ('-100','999')
and Z.dschrg_dt BETWEEN MSDRG.vld_fm_dt AND MSDRG.vld_to_dt

--CODE change: Commented the next Line
--AND cast(Z.dschrg_dt AS DATE) >= '2016-10-01'
--and cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--CODE change: commented the next Line
--Group by 1,2,3,4
Group by 1,2,3,4
) X;

--CASE A
--select 'processing table: ip_net_rvu_case_a ' as table_processing;
DROP TABLE ip_net_rvu_case_a IF EXISTS;
CREATE TABLE ip_net_rvu_case_a AS
-- Paid Cases i.e Account Balance <= 10%
Select
PAID.* ,
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt), 2) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_ip_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30') --85,855 Records
-- Unpaid Cases with a Drg (i.e Payments would be calculated based on Historical DRG Weights Ratio)
UNION
select UNPD.*,
ROUND(DRGWGHT.drg_wght * PAID.drg_weighted_pmnt_per_case, 2) as est_net_rev_amt
from intermediate_stage_temp_table_all_ip_rows UNPD
LEFT JOIN intermediate_stage_temp_ms_drg_dim_hist DRGWGHT
on UNPD.ms_drg_cd = DRGWGHT.ms_drg_cd
LEFT JOIN ip_hist_pymt_ratio_drg_case_a PAID
on UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  and (UNPD.ms_drg_cd IS NOT NULL  AND  UNPD.ms_drg_cd !='-100' AND UNPD.ms_drg_cd != '999')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
and dschrg_dt BETWEEN DRGWGHT.vld_fm_dt AND DRGWGHT.vld_to_dt
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30') --4578 Unpaid Cases with a Drg
UNION
-- Unpaid Cases without DRg (i.e Payments would be calcualted based on Historical Pymnt Ratio)
select UNPD.*,
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio, 2) as est_net_rev_amt
from intermediate_stage_temp_table_all_ip_rows UNPD
LEFT JOIN ip_hist_pymt_ratio_case_a PAID
on  UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')  and (UNPD.ms_drg_cd IS NULL  OR UNPD.ms_drg_cd = '-100' OR UNPD.ms_drg_cd = '999')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
;
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30');  --26 Records Unpaid Cases without Drg


--###################################################################
-- CASE B - 'Commercial'
--###################################################################
--Inpatient Payment Ratio only for 'Commercial' (PAID) irrespective of DRG is NULL OR NOT
--select 'processing table: ip_hist_pymt_ratio_case_b ' as table_processing;
DROP TABLE ip_hist_pymt_ratio_case_b IF EXISTS;
CREATE TABLE ip_hist_pymt_ratio_case_b AS
select
--CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
X.fiscal_yr,
X.fcy_nm,
in_or_out_patient_ind,
X.src_prim_payor_grp1,
'Oct 2016  thru till date' as algorithm_duration,
paid_cases as paid_cases,
tot_pymt_amt as payment,
tot_chrg_amt as charges,
tot_pymt_amt/tot_chrg_amt as pymt_ratio
--ROUND(tot_pymt_amt/tot_chrg_amt,2) as pymt_ratio
FROM
(
	select
	  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       fiscal_yr,
	   fcy_nm,
	   in_or_out_patient_ind ,
	   src_prim_payor_grp1,
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt )  as  tot_pymt_amt,
       sum(tot_chrg_amt)   as tot_chrg_amt
-- ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
-- ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_ip_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('Other')
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
Group by 1,2,3,4) X;

--select 'processing table: ip_hist_pymt_ratio_drg_case_b' as table_processing;
DROP TABLE ip_hist_pymt_ratio_drg_case_b IF EXISTS;
CREATE TABLE ip_hist_pymt_ratio_drg_case_b AS
select
--CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
X.fiscal_yr,
X.fcy_nm,
X.src_prim_payor_grp1 ,
'Oct 2016 thru till date' as algorithm_duration,
--CODE change: Commented the next Line
--X.ms_drg_cd,
X.paid_cases as total_paid_cases,
X.sum_drg_wghts,
X.tot_chrg_amt as total_charges,
X.tot_pymt_amt as paid_amount,
X.tot_pymt_amt/X.tot_chrg_amt as pymt_ratio,
--ROUND(X.tot_pymt_amt/X.tot_chrg_amt,2) as pymt_ratio,
X.tot_pymt_amt /X.sum_drg_wghts  as drg_weighted_pmnt_per_case
FROM
(
	select
		  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       fiscal_yr,
	   fcy_nm,
	   in_or_out_patient_ind ,
	   src_prim_payor_grp1,
--CODE change: Commented the next Line
--	   Z.ms_drg_cd,
	   sum(cnt)  as paid_cases ,
	   sum(MSDRG.drg_wght) as sum_drg_wghts,
	   sum(tot_pymt_amt ) as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--- ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--  ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_ip_rows Z
INNER JOIN intermediate_stage_temp_ms_drg_dim_hist MSDRG
on Z.ms_drg_cd = MSDRG.ms_drg_cd
WHERE Z.est_acct_paid_ind ='Y' and Z.src_prim_payor_grp1 in ('Other')
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--CODE change: Uncommented the next Line
and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
and MSDRG.drg_wght > 0.000
and Z.ms_drg_cd NOT IN ('-100', '999')
and Z.dschrg_dt BETWEEN MSDRG.vld_fm_dt AND MSDRG.vld_to_dt
--CODE change: Commented the next Line
--and cast(dschrg_dt AS DATE) >= '2016-10-01'
--and cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--CODE change: Commented the next Line
--Group by 1,2,3,4
Group by 1,2,3,4
) X;


--select 'processing table: ip_net_rvu_case_b ' as table_processing;
DROP TABLE ip_net_rvu_case_b IF EXISTS;
CREATE TABLE ip_net_rvu_case_b AS
-- Paid Cases i.e Account Balance <= 10%
Select
PAID.* ,
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt), 2 ) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_ip_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('Other')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30') --7,559 Records
-- Unpaid Cases with a Drg (i.e Payments would be calculated based on Historical DRG Weights Ratio)
UNION
select UNPD.*,
ROUND(DRGWGHT.drg_wght * PAID.drg_weighted_pmnt_per_case, 2)  as est_net_rev_amt
from intermediate_stage_temp_table_all_ip_rows UNPD
LEFT JOIN intermediate_stage_temp_ms_drg_dim_hist DRGWGHT
on UNPD.ms_drg_cd = DRGWGHT.ms_drg_cd
LEFT JOIN ip_hist_pymt_ratio_drg_case_b PAID
on UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('Other')   and (UNPD.ms_drg_cd IS NOT NULL  AND  UNPD.ms_drg_cd !='-100' AND UNPD.ms_drg_cd != '999')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
and dschrg_dt BETWEEN DRGWGHT.vld_fm_dt AND DRGWGHT.vld_to_dt
--and cast(UNPD.dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30') --4578 Unpaid Cases with a Drg
UNION
-- Unpaid Cases without Drg (i.e Payments would be calcualted based on Historical Pymnt Ratio)
SELECT UNPD.*,
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio ,2) as est_net_rev_amt
from intermediate_stage_temp_table_all_ip_rows UNPD
LEFT JOIN ip_hist_pymt_ratio_case_b PAID
on  UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('Other')  and (UNPD.ms_drg_cd IS NULL  OR UNPD.ms_drg_cd = '-100' OR UNPD.ms_drg_cd = '999' )
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
;
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30');  --4 Records Unpaid Cases without Drg

--###################################################################
-- CASE C - 'Domestic'
--###################################################################
--Inpatient Payment Ratio only for 'Domestic' (PAID)
--select 'processing table:  ip_net_rvu_case_c' as table_processing;
DROP TABLE ip_net_rvu_case_c IF EXISTS;
CREATE TABLE ip_net_rvu_case_c AS
select ALLCASES.*,
ROUND(ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio ,2)  as est_net_rev_amt
FROM intermediate_stage_temp_table_all_ip_rows ALLCASES
LEFT JOIN manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Domestic')
AND RATIO.ptnt_cgy= 'Inpatient' and RATIO.payor_group_1 = 'Domestic'
-- AND  cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--AND cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30');
;

--###################################################################
-- CASE D - 'Self Pay'
--###################################################################
--Inpatient Payment Ratio only for 'Self Pay'  (PAID)

--select 'processing table: ip_net_rvu_case_d ' as table_processing;
DROP TABLE ip_net_rvu_case_d IF EXISTS;
CREATE TABLE ip_net_rvu_case_d AS
SELECT
  ALLCASES.fcy_nm,
  ALLCASES.in_or_out_patient_ind,
  ALLCASES.encntr_num,
  ALLCASES.adm_dt,
  ALLCASES.dschrg_dt,
  ALLCASES.ms_drg_cd,
  ALLCASES.ptnt_tp_cd,
  ALLCASES.tot_pymt_amt,
  ALLCASES.tot_chrg_amt,
  ALLCASES.acct_bal_amt,
  CASE WHEN ALLCASES.acct_bal_amt = 0 THEN 'Y' ELSE 'N' END as est_acct_paid_ind,
  (ALLCASES.acct_bal_amt/ALLCASES.tot_chrg_amt) as acct_bal_pcnt,
 -- ROUND((ALLCASES.acct_bal_amt/ALLCASES.tot_chrg_amt),2) as acct_bal_pcnt,
  ALLCASES.src_prim_payor_grp1,
  1 as cnt,
  ALLCASES.fiscal_yr,
  CASE WHEN ALLCASES.acct_bal_amt = 0 THEN
      ROUND(ALLCASES.tot_pymt_amt ,2)
	ELSE
	  ROUND( ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio, 2)
    END AS  est_net_rev_amt
FROM intermediate_stage_temp_table_all_ip_rows ALLCASES
LEFT JOIN manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Self Pay' )
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
AND RATIO.ptnt_cgy= 'Inpatient' and RATIO.payor_group_1 = 'Self Pay' ;


--select 'processing table: ip_encntr_net_rvu ' as table_processing;
DROP TABLE ip_encntr_net_rvu IF EXISTS;
CREATE TABLE ip_encntr_net_rvu as
SELECT * FROM
(select * from ip_net_rvu_case_a UNION
select * from ip_net_rvu_case_b UNION
select * from ip_net_rvu_case_c UNION
select * from ip_net_rvu_case_d
) z
--WHERE cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
;
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))  AND  DATE ('2018-09-30')
--AND fcy_nm != 'Lansing';

----Outpatients
--Outpatient

--select 'processing table:  ip_encntr_net_rvu' as table_processing;
DROP TABLE intermediate_stage_temp_table_all_op_rows IF Exists;
CREATE TABLE intermediate_stage_temp_table_all_op_rows As
with all_op_recs as
(
  select
  X.company_id as fcy_nm,
  X.inpatient_outpatient_flag as in_or_out_patient_ind,
  X.patient_id as encntr_num,
  date(X.admission_ts) as adm_dt,
  date(X.discharge_ts) as dschrg_dt,
  X.msdrg_code as ms_drg_cd,
  X.patient_type as ptnt_tp_Cd,
  --CODE CHANGE : 08/31/2020 MLH-581  commenting the following
  X.reimbursement_amount as tot_pymt_amt,
  X.discharge_total_charges as tot_chrg_amt,
  --FCYPYMT.fcy_pymt_amt  as tot_pymt_amt,
  --FCYCHRG.fcy_chrg_amt  as tot_chrg_amt,
  X.accountbalance as acct_bal_amt,
  --CODE CHANGE : 08/31/2020 MLH-581
  case when (abs(X.accountbalance)/X.discharge_total_charges * 100) <= 10 THEN
  --case when (abs(X.accountbalance)/FCYCHRG.fcy_chrg_amt * 100) <= 10 THEN
  --case when ROUND((X.accountbalance/X.discharge_total_charges * 100),2) <= 10 THEN
      'Y'
	  ELSE
	   'N' END as est_acct_paid_ind,
 --CODE CHANGE: 08/31/2020 MLH-581 commenting the following
 (X.accountbalance/X.discharge_total_charges) as acct_bal_pcnt,
 -- (X.accountbalance/FCYCHRG.fcy_chrg_amt) as acct_bal_pcnt,
--ROUND((X.accountbalance/X.discharge_total_charges),2) as acct_bal_pcnt,
  Y.payor_group1 as src_prim_payor_grp1,
  1 as cnt ,
  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
 CASE WHEN ( (date(now()) BETWEEN DATE(year(now())||'-10-01') AND DATE(year(now())||'-12-31')) AND
             (date(Z.discharge_ts) BETWEEN DATE(year(now())||'-10-01') AND DATE(year(now())||'-12-31'))
           )
    THEN
                        'FY' || year(now())-1
        ELSE
                        Z.fiscal_yr
        END as fiscal_yr
from pce_qe16_oper_prd_zoom..cv_patdisch X
 INNER JOIN pce_qe16_oper_prd_zoom..cv_paymstr Y
 ON Y.company_id = X.company_id and X.primary_payer_code = Y.payer_code
 INNER JOIN intermediate_stage_temp_eligible_encntr_data Z
 on Z.company_id = X.company_id and Z.patient_id = X.patient_id
  --CODE CHANGE : 08/31/2020 MLH-581
 LEFT JOIN  intermediate_stage_chrg_agg_fct FCYCHRG
 on FCYCHRG.encntr_num = X.patient_id AND FCYCHRG.fcy_nm = X.company_id
  --CODE CHANGE : 08/31/2020 MLH-581
 LEFT JOIN  intermediate_stage_ptnt_fnc_txn_agg_fct FCYPYMT
 on FCYPYMT.encntr_num = X.patient_id AND FCYPYMT.fcy_nm = X.company_id
  WHERE
    X.inpatient_outpatient_flag = 'O' and X.discharge_total_charges > 0
-- AND round(FCYCHRG.fcy_chrg_amt) > 0
 --AND X.company_id !='Lansing'
  --Added Ptnt_tp_Cd Exclusions based on "Derived Net Revenue Reference Documents"
 --Code Change : Commented as per the Request from Lisa on 02/06
 --AND   upper(X.patient_type) NOT in ('LIP','MIP','BSCH','BSCHO','8','C','F','GCLK','LLOV','MCIV','OFCE','OFFICE','OFFICE SERIES','POV','PRO','Z','ZWH')
)
SELECT * FROM all_op_recs; --6,047,368


--###################################################################
-- CASE A - 'BlueCross','Medicare','Medicaid'
--###################################################################

--Outpatient Payment Ratio only for 'BlueCross','Medicare','Medicaid' (PAID)
--select 'processing table: op_hist_pymt_ratio_case_a ' as table_processing;
DROP TABLE op_hist_pymt_ratio_case_a IF EXISTS;
CREATE TABLE op_hist_pymt_ratio_case_a AS
select
 --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
X.fiscal_yr,
X.fcy_nm,
X.ptnt_tp_cd ,
X.src_prim_payor_grp1,
'Oct 2016 thru last week' as algorithm_duration,
paid_cases as paid_cases,
tot_pymt_amt as payment,
tot_chrg_amt as charges,
tot_pymt_amt/tot_chrg_amt as pymt_ratio
--ROUND(tot_pymt_amt/tot_chrg_amt,2) as pymt_ratio
FROM
(
	select
	  --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       Z.fiscal_yr,
	   fcy_nm,
	   in_or_out_patient_ind ,
	   ptnt_tp_cd,
	   src_prim_payor_grp1,
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt) as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_op_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--case(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
-- and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
Group by 1,2,3,4,5 ) X;

--'BlueCross','Medicare','Medicaid' Unpaid Encounter (Est.Net Revenue Amount) Union Paid Encounter
--select 'processing table:  op_net_rvu_case_a' as table_processing;
DROP TABLE op_net_rvu_case_a IF EXISTS;
CREATE TABLE op_net_rvu_case_a AS
--Unpaid Cases
select UNPD.*,
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio ,2) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows UNPD
LEFT JOIN op_hist_pymt_ratio_case_a PAID
on UNPD.ptnt_tp_cd  = PAID.ptnt_tp_cd and UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr  --CODE CHANGE : AUG 2019
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')
--AND cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
UNION
--Paid Cases
select PAID.*,
--PAID.tot_pymt_amt +  PAID.acct_bal_amt  as est_net_rev_amt
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt) ,2 ) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('BlueCross','Medicare','Medicaid')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
;
--;cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30'); --1,505,622

--###################################################################
-- CASE B - 'Commercial' (Use 'Other'' for now)
--###################################################################
--Outpatient Payment Ratio only for 'Commercial' (Use 'Other'' for now) (PAID)
--select 'processing table: op_hist_pymt_ratio_case_b ' as table_processing;
DROP TABLE op_hist_pymt_ratio_case_b IF EXISTS;
CREATE TABLE op_hist_pymt_ratio_case_b AS
select
--CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
X.fiscal_yr,
X.fcy_nm,
X.ptnt_tp_cd,
X.src_prim_payor_grp1,
'Oct 2016  thru till date' as algorithm_duration,
paid_cases as paid_cases,
tot_pymt_amt as payment,
tot_chrg_amt as charges,
tot_pymt_amt/tot_chrg_amt as pymt_ratio
--ROUND(tot_pymt_amt/tot_chrg_amt,2) as pymt_ratio
FROM
(
	select
     --CODE CHANGE : AUG 2019 ; Adding Fiscal Yr in order to calculate the Unpaid Encounters based on Paid Cases
       Z.fiscal_yr,
	   fcy_nm,
	   in_or_out_patient_ind ,
	   ptnt_tp_cd,
	   src_prim_payor_grp1,
	   sum(cnt)  as paid_cases ,
	   sum(tot_pymt_amt)  as  tot_pymt_amt,
       sum(tot_chrg_amt) as tot_chrg_amt
--ROUND(sum(tot_pymt_amt ) ,2) as  tot_pymt_amt,
--ROUND(sum(tot_chrg_amt),2) as tot_chrg_amt
FROM intermediate_stage_temp_table_all_op_rows Z
WHERE est_acct_paid_ind ='Y' and src_prim_payor_grp1 in ('Other')
--Code Change : 08/08 As per McLaren Requet adding patient_Type exclusion
and upper(Z.ptnt_tp_Cd) NOT in ('LIP','MIP')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
Group by 1,2,3 ,4,5
--Group by 1,2
) X;

--'Commercial' Unpaid Encounter (Est.Net Revenue Amount) Union Paid Encounter
--select 'processing table: op_net_rvu_case_b ' as table_processing;
DROP TABLE op_net_rvu_case_b IF EXISTS;
CREATE TABLE op_net_rvu_case_b AS
select UNPD.*,
ROUND(UNPD.tot_chrg_amt * PAID.pymt_ratio,2 )  as est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows UNPD
LEFT JOIN op_hist_pymt_ratio_case_b PAID
on UNPD.ptnt_tp_cd  = PAID.ptnt_tp_cd and UNPD.src_prim_payor_grp1 = PAID.src_prim_payor_grp1 and UNPD.fcy_nm = PAID.fcy_nm and UNPD.fiscal_yr = PAID.fiscal_yr
WHERE UNPD.est_acct_paid_ind ='N' and UNPD.src_prim_payor_grp1 in ('Other')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
UNION
select PAID.*,
--PAID.tot_pymt_amt +  PAID.acct_bal_amt  as est_net_rev_amt
ROUND(PAID.tot_pymt_amt +  abs(PAID.acct_bal_amt) ,2 ) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows PAID
WHERE PAID.est_acct_paid_ind ='Y' and PAID.src_prim_payor_grp1 in ('Other')
--and cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
;


--###################################################################
-- CASE C - 'Domestic'
--###################################################################
--Outpatient Payment Ratio only for 'Domestic' (PAID)
--select 'processing table: op_net_rvu_case_c ' as table_processing;
DROP TABLE op_net_rvu_case_c IF EXISTS;
CREATE TABLE op_net_rvu_case_c AS
select ALLCASES.*,
ROUND(ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio ,2) as est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows ALLCASES
LEFT JOIN manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Domestic')
AND RATIO.ptnt_cgy= 'Outpatient' and RATIO.payor_group_1 = 'Domestic'
--AND cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--AND cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
;
;

--###################################################################
-- CASE D - 'Self Pay'
--###################################################################
--Outpatient Payment Ratio only for 'Self Pay'  (PAID)
--select 'processing table:  op_net_rvu_case_d' as table_processing;
DROP TABLE op_net_rvu_case_d IF EXISTS;
CREATE TABLE op_net_rvu_case_d AS
SELECT
  ALLCASES.fcy_nm,
  ALLCASES.in_or_out_patient_ind,
  ALLCASES.encntr_num,
  ALLCASES.adm_dt,
  ALLCASES.dschrg_dt,
  ALLCASES.ms_drg_cd,
  ALLCASES.ptnt_tp_cd,
  ALLCASES.tot_pymt_amt,
  ALLCASES.tot_chrg_amt,
  ALLCASES.acct_bal_amt,
  CASE WHEN ALLCASES.acct_bal_amt = 0 THEN 'Y' ELSE 'N' END as est_acct_paid_ind,
  ALLCASES.acct_bal_amt/ALLCASES.tot_chrg_amt as acct_bal_pcnt,
 --ROUND((ALLCASES.acct_bal_amt/ALLCASES.tot_chrg_amt),2) as acct_bal_pcnt,
  ALLCASES.src_prim_payor_grp1,
  1 as cnt,
  ALLCASES.fiscal_yr,
  CASE WHEN ALLCASES.acct_bal_amt = 0 THEN
      ROUND(ALLCASES.tot_pymt_amt ,2)
	ELSE
	  ROUND( ALLCASES.tot_chrg_amt * RATIO.pymt_to_chrg_ratio ,2 )
    END AS  est_net_rev_amt
FROM intermediate_stage_temp_table_all_op_rows ALLCASES
LEFT JOIN manual_pymt_chrg_ratio RATIO
on RATIO.payor_group_1  = ALLCASES.src_prim_payor_grp1 AND RATIO.company_id = ALLCASES.fcy_nm
WHERE ALLCASES.src_prim_payor_grp1 in ('Self Pay' )
AND RATIO.ptnt_cgy= 'Outpatient' and RATIO.payor_group_1 = 'Self Pay'
--AND cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now())) ;
--AND cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
;

----Gross Revenue and Net Revenue (Revenue Model - Outpatient All 4 Cases/Scenario for the period October 2017 - September 2018 )

--select 'processing table: op_net_rvu_model ' as table_processing;
DROP TABLE op_net_rvu_model IF EXISTS;
CREATE  TABLE op_net_rvu_model AS
select fcy_nm,
sum(tot_chrg_amt) as grs_rev_amt ,
ROUND(sum(est_net_rev_amt),2) as drvd_net_rev_amt
--ROUND(sum(tot_chrg_amt),2) as grs_rev_amt ,
--ROUND(sum(est_net_rev_amt),2) as drvd_net_rev_amt
-- to_char(sum(tot_chrg_amt), '$999G999G999G999D99') as grs_rev_amt ,
--to_char(sum(est_net_rev_amt),'$999G999G999G999D99')  as drvd_net_rev_amt
FROM
(select * from op_net_rvu_case_a UNION
select * from op_net_rvu_case_b UNION
select * from op_net_rvu_case_c UNION
select * from op_net_rvu_case_d) Z
--WHERE cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--and Z.fcy_nm != 'Lansing'
group by fcy_nm;

--select 'processing table:  op_encntr_net_rvu' as table_processing;
DROP TABLE op_encntr_net_rvu IF EXISTS;
CREATE TABLE op_encntr_net_rvu as
SELECT * FROM
(select * from op_net_rvu_case_a UNION
select * from op_net_rvu_case_b UNION
select * from op_net_rvu_case_c UNION
select * from op_net_rvu_case_d
) z
--WHERE cast(dschrg_dt AS DATE) BETWEEN (Select DATE(Fiscal_start) - Interval '1 year' from intermediate_stage_temp_fiscal_year_tbl) and (now()- Day(Now()))
;
--cast(dschrg_dt AS DATE) BETWEEN DATE ('2017-10-01') AND  DATE ('2018-09-30')
--AND fcy_nm != 'Lansing';

--Resultant Table

--select 'processing table: ip_dept_revenue_charges ' as table_processing;
DROP TABLE ip_dept_revenue_charges IF EXISTS;
CREATE TABLE ip_dept_revenue_charges AS
select T.fcy_nm, T.encntr_num ,T.tot_pymt_amt,T.tot_chrg_amt, T.acct_bal_amt, sum(C.total_charge) as dept_or_revenue_total_charge_amt
,CASE WHEN sum(C.total_charge) > 0 THEN 'Y' ELSE 'N' END as prof_chrg_ind
FROM intermediate_stage_temp_table_all_ip_rows T
INNER JOIN  intermediate_stage_chrg_fct C
on C.company_id = T.fcy_nm and T.encntr_num = C.patient_id
WHERE (
--Department Exclusion
 C.dept in ('01.4405','01.4442','01.4444','01.4420','01.3175','01.3157','01.4412','01.4413','01.4416','01.4418','01.4419','01.4425')
 OR
--Revenue Code Exclusion
C.revenue_code in ('0960','0961','0969','0972','0977','0982','0983','0985','0987','0990')
)
group by 1,2,3, 4,5;

--select 'processing table: op_dept_revenue_charges ' as table_processing;
DROP TABLE op_dept_revenue_charges IF EXISTS;
CREATE TABLE op_dept_revenue_charges AS
select T.fcy_nm, T.encntr_num ,T.tot_pymt_amt,T.tot_chrg_amt, T.acct_bal_amt, sum(C.total_charge) as dept_or_revenue_total_charge_amt
,CASE WHEN sum(C.total_charge) > 0 THEN 'Y' ELSE 'N' END as prof_chrg_ind
FROM intermediate_stage_temp_table_all_op_rows T
INNER JOIN  intermediate_stage_chrg_fct C
on C.company_id = T.fcy_nm and T.encntr_num = C.patient_id
WHERE (
--Department Exclusion
 C.dept in ('01.4405','01.4442','01.4444','01.4420','01.3175','01.3157','01.4412','01.4413','01.4416','01.4418','01.4419','01.4425')
 OR
--Revenue Code Exclusion
C.revenue_code in ('0960','0961','0969','0972','0977','0982','0983','0985','0987','0990')
)
group by 1,2,3, 4,5;

--select 'processing table:  intermediate_stage_encntr_net_rvu_fct_x' as table_processing;
DROP TABLE intermediate_stage_encntr_net_rvu_fct_x IF EXISTS;
CREATE TABLE intermediate_stage_encntr_net_rvu_fct_x AS
with combined as
(select * from op_encntr_net_rvu
UNION
select * from ip_encntr_net_rvu),
prof_chrg_combined as
(
 select * from ip_dept_revenue_charges
UNION
select * from op_dept_revenue_charges
)
SELECT X.company_id as src_fcy_nm, X.patient_id as src_encntr_num,
--SELECT
-- X.fcy_nm
--,X.fcy_num
--,X.encntr_num
--,Y.est_acct_paid_ind
--,ROUND(Y.est_net_rev_amt, 2) as est_net_rev_amt
Y.*
,nvl(Z.prof_chrg_ind, 'N') as prof_chrg_ind
FROM intermediate_stage_temp_eligible_encntr_data X
LEFT JOIN combined Y
on X.company_id = Y.fcy_nm and X.patient_id   = Y.encntr_num
LEFT JOIN prof_chrg_combined Z
on X.company_id = Z.fcy_nm and X.patient_id   = Z.encntr_num;


--select 'processing table: intermediate_stage_encntr_net_rvu_fct ' as table_processing;
DROP TABLE intermediate_stage_encntr_net_rvu_fct IF EXISTS;
CREATE TABLE intermediate_stage_encntr_net_rvu_fct AS
with combined as
(select * from op_encntr_net_rvu
UNION
select * from ip_encntr_net_rvu),
prof_chrg_combined as
(
 select * from ip_dept_revenue_charges
UNION
select * from op_dept_revenue_charges
)
--SELECT X.fcy_nm as src_fcy_nm, X.fcy_num as src_fcy_num, X.encntr_num as src_encntr_num,
SELECT
 X.company_id as fcy_nm
,X.patient_id as encntr_num
,Y.est_acct_paid_ind
,ROUND(Y.est_net_rev_amt ,2) as est_net_rev_amt
--,ROUND(Y.est_net_rev_amt,2) as est_net_rev_amt
,nvl(Z.prof_chrg_ind, 'N') as prof_chrg_ind
--Y.*
FROM intermediate_stage_temp_eligible_encntr_data X
LEFT JOIN combined Y
on X.company_id = Y.fcy_nm and X.patient_id   = Y.encntr_num
LEFT JOIN prof_chrg_combined Z
on X.company_id = Z.fcy_nm and X.patient_id   = Z.encntr_num;

--Combining all the Intermediate table

--select 'processing table: intermediate_stage_hist_pymt_ratio ' as table_processing;
DROP TABLE intermediate_stage_hist_pymt_ratio IF EXISTS;
CREATE TABLE intermediate_stage_hist_pymt_ratio as
select 'INPATIENT  - Medicare, Medicaid, BSBS' as scenario,  * from ip_hist_pymt_ratio_case_a UNION
select 'OUTPATIENT - Medicare, Medicaid, BSBS' as scenario,  * from op_hist_pymt_ratio_case_a UNION
select 'INPATIENT  - Others' as scenario, * from ip_hist_pymt_ratio_case_b  UNION
select 'OUTPATIENT - Others, Medicaid, BSBS' as scenario,* from op_hist_pymt_ratio_case_b;

--select 'processing table: intermediate_stage_hist_pymt_ratio_drg_wghts ' as table_processing;
DROP TABLE intermediate_stage_hist_pymt_ratio_drg_wghts IF EXISTS;
CREATE TABLE intermediate_stage_hist_pymt_ratio_drg_wghts as
select 'INPATIENT  - Medicare, Medicaid, BSBS' as scenario,  * from ip_hist_pymt_ratio_drg_case_a UNION
select 'INPATIENT  - Others' as scenario,  * from ip_hist_pymt_ratio_drg_case_b;

--select 'processing table: intermediate_stage_net_rvu_model ' as table_processing;
DROP TABLE intermediate_stage_net_rvu_model IF EXISTS;
CREATE TABLE intermediate_stage_net_rvu_model as
select 'INPATIENT  - Medicare, Medicaid, BSBS' as scenario, * from ip_net_rvu_case_a UNION
select 'INPATIENT  - Others' as scenario, * from ip_net_rvu_case_b UNION
select 'INPATIENT  - Domestic' as scenario, * from ip_net_rvu_case_c UNION
select 'INPATIENT  - Self-Pay' as scenario, * from ip_net_rvu_case_d UNION
select 'OUTPATIENT  - Medicare, Medicaid, BSBS' as scenario, * from op_net_rvu_case_a UNION
select 'OUTPATIENT  - Others' as scenario, * from op_net_rvu_case_b UNION
select 'OUTPATIENT  - Domestic' as scenario, * from op_net_rvu_case_c UNION
select 'OUTPATIENT  - Self-Pay' as scenario, * from op_net_rvu_case_d;

--NET Revenue----------------