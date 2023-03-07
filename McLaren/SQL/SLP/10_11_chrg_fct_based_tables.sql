--Cost Model:  From the Charge Fact, do a sum total of the Indirect Cost for each encounter and add it in the Encounter Analysis Fact. Ditto Direct Cost and Total Cost Amt

--###########################################################################################
--           Charge Cost Fact
--###########################################################################################

--select 'processing table:  intermediate_stage_chrg_cost_fct' as table_processing;
DROP TABLE intermediate_stage_chrg_cost_fct IF EXISTS ;
CREATE TABLE intermediate_stage_chrg_cost_fct AS
(
select
  fcy_nm
, encntr_num
, sum(rcc_based_direct_cst_amt)   	as  agg_rcc_based_direct_cst_amt
, sum(rcc_based_indirect_cst_amt) 	as agg_rcc_based_indirect_cst_amt
, sum(rcc_based_total_cst_amt)      	as agg_rcc_based_total_cst_amt
, sum(calculated_or_hrs) 		as agg_calculated_or_hrs
, max(clnscpy_ind) clnscpy_ind
, max(mamgrphy_ind) mamgrphy_ind
FROM  intermediate_stage_chrg_fct
GROUP BY 1,2
);

--###########################################################################################
--           Endoscopy Cases
--###########################################################################################

--code change : Added logic to calculate Endoscopy Cases based on Rev Code 0750

--select 'processing table: intermediate_stage_temp_endoscopy_case ' as table_processing;
DROP TABLE intermediate_stage_temp_endoscopy_case IF EXISTS;
CREATE TABLE intermediate_stage_temp_endoscopy_case AS (
SELECT DISTINCT patient_id,company_id
 FROM  intermediate_stage_chrg_fct spl
WHERE spl.persp_clncl_smy_descr = 'ENDOSCOPIC PROCEDURES');

--###########################################################################################
--           Charge Agg Fact
--###########################################################################################

--CODE CHANGE :08/31/2020  MLH-581
--select 'processing table: intermediate_stage_chrg_agg_fct ' as table_processing;
DROP TABLE intermediate_stage_chrg_agg_fct  IF EXISTS;
CREATE TABLE intermediate_stage_chrg_agg_fct
AS
select fcy_nm as fcy_nm , encntr_num as encntr_num ,
max(X.prfssnl_chrg_ind) as prfssnl_chrg_ind,
max(X.fcy_chrg_ind) as fcy_chrg_ind,
sum(CASE WHEN prfssnl_chrg_ind =1  THEN X.total_charge
	    else NULL END ) as prfssnl_chrg_amt,
sum(CASE WHEN fcy_chrg_ind =1  THEN X.total_charge
	    else NULL END ) as fcy_chrg_amt,
sum(CASE WHEN prfssnl_chrg_ind =1  THEN X.rcc_based_direct_cst_amt
	    else 0 END ) as prfssnl_direct_cst_amt,
sum(CASE WHEN fcy_chrg_ind =1  THEN X.rcc_based_direct_cst_amt
	    else 0 END ) as fcy_direct_cst_amt,
sum(CASE WHEN prfssnl_chrg_ind =1  THEN X.rcc_based_indirect_cst_amt
	    else 0 END ) as prfssnl_indirect_cst_amt,
sum(CASE WHEN fcy_chrg_ind =1  THEN X.rcc_based_indirect_cst_amt
	    else 0 END ) as fcy_indirect_cst_amt,
sum(CASE WHEN prfssnl_chrg_ind =1  THEN X.rcc_based_total_cst_amt
	    else 0 END ) as prfssnl_total_cst_amt,
sum(CASE WHEN fcy_chrg_ind =1  THEN X.rcc_based_total_cst_amt
	    else 0 END ) as fcy_total_cst_amt
from  intermediate_stage_chrg_fct X
GROUP BY 1,2;

--###########################################################################################
--           Ptnt Fnc Txn Agg Fact
--###########################################################################################
--CODE CHANGE :08/31/2020  MLH-581
--select 'processing table: intermediate_stage_ptnt_fnc_txn_agg_fct ' as table_processing;
DROP TABLE intermediate_stage_ptnt_fnc_txn_agg_fct  IF EXISTS;
CREATE TABLE intermediate_stage_ptnt_fnc_txn_agg_fct
AS
select fcy_nm ,encntr_num ,
sum(CASE WHEN fcy_pymt_ind = 1  THEN amount
	    else NULL END ) as fcy_pymt_amt,
sum(CASE WHEN fcy_adj_ind =  1  THEN amount
	    else NULL END ) as fcy_adj_amt,
--MLH-723: New measure/indicator has been added and it should be used in the encntr_anl_Fct so aggregating it
max(X.excld_trnsfr_encntr_ind) as excld_trnsfr_encntr_ind
from  intermediate_stage_fnc_txn_fct X
GROUP BY 1,2;

--###########################################################################################
--           Blood and Lab Utilization
--###########################################################################################

 --CODE CHANGE : AUG 2019 Blood and Lab Utilization

--select 'processing table:  intermediate_stage_temp_blood_util_qty' as table_processing;
DROP TABLE intermediate_stage_temp_blood_util_qty IF EXISTS;
CREATE TABLE intermediate_stage_temp_blood_util_qty AS
SELECT X.fcy_nm, X.encntr_num,
SUM(X.quantity) AS blood_util_qty
FROM  intermediate_stage_chrg_fct X
WHERE X.cpt_code in ('P9011','P9012','P9016','P9017','P9019','P9021','P9033','P9034','P9035','P9037','P9040','P9044','P9052','P9059')
GROUP BY 1,2;

--###########################################################################################
--           Lab Utilization
--###########################################################################################

------CODE CHANGE : Aug 2019 Lab Utilization
--select 'processing table:  intermediate_stage_temp_lab_util_qty' as table_processing;
DROP TABLE intermediate_stage_temp_lab_util_qty IF EXISTS;

CREATE TABLE intermediate_stage_temp_lab_util_qty AS
SELECT X.fcy_nm, X.encntr_num,
SUM(X.quantity) AS lab_util_qty
FROM  intermediate_stage_chrg_fct X
WHERE
--CODE CHANGE: MAY 2020 Added total_charge <> 0.0000
X.total_charge <> 0.0000 AND
X.department_group =  'Lab' AND X.cpt_code NOT IN (SELECT cd
  FROM pce_qe16_prd_qadv..val_set_dim where cohrt_id ='lab_utils')
GROUP BY 1,2;