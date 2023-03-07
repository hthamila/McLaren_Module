----CODE CHANGE : April 2020 Financial Transaction Fact JIRA # MLH-505
--select 'processing table: intermediate_stage_fnc_txn_fct ' as table_processing;
DROP TABLE intermediate_stage_fnc_txn_fct IF EXISTS;

CREATE TABLE intermediate_stage_fnc_txn_fct AS
with fnc_txn_fct AS(
SELECT   facility  as fcy_nm
       , account  as encntr_num
       , department
       , nvl(dept_dim.department_description, 'UNKNOWN') as department_description
       , transcode
       , receiveddate
       , postdate
       , transcodedesc
       , transtype
       , amount
       , covered
       , noncovered
       , deductible
       , coinsurance
       , subaccount
       , revenuecode
       ,nvl(crev.revenue_code_description,'UNKNOWN') AS revenue_code_description
       , cpt4code
       , modifier1
       , modifier2
       , modifier3
       , modifier4
       , payorplancode
       , nvl(PAYER.payer_description , 'UNKNOWN') as payer_description
       , remitid
       , extracteddate
       , sourcesystem
       , invoiceid
       , CASE WHEN UPPER(transtype) in ('P','PAYMENT','PAYMENTS','RECEIPT') THEN 1 ELSE 0 END as fcy_pymt_ind
       , CASE WHEN UPPER(transtype) in ('A','ADJUSTMENTS','XFER') THEN 1 ELSE 0 END as fcy_adj_ind
--AUG 2021: MLH : 723 SLP/Integrated DataMart August 2021 Changes: Cross Over Accounts
       , CASE WHEN UPPER (transcodedesc) in ('AR TRANSFER TO CERNER') THEN 1 ELSE 0 END as excld_trnsfr_encntr_ind
  FROM pce_qe16_oper_prd_zoom.qe16zmp.cv_pattrans PFTF
  LEFT JOIN dept_dim
  on  PFTF.facility = dept_dim.company_id and PFTF.department = dept_dim.department_code
  LEFT JOIN intermediate_stage_temp_payer_fcy_std_code PAYER
  ON PAYER.company_id = PFTF.facility
	AND PAYER.fcy_payer_code = PFTF.payorplancode
LEFT JOIN  pce_qe16_oper_prd_zoom..cv_revcodemap crev
on PFTF.revenuecode = crev.revenue_code
)
SELECT FT.* FROM intermediate_stage_temp_eligible_encntr_data EA
INNER JOIN fnc_txn_fct FT
 on EA.company_id = FT.fcy_nm and EA.patient_id = FT.encntr_num;
