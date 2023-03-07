--intermediate_stage_cpt_fct tABLE
--select 'processing table:  intermediate_stage_cpt_fct' as table_processing;
DROP TABLE intermediate_stage_cpt_fct IF EXISTS;
CREATE TABLE intermediate_stage_cpt_fct AS
SELECT
 Z.company_id as fcy_nm
,z.patient_id AS encntr_num
,VSET_FCY.alt_cd as fcy_num
,CF.company_id
,CF.patient_id
,CF.cpt_code
,CPT_DIM.hcpcs_descr as cpt_descr
,CPT_DIM.hcpcs_descr_long as cpt_descr_long
,hccs.ccs_hcpcs_cgy_cd
,hccs.ccs_hcpcs_cgy_descr
,hbt.betos_cd
,hbt.betos_descr
,CF.cpt_modifier_1
,CF.cpt_modifier_2
,CF.cpt_modifier_3
,CF.cpt_modifier_4
,CF.procedure_practitioner_code as pcd_pract_cd
,SPCL.npi as pcd_pract_npi
,SPCL.practitioner_name as pcd_pract_nm
,SPCL.practitioner_spclty_description as pcd_pract_splcy_descr
,SPCL.mcare_spcly_cd as pcd_pract_splcy_cd
,CF.cpt4seq
,to_timestamp((cpt_code_date ||' '||nvl(substr(cpt4time,1,2),'00')||':'||nvl(substr(cpt4time,3,2),'00')||':00') ,'MMDDYYYY HH24":"MI":"SS') as cpt_code_ts
,cadd.cpt_si
,cadd.cpt_apc
,cadd.cpt_addnd_b_rltv_wght
,cadd.cpt_addnd_b_pymt_rt
,cadd.cpt_addnd_b_min_unadj_copymt
,case when hbt.betos_cd='P8D' then 1 else null end as clnscpy_ind
,row_number() over(partition by Z.company_id, Z.patient_id
Order by  CF.cpt_code_date) as rec_num
FROM  intermediate_stage_temp_eligible_encntr_data Z
  LEFT JOIN patcpt_fct CF
  on Z.company_id = CF.company_id and Z.patient_id = CF.patient_id
  LEFT JOIN val_set_dim VSET_FCY
  ON VSET_FCY.cd = Z.company_id AND VSET_FCY.cohrt_id = 'FACILITY_CODES'
  LEFT JOIN intermediate_stage_temp_physician_npi_spclty SPCL
  on SPCL.company_id = CF.company_id and SPCL.practitioner_code = CF.procedure_practitioner_code
  LEFT JOIN hcpcs_dim CPT_DIM
  on CPT_DIM.hcpcs_cd = UPPER(CF.cpt_code)
  LEFT JOIN hcpcs_ccs_dim hccs
  ON CPT_DIM.hcpcs_cd = hccs.hcpcs_cd
  LEFT JOIN hcpcs_betos_dim hbt
  ON CPT_DIM.hcpcs_cd = hbt.hcpcs_cd
  LEFT JOIN cpt_addndm_b_dim_h cadd on Z.discharge_yr=cadd.appl_yr and UPPER(CF.cpt_code)=cadd.cpt_cd
 DISTRIBUTE ON (fcy_nm, encntr_num);

--------------------------------------------------------------------------------------
--MLH - 651(03/08/2021) Algorithm to determine Primary HCPCS/BETOS for an Encounter--
--------------------------------------------------------------------------------------
drop table stg_encntr_prim_cpt if exists;
create table stg_encntr_prim_cpt as
select
        fcy_nm
        , encntr_num
        , service_date
        , trim(cpt_code) cpt_code
        , case when pcd_flag='N' then 'Non-Surgical' else 'Surgical' end as cpt_type
		, svc_line
		, sub_svc_line
		, svc_nm
		, sum(total_charge) as total_charge
        , 'CHARGE' as source
from intermediate_stage_chrg_fct chrg
--07/26/21: Replaced the LEFT JOIN Table
        left join hcpcs_cpt_svc_hier_dim x on chrg.cpt_code=x.hcpcs_5dgt_cd
where cpt_code <> '-100'
group by
        fcy_nm
        , encntr_num
        , service_date
        , cpt_code
        , pcd_flag
		, svc_line
		, sub_svc_line
		, svc_nm;

--07/26/21: Replaced LEFT JOIN table  xref_hcpcs_svc_ln with hcpcs_cpt_svc_hier_dim
insert into stg_encntr_prim_cpt
select
        fcy_nm
        , encntr_num
        , cpt_code_ts
        , trim(cpt_code) cpt_code
        , case when pcd_flag='N' then 'Non-Surgical' else 'Surgical' end as cpt_type
		, svc_line
		, sub_svc_line
		, svc_nm
        , 0 total_charge
        , 'PATCPT' as source
from intermediate_stage_cpt_fct cpt
        left join intermediate_stage_chrg_fct chrg using (fcy_nm,encntr_num,cpt_code)
        left join hcpcs_cpt_svc_hier_dim x on cpt.cpt_code=x.hcpcs_5dgt_cd
where chrg.cpt_code is null and cpt.cpt_code is not null;

drop table intermediate_stage_encntr_prim_cpt_fct if exists;
create table intermediate_stage_encntr_prim_cpt_fct as
select a.fcy_nm
        , a.encntr_num
        , a.service_date
        , a.cpt_code
        , a.cpt_type
        , a.svc_line
        , a.sub_svc_line
        , a.svc_nm
        , h.hcpcs_descr prim_hcpcs_descr
        , hc.ccs_hcpcs_cgy_cd prim_ccs_hcpcs_cgy_cd
        , hc.ccs_hcpcs_cgy_descr prim_ccs_hcpcs_cgy_descr
        , hb.betos_cd as prim_betos_cd
        , hb.betos_descr as prim_betos_descr
        , hb.betos_cgy_nm as prim_betos_cgy_nm
from (
select  e.fcy_nm
                , e.encntr_num
                , e.service_date
                , e.cpt_code
                , e.cpt_type
                , e.svc_line
                , e.sub_svc_line
                , e.svc_nm
                , e.total_charge
                , pw_gpci
                , pe_gpci
                , mp_gpci
                , wrk_rvu
                , fac_pe_rvu
                , mp_rvu
                , ((wrk_rvu * pw_gpci) + (fac_pe_rvu * pe_gpci) + (mp_rvu * mp_gpci)) as rvu
                , ROW_NUMBER() OVER (PARTITION BY e.fcy_nm, e.encntr_num ORDER BY e.cpt_type DESC, rvu DESC,e.total_charge DESC) as rownum
        from stg_encntr_prim_cpt e
                join xref_mcl_lctn lctn on e.fcy_nm=lctn.fcy_nm
                join gpci_rvu_dim grvu on e.cpt_code=grvu.cpt_hcpcs_cd and year(e.service_date)=year(grvu.eff_yr)
                join gpci_dim gpci on year(e.service_date)=year(gpci.eff_yr) and lctn.carrier=gpci.mac_num and lpad(lctn.locality,2,0)=gpci.lclty_num
                )a
                left join hcpcs_dim h on a.cpt_code=h.hcpcs_cd
                left join hcpcs_ccs_dim hc on a.cpt_code = hc.hcpcs_cd
                left join hcpcs_betos_dim hb on a.cpt_code = hb.hcpcs_cd
        where a.rownum=1
        distribute on (fcy_nm, encntr_num)
        ;


--------------------------------------------------------------------------------------
---Adding logic for Primary HCPCS/BETOS
-------------------------------------------------------------------------------------

select 'encntr_prim_hcpcs_fct';

drop table encntr_prim_hcpcs_fct if exists;
create table encntr_prim_hcpcs_fct as
select 	p.patient_id,
	p.company_id,
	primary_cpt_hcpcs prim_hcpcs_cd,
	h.hcpcs_descr prim_hcpcs_descr,
	hc.ccs_hcpcs_cgy_cd prim_ccs_hcpcs_cgy_cd,
	hc.ccs_hcpcs_cgy_descr prim_ccs_hcpcs_cgy_descr,
	hb.betos_cd as prim_betos_cd,
	hb.betos_descr as prim_betos_descr,
	hb.betos_cgy_nm as prim_betos_cgy_nm
from intermediate_stage_temp_eligible_encntr_data p
	inner join pce_qe16_prd_ct..stage_claim_gold sg on p.patient_id=sg.patient_account_number and p.company_id=sg.discharge_campus
	left join hcpcs_dim h on sg.primary_cpt_hcpcs=h.hcpcs_cd
	left join hcpcs_ccs_dim hc on h.hcpcs_cd = hc.hcpcs_cd
	left join hcpcs_betos_dim hb on h.hcpcs_cd = hb.hcpcs_cd
	group by 1,2,3,4,5,6,7,8,9
;


--------------------------------------------------------------------------------------
---CPT Aggregation Fact for New Measures
-------------------------------------------------------------------------------------
DROP TABLE intermediate_aggr_cpt_fct IF EXISTS;
create table intermediate_aggr_cpt_fct as
select  fcy_nm, encntr_num,
        max(clnscpy_ind) as cpt_clnscpy_ind
from intermediate_stage_cpt_fct
group by 1,2;