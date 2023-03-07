\set ON_ERROR_STOP ON;

insert into ccn_dim
select a.* from
(select string_to_int(substr(RAWTOHEX(hash(ccn, 0)), 17), 16) as ccn_sk,ccn as ccn_id, ccn_fac_name as fcy_nm, 

		case when strright(ccn,4)::integer between 1 and 879 then 'Acute Care Hospitals' 
			 when strright(ccn,4)::integer between 1000 and 1199 then 'Federally Qualified Health Center' 
                         when strright(ccn,4)::integer between 1300 and 1399 then 'Critical Access Hospitals'
                         when strright(ccn,4)::integer between 1500 and 1799 then 'Hospices'
			 when strright(ccn,4)::integer between 1800 and 1989 then 'Federally Qualified Health Center' 
			 when strright(ccn,4)::integer between 2000 and 2299 then 'Long-Term Care Hospital'
			 when strright(ccn,4)::integer between 2300 and 2999 then 'Dialysis Facility'
			 when strright(ccn,4)::integer between 3025 and 3099 then 'Psychiatric Hospitals'
			 when strright(ccn,4)::integer between 3100 and 3199 then 'Home Health Care Agency'
			 when strright(ccn,4)::integer between 3300 and 3399 then 'Childrens'
			 when strright(ccn,4)::integer between 3400 and 3499 then 'Rural Health Clinic'
			 when strright(ccn,4)::integer between 3800 and 3999 then 'Rural Health Clinic'
			 when strright(ccn,4)::integer between 8500 and 8999 then 'Rural Health Clinic'
			 when strright(ccn,4)::integer between 4000 and 4499 then 'Inpatient Rehab Facility'
			 when strright(ccn,4)::integer between 4500 and 4599 then 'Comprehensive Outpatient Rehab Facility'
			 when strright(ccn,4)::integer between 4800 and 4899 then 'Comprehensive Outpatient Rehab Facility'
			 when strright(ccn,4)::integer between 4600 and 4799 then 'Community Mental Health Center'
			 when strright(ccn,4)::integer between 5000 and 6499 then 'Nursing Home'
			 when strright(ccn,4)::integer between 6500 and 6989 then 'Outpatient Physical Therapy/Speech Pathology'
			 when strright(ccn,4)::integer between 7000 and 8499 then 'Home Health Care Agency'
			 
		else null end as fcy_type_descr, 
		ccn_st_adr as adr_line_1, null adr_line_2, 
		ccn_city_name as cty_nm, null cnty_nm, null ph_num, ccn_state_cd as ste_cd, ccn_zip_cd as zip_cd,
		case when strright(ccn,4)::integer between 1 and 879 then 'Short-Term Stay Hospital' 
			 when strright(ccn,4)::integer between 1300 and 1399 then 'Short-Term Stay Hospital'
			 when strright(ccn,4)::integer between 2000 and 2299 then 'Long-Term Stay Hospital'
			 when strright(ccn,4)::integer between 3025 and 3099 then 'Psychiatric Hospital or Unit'
			 when strright(ccn,4)::integer between 4000 and 4499 then 'Rehabilitation Hospital or Unit'
			 else null end as  hsptl_pvdr_type, 
	now() as rcrd_isrt_ts, rcrd_src_file_nm  
from pce_qe16_aco_prd_lnd..cv_ref_ccn where
length(translate(ccn,'0123456789',''))=0
)a
inner join (select distinct prm_prv_id_ccn from pce_qe16_aco_prd_lnd..cv_outclaims)b on a.ccn_id=b.prm_prv_id_ccn
left join ccn_dim on a.ccn_sk=ccn_dim.ccn_sk
where ccn_dim.ccn_sk is null;

\unset ON_ERROR_STOP
