\set ON_ERROR_STOP ON;

CREATE TEMP TABLE indx_inp AS
with indx_inp as 		
	(
        SELECT 	cf.member_id
                ,cf.claimid
		,cf.riskpool
    		,min(cf.prm_fromdate) svc_fm_dt
    		,max(cf.prm_todate) svc_to_dt
                ,'' toc_pst_dschrg_vst_hcpcs_cd
                ,'' toc_pst_dschrg_vst_hcpcs_descr
        FROM pce_qe16_aco_prd_lnd..cv_outclaims cf
       	WHERE cf.riskpool = 'IP' and cf.dischargestatus <>'30'
		GROUP BY cf.member_id, cf.claimid, cf.riskpool
	HAVING sum(prm_admits)>0
        
		UNION
        --Added logic for SNF Discharges
	SELECT 	cf.member_id
                ,cf.claimid
		,cf.riskpool
    		,min(cf.prm_fromdate) svc_fm_dt
    		,max(cf.prm_todate) svc_to_dt
                ,'' toc_pst_dschrg_vst_hcpcs_cd
                ,'' toc_pst_dschrg_vst_hcpcs_descr
        FROM pce_qe16_aco_prd_lnd..cv_outclaims cf
       	WHERE cf.riskpool = 'SNF' and cf.dischargestatus <>'30'
	GROUP BY cf.member_id, cf.claimid, cf.riskpool
        ),

pst_dschrg_vsts as
        (
        SELECT 	cf.member_id
                ,cf.claimid
		,cf.riskpool
		,min(cf.prm_fromdate) svc_fm_dt 
		,max(cf.prm_todate) svc_to_dt
                ,toc.hcpcs_cd as toc_pst_dschrg_vst_hcpcs_cd
                ,toc.hcpcs_descr as toc_pst_dschrg_vst_hcpcs_descr
        FROM pce_qe16_aco_prd_lnd..cv_outclaims cf
            INNER JOIN cst_modl_dim cd ON cf.prm_line = cd.cst_modl_line_cd
            INNER JOIN hcpcs_dim toc on cf.hcpcs=toc.hcpcs_cd and toc.toc_pst_dschrg_ind=1
        	WHERE 
		(cd.care_setting_cgy_nm in ('Professional Office/Other','Outpatient (facility and professional)')
		GROUP BY cf.member_id, cf.claimid, cf.riskpool, hcpcs_cd, toc.hcpcs_descr
        )

select * from indx_inp
union all
select * from pst_dschrg_vsts;

drop table post_dschrg_vst_fct if exists;

create table post_dschrg_vst_fct as
select a.member_id as mbi_id
	,a.claimid as clm_id
	,a.riskpool as rsk_pool
	,a.fop_clm_id 
	,fop_svc_fm_dt-svc_to_dt as days_to_fwp_vst
	,1 as toc_pst_dschrg_ind
        ,case when (fop_svc_fm_dt-svc_to_dt) between 0 and 7 then 1 end as toc_7dy_fwp_vst_ind
        ,case when (fop_svc_fm_dt-svc_to_dt) between 0 and 14 then 1 end as toc_14dy_fwp_vst_ind
        ,case when (fop_svc_fm_dt-svc_to_dt) between 0 and 30 then 1 end as toc_30dy_fwp_vst_ind
	,fop_toc_pst_dschrg_vst_hcpcs_cd as toc_pst_dschrg_vst_hcpcs_cd
	,fop_toc_pst_dschrg_vst_hcpcs_descr as toc_pst_dschrg_vst_hcpcs_descr
from 
(
select 	member_id
        ,claimid
		,riskpool
		,svc_fm_dt
		,svc_to_dt
		,lead(claimid) over (partition by member_id order by svc_fm_dt) as fop_clm_id
		,lead(riskpool) over (partition by member_id order by svc_fm_dt) as fop_riskpool
		,lead(svc_fm_dt) over (partition by member_id order by svc_fm_dt) as fop_svc_fm_dt
		,lead(svc_to_dt) over (partition by member_id order by svc_fm_dt) as fop_svc_to_dt
		,lead(toc_pst_dschrg_vst_hcpcs_cd) over (partition by member_id order by svc_fm_dt) as fop_toc_pst_dschrg_vst_hcpcs_cd
		,lead(toc_pst_dschrg_vst_hcpcs_descr) over (partition by member_id order by svc_fm_dt) as fop_toc_pst_dschrg_vst_hcpcs_descr
from indx_inp
)a
where riskpool in ('IP','SNF') 
	and fop_riskpool not in ('IP','SNF') 
	and fop_svc_fm_dt-svc_to_dt<=30;

\unset ON_ERROR_STOP
