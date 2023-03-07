\set ON_ERROR_STOP ON;

with att_name as
(
select distinct 'ttl_chrg_amt' cst_type, replace(lower(translate(client_rev_cd_grp,'aeiou-&','')),' ','_') att_nm, client_rev_cd_grp from client_revcode_grouper_dim
union all
select distinct 'ttl_cst_amt' cst_type, replace(lower(translate(client_rev_cd_grp,'aeiou-&','')),' ','_') att_nm, client_rev_cd_grp from client_revcode_grouper_dim
union all
select distinct 'indrct_cst_amt' cst_type, replace(lower(translate(client_rev_cd_grp,'aeiou-&','')),' ','_') att_nm, client_rev_cd_grp from client_revcode_grouper_dim
union all
select distinct 'drct_cst_amt' cst_type, replace(lower(translate(client_rev_cd_grp,'aeiou-&','')),' ','_') att_nm, client_rev_cd_grp from client_revcode_grouper_dim
)


select 
'max(case when client_revenue_code_group='''||client_rev_cd_grp||''' then '||cst_type||' end ) as '||att_nm||'_'||cst_type||',' from att_name

\unset ON_ERROR_STOP
