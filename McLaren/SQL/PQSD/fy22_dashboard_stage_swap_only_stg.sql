\set ON_ERROR_STOP ON;
--------SWAP Staging to Target Tables
drop table tmp_ob_trn_fct IF EXISTS;
drop table ptnt_exrnc_pct_msr_fct IF EXISTS;
drop table mmg_ovrl_ptnt_exrnc_msr_fct IF EXISTS;
drop table tmp_mmg_ovrl_ptnt_exrnc_msr_fct IF EXISTS;
drop table aco_mpp_msr_fct IF EXISTS;
drop table tmp_card_rehab_fct IF EXISTS;
drop table card_rhb_movm_fct IF EXISTS;
drop table mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct IF EXISTS;
drop table ptnt_exrnc_pct_msr_fct_mnth_over_mnth IF EXISTS;
drop table encntr_qs_anl_fct_vw IF EXISTS;
drop table harm_events_fct IF EXISTS;
drop table tmp_harm_events_fct IF EXISTS;
drop table nhsn_zero_event_fct IF EXISTS;
drop table hac_zero_event_fct IF EXISTS;
drop table psi_zero_event_fct IF EXISTS;
drop table zero_event_fct IF EXISTS;
drop table tmp_zero_events_fct IF EXISTS;
drop table tmp_onc_awbi_fct IF EXISTS;
drop table tmp_onc_awbi_r12m_fct IF EXISTS;
drop table onc_ptnt_exrnc_pct_msr_fct IF EXISTS;
drop table hcaphs_ptnt_exrnc_pct_msr_fct IF EXISTS; 
drop table pqsd_cons_metrics IF EXISTS;

--tmp_ob_trn_fct
create table tmp_ob_trn_fct as select * from pce_qe16_prd..stg_tmp_ob_trn_fct;

--ptnt_exrnc_pct_msr_fct
create table ptnt_exrnc_pct_msr_fct as select * from pce_qe16_prd..stg_ptnt_exrnc_pct_msr_fct;

--mmg_ovrl_ptnt_exrnc_msr_fct
create table mmg_ovrl_ptnt_exrnc_msr_fct as select * from pce_qe16_prd..stg_mmg_ovrl_ptnt_exrnc_msr_fct;

--tmp_mmg_ovrl_ptnt_exrnc_msr_fct
create table tmp_mmg_ovrl_ptnt_exrnc_msr_fct as select * from pce_qe16_prd..stg_tmp_mmg_ovrl_ptnt_exrnc_msr_fct;

--aco_mpp_msr_fct
create table aco_mpp_msr_fct as select * from pce_qe16_prd..stg_aco_mpp_msr_fct;

--tmp_card_rehab_fct
create table tmp_card_rehab_fct as select * from pce_qe16_prd..stg_tmp_card_rehab_fct;

--card_rhb_movm_fct
create table card_rhb_movm_fct as select * from pce_qe16_prd..stg_card_rhb_movm_fct;

--mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct
create table mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct as select * from pce_qe16_prd..stg_mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct;

--ptnt_exrnc_pct_msr_fct_mnth_over_mnth
create table ptnt_exrnc_pct_msr_fct_MNTH_OVER_MNTH as select * from pce_qe16_prd..stg_ptnt_exrnc_pct_msr_fct_mnth_over_mnth;

--encntr_qs_anl_fct_vw
create table encntr_qs_anl_fct_vw as select * from pce_qe16_prd..stg_encntr_qs_anl_fct_vw;

--harm_events_fct
create table harm_events_fct as select * from pce_qe16_prd..stg_harm_events_fct;

--tmp_harm_events_fct
create table TMP_HARM_EVENTS_FCT as select * from pce_qe16_prd..stg_tmp_harm_events_fct;

--nhsn_zero_event_fct
create table nhsn_zero_event_fct as select * from pce_qe16_prd..stg_nhsn_zero_event_fct;

--hac_zero_event_fct
create table hac_zero_event_fct as select * from pce_qe16_prd..stg_hac_zero_event_fct;

--psi_zero_event_fct
create table psi_zero_event_fct as select * from pce_qe16_prd..stg_psi_zero_event_fct;

--zero_event_fct
create table zero_event_fct as select * from pce_qe16_prd..stg_zero_event_fct;

--tmp_zero_events_fct
create table TMP_ZERO_EVENTS_FCT as select * from pce_qe16_prd..stg_tmp_zero_events_fct;

--tmp_onc_awbi_fct
create table tmp_onc_awbi_fct as select * from pce_qe16_prd..stg_tmp_onc_awbi_fct;

--tmp_onc_awbi_r12m_fct
create table tmp_onc_awbi_r12m_fct as select * from pce_qe16_prd..stg_tmp_onc_awbi_r12m_fct;

--onc_ptnt_exrnc_pct_msr_fct
create table onc_ptnt_exrnc_pct_msr_fct as select * from pce_qe16_prd..stg_onc_ptnt_exrnc_pct_msr_fct;

--hcaphs_ptnt_exrnc_pct_msr_fct
create table hcaphs_ptnt_exrnc_pct_msr_fct as select * from pce_qe16_prd..stg_hcaphs_ptnt_exrnc_pct_msr_fct;

--pqsd_cons_metrics
create table pqsd_cons_metrics as select * from pce_qe16_prd..stg_pqsd_cons_metrics;

--drop table tmp_lab_utlz_fct_prev_run IF EXISTS;
--alter table tmp_lab_utlz_fct RENAME TO tmp_lab_utlz_fct_prev_run;
--alter table stg_tmp_lab_utlz_fct  RENAME TO tmp_lab_utlz_fct; 

--drop table tmp_sep_compl_fct_prev_run IF EXISTS;
--alter table tmp_sep_compl_fct RENAME TO tmp_sep_compl_fct_prev_run;
--alter table stg_tmp_sep_compl_fct  RENAME TO tmp_sep_compl_fct;

--drop table clncl_outc_scor_fct_prev_run IF EXISTS;
--alter table clncl_outc_scor_fct RENAME TO clncl_outc_scor_fct_prev_run;
--alter table stg_clncl_outc_scor_fct  RENAME TO clncl_outc_scor_fct;

--drop table pqsd_cmplc_idnx_fct_prev_run IF EXISTS;
--alter table pqsd_cmplc_idnx_fct RENAME TO pqsd_cmplc_idnx_fct_prev_run;
--alter table stg_pqsd_cmplc_idnx_fct  RENAME TO pqsd_cmplc_idnx_fct; 

--drop table tmp_mrtly_ind_prev_run IF EXISTS;
--alter table tmp_mrtly_ind RENAME TO tmp_mrtly_ind_prev_run;
--alter table stg_tmp_mrtly_ind  RENAME TO tmp_mrtly_ind;

--drop table tmp_sep_mrtly_ind_prev_run IF EXISTS;
--alter table tmp_sep_mrtly_ind RENAME TO tmp_sep_mrtly_ind_prev_run;
--alter table stg_tmp_sep_mrtly_ind  RENAME TO tmp_sep_mrtly_ind;

--drop table tmp_readm_ind_prev_run IF EXISTS;
--alter table tmp_readm_ind RENAME TO tmp_readm_ind_prev_run;
--alter table stg_tmp_readm_ind  RENAME TO tmp_readm_ind;

--drop table lab_utlz_fct_prev_run IF EXISTS;
--alter table lab_utlz_fct RENAME TO lab_utlz_fct_prev_run;
--alter table stg_lab_utlz_fct  RENAME TO lab_utlz_fct;

--drop table tmp_mrtly_ind_wo_covid_prev_run IF EXISTS;
--alter table tmp_mrtly_ind_wo_covid RENAME TO tmp_mrtly_ind_wo_covid_prev_run;
--alter table stg_tmp_mrtly_ind_wo_covid RENAME TO tmp_mrtly_ind_wo_covid;

--drop table tmp_sep_mrtly_ind_wo_covid_prev_run IF EXISTS;
--alter table tmp_sep_mrtly_ind_wo_covid RENAME TO tmp_sep_mrtly_ind_wo_covid_prev_run;
--alter table stg_tmp_sep_mrtly_ind_wo_covid RENAME TO tmp_sep_mrtly_ind_wo_covid;

--drop table tmp_readm_ind_wo_covid_prev_run IF EXISTS;
--alter table tmp_readm_ind_wo_covid RENAME TO tmp_readm_ind_wo_covid_prev_run;
--alter table stg_tmp_readm_ind_wo_covid RENAME TO tmp_readm_ind_wo_covid;

--drop table tmp_lab_utlz_fct_wo_covid_prev_run IF EXISTS;
--alter table tmp_lab_utlz_fct_wo_covid RENAME TO tmp_lab_utlz_fct_wo_covid_prev_run;
--alter table stg_tmp_lab_utlz_fct_wo_covid RENAME TO tmp_lab_utlz_fct_wo_covid; 

--drop table clncl_outc_scor_fct_wo_covid_prev_run IF EXISTS;
--alter table clncl_outc_scor_fct_wo_covid RENAME TO clncl_outc_scor_fct_wo_covid_prev_run;
--alter table stg_clncl_outc_scor_fct_wo_covid RENAME TO clncl_outc_scor_fct_wo_covid; 

--drop table tmp_pci_radial_access_fct_prev_run IF EXISTS;
--alter table tmp_pci_radial_access_fct RENAME TO tmp_pci_radial_access_fct_prev_run;
--alter table stg_tmp_pci_radial_access_fct RENAME TO tmp_pci_radial_access_fct;

--drop table tmp_pci_radial_access_r12m_fct_prev_run IF EXISTS;
--alter table tmp_pci_radial_access_r12m_fct RENAME TO tmp_pci_radial_access_r12m_fct_prev_run;
--alter table stg_tmp_pci_radial_access_r12m_fct RENAME TO tmp_pci_radial_access_r12m_fct; 

--drop table onc_clncl_outc_scor_fct_wo_covid_prev_run IF EXISTS;
--alter table onc_clncl_outc_scor_fct_wo_covid RENAME TO onc_clncl_outc_scor_fct_wo_covid_prev_run;
--alter table stg_onc_clncl_outc_scor_fct_wo_covid RENAME TO onc_clncl_outc_scor_fct_wo_covid;

\unset ON_ERROR_STOP

