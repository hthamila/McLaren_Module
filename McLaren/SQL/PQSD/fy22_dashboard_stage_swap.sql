\set ON_ERROR_STOP ON;
--------SWAP Staging to Target Tables

--tmp_ob_trn_fct
drop table tmp_ob_trn_fct_prev_run IF EXISTS;
alter table tmp_ob_trn_fct RENAME TO tmp_ob_trn_fct_prev_run;
alter table stg_tmp_ob_trn_fct  RENAME TO tmp_ob_trn_fct; 

--ptnt_exrnc_pct_msr_fct
drop table ptnt_exrnc_pct_msr_fct_prev_run IF EXISTS;
alter table ptnt_exrnc_pct_msr_fct RENAME TO ptnt_exrnc_pct_msr_fct_prev_run;
alter table stg_ptnt_exrnc_pct_msr_fct  RENAME TO ptnt_exrnc_pct_msr_fct;

--mmg_ovrl_ptnt_exrnc_msr_fct
drop table mmg_ovrl_ptnt_exrnc_msr_fct_prev_run IF EXISTS;
alter table mmg_ovrl_ptnt_exrnc_msr_fct RENAME TO mmg_ovrl_ptnt_exrnc_msr_fct_prev_run;
alter table stg_mmg_ovrl_ptnt_exrnc_msr_fct  RENAME TO mmg_ovrl_ptnt_exrnc_msr_fct;

--tmp_mmg_ovrl_ptnt_exrnc_msr_fct
drop table tmp_mmg_ovrl_ptnt_exrnc_msr_fct_prev_run IF EXISTS;
alter table tmp_mmg_ovrl_ptnt_exrnc_msr_fct RENAME TO tmp_mmg_ovrl_ptnt_exrnc_msr_fct_prev_run;
alter table stg_tmp_mmg_ovrl_ptnt_exrnc_msr_fct  RENAME TO tmp_mmg_ovrl_ptnt_exrnc_msr_fct;

--aco_mpp_msr_fct
drop table aco_mpp_msr_fct_prev_run IF EXISTS;
alter table aco_mpp_msr_fct RENAME TO aco_mpp_msr_fct_prev_run;
alter table stg_aco_mpp_msr_fct  RENAME TO aco_mpp_msr_fct;

--tmp_card_rehab_fct
drop table tmp_card_rehab_fct_prev_run IF EXISTS;
alter table tmp_card_rehab_fct RENAME TO tmp_card_rehab_fct_prev_run;
alter table stg_tmp_card_rehab_fct  RENAME TO tmp_card_rehab_fct;

--card_rhb_movm_fct
drop table card_rhb_movm_fct_prev_run IF EXISTS;
alter table card_rhb_movm_fct RENAME TO card_rhb_movm_fct_prev_run;
alter table stg_card_rhb_movm_fct  RENAME TO card_rhb_movm_fct;

--mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct
drop table mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct_prev_run IF EXISTS;
alter table mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct RENAME TO mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct_prev_run;
alter table stg_mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct  RENAME TO mmg_ovrl_ptnt_exrnc_msr_mnth_over_mnth_fct;

--ptnt_exrnc_pct_msr_fct_mnth_over_mnth
drop table ptnt_exrnc_pct_msr_fct_mnth_over_mnth_prev_run IF EXISTS;
alter table ptnt_exrnc_pct_msr_fct_MNTH_OVER_MNTH RENAME TO ptnt_exrnc_pct_msr_fct_MNTH_OVER_MNTH_prev_run;
alter table stg_ptnt_exrnc_pct_msr_fct_mnth_over_mnth  RENAME TO ptnt_exrnc_pct_msr_fct_mnth_over_mnth;

--encntr_qs_anl_fct_vw
drop table encntr_qs_anl_fct_vw_prev_run IF EXISTS;
alter table encntr_qs_anl_fct_vw RENAME TO encntr_qs_anl_fct_vw_prev_run;
alter table stg_encntr_qs_anl_fct_vw  RENAME TO encntr_qs_anl_fct_vw;

--harm_events_fct
drop table harm_events_fct_prev_run IF EXISTS;
alter table harm_events_fct RENAME TO harm_events_fct_prev_run;
alter table stg_harm_events_fct  RENAME TO harm_events_fct;

--tmp_harm_events_fct
drop table tmp_harm_events_fct_prev_run IF EXISTS;
alter table TMP_HARM_EVENTS_FCT RENAME TO TMP_HARM_EVENTS_FCT_prev_run;
alter table stg_tmp_harm_events_fct  RENAME TO tmp_harm_events_fct;

--nhsn_zero_event_fct
drop table nhsn_zero_event_fct_prev_run IF EXISTS;
alter table nhsn_zero_event_fct RENAME TO nhsn_zero_event_fct_prev_run;
alter table stg_nhsn_zero_event_fct  RENAME TO nhsn_zero_event_fct;

--hac_zero_event_fct
drop table hac_zero_event_fct_prev_run IF EXISTS;
alter table hac_zero_event_fct RENAME TO hac_zero_event_fct_prev_run;
alter table stg_hac_zero_event_fct  RENAME TO hac_zero_event_fct;

--psi_zero_event_fct
drop table psi_zero_event_fct_prev_run IF EXISTS;
alter table psi_zero_event_fct RENAME TO psi_zero_event_fct_prev_run;
alter table stg_psi_zero_event_fct  RENAME TO psi_zero_event_fct;

--zero_event_fct
drop table zero_event_fct_prev_run IF EXISTS;
alter table zero_event_fct RENAME TO zero_event_fct_prev_run;
alter table stg_zero_event_fct  RENAME TO zero_event_fct;

--tmp_zero_events_fct
drop table tmp_zero_events_fct_prev_run IF EXISTS;
alter table TMP_ZERO_EVENTS_FCT RENAME TO TMP_ZERO_EVENTS_FCT_prev_run;
alter table stg_tmp_zero_events_fct  RENAME TO tmp_zero_events_fct;

--tmp_onc_awbi_fct
drop table tmp_onc_awbi_fct_prev_run IF EXISTS;
alter table tmp_onc_awbi_fct RENAME TO tmp_onc_awbi_fct_prev_run;
alter table stg_tmp_onc_awbi_fct RENAME TO tmp_onc_awbi_fct;

--tmp_onc_awbi_r12m_fct
drop table tmp_onc_awbi_r12m_fct_prev_run IF EXISTS;
alter table tmp_onc_awbi_r12m_fct RENAME TO tmp_onc_awbi_r12m_fct_prev_run;
alter table stg_tmp_onc_awbi_r12m_fct RENAME TO tmp_onc_awbi_r12m_fct;

--onc_ptnt_exrnc_pct_msr_fct
drop table onc_ptnt_exrnc_pct_msr_fct_prev_run IF EXISTS;
alter table onc_ptnt_exrnc_pct_msr_fct RENAME TO onc_ptnt_exrnc_pct_msr_fct_prev_run;
alter table stg_onc_ptnt_exrnc_pct_msr_fct  RENAME TO onc_ptnt_exrnc_pct_msr_fct;

--hcaphs_ptnt_exrnc_pct_msr_fct
drop table hcaphs_ptnt_exrnc_pct_msr_fct_prev_run IF EXISTS; 
alter table hcaphs_ptnt_exrnc_pct_msr_fct RENAME TO hcaphs_ptnt_exrnc_pct_msr_fct_prev_run; 
alter table stg_hcaphs_ptnt_exrnc_pct_msr_fct RENAME TO hcaphs_ptnt_exrnc_pct_msr_fct; 

--pqsd_cons_metrics
drop table pqsd_cons_metrics_prev_run IF EXISTS;
alter table pqsd_cons_metrics RENAME TO pqsd_cons_metrics_prev_run;
alter table stg_pqsd_cons_metrics RENAME TO pqsd_cons_metrics;

--Patient Satisfaction survey metrics
drop table ptnt_stsfctn_pct_msr_fct_prev_run IF EXISTS;
alter table ptnt_stsfctn_pct_msr_fct RENAME TO ptnt_stsfctn_pct_msr_fct_prev_run;
alter table stg_ptnt_stsfctn_pct_msr_fct RENAME TO ptnt_stsfctn_pct_msr_fct;


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

