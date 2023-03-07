alter table pce_qe16_slp_prd_dm..encntr_anl_fct rename to pce_qe16_slp_prd_dm..encntr_anl_fct_curr;
create table pce_qe16_slp_prd_dm..encntr_anl_fct AS SELECT * from pce_qe16_slp_prd_dm..encntr_anl_fct_prev; 

alter table pce_qe16_slp_prd_dm..encntr_qly_anl_fct rename to pce_qe16_slp_prd_dm..encntr_qly_anl_fct_curr;
create table pce_qe16_slp_prd_dm..encntr_qly_anl_fct AS SELECT * from pce_qe16_slp_prd_dm..encntr_qly_anl_fct_prev;
