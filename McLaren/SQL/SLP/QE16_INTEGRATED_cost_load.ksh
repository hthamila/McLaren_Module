#!/usr/bin/ksh

v_chk_prv='drop table stage_encntr_cst_anl_fct_prev if exists;'
v_alt_bkp='alter table prd_encntr_cst_anl_fct rename to stage_encntr_cst_anl_fct_prev;'
v_alt_stage='alter table stage_encntr_cst_anl_fct rename to prd_encntr_cst_anl_fct;'
v_crt_stm_1='create table stage_encntr_cst_anl_fct as select fcy_nm, encntr_num,'
v_crt_stm_2='1 as cnt, hash8(fcy_nm || cast(chr(45) as varchar(1)) || encntr_num) as fcy_encntr_hashkey from pce_qe16_slp_prd_dm..intermediate_encntr_cst_fct group by fcy_nm, encntr_num distribute on (fcy_nm, encntr_num,fcy_encntr_hashkey);'
v_drp_stg='drop table stage_encntr_cst_anl_fct if exists;'

case `hostname` in
h2dudodseng11) export env="uat"; export v_nzHost="c3pzmart2"; export v_nzUsr="prmretlt"; export v_project="DI_PCE_UAT";;
h3puprmrdseng11) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="slp_etlp"; export v_project="DI_PCE_PROD";;
esac

#create a sql file to execute and create Encounter Cost Analysis Fact Table
echo -e "${v_drp_stg} \n${v_crt_stm_1}\n " >  /ds_data1/${v_project}/sql/QE16/SLP/encntr_cost_anl_fct.sql

nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_slp_${env}_dm -r -t -f /ds_data1/${v_project}/sql/QE16/SLP/QE16_INTEGRATED_att_gen.sql >> /ds_data1/${v_project}/sql/QE16/SLP/encntr_cost_anl_fct.sql
        if [ $? -ne 0 ]; then
                echo "Error in generation of Encounter Cost Analysis Fact SQL Script"
                exit 1
        else
                echo "Completed generating Encounter Cost Analysis Fact SQL Script"
        fi

#Now add create & subsequent statements to the SQL

echo -e  "\n${v_crt_stm_2} \n\n${v_chk_prv} \n${v_alt_bkp} \n${v_alt_stage}" >> /ds_data1/${v_project}/sql/QE16/SLP/encntr_cost_anl_fct.sql

#Execute now the SQL for creation of Fact Table
nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_slp_${env}_dm -r -t -f /ds_data1/${v_project}/sql/QE16/SLP/encntr_cost_anl_fct.sql

        if [ $? -ne 0 ]; then
                echo "Error in Load of Encounter Cost Analysis Fact"
                exit 1
        else
                echo "Completed Loading Encounter Cost Analysis Fact"
        fi



