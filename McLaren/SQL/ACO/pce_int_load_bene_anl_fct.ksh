#!/usr/bin/ksh
v_sql_file_nm=$1
v_pcp_algr_file_nm=hist_bene_pcp_attr.sql

case `hostname` in
h3tudseng1) export env="uat"; export v_nzHost="c3pzmart2"; export v_nzUsr="prmretlt"; export v_project="DI_PCE_UAT";;
h3pudseng2) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp"; export v_project="DI_PCE_PROD";;
h3puprmrdseng11) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp"; export v_project="DI_PCE_PROD";;
esac


nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "truncate benf_anl_fct;"

v_cnt_parms=`nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_lnd -r -t -c "select max(qtr_sk)||'|'|| max(qtr_sk)-12 from qtr_cdr;"`
v_strt=$(echo $v_cnt_parms|tr -d ' ' | awk -F"|" '{print $2}')
v_end=$(echo $v_cnt_parms|tr -d ' ' | awk -F"|" '{print $1}')

for v_cnt in {${v_strt}..${v_end}}
do

echo "Execution of Loop ${v_cnt}"

v_dt_parms=`nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_lnd -r -t -c "select add_months(qtr_end_dt,-12)+1||'|'||qtr_end_dt||'|'||to_char(add_months(qtr_end_dt,-1) + interval '15 days','YYYY-MM-DD') from qtr_cdr where qtr_sk=${v_cnt};"`

v_rpt_prd_strt_dt=$(echo $v_dt_parms|tr -d ' ' | awk -F"|" '{print $1}')
v_rpt_prd_end_dt=$(echo $v_dt_parms|tr -d ' ' | awk -F"|" '{print $2}')
v_elig_month=$(echo $v_dt_parms|tr -d ' ' | awk -F"|" '{print $3}')

nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -v v_cntr=$v_cnt -v v_rpt_prd_strt_dt="'"${v_rpt_prd_strt_dt}"'" -v v_rpt_prd_end_dt="'"${v_rpt_prd_end_dt}"'" -v v_elig_month="'"${v_elig_month}"'" -f /ds_data1/${v_project}/sql/QE16/ACO/${v_pcp_algr_file_nm}

#Trigger the process for PCP Attribution for rolling 12 months
 ksh /ds_data1/${v_project}/sql/QE16/ACO/hist_pcp_attrb_algr.ksh > /ds_data1/${v_project}/log/hist_pcp_attrb_algr.log

#Triggers the process to Load Benificiary Attrition Analysis Fact
	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -v v_cntr=$v_cnt -f $1
    
   	if [ $? -ne 0 ]; then
                echo "Error in loading data into Beneficiary Attrition Analysis Fact"
                exit 1
        else
                echo "Completed loading data into Beneficiary Attrition Analysis Fact for Loop : ${v_cnt}"
        fi

done

exit
