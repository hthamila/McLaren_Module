#!/usr/bin/ksh
v_sql_file_nm=$1
v_pcp_algr_file_nm=hist_bene_pcp_attr.sql
v_tbl=hist_bene_clm_aggr_fct

case `hostname` in
h3tudseng1) export env="uat"; export v_nzHost="c3pzmart2"; export v_nzUsr="prmretly"; export v_project="DI_PCE_UAT";;
h3pudseng2) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp"; export v_project="DI_PCE_PROD";;
h3puprmrdseng11) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp"; export v_project="DI_PCE_PROD";;
esac

##Building Historical Beneficiary Claims Aggregation Fact Table

nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "drop table ${v_tbl}_prev if exists;"
nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "create table ${v_tbl}_prev as select * from ${v_tbl};"
nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "truncate ${v_tbl};"

for v_cnt in {0..12}
do

echo "Execution of Loop ${v_cnt}"

##Beneficiary Assignment Step for Historicals

v_dt_parms=`nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -r -t -c "SELECT add_months(val,-(12+${v_cnt}))+1||'|'||to_char(add_months(val,-${v_cnt}),'YYYY-MM-DD')||'|'||to_char(add_months(val,-(${v_cnt}+1)) + interval '15 days','YYYY-MM-DD') FROM dt_meta WHERE descr = 'roll_yr_end';"`

v_rpt_prd_strt_dt=$(echo $v_dt_parms|tr -d ' ' | awk -F"|" '{print $1}')
v_rpt_prd_end_dt=$(echo $v_dt_parms|tr -d ' ' | awk -F"|" '{print $2}')
v_elig_month=$(echo $v_dt_parms|tr -d ' ' | awk -F"|" '{print $3}')

nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -v v_cntr=$v_cnt -v v_rpt_prd_strt_dt="'"${v_rpt_prd_strt_dt}"'" -v v_rpt_prd_end_dt="'"${v_rpt_prd_end_dt}"'" -v v_elig_month="'"${v_elig_month}"'" -f /ds_data1/${v_project}/sql/QE16/ACO/${v_pcp_algr_file_nm}

#Trigger the process for PCP Attribution for rolling 12 months
  ksh /ds_data1/${v_project}/sql/QE16/ACO/hist_pcp_attrb_algr.ksh > /ds_data1/${v_project}/log/hist_pcp_attrb_algr.log

#Triggers to load Rolling 12 Months Historical Beneficiary Loads
	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -v v_cntr=$v_cnt -f $1
	
        if [ $? -ne 0 ]; then
                echo "Error in loading data into Historical Beneficiary Aggregation Fact"
                exit 1
        else
                echo "Completed loading data into Historical Beneficiary Aggregation Fact for Loop : ${v_cnt}"
        fi

done

nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "update ${v_tbl} set max_rpt_prd=(select val from dt_meta where descr='roll_yr_end');" 

exit
