#!/usr/bin/ksh
v_sql_file_nm=$1
v_pcp_algr_file_nm=hist_bene_pcp_attr.sql


case `hostname` in
h3tudseng1) export env="uat"; export v_nzHost="c3pzmart2"; export v_nzUsr="prmretlt"; export v_project="DI_PCE_UAT";;
h3pudseng2) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp"; export v_project="DI_PCE_PROD";;
h3puprmrdseng11) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp"; export v_project="DI_PCE_PROD";;
esac


#Drop and recreate the table for Outclaims
#nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "drop table cv_outclaims;"
#nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -f /ds_data1/${v_project}/sql/QE16/ACO/cv_outclaims.sql


#Truncate the previous loads 
nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "truncate cv_benf_assgnt_smry;"
##nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "truncate cv_orphan_outclaims;"

v_rpt_mnths=`nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -r -t -c "select months_between(val,'2018-11-30')::int||'|'||0 from dt_meta WHERE descr = 'paid_date_mnth';"`

v_mnths=$(echo $v_rpt_mnths|tr -d ' ' | awk -F"|" '{print $1}')

for v_cnt in {0..${v_mnths}}
do
echo "Execution of Loop ${v_cnt}"

##Beneficiary Assignment Step for Historicals

v_dt_parms=`nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -r -t -c "SELECT add_months(val,-(12+${v_cnt}))+1||'|'||to_char(add_months(val,-${v_cnt}),'YYYY-MM-DD')||'|'||to_char(add_months(val,-(${v_cnt}+1)) + interval '15 days','YYYY-MM-DD') FROM dt_meta WHERE descr = 'paid_date_mnth';"`

v_rpt_prd_strt_dt=$(echo $v_dt_parms|tr -d ' ' | awk -F"|" '{print $1}')
v_rpt_prd_end_dt=$(echo $v_dt_parms|tr -d ' ' | awk -F"|" '{print $2}')
v_elig_month=$(echo $v_dt_parms|tr -d ' ' | awk -F"|" '{print $3}')

echo "Beneficiary Assignment running for periods v_cntr=$v_cnt,v_rpt_prd_strt_dt=${v_rpt_prd_strt_dt},v_rpt_prd_end_dt=${v_rpt_prd_end_dt},v_elig_month=${v_elig_month}"

nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -v v_cntr=$v_cnt -v v_rpt_prd_strt_dt="'"${v_rpt_prd_strt_dt}"'" -v v_rpt_prd_end_dt="'"${v_rpt_prd_end_dt}"'" -v v_elig_month="'"${v_elig_month}"'" -f /ds_data1/${v_project}/sql/QE16/ACO/${v_pcp_algr_file_nm}


#Trigger the process for PCP Attribution for rolling 12 months
 ksh /ds_data1/${v_project}/sql/QE16/ACO/hist_pcp_attrb_algr.ksh > /ds_data1/${v_project}/log/hist_pcp_attrb_algr.log

#Loop the process to load Benficiary Assignment & Orphan Benficiaries into Outclaims

	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -v v_cntr=$v_cnt -f $1

	
        if [ $? -ne 0 ]; then
                echo "Error in loading data into Beneficiary Summary Dimension Table"
                exit 1
        else
                echo "Completed loading data into Beneficiary Summary Dimension for Loop : ${v_cnt}"
        fi


done

exit
