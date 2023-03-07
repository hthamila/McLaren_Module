#!/usr/bin/ksh
v_sql_file_nm=$1

case `hostname` in
h3tudseng1) export env="uat"; export v_nzHost="c3pzmart2"; export v_nzUsr="prmretlt"; export v_project="DI_PCE_UAT";;
h3pudseng2) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp"; export v_project="DI_PCE_PROD";;
h3puprmrdseng11) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp"; export v_project="DI_PCE_PROD";;
esac


echo "Script Stared to SPLIT Outclaims Diagnosis.."

nzsql -h ${v_nzHost} -u ${v_nzUsr}  -d pce_qe16_aco_${env}_lnd -c "truncate outclaims_diag;"

echo "Inserting Admitting Diagnosis....."

nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_lnd -c "
insert into outclaims_diag
select distinct claimid, member_id, prm_fromdate, prm_todate,
        admitdiag as icd_code, 0 as seq, NULL as poa, 'D' code_type
from pce_qe16_aco_${env}_lnd..outclaims where admitdiag is not null;"

for v_cnt in {1..15}
do
nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_lnd -v v_cntr=$v_cnt -f $1
done

echo "Now inserting Data on to Beneficiary HCC Detail & HCC Coded Fact"

nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -f /ds_data1/${v_project}/sql/QE16/ACO/benf_hcc_dtl_fct.sql

     if [ $? -ne 0 ]; then
                echo "Error in loading benf_hcc_dtl_fct Table"
                exit 1
        else
                echo "Completed loading benf_hcc_dtl_fct Table"
        fi

nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -f /ds_data1/${v_project}/sql/QE16/ACO/benf_hcc_rsk_adj_fct.sql

     if [ $? -ne 0 ]; then
                echo "Error in loading benf_hcc_rsk_adj_fct Table"
                exit 1
        else
                echo "Completed loading benf_hcc_rsk_adj_fct Table"
        fi

exit
