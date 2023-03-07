#!/usr/bin/ksh
##############################################################################
#                                                                            #
#  Source File   : pce_lake_tbl_vldtn.ksh                                    #
#                                                                            #
#  Description   : Check if current batch id can be processed                #
#                                                                            #
#  NOTE          : Needs to be executed as DSADM                             #
#                  USAGE: pce_lake_tbl_vldtn.ksh                             #
#                 -h HostName                                                #
#                 -s SourceDb                                                #
#                 -u nzUsr                                                   #
#                 -c v_custName                                              #
#                 -r v_sourceName                                            #
#                                                                            #
##############################################################################
#                                                                            #
# Mod  Date       Developer               Description of Modification(s)     #
# ---  --------   ------------- -------------------------------------------- #
#                                                                            #
# 001  2019-10-11  Ravi Kumar Pola       Initial Release                     #
#                                                                            #
##############################################################################
v_program_name=$(basename $0)
v_program_name_no_ext=`echo ${v_program_name} | cut -d\. -f1`
v_inp_dir=/ds_data1/DI_PCE_PROD/output/QE16
v_email_list=PCE_PROD_SUPPORT@premierinc.com

USAGE=" USAGE: $v_program_name
                -h v_nzHost
                -s v_nzSourceDb
                -u v_nzUsr
                -c v_custName
                -r v_sourceName"

#get command line options******************************************
while getopts ":h:s:u:c:r:" ARG
do
        case $ARG in
                h) v_nzHost=$OPTARG ;;
                s) v_nzSourceDb=$OPTARG ;;
                u) v_nzUsr=$OPTARG ;;
                c) v_custName=$OPTARG ;;
                r) v_sourceName=$OPTARG ;;
                -) echo "$USAGE"
                   exit 1 ;;
                :) echo "$v_program_name $( date +%D-%H:%M:%S ) flag $OPTARG must have an argument "
                   echo "$USAGE"
                   exit 1 ;;
                \?) echo "$v_program_name $( date +%D-%H:%M:%S ) flag $OPTARG is not a valid option "
                   echo "$USAGE"
                   exit 1 ;;
  esac
done

#check command line parameters
if (( ${#v_nzHost} == 0 || ${#v_nzSourceDb} == 0  || ${#v_nzUsr} == 0 || ${#v_custName} == 0  || ${#v_sourceName} == 0 ))
then
   echo "Required arguments not provided:"
   echo "$USAGE"
   exit 1
fi

#################################################################
##########   SEND MAIL WITH ERROR DETAILS FUNCTION     ##########
#################################################################

send_mail()
{
if [[ ${exit_code} -ne 0 ]];
then
   echo "Batch Validation Failed with Errors .........." | mailx -a $1 -s "Error in Batch Validation for Customer: ${v_custName} - Source : ${v_sourceName}" ${v_email_list}
exit $exit_code
fi
}

#################################################################
########   Batch Header Updation based on Validation     ########
#################################################################

batch_header()
{
echo "nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r -t -c update batch_header set pce_vldtn_status=$1 where customer = '${v_custName}' and source = '${v_sourceName}' and batch_id in (select distinct batch_id from vld_cnt_tbl)"
	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r -t -c "update batch_header set pce_vldtn_status=$1 where customer = '${v_custName}' and source = '${v_sourceName}' and batch_id in (select distinct batch_id from vld_cnt_tbl);" 
	exit_code=$?
	if [ ${exit_code} -ne 0 ]; then
		echo "Error in Updation of Batch Header for Batch Validation.. Please manually mark the validation as COMPLETED" > ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
		send_mail ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
	else
		echo "Batch Validation either marked as COMPLETED/VALIDATED WITH ERRORS"
	fi
}

#check if batch is ready to process

	echo "nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r-t -c select count(batch_id) from batch_header WHERE status='COMPLETED' and  pce_vldtn_status = 'NOT_VALIDATED' AND customer = '${v_custName}' and source = '${v_sourceName}')"
	CMP_BTCH_CNT=$(nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -c "SELECT count(batch_id) FROM batch_header WHERE status = 'COMPLETED' AND pce_vldtn_status = 'NOT_VALIDATED' AND customer = '${v_custName}' and source = '${v_sourceName}'" -r -t)
	if [${CMP_BTCH_CNT} -eq 0 ]; then
		echo "There are no active batches to process the loads.....All the batches has been successfully processed"
		exit 0
	fi

#check if the batch is ready for processing
	echo "nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r-t -c SELECT count(batch_id) FROM batch_header WHERE status = 'STARTED' AND pce_vldtn_status = 'NOT_VALIDATED' AND customer = '${v_custName}' and source = '${v_sourceName}'"
	BTCH_CNT=$(nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -c "SELECT count(batch_id) FROM batch_header WHERE status = 'STARTED' AND pce_vldtn_status = 'NOT_VALIDATED' AND customer = '${v_custName}' and source = '${v_sourceName}'" -r -t)
	if [ ${BTCH_CNT} -ne 0 ]; then
		echo "PULSE Loads are not Completed. Validate Batch Header table and send an email to PULSE Support" > ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
		exit_code=${BTCH_CNT}
		send_mail ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
		exit ${BTCH_CNT}
	else
		echo "PULSE Loads are Complete. Batch Validation in Progress..."
	fi


#generate lake validation sql
#execute NZSQL
	echo " nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r-t -c SELECT 'SELECT CAST(COUNT(*) as INT) as pce_extract_cnt,' || detail.dst_table_record_cnt || ' AS dst_table_record_cnt, CAST(' || detail.batch_detail_id || ' as BIGINT) as batch_detail_id, CAST(' || header.batch_id || ' as BIGINT) as batch_id, '''|| detail.dst_table_name || ''' as dst_table_name FROM '||detail.dst_db_name||'..'|| detail.dst_table_name || ' WHERE rcrd_btch_audt_id= '|| header.batch_id || ' and rcrd_src_file_nm= '''|| detail.src_file_name ||'''' as sql FROM batch_header header INNER JOIN batch_detail detail ON header.batch_id = detail.batch_id and header.dst_db_name=detail.dst_db_name WHERE header.status = 'COMPLETED' AND header.pce_vldtn_status = 'NOT_VALIDATED' AND header.customer = '${v_custName}' AND header.source = '${v_sourceName}' "

	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r -t -c "SELECT 'SELECT CAST(COUNT(*) as INT) as pce_extract_cnt,' || detail.dst_table_record_cnt || ' AS dst_table_record_cnt, CAST(' || detail.batch_detail_id || ' as BIGINT) as batch_detail_id, CAST(' || header.batch_id || ' as BIGINT) as batch_id, '''|| detail.dst_table_name || ''' as dst_table_name FROM '||detail.dst_db_name||'..'|| detail.dst_table_name || ' WHERE rcrd_btch_audt_id= '|| header.batch_id || ' and rcrd_src_file_nm= '''|| detail.src_file_name ||'''' as sql FROM batch_header header INNER JOIN batch_detail detail ON header.batch_id = detail.batch_id and header.dst_db_name=detail.dst_db_name WHERE header.status = 'COMPLETED' AND header.pce_vldtn_status = 'NOT_VALIDATED' AND header.customer = '${v_custName}' AND header.source = '${v_sourceName}'" > ${v_inp_dir}/${v_custName}_${v_sourceName}_lake_validation.sql
	exit_code=$?
#check for return code
        if [ ${exit_code} -ne 0 ]; then
                echo "Error in generation of Lake Validation SQL" > ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
		send_mail ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
		exit ${exit_code}
	else
		echo "Lake Validation Query generated successfully"
	fi
#remove the last empty line on the file & add union to the sql
	sed -i.bak '$d' ${v_inp_dir}/${v_custName}_${v_sourceName}_lake_validation.sql
	sed -i.bak '$!s/$/ UNION /' ${v_inp_dir}/${v_custName}_${v_sourceName}_lake_validation.sql
	sed -i '1i create table vld_cnt_tbl as ' ${v_inp_dir}/${v_custName}_${v_sourceName}_lake_validation.sql

	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -c "drop table vld_cnt_tbl if exists;"
	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -f ${v_inp_dir}/${v_custName}_${v_sourceName}_lake_validation.sql

#check for return code
	exit_code=$?
        if [ $? -ne 0 ]; then
                echo "Error in creation of validation table on UTL Database" > ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
		send_mail ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
                exit ${exit_code}
        else
                echo "Lake Validation count table is created on UTL Database"
        fi

#update the counts on batch_detail table
	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r -t -c "update batch_detail bd set pce_extract_cnt=vld.pce_extract_cnt, pce_match_cnt_ind=case when vld.dst_table_record_cnt=vld.pce_extract_cnt then 'Y' ELSE 'N' END from vld_cnt_tbl vld where bd.batch_id=vld.batch_id and bd.batch_detail_id=vld.batch_detail_id"

	exit_code=$?
	if [ $? -ne 0 ]; then
                echo "Error in updation of batch detail table with validation counts" > ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
                send_mail ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
		exit ${exit_code}
        else
                echo "Successfully updated batch detail with validation counts"
        fi

#validate if landing counts matching with pulse counts
	ERR_CNT=$(nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -c "select sum(pce_extract_cnt-dst_table_record_cnt) from vld_cnt_tbl;" -r -t)
	if [ ${ERR_CNT} -ne 0 ]; then
		echo "Mismatch between Batch Header Pulse Load and Landing Database ... Please validate the counts and send an email to PULSE Suppport" > ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
		echo "---------------------------------------------------------------------------------------------------------------------------------" >> ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
		nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -c "select *, case when pce_extract_cnt=dst_table_record_cnt then 'Y' ELSE 'N' end as vldtn_ind from vld_cnt_tbl"  >> ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
		exit_code=${ERR_CNT}
		
		batch_header "'VALIDATED_WITH_ERRORS'"
		send_mail ${v_inp_dir}/${v_custName}_${v_sourceName}_batch_validation.err
		exit ${exit_code}
	else
		echo "No Mismatch found between Batch Header and Landing Database, proceeding with batch process"
		batch_header "'COMPLETED'"
	fi
