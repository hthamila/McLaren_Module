#!/usr/bin/ksh
##############################################################################
#                                                                            #
#  Source File   : ValidateStartProcess.ksh                                  #
#                                                                            #
#  Description   : Check if current batch id can be processed                #
#                                                                            #
#  NOTE          : Needs to be executed as DSADM                             # 
#				   USAGE: CopyTableStg3ToREP.ksh                             # 
#				  -h HostName                                                #
# 				  -s SourceDb                                                #
#                 -u nzUsr                                                   #                                      
#                 -c v_custName                                              #                                      
#                 -r v_sourceName                                            #                                      
#                                                                            #
##############################################################################
#                                                                            #
# Mod  Date       Developer               Description of Modification(s)     #
# ---  --------   ------------- -------------------------------------------- #
# 001  2014-08-11  Essex De Guzman       Initial Release                     # 
#                                                                            #
##############################################################################
v_program_name=$(basename $0)
v_program_name_no_ext=`echo ${v_program_name} | cut -d\. -f1`


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

update_etl_process_time()
{
	#execute NZSQL 
	echo
	echo "nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -c 'UPDATE batch_header SET pce_prcs_status = 'STARTED', pce_prcs_start_ts = CURRENT_TIMESTAMP WHERE batch_id = ${v_CurrBatchId}' and customer='${v_custName}' and source='${v_sourceName}';" 
	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -c "UPDATE batch_header SET pce_prcs_status = 'STARTED', pce_prcs_start_ts = CURRENT_TIMESTAMP WHERE batch_id = ${v_CurrBatchId} and customer='${v_custName}' and source='${v_sourceName}';" 
	#check for return code 
	if [ $? -ne 0 ]; then
		echo "Error updating pce_prcs_status and pce_prcs_start_ts ${v_nzSourceDb}..${v_tableName} using batch id:${v_CurrBatchId} for ${v_Customer} ${v_Source}" 
		return 1
	else
		echo "Completed updating pce_prcs_status and pce_prcs_start_ts ${v_nzSourceDb}..${v_tableName} using batch id:${v_CurrBatchId} for ${v_Customer} ${v_Source}" 
	fi
	return 0
}

#inform scheduler 
echo "Script started....."

echo "Check if most current batch id has no errors" 

echo "nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -c 'SELECT header.pce_vldtn_status || '|' || header.batch_id || '|' || header.customer || '|' || header.source FROM batch_header header inner join (SELECT customer, source, min(batch_id) AS curr_batch_id FROM batch_header WHERE pce_prcs_status IS NULL AND status = 'COMPLETED' AND customer = '${v_custName}' AND source = '${v_sourceName}' GROUP BY batch_header.customer, batch_header.source ) current_batch on header.batch_id = current_batch.curr_batch_id '" 

for v_CurrBatchRec in `nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r -t -c "SELECT header.customer || '|' || header.source || '|' || header.batch_id || '|' || header.pce_vldtn_status|| '|' ||header.dst_db_name  FROM batch_header header inner join (SELECT customer, source, min(batch_id) AS curr_batch_id FROM batch_header WHERE pce_prcs_status IS NULL AND status = 'COMPLETED' AND customer = '${v_custName}' AND source = '${v_sourceName}' GROUP BY batch_header.customer, batch_header.source ) current_batch on header.batch_id = current_batch.curr_batch_id and header.customer=current_batch.customer and header.source=current_batch.source;"`
do 
    
	#check current batch id status
	v_Customer=$(echo ${v_CurrBatchRec} | awk -F"|" '{print $1}')
	v_Source=$(echo ${v_CurrBatchRec} | awk -F"|" '{print $2}')
	v_CurrBatchId=$(echo ${v_CurrBatchRec} | awk -F"|" '{print $3}')
	v_Status=$(echo ${v_CurrBatchRec} | awk -F"|" '{print $4}')
	v_dst_db=$(echo ${v_CurrBatchRec} | awk -F"|" '{print $5}')
	echo "Validating Customer: ${v_Customer}"
	echo "Using Source System: ${v_Source}"
	echo "Current Batch Id: ${v_CurrBatchId}"
	echo "Status: ${v_Status}"
done

echo "Current Batch Id: ${v_CurrBatchId}"

v_error_rcrd_cnt=`nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r -t -c "select count(*) from batch_detail where batch_id = '${v_CurrBatchId}' and dst_db_name='${v_dst_db}' and error_record_cnt >0;"`

echo "Error Record Count for current Batch: ${v_error_rcrd_cnt}"

if [ ${v_Status} != 'COMPLETED' ]; then
	echo "Batch ID: ${v_CurrBatchId} has incomplete data. Stopping data processing......" 
	exit 1
else if [[ ${v_Status} == 'COMPLETED' && ${v_error_rcrd_cnt} -gt 0 ]] 
then
	echo "Batch ID: ${v_CurrBatchId} is completed but it has some error records....Stopping data processing......."
	exit 1     
fi
fi
echo "Batch ID is ready to process. Updating Batch Header table...." 
update_etl_process_time
if [ $? -ne 0 ]; then
	echo "Failing script..." 
	exit 1
else
	echo "Script Finished...." 
	exit 0
fi

