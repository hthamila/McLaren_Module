#!/usr/bin/ksh
##############################################################################
#                                                                            #
#  Program Name   : mclarenLandingCheck.sh                                   #
#                                                                            #
#  Description    : This script checks for batch completion in landing       #
#                   and creates trigger file for extraction                  #
#                                                                            #
#                                                                            #
##############################################################################
#                                                                            #
# Mod  Date       Developer               Description of Modification(s)     #
# ---  --------   -------------           ---------------------------------- #
# 001  2021-10-18  Dharmeswaran P         Initial Release                    #
#                                                                            #
##############################################################################
v_program_name=$(basename $0)

USAGE="USAGE: $v_program_name
                -h v_nzHost
                -d v_nzSourceDb
                -u v_nzUsr
                -c v_customerName
                -p v_processType"


#get command line options
while getopts ":h:d:u:c:p:" ARG
        do
        case $ARG in
                h) v_nzHost=$OPTARG ;;
                d) v_nzSourceDb=$OPTARG ;;
                u) v_nzUsr=$OPTARG ;;
                c) v_customerName=$OPTARG ;;
                p) v_processType=$OPTARG ;;
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

run_date=$(date +'%Y-%m-%d')
v_mlh_dir="/share/qe16/etl/to_premier/from_pulse/triggers"
v_mlh_dir_arc="/ds_data1/DI_PCE_PROD/sql/QE16/trigger/archive/"

#check command line parameters
if (( ${#v_nzHost} == 0 || ${#v_nzSourceDb} == 0  || ${#v_nzUsr} == 0 || ${#v_customerName} == 0 || ${#v_processType} == 0 ))
then
   echo "Required arguments not provided:"
   echo "$USAGE"
   exit 1
fi

#inform scheduler
echo "Script started....."

if [[ $v_processType == "LANDING_CHECK" ]]
then
	for i in {1..2}
	do

	  echo "Check for batch completion" 
	  echo "nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r -t -c 'SELECT COUNT(DISTINCT source) FROM pce_qe16_prd_utl..batch_header_zoom_test WHERE customer=upper('${v_customerName}') and upper(source) IN ('INST_BILL', 'CERNER', 'STLUKES') and status='COMPLETED' and pce_prcs_start_ts is NULL and pce_prcs_end_ts is NULL'"
          SRC_CNT=$(nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r -t -c "SELECT COUNT(DISTINCT source) FROM pce_qe16_prd_utl..batch_header_zoom_test WHERE customer=upper('${v_customerName}') and upper(source) IN ('INST_BILL', 'CERNER', 'STLUKES') and status='COMPLETED' and pce_prcs_start_ts is NULL and pce_prcs_end_ts is NULL")

          echo "Total number of source completed : ${SRC_CNT}"

	  if [[ ${SRC_CNT} -ne 3 ]]
	  then
		  echo "Waiting for all source to get loaded..."
		  sleep 2m
		  if [[ $i -eq 2 ]]
		  then
			echo "McLaren landing completion check job failed at " $(date)
			subject="FAILED:McLaren PROD LANDING CHECK FAILED - ZOOM PROCESS NOT STARTED"
			email="Dharmeswaran_Paramasivam@PremierInc.com"
			echo "" | mailx -s "$subject" "$email"
			exit 1
		  fi
	  else
		echo "All McLaren source load completed" $(date)
		subject="SUCCESS:McLaren PROD LANDING CHECK COMPLETED"
		email="Dharmeswaran_Paramasivam@PremierInc.com"
		echo "" | mailx -s "$subject" "$email"
#		mv -f $v_mlh_dir/trigger/MCLARN_PULSE_LANDING_PROD_BATCH_COMPLETED_* $v_mlh_dir/trigger/archive/
		touch $v_mlh_dir/trigger/MCLARN_PULSE_LANDING_PROD_BATCH_COMPLETED_`date +'%Y%d%m_%H%M%S'`.txt
                echo "Job completed at "$(date)
		exit 0
	fi
	done
else
	if [[ $v_processType == "LANDING_UPDATE" ]]
	then
	  echo "Updating batch header after zoom process"
	  echo "nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r -t -c 'update pce_qe16_prd_utl..batch_header set pce_prcs_start_ts=now(), pce_prcs_end_ts=now(), pce_prcs_status = 'COMPLETED' where customer=upper('${v_customerName}') and upper(source) IN ('INST_BILL', 'CERNER', 'STLUKES') and status='COMPLETED' and pce_vldtn_status = 'COMPLETED' and pce_prcs_start_ts is NULL and pce_prcs_end_ts is NULL'"
	  nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -r -t -c "update pce_qe16_prd_utl..batch_header set pce_prcs_start_ts=now(), pce_prcs_end_ts=now(), pce_prcs_status = 'COMPLETED' where customer=upper('${v_customerName}') and upper(source) IN ('INST_BILL', 'CERNER', 'STLUKES') and status='COMPLETED' and pce_vldtn_status = 'COMPLETED' and pce_prcs_start_ts is NULL and pce_prcs_end_ts is NULL"
         mv -f $v_mlh_dir/InstBill/MCLAREN_HEALTH_INST_BILL_PROD_BATCH_COMPLETED_* $v_mlh_dir_arc/InstBill
         mv -f $v_mlh_dir/Cerner/MCLAREN_HEALTH_CERNER_PROD_BATCH_COMPLETED_* $v_mlh_dir_arc/Cerner
	 mv -f $v_mlh_dir/Stlukes/MCLAREN_HEALTH_STLUKES_PROD_BATCH_COMPLETED_* $v_mlh_dir_arc/Stlukes
         mv -f $v_mlh_dir/CernerEMPI/MCLAREN_HEALTH_CERNER_EMPI_PROD_BATCH_COMPLETED_* $v_mlh_dir_arc/CernerEMPI
        fi
fi

