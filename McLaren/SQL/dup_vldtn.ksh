#!/usr/bin/ksh
##############################################################################
#                                                                            #
#  Source File   : BatchValidation.ksh                                       #
#                                                                            #
#  Description   : Validate for any duplicate batches or duplicate           #
#                  BK's in the given set of tables                           #
#                                                                            #
#  NOTE          : Needs to be executed as DSADM                             #
#                                                                            #
##############################################################################
#                                                                            #
# Mod  Date       Developer               Description of Modification(s)     #
# ---  --------   ------------- -------------------------------------------- #
# 001  2015-04-16  Mano Sridharan & Sree Madabhushi   Initial Release        #
# 002  2015-04-22  Mano  & Sree  Review comments incorporated.               #
#                                Alert email functionality added.            #
# 003  2015-07-30  Mano & Nikhil Added vld_fm_dt for fact table validation   #
#                                & fixed the T2CC table validation issue     #
#                                                                            #
##############################################################################
v_program_name=$(basename $0)
v_program_name_no_ext=`echo ${v_program_name} | cut -d\. -f1`
v_date=$(date +"%Y%d%m_%H%M%S")
v_inp_dir=/ds_data1/DI_PCE_PROD/output/QE16

USAGE=" USAGE: $v_program_name
                -h v_nzHost
                -d v_nzDb
                -u v_nzUsr
                -e v_emailid"

export PATH=$PATH:$HOME/bin:/opt/app/nz/nz/bin

#get command line options******************************************
while getopts ":h:d:u:e:" ARG
do
        case $ARG in
                h) v_nzHost=$OPTARG ;;
                d) v_nzDb=$OPTARG ;;
                u) v_nzUsr=$OPTARG ;;
                e) v_emailid=$OPTARG;;
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


#inform scheduler
echo "Script started....."


#create log directory if it doesn't exist
v_log_dir="/ds_data2/Projects/DI_PCD_INT/log"
if [ ! -d "${v_log_dir}" ]; then
    echo "creating directory ${v_log_dir}"
    mkdir "${v_log_dir}"
fi

#create today's log file , pids and error pid file
v_pid_file="${v_log_dir}/${v_program_name_no_ext}.${v_date}.pid"
v_err_pid_file="${v_log_dir}/${v_program_name_no_ext}.${v_date}.err"
echo "creating PIDs file ${v_pid_file}"
touch ${v_pid_file}
echo "creating error PIDs file ${v_err_pid_file}"
touch ${v_err_pid_file}


#execute NZSQL
BatchValidation_fct()
{
colnm=`nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -r -t -c "select column_name from _V_SYS_COLUMNS where column_name like '%vld_fm_dt%' and table_name = '${aTable}';"`
echo $colnm  |sed 's/^[ \t]*//;s/[ \t]*$//' |& read -p v_colnm
if [[ -z "$v_colnm" ]] then


 echo "Checking for duplicate Bk's in ${aTable}"
 echo "nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -c 'select count(*) as BK_CNT from (SELECT upper(${v_Bk}), count(1) FROM ${aTable} group by 1  having count(1) >1) as T1'"
 echo "--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
 BK_CNT=$(nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -c "select count(*) as BK_CNT from (SELECT upper(${v_Bk}), count(1) FROM ${aTable} group by 1  having count(1) >1) as T1" -r -t)
    if [[ ${BK_CNT} -ne 0 ]]; then
        echo "Duplicate Bk present in the table : ${aTable}"
        return 1
    else
        return 0
    fi


else


 echo "Checking for duplicate Bk's in Fact table ${aTable}"

 echo "nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -c 'select count(*) as BK_CNT from (SELECT upper(${v_Bk}), ${v_colnm}, count(1) FROM ${aTable} group by 1,2  having count(1) >1) as T1'"
 echo "--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
 BK_CNT=$(nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -c "select count(*) as BK_CNT from (SELECT upper(${v_Bk}), ${v_colnm}, count(1) FROM ${aTable} group by 1,2  having count(1) >1) as T1" -r -t)
   if [[ ${BK_CNT} -ne 0 ]]; then
        echo "Duplicate Bk present in the Fact table : ${aTable}"
        return 1
    else
        return 0
    fi

fi
}



BatchValidation_dim()
{
        echo "Checking for duplicate Bk's in Dim table ${aTable}"
        echo "nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -c 'select count(*) as BK_CNT from (SELECT ${v_Bk}, count(1) FROM ${aTable} group by ${v_Bk} having count(1) >1) as T1'"
        echo "-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------"
        BK_CNT=$(nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -c "select count(*) as BK_CNT from (SELECT ${v_Bk}, count(1) FROM ${aTable} group by ${v_Bk} having count(1) >1) as T1" -r -t)
        if [[ ${BK_CNT} -ne 0 ]]; then
            echo "Duplicate Bk present in the Dim table : ${aTable}"
            return 1
	else
	    return 0
        fi

}

#create source table primary key file for validation
 nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -c "select 'cv_'||src_tbl_name||'~'||src_tbl_pk from dearchive_metadata where vldtn_ind='Y'" -r -t > $v_inp_dir/zoom_src_tbl_pk.txt

#read input file 
cat $v_inp_dir/zoom_src_tbl_pk.txt | while read line
do
        #Read the arguments
        aTable=$(echo $line | awk -F"~" '{print $1}')
        v_Bk=$(echo $line | awk -F"~" '{print $2}')
        #move individual tables by calling function in background
		echo "Validating duplicates in table..."
			BatchValidation_dim &
			echo "$aTable~${v_nzDb}~${v_Bk}~$!">>${v_pid_file}
done

#read pid file
cat $v_pid_file | while read line
do
        #check completion for each process
        v_table_name=$(echo $line | awk -F"~" '{print $1}')
        v_db=$(echo $line | awk -F"~" '{print $2}')
        v_Bk=$(echo $line | awk -F"~" '{print $3}')
        v_pid=$(echo $line | awk -F"~" '{print $4}')
        echo "Waiting for table $v_table_name to finish ...."
        wait $v_pid
        v_ret_code=${?}
        #check for return code for each process
        if [[ ${v_ret_code} -ne 0 ]]; then
                echo "ERROR! $v_table_name failed validation! dbname: $v_db pid: $v_pid"
                echo "'ERROR! FAILED VALIDATION! '$v_table_name' has duplicates on '$v_Bk' in the DB: $v_db'" >> ${v_err_pid_file}
        else
                echo "$v_table_name finished! pid: $v_pid"
        fi
done

#check if error is created in err file
v_count_err_line=$(wc -l ${v_err_pid_file} | awk -F" " '{print $1}')
if [ ${v_count_err_line} -ne 0 ]; then
                echo "Validation Failed!!!"
                echo "Script end..."
                echo | mutt -s "ALERT EMAIL!!! '${v_nzDb}' DATABASE HAS DUPLICATES! PLEASE VALIDATE LANDING DATABASE!" -i ${v_err_pid_file} ${v_emailid}
                exit 1
else
                echo "Validation Successful!!!"
                echo "Script end..."
                exit 0
fi

