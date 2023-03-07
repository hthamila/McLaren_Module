#!/bin/ksh
#!/usr/bin/ksh
##############################################################################
#                                                                            #
#  Program Name   : pce_int_load_hcc_scr.ksh                                 #
#                                                                            #
#  Description    : Script to maintain Historical HCC Score                  #
#                                                                            #
#  NOTE           : Needs to be executed as DSADM                            #
#                                                                            #
##############################################################################
#                                                                            #
# Mod  Date       Developer               Description of Modification(s)     #
# ---  --------   -------------           ---------------------------------- #
# 001  2018-04-27 Ravi Kumar Pola         Initial Release                    #
##############################################################################

v_program_name=$(basename $0)
v_program_name_no_ext=`echo ${v_program_name} | cut -d\. -f1`
v_date=$(date +"%Y%d%m_%H%M%S")
v_log_dir=/ds_data1/DI_PCE_PROD/log

export PATH=$PATH:$HOME/bin:/opt/app/nz/nz/bin

USAGE=" USAGE: $v_program_name
                -h v_nzHost
                -u v_nzUsr
                -d v_nzDb
                -c v_custnm
                -f v_filenm
		-l v_loadtype"

#get command line options
while getopts ":h:u:d:c:f:l:" ARG
        do
        case $ARG in
                h) v_nzHost=$OPTARG ;;
                u) v_nzUsr=$OPTARG ;;
                d) v_nzDb=$OPTARG ;;
                c) v_custnm=$OPTARG ;;
                f) v_filenm=$OPTARG ;;
		l) v_loadtype=$OPTARG ;;
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
if (( ${#v_nzHost} == 0 ||  ${#v_nzUsr} == 0 || ${#v_nzDb} == 0 || ${#v_custnm} == 0 || ${#v_filenm} == 0 || ${#v_loadtype} == 0))
then
   echo "Required arguments not provided:"
   echo "$USAGE"
   exit 1
fi

#inform scheduler
echo "Script started....."

v_process_file="${v_log_dir}/${v_program_name_no_ext}_${v_custnm}_$(date +"%Y%d%m_%s").out"
v_err_file="${v_log_dir}/${v_program_name_no_ext}_${v_custnm}_$(date +"%Y%d%m_%s").err"
v_sql_dir=/ds_data1/DI_PCE_PROD/sql/${v_custnm}


#identify the load type

if [ ${v_loadtype} = 'R' ]; then

echo "Claim Line Data source Trigger is a Reload"
        else
echo "Claim Line Data source Trigger is a Standard Load, to take a backup of existing Data"
       nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -c "drop table clm_line_fct_ds_prev if exists;"
       nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -c "alter table clm_line_fct_ds rename to clm_line_fct_ds_prev;"

fi
#Execute the Insert Script
echo "nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -f ${v_sql_dir}/$v_filenm" >> ${v_process_file}
nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzDb} -f ${v_sql_dir}/$v_filenm
if [ $? -ne 0 ]; then
        echo "Error executing the SQL from the file ${v_filenm}" > ${v_process_file}
else
        echo "SQL executed successfully from the file ${v_filenm}" > ${v_process_file}
fi

v_count_err_line=`grep Error ${v_process_file}|wc -l`
if [ ${v_count_err_line} -ne 0 ]; then
                echo "Failed to execute all the SQLs, please check error logs ${v_process_file}"
                echo "Script end..."
                exit 1
else
                echo "Successfully executed all the SQLs!!!"
                echo "Script end..."
                exit 0
fi
