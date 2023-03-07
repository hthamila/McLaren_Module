#!/usr/bin/ksh
##############################################################################
#                                                                            #
#  Source File   : mlhreport.ksh                                             #
#                                                                            #
#  Description   : Extract data McLaren Discrepancies Discharge vs Charge    #
#                  Total report to users                                     # 
#                                                                            #
#  NOTE          : Needs to be executed as DSADM                             #
#                  USAGE: mlhreport.ksh                                      #
#                  -h HostName                                               #
#                  -d SourceDb                                               #
#                  -u nzUsr                                                  #
#                  -f SQLFile                                                #
#                  -o OutputCSVFile                                          #
#                                                                            #
##############################################################################
#                                                                            #
# Mod  Date        Developer            Description of Modification(s)       #
# ---  --------    ------------- ------------------------------------------- #
# 001  2021-09-14  Dharmeswaran P       Initial Release                      #
#                                                                            #
##############################################################################
v_program_name=$(basename $0)

USAGE="USAGE: $v_program_name
                -h v_nzHost
                -d v_nzSourceDb
                -u v_nzUsr
                -f v_sourceSQLFile
                -o v_outputCSVFile"

#get command line options
while getopts ":h:d:u:f:o:" ARG
        do
        case $ARG in
                h) v_nzHost=$OPTARG ;;
                d) v_nzSourceDb=$OPTARG ;;
                u) v_nzUsr=$OPTARG ;;
                f) v_sourceSQLFile=$OPTARG ;;
                o) v_outputCSVFile=$OPTARG ;;
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
v_script_dir="/ds_data1/DI_PCE_PROD/sql/QE16/ZOOM/${v_sourceSQLFile}"
v_output_dir="/ds_data1/DI_PCE_PROD/sql/QE16/ZOOM/${v_outputCSVFile}"

#check command line parameters
if (( ${#v_nzHost} == 0 || ${#v_nzSourceDb} == 0  || ${#v_nzUsr} == 0 || ${#v_sourceSQLFile} == 0  || ${#v_outputCSVFile} == 0 ))
then
   echo "Required arguments not provided:"
   echo "$USAGE"
   exit 1
fi

#inform scheduler
echo "Script started....."


# Extract report
nzsql -h ${v_nzHost} -u ${v_nzUsr} -d ${v_nzSourceDb} -F "," -r -f ${v_script_dir} -o ${v_output_dir}


#check for return code for each process
if [ $? -ne 0 ]; then
   echo "Discrepancies Discharge vs Charge Total report extraction SQL execution failed."
   return 1
else
   echo "Completed Discrepancies Discharge vs Charge Total report extraction."
fi


#send HDML report file to user
(echo Hi All,
 echo
 echo Please find attached Discrepancies Discharge vs Charge Total Report for $(date +'%Y-%m-%d').
 echo
 echo Thanks,
 echo Premier Team) | mail -s "McLaren: Discrepancies Discharge vs Charge Total - $(date +'%Y-%m-%d')" -a ${v_output_dir} "Ravi_Pola@PremierInc.com Lisa.Vismara@mclaren.org Rushir_Shah@PremierInc.com pce_prod_support@premierinc.com"


