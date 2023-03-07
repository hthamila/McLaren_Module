#!/bin/ksh
#!/usr/bin/ksh
##############################################################################
#                                                                            #
#  Program Name   : pce_int_load_hcc_scr.ksh                                 #
#                                                                            #
#  Description    : Script to maintain Historical HCC Score		     #
#                                                                            #
#  NOTE           : Needs to be executed as DSADM                            #
#                                                                            #
##############################################################################
#                                                                            #
# Mod  Date       Developer               Description of Modification(s)     #
# ---  --------   -------------           ---------------------------------- #
# 001  2018-04-27 Ravi Kumar Pola         Initial Release                    #
##############################################################################

#Prepare Parameters

case `hostname` in
h3tudseng1) export env="uat"; export v_nzHost="c3pzmart2"; export v_nzUsr="prmretlt";;
h3pudseng2) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp";;
h3puprmrdseng11) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp";;
esac

v_qtr_flg=$1

if [ ${v_qtr_flg} == 'YES' ]; then

echo "Process to load Quarterly HCC Score"

echo "Clean-up process on HCC Score until Prior Quarter"

nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_lnd -c "delete from hist_bene_hcc_scr where elig_month >= (select max(qtr_beg_dt) from pce_qe16_aco_${env}_lnd..qtr_cdr);"
	if [ $? -ne 0 ]; then
        	echo "Error in purge process of HCC Score"
                exit 1
        else
                echo "Completed purging Beneficiary HCC Score until Prior Quarter"
        fi

for i in {0..2}
do
echo "Loading $i Month of Current Quarter HCC Score to the existing Data"

nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_lnd -c "insert into hist_bene_hcc_scr select distinct member_id as mbr_id_num, hcc_scr as cms_hcc_scor_num, elig_month,
                date_part('year',to_date(elig_month,'YYYY-MM-DD')) yr_num, date_part('month',to_date(elig_month,'YYYY-MM-DD')) mo_of_yr_num,
                trim(to_char(date(elig_month),'MONTH')) mo_of_yr_nm, trim(to_char(date(elig_month),'MONTH'))||' '||date_part('year',to_date(elig_month,'YYYY-MM-DD')) mo_and_yr_nm, hcc_cnt
        from pce_qe16_aco_${env}_lnd..cv_member_time_windows mtw
                left join pce_qe16_aco_${env}_dm..benf_hcc_vw on member_id=mbi
                left join (select max(qtr_beg_dt) qtr_beg_dt, add_months(max(qtr_beg_dt),$i)+interval '14 days' elig_months from pce_qe16_aco_${env}_lnd..qtr_cdr)d on 1=1
        where assignment_indicator='Y' and elig_month=elig_months;"

       if [ $? -ne 0 ]; then
                echo "Error in processing Beneficiary HCC Score Data"
                exit 1
       else
                echo "Completed loading Beneficiary HCC Score for $i Month of Current Quarter"
       fi

done

else

v_max_mnth=`nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_lnd -c "select date_part('month',max(elig_month)) from hist_bene_hcc_scr;" -t -r`
v_cur_mnth=`nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "select date_part('month',val::date) from dt_meta where descr='roll_yr_end';" -t -r`

if [ ${v_max_mnth} -gt ${v_cur_mnth} ]; then
	echo "HCC Score for beneficiary is up-to date, no loads necessary"
		exit 0
	else

	echo "Loading Current Month HCC Score to the existing Data"
        nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_lnd -c "insert into hist_bene_hcc_scr select distinct member_id as mbr_id_num, hcc_scr as cms_hcc_scor_num, elig_month,
		date_part('year',to_date(elig_month,'YYYY-MM-DD')) yr_num, date_part('month',to_date(elig_month,'YYYY-MM-DD')) mo_of_yr_num,
		trim(to_char(date(elig_month),'MONTH')) mo_of_yr_nm, trim(to_char(date(elig_month),'MONTH'))||' '||date_part('year',to_date(elig_month,'YYYY-MM-DD')) mo_and_yr_nm, hcc_cnt
	from pce_qe16_aco_${env}_lnd..cv_member_time_windows mtw
        	left join pce_qe16_aco_${env}_dm..benf_hcc_vw on member_id=mbi
        	left join (select val, date_trunc('month',(add_months(val,-0)))+interval '14 days' elig_months from pce_qe16_aco_${env}_dm..dt_meta where descr='roll_yr_end')d on 1=1
        where assignment_indicator='Y' and 
			elig_month=elig_months;"

        if [ $? -ne 0 ]; then
                echo "Error in processing Beneficiary HCC Score Data"
                exit 1
        else
                echo "Completed loading Beneficiary HCC Score for Current Month"
        fi



fi

fi
exit 0
