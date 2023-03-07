#!/bin/ksh
#!/usr/bin/ksh
##############################################################################
#                                                                            #
#  Program Name   : pcp_attrb_algr.ksh                                       #
#                                                                            #
#  Description    : Algorithm for PCP Assignment on ACO 		     #
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

export PATH=$PATH:$HOME/bin:/opt/app/nz/nz/bin

case `hostname` in
h3tudseng1) export env="uat"; export v_nzHost="c3pzmart2"; export v_nzUsr="prmretlt";;
h3pudseng2) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp";;
h3puprmrdseng11) export env="prd"; export v_nzHost="pzpceqe16"; export v_nzUsr="prmretlp";;
esac


##Load data to a temp table
echo "Backup and re-create Beneficiary PCP Temp Table"
nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "drop table temp_bene_pcp_services if exists;"
nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "create table temp_bene_pcp_services as select * from bene_pcp_vw;"

v_loop_cnt=`nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "select max(rnk) from temp_bene_pcp_services;" -t -r`

##Do the First Insert before loop for MHPN Physician
echo "Delete old data from Beneficiary Attribution Table"
	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "delete from bene_pcp_attr;" 

echo "First Insert with PCP's ranked as 1"
	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "
		insert into bene_pcp_attr
			select distinct pln_mbr_sk, mbr_id_num, bill_pvdr_sk, pcs_cnt, paid_amt, last_date,strt_mn, end_mn,npi,actv_sts, rnk,
		1 in_ntw_ind, 'MHPN Physician' as attr from temp_bene_pcp_services
		where rnk=1 and mhpn_indicator=1 and actv_sts=1;"

     if [ $? -ne 0 ]; then
                echo "Error in processing Beneficiary PCP attribution for Ranked 1"
                exit 1
        else
                echo "Completed loading Beneficiary PCP attribution for Ranked 1"
        fi



##Now loop it to load MHPN Physician from Rank 2

echo "Loop it to load MHPN Physician from Rank 2"
for v_cnt in {2..$v_loop_cnt}
do

	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "
		insert into bene_pcp_attr
			select distinct pln_mbr_sk, mbr_id_num, bill_pvdr_sk, pcs_cnt, paid_amt, last_date,strt_mn, end_mn,npi,actv_sts, rnk,
                1 in_ntw_ind, 'MHPN Physician' as attr from temp_bene_pcp_services
        where rnk=$v_cnt and mhpn_indicator=1 and actv_sts=1 and pln_mbr_sk not in (select pln_mbr_sk from bene_pcp_attr);"

    if [ $? -ne 0 ]; then
                echo "Error in processing Beneficiary PCP attribution for MHPN Ranked $v_cnt"
                exit 1
        else
                echo "Completed loading Beneficiary PCP attribution for MHPN Ranked $v_cnt"
        fi

done

echo "Loop it to load MPP Physician from if available from Rank 1"
##Now loop it to load MPP Physician from Rank 1
for v_cnt in {1..$v_loop_cnt}
do

	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "
		insert into bene_pcp_attr
			select distinct pln_mbr_sk, mbr_id_num, bill_pvdr_sk, pcs_cnt, paid_amt, last_date,strt_mn, end_mn,npi,actv_sts, rnk,
                1 in_ntw_ind, 'MPP Physician' as attr from temp_bene_pcp_services
        where rnk=$v_cnt and mpp_indicator=1 and actv_sts=1 and pln_mbr_sk not in (select pln_mbr_sk from bene_pcp_attr);"


    if [ $? -ne 0 ]; then
                echo "Error in processing Beneficiary PCP attribution for MPP Ranked $v_cnt"
                exit 1
        else
                echo "Completed loading Beneficiary PCP attribution for MPP Ranked $v_cnt"
        fi

done

echo "Last set to include Retired/Inactive Physicians from McLaren"
##Assignment of Inactive/Retired Physicians
	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "
		insert into bene_pcp_attr
			select distinct pln_mbr_sk, mbr_id_num, bill_pvdr_sk, pcs_cnt, paid_amt, last_date,strt_mn, end_mn,npi,actv_sts, rnk,  
		1 as in_ntw_ind, 'Retired/Inactive Physician' as attr
		from temp_bene_pcp_services 
			where rnk=1 and mhpn_indicator=1 and  actv_sts=0 and pln_mbr_sk not in (select pln_mbr_sk from bene_pcp_attr);"
        nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "
                insert into bene_pcp_attr
                        select distinct pln_mbr_sk, mbr_id_num, bill_pvdr_sk, pcs_cnt, paid_amt, last_date,strt_mn, end_mn,npi,actv_sts, rnk,
                1 as in_ntw_ind, 'Retired/Inactive Physician' as attr
                from temp_bene_pcp_services
                        where rnk=1 and mpp_indicator=1 and  actv_sts=0 and pln_mbr_sk not in (select pln_mbr_sk from bene_pcp_attr);"

    if [ $? -ne 0 ]; then
                echo "Error in processing Beneficiary PCP attribution for Retired/Inactive Physicians"
                exit 1
        else
                echo "Completed loading Beneficiary PCP attribution for Retired/Inactive Physicians"
        fi

##Remove if duplicates are found
	nzsql -h ${v_nzHost} -u ${v_nzUsr} -d pce_qe16_aco_${env}_dm -c "delete from bene_pcp_attr where rowid in (select min(rowid) from bene_pcp_attr group by mbr_id_num having count(1)>1);"

	if [ $? -ne 0 ]; then
                echo "Error in Removing Duplicate Records for Beneficiary assigned with two or more Providers"
                exit 1
        else
                echo "Removed Duplicates and assigned only 1 provider to Beneficiary"
        fi

exit 0
