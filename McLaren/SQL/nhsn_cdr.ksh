#!/usr/bin/ksh
TODAY=$(date '+%F')
export v_date=$(date -d "$TODAY" '+%Y-%m-01')
export v_fcy_num='MI2302,634342,MI2061,MI2055,MI5020,637619,MI2191,MI2048,MI2001,600816'
for i in $(echo $v_fcy_num | sed "s/,/ /g")
do
echo "Inserting values for Facility Num $i"
echo "insert into nhsn_cdr select $v_date,$i"
nzsql -h pzpceqe16 -u prmretlp -d pce_qe16_prd -c "insert into nhsn_cdr select '$v_date','$i';"
done
