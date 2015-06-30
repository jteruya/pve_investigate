#!/bin/sh

psql -h 10.208.97.116 -p 5432 etl analytics -f "sql/pve_initial_tables.sql"

psql -h 10.208.97.116 -p 5432 etl analytics -f "sql/pve_pride_views_sequence.sql"

keep_ind=0
running_total=0

if [ -e "csv/pve_pride_views_sequence_new.csv" ]; then
  rm -f csv/pve_pride_views_sequence_new.csv
fi

while read line; do
    group_id=`echo ${line} | awk -F"," '{print $2}'`
    vieworder=`echo ${line} | awk -F"," '{print $3}'`
    day_diff=`echo ${line} | awk -F"," '{print $4}'`
    
    running_total=$((${running_total} + ${day_diff}))
    
    if [ ${vieworder} -eq 1 ]
    then 
       running_total=0
       keep_ind=1 
    elif [ ${running_total} -ge 5 -a ${vieworder} -gt 1 ]
    then 
       keep_ind=1
       running_total=0
    else
       keep_ind=0
    fi

    echo "${line},${keep_ind}" >> csv/pve_pride_views_sequence_new.csv    
               
done < csv/pve_pride_views_sequence.csv 

psql -h 10.208.97.116 -p 5432 etl analytics -f "sql/pve_pride_views_sequence_new.sql"

psql -h 10.208.97.116 -p 5432 etl analytics -f "sql/pve_non_pride_views_sequence.sql"

nd=0
running_total=0

if [ -e "csv/pve_non_pride_views_sequence_new.csv" ]; then
   rm -f csv/pve_non_pride_views_sequence_new.csv
fi

while read line; do
    group_id=`echo ${line} | awk -F"," '{print $2}'`
    vieworder=`echo ${line} | awk -F"," '{print $3}'`
    day_diff=`echo ${line} | awk -F"," '{print $4}'`

    running_total=$((${running_total} + ${day_diff}))

    if [ ${vieworder} -eq 1 ]
    then
       running_total=0
       keep_ind=1
    elif [ ${running_total} -ge 5 -a ${vieworder} -gt 1 ]
    then
       keep_ind=1
       running_total=0
    else
       keep_ind=0
    fi

    echo "${line},${keep_ind}" >> csv/pve_non_pride_views_sequence_new.csv

done < csv/pve_non_pride_views_sequence.csv

psql -h 10.208.97.116 -p 5432 etl analytics -f "sql/pve_non_pride_views_sequence_new.sql"
