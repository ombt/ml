#!/bin/bash
#
echo "table,start_date,end_date,uniq_modsn_cnt" > dx.210.tbls.modsn.cnt.csv
#
cat dx.210.tbls.modsn.cnt | 
grep -v Table: | 
grep -A2 'select count' | 
grep -v '^  count' | 
paste - - | 
sed 's/[	 ][	 ]*/ /g' | 
cut -d' ' -f8,10,19,24 | 
sed 's/date_parse..//g'|
sed 's/ /,/g'  >> dx.210.tbls.modsn.cnt.csv
#
exit 0
