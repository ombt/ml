#!/bin/bash
#
echo 'table,maxdate,mindata' > all.dates.csv
#
grep -B2 '^1 201' all.dx.tbls.dates | 
grep -v -- -- | 
sed  's///g' | 
grep -v '^ *max_day' | 
paste - - | 
sed 's/^.*from //' | 
sed 's/"[^1]*/ /' | 
cut -d' ' -f1,3,5 | 
sed 's/ /,/g' >> all.dates.csv

