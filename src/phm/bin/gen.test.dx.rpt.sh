#!/bin/bash
#
cat ${1:-"test.dx.out"} |
egrep '(main|user|Running|Testing|"|^ *[0-9][0-9]*\.)' |
sed -e 's/"//g' -e 's/\[1\]//' -e 's/^  *//' -e 's/  */ /g' -e 's/,//g' |
gawk '
BEGIN {
	group = "NONE"
	main = "NONE"
	start_date = "NONE"
	end_date = "NONE"
	in_record = 0
	nrow = "NONE"
	algorithm = "NONE"
	time_user = 0
	time_system = 0
	time_elapsed = 0
}
NR == 1 {
	printf "GROUP,ALGORITHM,MAIN,START_DATE,END_DATE,NROW,USER,SYSTEM,ELAPSED\n"
}
$0 ~ /^Testing:/ {
	group = $2
	next
}
$0 ~ /^Running/ {
	main = $2
	next
}
$0 ~ /^START/ {
	start_date = $3
	end_date = $6
	next
}
$0 ~ /^Alinity/ {
	algorithm = $0
	in_record = 1
	next
}
$0 ~ /^NROW/ {
	nrow = $3
	next
}
$0 ~ /^user/ {
	# skip it
	next
}
$0 ~ /^[0-9]/ {
	if (in_record) {
		time_user = $1
		time_system = $2
		time_elapsed = $3
		printf "%s,%s,%s,%s,%s,%s,%s,%s,%s\n", 
			group, 
			algorithm, 
			main, 
			start_date, 
			end_date, 
			nrow, 
			time_user,
			time_system,
			time_elapsed
		in_record = 0
	}
	next
}
END {
} '
#
exit 0
