#!/bin/sh
sql=$1
sh q.sh  "$sql"  | awk '{for(i=1;i<=NF;i++){a[i] += $i}}END{ for(j=1;j<=length(a);j++) {printf a[j]" "
 };printf "\n" }'