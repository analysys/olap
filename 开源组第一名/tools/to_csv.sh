#!/bin/sh
## 替换user_id中的id

prefix=$1
echo ${prefix}

cd output
for f in `ls ${prefix}*`;do
    awk -F"\t"  'OFS="\t"{gsub("id", "",$1);$1=$1;print $0}' ${f}  > ../csv/${f}
done