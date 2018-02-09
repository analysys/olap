#!/bin/sh
# 2631201798数据
# 668726242   668904531  637996737 655574288

prefix=$1
output="`pwd`/output"
mkdir -p $output
for f in `ls /data/zhaoshu/${prefix}*`;do
	./index -file="$f" -out="`pwd`/output"
done

