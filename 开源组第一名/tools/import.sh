#!/bin/sh

prefix=$1
echo ${prefix}
for f in `ls csv/${prefix}*`;do
    /data/ccc/ccc --client --query="INSERT INTO dis_event FORMAT TabSeparated"
done
