#!/bin/sh
sql=$1
clients="10.9.161.77 10.9.113.205 10.9.83.235 10.9.169.253"

function fetch(){
	echo "$sql" | /data/ccc/ccc --client -m --host $1
}
for c in $clients;do
	fetch "$c" &
done

wait 