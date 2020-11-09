#!/bin/bash

set -ex

. runBench.functions 
. runBench.config 

PROTEUS_DIR=$1
TAG=$2
SHEET_ID=$3
ROW=$4
SYSTEM=$5

export CREDENTIAL_FILE=
export SPREADSHEET_ID=1OeeBC0vOM5qecenPMiiYH-2zCmaWz9BmJVEvCgtHnVc
export SHEET_ID=$SHEET_ID


docker stack rm qpu-graph
docker stack rm datastore-proteus
docker stack rm datastore-plain
docker stack rm datastore-mv

#pul
build

sync proteus-eu02 proteus-eu03 proteus-eu04 #proteus-na01 proteus-na02

docker network create -d overlay --attachable proteus_net || true

echo "Proteus image tag: $TAG" > /tmp/bench-config

./format-and-import.py -r $ROW --desc template-config
ROW=$((ROW+1))
$PROTEUS_DIR/pkg/lobsters-bench/bin/benchmark -c $PROTEUS_DIR/pkg/lobsters-bench/config/config.toml -d >> /tmp/bench-config
./format-and-import.py -r $ROW template-config /tmp/bench-config
ROW=$((ROW+1))

./format-and-import.py -r $ROW --desc template-run
ROW=$((ROW+1))

for ((i = 0; i < ${#runParams[@]}; i++))
do
	
	set -- ${runParams[$i]}

	logfile=$(timestamp_filename).txt
	echo "Timestamp: $(timestamp)" > /tmp/$logfile
	touch /tmp/$logfile

	if [ $SYSTEM == "mysql" ] ; then
		env TAG_DATASTORE=$TAG docker stack deploy --compose-file $PROTEUS_DIR/deployments/compose-files/lobsters-benchmarks/datastore-mv.yml datastore-mv
	fi
	if [ $SYSTEM == "proteus" ] ; then
		env TAG_DATASTORE=$TAG docker stack deploy --compose-file $PROTEUS_DIR/deployments/compose-files/lobsters-benchmarks/datastore-proteus.yml datastore-proteus
	fi

	wait_services_running
	sleep 10

	if [ $SYSTEM == "proteus" ] ; then
		env TAG_DATASTORE=$TAG env TAG_QPU=$TAG docker stack deploy --compose-file $PROTEUS_DIR/deployments/compose-files/lobsters-benchmarks/qpu-graph.yml qpu-graph
		wait_services_running
		sleep 10
	fi

	$PROTEUS_DIR/scripts/utils/getPlacement.sh >> /tmp/$logfile

	if [ $SYSTEM == "mysql" ] ; then
		$PROTEUS_DIR/pkg/lobsters-bench/bin/benchmark -c $PROTEUS_DIR/pkg/lobsters-bench/config/config-mysql.toml -l $1 --fr $2 --fw $3 -t $4 >> /tmp/$logfile
	fi
	if [ $SYSTEM == "proteus" ] ; then
		$PROTEUS_DIR/pkg/lobsters-bench/bin/benchmark -c $PROTEUS_DIR/pkg/lobsters-bench/config/config-proteus.toml -l $1 --fr $2 --fw $3 -t $4 >> /tmp/$logfile
	fi

	./format-and-import.py -r $ROW template-run /tmp/$logfile
#	curl -u $NUAGE_LIP6_U:$NUAGE_LIP6_P -T /tmp/$logfile https://nuage.lip6.fr/remote.php/dav/files/$NUAGE_LIP6_U/proteus_bench_logs/$logfile

	docker stack rm qpu-graph
	docker stack rm datastore-proteus

	sleep 10
	ROW=$((ROW+1))
done
