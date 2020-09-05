#!/bin/bash

set -ex

. runBench.functions 

PROTEUS_DIR=$1
TAG=$2
SHEET_ID=$3
ROW=$4

export CREDENTIAL_FILE=
export SPREADSHEET_ID=1OeeBC0vOM5qecenPMiiYH-2zCmaWz9BmJVEvCgtHnVc
export SHEET_ID=$SHEET_ID


docker stack rm qpu-graph
docker stack rm datastore-proteus

pull
build

sync proteus04 proteus-eu02 proteus-eu03 proteus-na-02

docker network create -d overlay --attachable proteus_net || true

echo "Proteus image tag: $TAG" > /tmp/bench-config

./format-and-import.py -r $ROW --desc template-config
ROW=$((ROW+1))
$PROTEUS_DIR/pkg/lobsters-bench/bin/benchmark -c $PROTEUS_DIR/pkg/lobsters-bench/config/config.toml -d >> /tmp/bench-config
./format-and-import.py -r $ROW template-config /tmp/bench-config
ROW=$((ROW+1))

./format-and-import.py -r $ROW --desc template-run
ROW=$((ROW+1))

#for i in "500 32 1" "1000 32 1" "1500 32 1" "2000 16 2" "2500 64 2" "2500 16 4" "2500 32 4" "3000 128 1" "3000 64 2" "3000 64 4" "3500 256 1"
#for i in "2500 1024 1" "2500 2048 1"
for i in "3500 1024 1" "3500 512 2"
do
	set -- $i
	
	echo "Timestamp: $(timestamp)" > /tmp/$1_$2_$3.out
	logfile=$(timestamp_filename).txt
	touch /tmp/$logfile
	echo "Logfile: $logfile" >> /tmp/$1_$2_$3.out

	env TAG_DATASTORE=$TAG docker stack deploy --compose-file $PROTEUS_DIR/deployments/compose-files/lobsters-benchmarks/datastore-proteus.yml datastore-proteus
	wait_services_running

	sleep 10

	env TAG_QPU=$TAG docker stack deploy --compose-file $PROTEUS_DIR/deployments/compose-files/lobsters-benchmarks/qpu-graph.yml qpu-graph
	wait_services_running

	$PROTEUS_DIR/scripts/utils/getPlacement.sh >> /tmp/$logfile

	sleep 10

	$PROTEUS_DIR/pkg/lobsters-bench/bin/benchmark -c $PROTEUS_DIR/pkg/lobsters-bench/config/config.toml -l $1 -f $2 -t $3 >> /tmp/$1_$2_$3.out

	curl -u $NUAGE_LIP6_U:$NUAGE_LIP6_P -T /tmp/$logfile https://nuage.lip6.fr/remote.php/dav/files/$NUAGE_LIP6_U/proteus_bench_logs/$logfile
	./format-and-import.py -r $ROW template-run /tmp/$1_$2_$3.out

	docker stack rm qpu-graph
	docker stack rm datastore-proteus

	sleep 10
	ROW=$((ROW+1))
done
