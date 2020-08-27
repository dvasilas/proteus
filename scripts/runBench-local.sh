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

docker network create -d overlay --attachable proteus_net || true

echo "Proteus image tag: $TAG" > /tmp/bench-config

./format-and-import.py -r $ROW --desc template-config
ROW=$((ROW+1))
$PROTEUS_DIR/pkg/lobsters-bench/bin/benchmark -c $PROTEUS_DIR/pkg/lobsters-bench/config/config.toml -d >> /tmp/bench-config
./format-and-import.py -r $ROW template-config /tmp/bench-config
ROW=$((ROW+1))

threads=(1 2 4 8 16 32 64 128 256)

./format-and-import.py -r $ROW --desc template-run
ROW=$((ROW+1))

for i in "${threads[@]}"
do

	echo "Timestamp: $(timestamp)" > /tmp/$i.out
	logfile=$(timestamp_filename).txt
	touch /tmp/$logfile
	echo "Logfile: $logfile" >> /tmp/$i.out

	ssh -t proteus-eu02 sudo rm -r /opt/lobsters-dataset
	ssh -t proteus-eu02 sudo cp -r /opt/lobsters-dataset-init-small /opt/lobsters-dataset

	env TAG_DATASTORE=$TAG docker stack deploy --compose-file $PROTEUS_DIR/deployments/compose-files/lobsters-benchmarks/datastore-proteus.yml datastore-proteus
	wait_services_running

	env TAG_QPU=$TAG docker stack deploy --compose-file $PROTEUS_DIR/deployments/compose-files/lobsters-benchmarks/qpu-graph.yml qpu-graph
	wait_services_running

	$PROTEUS_DIR/scripts/utils/getPlacement.sh >> $logfile

	$PROTEUS_DIR/pkg/lobsters-bench/bin/benchmark -c $PROTEUS_DIR/pkg/lobsters-bench/config/config.toml -t $i >> /tmp/$i.out

	curl -u $NUAGE_LIP6_U:$NUAGE_LIP6_P -T $logfile https://nuage.lip6.fr/remote.php/dav/files/$NUAGE_LIP6_U/proteus_bench_logs/$logfile
	./format-and-import.py -r $ROW template-run /tmp/$i.out

	docker stack rm qpu-graph
	docker stack rm datastore-proteus

	sleep 10
	ROW=$((ROW+1))
done
