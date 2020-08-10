#!/bin/bash

set -ex

PROTEUS_DIR=$1
TAG=$2
SHEET_ID=$3
SYSTEM=$4

export CREDENTIAL_FILE=
export SPREADSHEET_ID=1OeeBC0vOM5qecenPMiiYH-2zCmaWz9BmJVEvCgtHnVc
export SHEET_ID=$SHEET_ID

pull() {
	GIT_DIR=$PROTEUS_DIR/.git GIT_WORK_TREE=$PROTEUS_DIR git pull origin master
}

build() {
	cd $PROTEUS_DIR
	make bench
	cd -
}

sync() {
	for host in "$@"
	do
		ssh $host 'sudo rm -r /mount/; sudo mkdir -p /mount/; mkdir -p mount'
		scp -r $PROTEUS_DIR/build/datastore/lobsters-MySQL/docker-entrypoint-init.d/* $host:~/mount
		scp -r $PROTEUS_DIR/configs $host:~/mount
		scp -r $PROTEUS_DIR/scripts $host:~/mount
		ssh $host 'sudo mv ~/mount/* /mount/'
	done
}

docker stack rm qpu-graph
docker stack rm datastore-proteus

pull
build
sync proteus04 proteus-eu02 proteus-eu03 proteus-na01 proteus-na02

docker network create -d overlay --attachable proteus_net || true

threads=(1 2 4 8 16 32 64 128 256)

rowID=0
./format-and-import.py -r $rowID --desc template
rowID=$((rowID+1))

for i in "${threads[@]}"
do
	env TAG_DATASTORE=$TAG docker stack deploy --compose-file $PROTEUS_DIR/deployments/compose-files/lobsters-benchmarks/datastore-proteus.yml datastore-proteus
 	$PROTEUS_DIR/bin/benchmark -c $PROTEUS_DIR/configs/benchmark/config.toml -p
	
	sleep 5

	env TAG_QPU=$TAG docker stack deploy --compose-file $PROTEUS_DIR/deployments/compose-files/lobsters-benchmarks/qpu-graph.yml qpu-graph

	sleep 5

	/home/scality/go/proteus/bin/benchmark -c $PROTEUS_DIR/configs/benchmark/config.toml -s $SYSTEM -t $i > /tmp/$i.out

#     	/home/scality/go/proteus/scripts/service-exec.sh qpu-graph_join ./scripts/freshness.sh
#     	scp proteus03.novalocal:/mount/configs/query-log.txt ./
#     	scp proteus03.novalocal:/mount/configs/write-log.txt ./
#     	go run /home/scality/go/proteus/scripts/freshness.go ./write-log.txt ./query-log.txt >> $i.out
#     	ssh -t proteus03.novalocal rm -f /mount/configs/query-log.txt
#     	ssh -t proteus03.novalocal rm -f /mount/configs/write-log.txt
	./format-and-import.py -r $rowID template /tmp/$i.out
   
	docker stack rm qpu-graph
	docker stack rm datastore-proteus

	sleep 10
	rowID=$((rowID+1))
done
