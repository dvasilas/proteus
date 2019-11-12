#!/bin/bash

# usage
# $1: directory with .yml files fro storage and query engine
# $2: directory to store output result files (absolute path)

storage_engine_compose_file=1dataStore.yml
query_engine_compose_file=localIndex.yml
record_count=1000
execution_time=60
threads=1

# pull newer images
docker pull dvasilas/ycsb:proteus
docker pull dvasilas/proteus:swarm

# create overlay network
docker network create -d overlay --attachable proteus_net

# deploy storage engine
docker stack deploy --compose-file $1/$storage_engine_compose_file storage_engine
sleep 1m

# load dataset
docker run --name ycsb --rm --network=proteus_net -e TYPE=load \
-e RECORDCOUNT=$record_count dvasilas/ycsb:proteus

# deploy query engine
docker stack deploy --compose-file $1/$query_engine_compose_file query_engine
sleep 1m
# run workload with given parameters
docker run --name ycsb --rm --network=proteus_net -v $2:/ycsb -e TYPE=run \
-e WORKLOAD=workloada_overTime \
-e THREADS=$threads \
-e RECORDCOUNT=$record_count \
-e QUERYPROPORTION=1.0 \
-e UPDATEPROPORTION=0.0 \
-e CACHEDQUERYPROPORTION=0.0 \
-e FILEOUTPUT=True \
-e EXECUTIONTIME=$execution_time \
dvasilas/ycsb:proteus

# tear down query engine
docker stack rm query_engine

# tear down query engine
docker stack rm storage_engine

# delete ovelay network
docker network rm proteus_net