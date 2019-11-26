#!/bin/bash

# Adds labels to the swarm nodes, to be used for placement constraints.

dc=$1
count=$2
for (( i=1; i<=$count; i+=1 ))
do
    j=$(($i+2))
    docker node update --label-add dc$(($dc))_node$(($i-1))=true ${!j}
    docker node inspect --format '{{ .Spec.Labels }}' ${!j}
done