#!/bin/bash

# Adds labels to the swarm nodes, to be used for placement constraints.
# adds the labels node0, node1, ... to the nodes of the swarm
# ./label.sh <hostname1> <hostname2> ...
# to get the node host names do use: 'docker node ls'

args=$#
for (( i=1; i<=$args; i+=1 ))
do
    docker node update --label-add node$(( $i-1  ))=true ${!i}
    docker node inspect --format '{{ .Spec.Labels }}' ${!i}
done