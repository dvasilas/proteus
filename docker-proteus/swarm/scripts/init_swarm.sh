#!/bin/bash

leader_ip=$(hostname -i)

echo "### Initializing Swarm mode ..."
sudo docker swarm init --advertise-addr $leader_ip

manager_token=$(sudo docker swarm join-token manager -q)
worker_token=$(sudo docker swarm join-token worker -q)

echo "### Joining worker modes ..."
for ip in $@; do
    ssh scality@$ip "sudo docker swarm join --token $worker_token $leader_ip:2377"
done
