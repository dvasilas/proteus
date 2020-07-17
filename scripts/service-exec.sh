#!/bin/bash
set -e

SERVICE=$1; shift

TASK_ID=$(docker service ps --filter 'desired-state=running' $SERVICE -q)
NODE_ID=$(docker inspect --format '{{ .NodeID }}' $TASK_ID)
CONTAINER_ID=$(docker inspect --format '{{ .Status.ContainerStatus.ContainerID }}' $TASK_ID)
NODE_HOST=$(docker node inspect --format '{{ .Description.Hostname }}' $NODE_ID)

ssh -tt $NODE_HOST docker exec -ti $CONTAINER_ID $@