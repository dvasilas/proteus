#!/bin/bash
set -e

for host in "$@"
do
  ssh $host 'sudo rm -r /mount/; sudo mkdir -p /mount/; mkdir -p mount'
  scp -r build/datastore/lobsters-MySQL/docker-entrypoint-init.d/* $host:~/mount
  scp -r configs $host:~/mount
  scp -r scripts $host:~/mount
  ssh $host 'sudo mv ~/mount/* /mount/'
done