#!/bin/bash

$GOPATH/src/github.com/dvasilas/proteus/docker-proteus/wait-for-it.sh --host=$1 --port=$2 --timeout=0
$GOPATH/src/github.com/dvasilas/proteus/server -conf=$3