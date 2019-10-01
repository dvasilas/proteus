#!/bin/bash

$GOPATH/src/github.com/dvasilas/proteus/docker-proteus/wait-for-it.sh --host=$1 --port=$2 --timeout=0
go run $GOPATH/src/github.com/dvasilas/proteus/launcher/launcher.go $3 $4 $5 $6 $7