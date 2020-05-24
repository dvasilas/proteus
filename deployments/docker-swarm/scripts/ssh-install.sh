#!/bin/bash

KEYFILE=$1
USER=$2
HOST=$3

ssh -i $KEYFILE $USER@$HOST 'bash -s' < docker-install.sh