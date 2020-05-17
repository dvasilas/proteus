#!/bin/bash

docker build --build-arg MYSQL_PASSWORD=$1 -t mysql/test .
