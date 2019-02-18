#!/bin/bash

docker-compose stop
docker-compose rm -f antidote1
docker-compose up --build antidote1
