#!/bin/sh
docker build -t mysql/test .
docker run --env-file .env --rm -ti --name mysql mysql/test