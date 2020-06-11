#!/bin/sh
set -e

/entrypoint.sh mysqld &

while ! nc -z localhost 3306; do
  sleep 1
done

/app/proteus/bin/qpu -c $1 -l $2