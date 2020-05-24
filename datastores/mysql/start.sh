#!/bin/sh
set -e

export PROTEUS_PUBLISH_PORT=50000

/root/go/src/proteus-mysql-notifications/server &

/usr/local/mysql/support-files/mysql.server start

tail -F /usr/local/mysql/data/*.err &
wait ${!}