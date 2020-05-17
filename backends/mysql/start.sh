#!/bin/sh
set -e

export PROTEUS_PUBLISH_PORT=50000
export PROTEUS_LOG_PATH=/opt/proteus-mysql/votes
/root/go/src/proteus-mysql-notifications/server &

/usr/local/mysql/support-files/mysql.server start

tail -F /usr/local/mysql/data/*.err &
wait ${!}