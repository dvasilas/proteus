#!/bin/sh
set -ex

mysql -uroot -p$MYSQL_ROOT_PASSWORD < /opt/proteus-lobsters/schema-trigger.sql
/opt/proteus-lobsters/server &