#!/bin/bash

set -e

mysql -uroot -pverySecrectQPUPwd < /opt/proteus-lobsters/freshness/get-write-log.sql
cp /var/lib/mysql/write-log.txt /opt/proteus-lobsters/freshness
mysql -uroot -pverySecrectQPUPwd < /opt/proteus-lobsters/freshness/get-query-log.sql
cp /var/lib/mysql/query-log.txt /opt/proteus-lobsters/freshness
