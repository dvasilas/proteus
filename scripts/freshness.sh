#!/bin/bash

set -e

mysql -uroot -pverySecrectQPUPwd < /app/proteus/scripts/get-write-log.sql
cp /var/lib/mysql/write-log.txt /configs
mysql -uroot -pverySecrectQPUPwd < /app/proteus/scripts/get-query-log.sql
cp /var/lib/mysql/query-log.txt /configs
