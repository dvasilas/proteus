#!/bin/bash

mysql -uroot -pverySecrectQPUPwd < /opt/proteus-lobsters/freshness/get-write-log-dsdriver.sql && cp /var/lib/mysql/write-log.txt /opt/proteus-lobsters/freshness
