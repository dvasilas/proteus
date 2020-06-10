#!/bin/sh
set -e

/usr/local/mysql/support-files/mysql.server start

tail -F /usr/local/mysql/data/*.err &
wait ${!}