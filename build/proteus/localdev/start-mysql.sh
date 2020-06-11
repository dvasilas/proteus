#!/bin/sh
set -e

echo "[mysqld]" >> /etc/mysql/my.cnf
echo "skip-networking=0" >> /etc/mysql/my.cnf
echo "skip-bind-address" >> /etc/mysql/my.cnf

/usr/bin/mysqld_safe --datadir='/var/lib/mysql' &

while ! nc -z localhost 3306; do
  sleep 1
done

/usr/bin/mysqladmin -u root password $MYSQL_PASSWORD

/usr/bin/mysqladmin -uroot -pverySecretQPUPwd shutdown

/usr/bin/mysqld_safe --datadir='/var/lib/mysql' &

while ! nc -z localhost 3306; do
  sleep 1
done

/app/proteus/bin/qpu -c $1 -l $2

# docker run --rm -ti --name sum-stories -p 127.0.0.1:50250:50250 -v /Users/dimvas/go/src/github.com/dvasilas/proteus/configs:/configs -e MYSQL_PASSWORD="verySecrectQPUPwd" qpu/dev /configs/lobsters/sum-stories.toml trace

