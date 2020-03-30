#!/bin/sh

echo "USE db1;" > /tmp/insert.sql
echo "INSERT INTO trig_test (id, random_data) VALUES($1, \"$2\");" >> /tmp/insert.sql
/usr/local/mysql/bin/mysql --defaults-extra-file=/opt/mysql_trigger/cred.cnf  < /tmp/insert.sql