#!/bin/sh

echo "USE db1;" > /tmp/insert.sql
echo "INSERT INTO votes (user_id, story_id) VALUES($1, $2);" >> /tmp/insert.sql
/usr/local/mysql/bin/mysql --defaults-extra-file=/opt/mysql_trigger/cred.cnf  < /tmp/insert.sql