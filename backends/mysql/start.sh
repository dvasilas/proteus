#!/bin/sh
set -e


  # /usr/local/mysql/bin/mysqld --initialize --user=mysql &> log
  # export OLDPASS=$(cat log | grep password | awk '{print $NF}')
  # /usr/local/mysql/bin/mysql_ssl_rsa_setup
  # /usr/local/mysql/support-files/mysql.server start
  # cp /opt/mysql_trigger/cred_template.cnf /tmp/cred1.cnf
  # sleep 2
  # echo "" >> /tmp/cred1.cnf && echo "" >> /opt/mysql_trigger/cred_template.cnf
  # echo "password=$OLDPASS" >> /tmp/cred1.cnf
  # echo "password=$MYSQL_PASSWORD" >> /opt/mysql_trigger/cred_template.cnf
  # echo "ALTER USER 'root'@'localhost' IDENTIFIED BY '$MYSQL_PASSWORD'" > /tmp/changepass.sql
  # /usr/local/mysql/bin/mysql --defaults-extra-file=/tmp/cred1.cnf  --connect-expired-password < /tmp/changepass.sql
  # cp /opt/mysql_trigger/cred_template.cnf /opt/mysql_trigger/cred.cnf
  # mkdir -p /opt/trigger_data/votes/
  # chown -R mysql:mysql /opt/trigger_data/

/usr/local/mysql/support-files/mysql.server start
/usr/local/mysql/bin/mysql --defaults-extra-file=/opt/mysql_trigger/cred.cnf < install_trigger.sql
/root/go/src/server/server &

tail -F /usr/local/mysql/data/*.err &
wait ${!}