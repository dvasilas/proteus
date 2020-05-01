FROM centos:centos7

ARG mysql_password

RUN set -xe && \
  yum update -y && \
  yum install gcc wget libaio numactl-libs openssl vim -y && \
  cd /usr/local && \
  wget https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-8.0.19-linux-glibc2.12-x86_64.tar.xz && \
  tar -C /usr/local -xvf mysql-8.0.19-linux-glibc2.12-x86_64.tar.xz && \
  wget https://dl.google.com/go/go1.14.2.linux-amd64.tar.gz && \
  tar -C /usr/local -xzf go1.14.2.linux-amd64.tar.gz && \
  mkdir /root/go && mkdir /root/go/bin && mkdir /root/go/src

RUN set -xe && \
    groupadd mysql && \
    useradd -r -g mysql -s /bin/false mysql && \
    cd /usr/local && \
    chown -R mysql:mysql mysql-8.0.19-linux-glibc2.12-x86_64 && \
    ln -s mysql-8.0.19-linux-glibc2.12-x86_64 mysql && \
    mkdir mysql-files && \
    chown mysql:mysql mysql-files && \
    chmod 750 mysql-files

COPY . /opt/mysql_trigger

COPY ./server/ /root/go/src/server

WORKDIR /opt/mysql_trigger

RUN set -xe && \
    make && \
    cp lib_sys_exec.so /usr/local/mysql/lib/plugin/ && \
    export PATH=$PATH:/usr/local/go/bin && \
    cd /root/go/src/server && go build && \
    /usr/local/mysql/bin/mysqld --initialize --user=mysql &> log && \
    export OLDPASS=$(cat log | grep password | awk '{print $NF}') && \
    /usr/local/mysql/bin/mysql_ssl_rsa_setup && \
    /usr/local/mysql/support-files/mysql.server start && \
    cp /opt/mysql_trigger/cred.cnf /tmp/cred.cnf && \
    export MYSQL_PASSWORD=mysql_password && \
    echo "" >> /tmp/cred.cnf && echo "" >> /opt/mysql_trigger/cred.cnf && \
    echo "password=$OLDPASS" >> /tmp/cred.cnf && \
    echo "password=$MYSQL_PASSWORD" >> /opt/mysql_trigger/cred.cnf && \
    echo "ALTER USER 'root'@'localhost' IDENTIFIED BY '$MYSQL_PASSWORD'" > /tmp/changepass.sql && \
    /usr/local/mysql/bin/mysql --defaults-extra-file=/tmp/cred.cnf  --connect-expired-password < /tmp/changepass.sql && \
    mkdir -p /opt/trigger_data/votes/ && \
    chown -R mysql:mysql /opt/trigger_data/ && \
    /usr/local/mysql/support-files/mysql.server stop

CMD ["/opt/mysql_trigger/start.sh"]