FROM centos:centos7 AS stage0

FROM stage0 AS stage1
WORKDIR /usr/local/
RUN set -xe && \
  yum update -y && \
  yum install gcc wget libaio numactl-libs openssl -y && \
  wget https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-8.0.19-linux-glibc2.12-x86_64.tar.xz && \
  tar xvf mysql-8.0.19-linux-glibc2.12-x86_64.tar.xz && \
  wget https://dl.google.com/go/go1.14.2.linux-amd64.tar.gz && \
  tar -xzf go1.14.2.linux-amd64.tar.gz && \
  mkdir /root/go && mkdir /root/go/bin && mkdir /root/go/src

FROM stage1 AS stage2
WORKDIR /usr/local/
RUN set -xe && \
    groupadd mysql && \
    useradd -r -g mysql -s /bin/false mysql && \
    chown -R mysql:mysql mysql-8.0.19-linux-glibc2.12-x86_64 && \
    ln -s mysql-8.0.19-linux-glibc2.12-x86_64 mysql && \
    mkdir mysql-files && \
    chown mysql:mysql mysql-files && \
    chmod 750 mysql-files

FROM stage2 AS stage3
WORKDIR /usr/local/mysql/
COPY proteus-mysql /opt/proteus-mysql/
ARG MYSQL_PASSWORD
RUN set -xe && \
    bin/mysqld --initialize --user=mysql &> log && \
    export OLDPASS=$(cat log | grep password | awk '{print $NF}') && \
    bin/mysql_ssl_rsa_setup && \
    support-files/mysql.server start && \
    cp /opt/proteus-mysql/cred.cnf /tmp/cred1.cnf && \
    echo "" >> /tmp/cred1.cnf && echo "" >> /opt/proteus-mysql/cred.cnf && \
    echo "password=$OLDPASS" >> /tmp/cred1.cnf && \
    echo "password=$MYSQL_PASSWORD" >> /opt/proteus-mysql/cred.cnf && \
    echo "ALTER USER 'root'@'localhost' IDENTIFIED BY '$MYSQL_PASSWORD';" > /tmp/changepass.sql && \
    echo "CREATE USER 'user'@'%' IDENTIFIED BY '$MYSQL_PASSWORD';" > /tmp/adduser.sql && \
    echo "GRANT ALL ON *.* TO 'user'@'%';" >> /tmp/adduser.sql && \
    echo "flush privileges;" >> /tmp/adduser.sql && \
    cat /tmp/adduser.sql && \
    cat /opt/proteus-mysql/cred.cnf && \
    cat /tmp/cred1.cnf && \
    bin/mysql --defaults-extra-file=/tmp/cred1.cnf --connect-expired-password < /tmp/changepass.sql && \
    bin/mysql --defaults-extra-file=/opt/proteus-mysql/cred.cnf < /tmp/adduser.sql && \
    cd /opt/proteus-mysql/ && \
    make && \
    cp lib_sys_exec.so /usr/local/mysql/lib/plugin/ && \
    mkdir votes && chown -R mysql:mysql votes && \
    /usr/local/mysql/bin/mysql --defaults-extra-file=/opt/proteus-mysql/cred.cnf  < /opt/proteus-mysql/schema.sql && \
    /usr/local/mysql/support-files/mysql.server stop

FROM stage3 AS stage4
COPY proteus-mysql/proteus-mysql-notifications/ /root/go/src/proteus-mysql-notifications/
COPY start.sh /opt/proteus-mysql/
WORKDIR /root/go/src/proteus-mysql-notifications/
RUN /usr/local/go/bin/go build server.go

CMD ["/opt/proteus-mysql/start.sh"]