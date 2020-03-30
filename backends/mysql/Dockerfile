FROM centos:centos7

RUN set -xe && \
  yum update -y && \
  yum install gcc wget libaio numactl-libs openssl -y && \
  cd /usr/local && \
  wget https://dev.mysql.com/get/Downloads/MySQL-8.0/mysql-8.0.19-linux-glibc2.12-x86_64.tar.xz

RUN set -xe && \
    groupadd mysql && \
    useradd -r -g mysql -s /bin/false mysql && \
    cd /usr/local && \
    tar xvf mysql-8.0.19-linux-glibc2.12-x86_64.tar.xz && \
    chown -R mysql:mysql mysql-8.0.19-linux-glibc2.12-x86_64 && \
    ln -s mysql-8.0.19-linux-glibc2.12-x86_64 mysql && \
    mkdir mysql-files && \
    chown mysql:mysql mysql-files && \
    chmod 750 mysql-files

COPY . /opt/mysql_trigger

WORKDIR /opt/mysql_trigger

RUN set -xe && \
    make && \
    cp lib_sys_exec.so /usr/local/mysql/lib/plugin/

CMD ["/opt/mysql_trigger/start.sh"]