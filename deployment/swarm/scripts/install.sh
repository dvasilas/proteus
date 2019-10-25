#!/bin/bash

sudo yum update -y
sudo yum remove docker docker-client docker-client-latest docker-common docker-latest docker-latest-logrotate docker-logrotate docker-engine
sudo yum install -y yum-utils device-mapper-persistent-data lvm2
sudo yum-config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo -y
sudo yum install -y docker-ce docker-ce-cli containerd.io
sudo systemctl start docker
