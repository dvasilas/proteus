#!/bin/bash

docker run --rm -ti -p 3307:3306 -p 50000:50000 --name mysql mysql/test
