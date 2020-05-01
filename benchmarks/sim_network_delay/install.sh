#!/bin/bash

docker inspect --format='{{.NetworkSettings.Networks.sim_network_delay_local.MacAddress}}' server