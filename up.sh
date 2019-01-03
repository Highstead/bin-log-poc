#!/bin/bash

docker-compose up -d kafka zookeeper mysql
[[ $(docker-compose ps kafka zookeeper mysql| grep -c 'Up') == 3 ]] 2> /dev/null

script/mysql.sh
