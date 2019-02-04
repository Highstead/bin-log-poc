#!/bin/bash

docker-compose up -d kafka zookeeper mysqlmaster mysqlslave
[[ $(docker-compose ps kafka zookeeper mysql| grep -c 'Up') == 4 ]] 2> /dev/null

script/mysql.sh
