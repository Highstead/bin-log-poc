#!/bin/bash
set -e

HOST=${MYSQL_HOST:-localhost}

# MySQL takes time to startup
until $(mysql --protocol TCP -h $HOST -u root -proot -e "select 1\G" &> /dev/null)
do
  echo "Waiting for database connection..."
  sleep 3
done

mysql --protocol TCP -h $HOST -u root -proot -e status;

echo "Creating MySQL user"
mysql --protocol TCP -h $HOST -u root -proot information_schema < script/mysql/setup-user.sql;

echo "Creating MySQL schemas"
for s in script/mysql/schemas/*.sql;
do
  echo "Loading ${s}"
  mysql --protocol TCP -h $HOST -u root -proot information_schema < $s;
done


