#!/bin/sh

DBNAME=1exome
SQLNAME=1_exome.sql
COSMOS_USER=cosmos
COSMOS_PASSWD=cosmos_pass

GLOBIGNORE="*"

SQL_STATEMENT="CREATE DATABASE ${DBNAME}; USE ${DBNAME}; CREATE USER '${COSMOS_USER}'@'localhost' IDENTIFIED BY '${COSMOS_PASSWD}'; GRANT ALL PRIVILEGES ON *.* TO '${COSMOS_USER}'@'localhost'  WITH GRANT OPTION; FLUSH PRIVILEGES;"

echo $SQL_STATEMENT

cmd="mysql -u root -p -e \"${SQL_STATEMENT}\""
echo $cmd
eval $cmd

cmd="mysql -u ${COSMOS_USER} -p${COSMOS_PASSWD} -h localhost ${DBNAME} < ${SQLNAME}"
echo $cmd
eval $cmd
