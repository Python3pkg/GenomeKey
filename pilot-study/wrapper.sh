#!/bin/bash

#####################################################################################
## Pilot study wrapper script													   ##
## 1) dump the MySQL cosmos db													   ##
## 2) whipe the db																   ##
## 3) cp the outputs: dumped db; annotated vcf; output file to a single directory  ##
## 4) compress all the outputes 												   ##
## 5) push all to the pilot stydy S3 bucket 									   ##
## 6) re-enitialyze cosmos db 													   ##
#####################################################################################

# 0) Create the outputes cp directory

# mkdir /gluster/gv0/exports

# 1) dump the MySQL cosmos db
mysqldump -u cosmos -pcosmos cosmos > /gluster/gv0/exports/cosmos.sql

# 2) whipe the db

MYSQL=$(which mysql)
AWK=$(which awk)
GREP=$(which grep)
 
TABLES=$($MYSQL -u cosmos -pcosmos cosmos -e 'show tables' | $AWK '{ cosmos}' | $GREP -v '^Tables' )
 
for t in $TABLES
do
	$MYSQL -u cosmos -pcosmos cosmos -e "drop table $t"
done

# 3) cp outputes
cp 

# 4) compress all the outputes
ls /gluster/gv0/exports/ >> /gluster/gv0/exports/backups.list #creat the list of files to compress
tar -cZf --no-recursion /gluster/gv0/exports/backup/tgz 'cat backups.list'

# 5) push all to the pilot stydy S3 bucket

# 6) re-enitialyze cosmos db 





