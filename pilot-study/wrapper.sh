#!/bin/bash

#####################################################################################
## Pilot study wrapper script	
## 1) dump the MySQL cosmos db	
## 2) wipe the db
## 3) do the genomekey run		
## 4) cp the outputs: dumped db; annotated VCF; output file to a single directory
## 5) compress all the outputs 
## 6) push all to the pilot study S3 bucket
## 7) re-initialize cosmos db
#####################################################################################

# 0) Create the output cp directory

# mkdir /gluster/gv0/exports

# 1) dump the MySQL cosmos db
mysqldump -u cosmos -pcosmos cosmos > /gluster/gv0/exports/cosmos.sql

# 2) wipe the db

MYSQL=$(which mysql)
AWK=$(which awk)
GREP=$(which grep)
 
TABLES=$($MYSQL -u cosmos -pcosmos cosmos -e 'show tables' | $AWK '{ cosmos}' | $GREP -v '^Tables' )
 
for t in $TABLES
do
	$MYSQL -u cosmos -pcosmos cosmos -e "drop table $t"
done

# 3) do the genomekey run

# 4) cp outputs
cp 

# 5) compress all the outputes
ls /gluster/gv0/exports/ >> /gluster/gv0/exports/backups.list #creat the list of files to compress
tar -cZf --no-recursion /gluster/gv0/exports/backup/tgz 'cat backups.list'

# 6) push all to the pilot study S3 bucket

# 7) re-initialize cosmos db 





