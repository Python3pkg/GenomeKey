#!/bin/sh

DO_DB=$1
shift
DBNAME=cosmos
COSMOS_USER=cosmos
COSMOS_PASSWD=cosmos_pass

for SQLNAME in $*; do
    
    NAME=${SQLNAME%%.sql}


    if [[ ${DO_DB} == "yes" ]]; then
	
	GLOBIGNORE="*"

	# only run once: CREATE USER '${COSMOS_USER}'@'localhost' IDENTIFIED BY '${COSMOS_PASSWD}'; 

	SQL_STATEMENT="DROP DATABASE ${DBNAME}; CREATE DATABASE ${DBNAME}; USE ${DBNAME}; GRANT ALL PRIVILEGES ON *.* TO '${COSMOS_USER}'@'localhost'  WITH GRANT OPTION; FLUSH PRIVILEGES;"
	echo $SQL_STATEMENT
	
	cmd="mysql -u root -pcosmos -e \"${SQL_STATEMENT}\""
	echo $cmd
	eval $cmd
	
	cmd="mysql -u ${COSMOS_USER} -p${COSMOS_PASSWD} -h localhost ${DBNAME} < ${SQLNAME}"
	echo $cmd
	eval $cmd

	cmd="./cluster_usage.py > cluster-${NAME}.txt"
	echo $cmd
	eval $cmd
    fi

    # visualize 
    cmd="Rscript -e \"library(lattice); library(directlabels); jobs=read.csv('cluster-${NAME}.txt', head=T); pdf('cluster-${NAME}.pdf'); densityplot(~stop, xlab='wall time (sec)', data=subset(jobs, stage!='Load_BAMs'));dev.off()\""
    echo $cmd
    eval $cmd
done
