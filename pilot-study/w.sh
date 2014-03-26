#!/bin/bash


# Dependencies: aws cli (fully intalled and configured); COSMOS Virtualenv (fully intalled and configured)
#
# main script (loop)
#
# the lists should be in the same directory as the two scripts
#
##################
###Example:
# > ./w.sh lists s3://COSMOS_Pilot/Out/ port eg@email.com
#
##################
LISTS=$1     # $1: list of lists
OUTBUCKET=$2 # $2 output bucket.. In the pilot study it's s3://COSMOS_Pilot/Out/
PORT=$3      # $3 port
EMAIL=$4     # $4 email address
GKARGS=$5    # $5 extra args for genomekey
GK_PATH=$6   # $6 the full path to the GenomeKey installation directory

##################
if [ $# -eq 4 ]; then
    GKARGS=""
fi

################
#change permissions on the files
chmod +x ./CosmosRes.sh
#getting the DB info from the Cosmos config file
~/.cosmos/ config.ini

#getting the tools path from the GenomeKey settings
${GK_PATH}/genomekey/settings.py

################
#Launching the webserver

cosmos runweb -p ${PORT}

################
# Starting the run

while read F
do
./CosmosRes.sh $F ${OUTBUCKET} $NAME $USER $PASSWORD $default_root_output_dir $working_directory ${EMAIL} ${GKARGS} ${GK_PATH} $tools_path
done <${LISTS}

################
