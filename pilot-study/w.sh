#!/bin/bash

#
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
Lists=$1     # $1: list of lists
OutBucket=$2 # $2 output bucket.. In the pilot study it's s3://COSMOS_Pilot/Out/
Port=$3      # $3 port
Email=$4     # $4 email address
GKargs=$5    # $5 extra args for genomekey

if [ $# -eq 4 ]; then
    Gkargs=""
fi

#change permissions on the files
chmod +x ./CosmosRes.sh
#getting the DB info from the Cosmos config file
~/.cosmos/ config.ini


################
# Starting the run

while read F
do
./CosmosRes.sh $F $OutBucket $NAME $USER $PASSWORD $default_root_output_dir $working_directory $Email $GKargs
done <$1

################
