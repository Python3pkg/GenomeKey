#!/bin/bash

# this script runs on th COSMOS virtualenv only
#
# main script (loop)
#
# the lists should be in the same directory as the two scripts
#
# > ./w.sh lists s3://COSMOS_Pilot/Out/ port
#
##################
# $1: list of lists
# $2 output bucket
# $3 port

mkdir ~/Out/

#change permissions on the files
chmod +x ./CosmosRes.sh

while read F
do
./CosmosRes.sh $F $2 $3 #download the file
done <$1
