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
# $1: list of lists
# $2 output bucket
# $3 port
# $4 email address

#change permissions on the files
chmod +x ./CosmosRes.sh
#getting the DB info from the Cosmos config file
~/.cosmos/ config.ini

################
# Defining working directory path
if [$server_name -eq orchestra]; then
    scratch=/hms/scratch1/"$USER"/

else
    scratch=/gluster/gv0/

fi

# Output directory
OutDir=$scratch/out/

if [ -d $directory ]; then
  echo "Directory exists"
else
  echo "Creating Dirctory"
  mkdir $scratch/out/
fi

################
# Starting the run

while read F
do
./CosmosRes.sh $F $2 $3 $NAME $USER $PASSWORD $server_name $scratch $4
done <$1

################
