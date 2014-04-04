#!/bin/bash
# Script to automate COSMOS runs and backing up to AWS S3
# Author: Yassine Souilmi
# March 2014

S3LIST=$1                          # input list
OUTBUCKET=$2                       # output AWS S3 bucket
DBNAME=$3
DBUSER=$4
DBPASSWD=$5
COSMOS_DEFAULT_ROOT_OUTPUT_DIR=$6  # root of scratch directory; COSMOS paths (temp and output)
COSMOS_WORKING_DIRECTORY=$7        # COSMOS working directory
EMAIL=$8                           # email address to send report
GK_ARGS=$9                         # extra args to GenomeKey  (give an empty string if none needed)
GK_PATH=${10}
TOOLS_PATH=${11}

RUNNAME=$(basename "$S3LIST")
echo $RUNNAME

echo $COSMOS_DEFAULT_ROOT_OUTPUT_DIR
echo $COSMOS_WORKING_DIRECTORY

##################
# Download the data from S3
# first make sure the directory is empty
# FIXME: this is a bit fragile (see FIXME, below)
rm -rf $COSMOS_WORKING_DIRECTORY/"${RUNNAME}"/Inputs/
mkdir -p $COSMOS_WORKING_DIRECTORY/"${RUNNAME}"/Inputs/ # now recreate directory

while read F
do
    aws s3 cp $F $COSMOS_WORKING_DIRECTORY/${RUNNAME}/Inputs/
    
    aws s3 cp "$F".bai $COSMOS_WORKING_DIRECTORY/${RUNNAME}/Inputs/
done <$S3LIST

# getting the local list of files
# FIXME: this is extremely fragile, requires that
# no other files may have got inadvertantly added to
# this directory, also "ls" not best command for this
ls $COSMOS_WORKING_DIRECTORY/"${RUNNAME}"/Inputs/*.bam > ${COSMOS_WORKING_DIRECTORY}/"${RUNNAME}"/Inputs/"${RUNNAME}".idx #creating the local files index

##################
# Generating the index files
while read F
do
	#cmd="${TOOLS_PATH}/samtools index ${COSMOS_WORKING_DIRECTORY}/\"${RUNNAME}\"/Inputs/\"$F\""
	cmd="${TOOLS_PATH}/samtools index ${F}"
	echo $cmd
	eval $cmd
done <${COSMOS_WORKING_DIRECTORY}/"${RUNNAME}"/Inputs/"${RUNNAME}".idx

# Notify the user of the start
echo "GenomeKey run \"${RUNNAME}\" started" | mail -s "GenomeKey \"${RUNNAME}\" run Started" "${EMAIL}"

# Step 2) Launch the run
# give '-y' option which assumes "yes" answers to re-running/deleting workflows
# also '-r' to restart workflow from scratch by deleting existing files
cmd="${GK_PATH}/bin/genomekey bam -n \"${RUNNAME}\" -r -y -il ${COSMOS_WORKING_DIRECTORY}/${RUNNAME}/Inputs/${RUNNAME}.idx ${GK_ARGS} &> ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/GK${RUNNAME}.out"
echo $cmd
eval $cmd

if [ $? -eq 0 ]; then

    echo "GenomeKey run \"${RUNNAME}\" was successful" | mail -s "GenomeKey run Successful" "${EMAIL}"

   # Dump the DB
    mysqldump -u ${DBUSER} -p${DBPASSWD} --no-create-info ${DBNAME} > ${COSMOS_WORKING_DIRECTORY}/\"${RUNNAME}\".sql

   # Copy files to S3

    #cp the MySQL DB
    aws s3 cp ${COSMOS_WORKING_DIRECTORY}/"${RUNNAME}".sql ${OUTBUCKET}/Out"${LIST}"/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers

    #cp the BAM after BQSR
    aws s3 cp ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${RUNNAME}"/BQSR/ ${OUTBUCKET}/Out"${RUNNAME}"/BQSR/ --recursive --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers

    #cp the gVCF
    aws s3 cp ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${RUNNAME}"/HaplotypeCaller/ ${OUTBUCKET}/Out"${RUNNAME}"/HaplotypeCaller/ --recursive --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers

    #cp std.out std.err for the cosmos run
    aws s3 cp ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/GK"${RUNNAME}".out ${OUTBUCKET}/Out"${RUNNAME}"/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
    aws s3 cp ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/GK"${RUNNAME}".err ${OUTBUCKET}/Out"${RUNNAME}"/  --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers
   
    #cp the GenomeKey out
    aws s3 cp ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/GK"${RUNNAME}".out ${OUTBUCKET}"${RUNNAME}"/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers

    # Clean-up
    rm -R -f ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/*
    rm -R -f ${COSMOS_WORKING_DIRECTORY}/*
    
    # Reset cosmos DB
    yes | cosmos resetdb
 

   echo "Genomekey run \"${S3LIST}\" data successfully backup on S3" | mail -s "GenomeKey Backup" "${EMAIL}"
else
    echo "Genomekey run \"${S3LIST}\" failed" | mail -s "GenomeKey run Failure" "${EMAIL}"
fi
