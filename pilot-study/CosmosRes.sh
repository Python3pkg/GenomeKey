#!/bin/bash
# Script to automate COSMOS runs and backing up to AWS S3
# Author: Yassine Souilmi
# March 2014

S3LIST=$1  # list
OUTBUCKET=$2                       # output AWS S3 bucket
DBNAME=$3
DBUSER=$4
DBPASSWD=$5
COSMOS_DEFAULT_ROOT_OUTPUT_DIR=$6  # root of scratch directory; COSMOS paths (temp and output)
COSMOS_WORKING_DIRECTORY=$7        # COSMOS working directory
EMAIL=$8                           # email address to send report
GK_ARGS=$9                         # extra args to GenomeKey  (give an empty string if none needed)

# Download the data from S3
mkdir -p ${COSMOS_WORKING_DIRECTORY}/"${S3LIST}"/Inputs/ #making an Input directory under ${COSMOS_WORKING_DIRECTORY}

while read F
do
aws s3 cp $F ${COSMOS_WORKING_DIRECTORY}/"${S3LIST}"/Inputs/
done <$LISTS

ls ${COSMOS_WORKING_DIRECTORY}/"${S3LIST}"/Inputs/ >> ${COSMOS_WORKING_DIRECTORY}/"${S3LIST}"/Inputs/"${S3LIST}".idx #creating the local list

LIST=${COSMOS_WORKING_DIRECTORY}/"${S3LIST}"/Inputs/"${S3LIST}".idx

# Notify the user of the start

echo "GenomeKey run \"${S3LIST}\" started" | mail -s "GenomeKey \"${S3LIST}\" run Started" "${EMAIL}"

# Step 2) Launch the run

genomekey bam -n "${S3LIST}" -il ${LIST} ${GK_ARGS}  #### Here we still need to test if the run was successful or not

if [ $? -eq 0 ]; then

    echo "GenomeKey run \"${S3LIST}\" was successful" | mail -s "GenomeKey run Successful" "${EMAIL}"

    # Dump the DB
    cmd="mysqldump -u ${DBUSER} -p${DBPASSWD} --no-create-info ${DBNAME} > ${COSMOS_WORKING_DIRECTORY}/\"${S3LIST}\".sql"
    echo $cmd
    eval $cmd

    # Reset cosmos DB

    yes | cosmos resetdb

    # Copy files to S3

    #cp the MySQL DB
    aws s3 cp ${COSMOS_WORKING_DIRECTORY}/"${S3LIST}".sql ${OUTBUCKET}"${LIST}"/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers

    #cp the BAM after BQSR
    aws s3 cp ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${S3LIST}"/BQSR/ ${OUTBUCKET}"${S3LIST}"/BQSR/ --recursive --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers

    #cp the gVCF
    aws s3 cp ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${S3LIST}"/HaplotypeCaller/ ${OUTBUCKET}"${S3LIST}"/HaplotypeCaller/ --recursive --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers

    #cp the masterVCF
    # aws s3 cp $default_root_output_dir/"${LIST}"/MasterVCF/ ${OUTBUCKET}"${LIST}"/ --recursive #stage not available yet

    # Clean-up
    rm -R -f ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/*
    rm -R -f ${COSMOS_WORKING_DIRECTORY}/*

    echo "Genomekey run \"${S3LIST}\" data successfully backup on S3" | mail -s "GenomeKey Backup" "${EMAIL}"
else
    echo "Genomekey run \"${S3LIST}\" failed" | mail -s "GenomeKey run Failure" "${EMAIL}"
fi
