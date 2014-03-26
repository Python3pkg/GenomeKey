#!/bin/bash
# Script to automate COSMOS runs and backing up to AWS S3
# Author: Yassine Souilmi
# March 2014

LIST=$1  # list
OUTBUCKET=$2                       # output AWS S3 bucket
DBNAME=$3
DBUSER=$4
DBPASSWD=$5    
COSMOS_DEFAULT_ROOT_OUTPUT_DIR=$6  # root of scratch directory; COSMOS paths (temp and output)
COSMOS_WORKING_DIRECTORY=$7        # COSMOS working directory
EMAIL=$8                           # email address to send report
GK_ARGS=$9                         # extra args to GenomeKey  (give an empty string if none needed)

# Notify the user of the start

echo "GenomeKey run \"${LIST}\" started" | mail -s "GenomeKey \"${LIST}\" run Started" "${EMAIL}"

# Step 0) Make an output dir
# FIXME: remove
#mkdir -p $HOME/Out/"${LIST}"

# Step 1) Launch the run

genomekey bam -n "${LIST}" -il ${LIST} ${GK_ARGS}  #### Here we still need to test if the run was successful or not

if [ $? -eq 0 ]; then

    echo "GenomeKey run \"${LIST}\" was successful" | mail -s "GenomeKey run Successful" "${EMAIL}"

    # Step 2) Dump the DB 
    cmd="mysqldump -u ${DBUSER} -p${DBPASSWD} --no-create-info ${DBNAME} > ${COSMOS_WORKING_DIRECTORY}/\"${LIST}\".sql"
    echo $cmd
    eval $cmd

    # Step 3) Reset cosmos DB
    # FIXME: need to force a reset by overriding the interactive yes/no prompt
    cosmos resetdb

    # Step 3) Copy files to S3

    #cp the MySQL DB
    aws s3 cp ${COSMOS_WORKING_DIRECTORY}/"${LIST}".sql ${OUTBUCKET}"${LIST}"/ --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers

    #cp the BAM after BQSR
    aws s3 cp ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${LIST}"/BQSR/ ${OUTBUCKET}"${LIST}"/BQSR/ --recursive --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers

    #cp the gVCF
    aws s3 cp ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${LIST}"/HaplotypeCaller/ ${OUTBUCKET}"${LIST}"/HaplotypeCaller/ --recursive --grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers

    #cp the masterVCF
    # aws s3 cp $default_root_output_dir/"${LIST}"/MasterVCF/ ${OUTBUCKET}"${LIST}"/ --recursive #stage not available yet

    # Step 3) Clean-up
    rm -R -f ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/*
    rm -R -f ${COSMOS_WORKING_DIRECTORY}/*

    echo "Genomekey run \"${LIST}\" data successfully backup on S3" | mail -s "GenomeKey Backup" "${EMAIL}"
else
    echo "Genomekey run \"${LIST}\" failed" | mail -s "GenomeKey run Failure" "${EMAIL}"
fi
