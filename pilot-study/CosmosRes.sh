#!/bin/bash

List=$1  # list
OutBucket=$2  # output bucket
DBname=$DBname
DBuser=$4
DBpasswd=$5    # (rectively name, user and password::$3 $4 $5)
default_root_output_dir=$6  # root of scratch directory; cosmos paths (temp and output)
working_directory=$7
Email=$8  # email address
GKargs=$9 # extra args to genomekey  (give an empty string if none needed)

# Notify the user of the start

echo "Genomekey run \"$List\" started" | mail -s "GenomeKey \"$List\" run Started" "$Email"

# Step 0) Make an output dir

mkdir -p $HOME/Out/"$List"

# Step 1) Launch the run

genomekey bam -n "$List" -il $List ${Gkargs}  #### Here we still need to test if the run was successful or not

if [ $? -eq 0 ]; then

    echo "Genomekey run \"$List\" was successful" | mail -s "GenomeKey run Successful" "$Email"

    # Step 2) Dump the DB (the username and the password are hard-coded here)

    mysqldump -u $DBuser -p $DBpasswd --no-create-info $DBname > $HOME/Out/"$List".sql

    # Step 3) Reset cosmos DB

    cosmos resetdb

    # Step 3) Copy files to S3

      #cp the DB
      aws s3 cp $HOME/Out/"$List".sql $OutBucket/"$List"/

      #cp the BAM after BQSR
      aws s3 cp $default_root_output_dir/"$List"/BQSR/ $OutBucket/"$List"/ --recursive

      #cp the gVCF
      aws s3 cp $default_root_output_dir/"$List"/HaplotypeCaller/ $OutBucket/"$List"/ --recursive

      #cp the masterVCF
      # aws s3 cp $default_root_output_dir/"$List"/MasterVCF/ $OutBucket/"$List"/ --recursive #stage not available yet

    # Step 3) Clean-up
    rm -R -f $default_root_output_dir/*
    rm -R -f $working_directory/*

    echo "Genomekey run \"$List\" data successfully backup on S3" | mail -s "GenomeKey Backup" "$Email"
else
    echo "Genomekey run \"$List\" failed" | mail -s "GenomeKey run Failure" "$Email"
fi
