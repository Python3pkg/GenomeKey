#!/bin/bash

# FIXME: use variable names, e.g. LIST=$1 etc.
# $1:: list
# $2:: output bucket
# $3:: port
# (respectively name, user and password::$4 $5 $6)
# $7:: cosmos paths (temp and output)
# $8:: root of scratch directory
# $9:: email address
# $10:: extra args to genomekey  (give an empty string if none needed)

# Notify the user of the start

echo "Genomekey run \"$1\" started" | mail -s "GenomeKey \"$1\" run Started" "$9"

# Step 0) Make an output dir

mkdir -p $8/Out/"$1"

# Step 1) Launch the run

genomekey bam -n "$1" -il $1 ${10}  #### Here we still need to test if the run was successful or not

if [ $? -eq 0 ]; then

    echo "Genomekey run \"$1\" was successful" | mail -s "GenomeKey run Successful" "$9"
   
    # Step 2) Dump the DB (the username and the password are hard-coded here)

    mysqldump -u $5 -p $6 –no-create-info $4 > $8/Out/"$1".sql

    # Step 3) Reset cosmos DB

    cosmos resetdb

    # Step 4) Copy files to S3

      #cp the DB
      aws s3 cp $8/Out/"$1"/ $2/"$1"/

      #cp the VCF
      aws s3 cp $8/cosmos/out/stage_name/.../annotated.vcf $2/"$1"/

    # Step 5) Clean-up
    # FIXME: figure out how to make this generic
    if [$8 -eq orchestra]; then
        rm -R $8/cosmos/out/*
        rm -R $8/cosmos/tmp/*
    else
        rm -R $8/cosmos_out/*
        rm -R $8/cosmos/erik/*
    fi

    rm $8/Out/*
    echo "Genomekey run \"$1\" data successfully backup on S3" | mail -s "GenomeKey Backup" "$9"
else
    echo "Genomekey run \"$1\" failed" | mail -s "GenomeKey run Failure" "$9"
fi
