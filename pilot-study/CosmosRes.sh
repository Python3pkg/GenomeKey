#!/bin/bash

# $1 list
# $2 output bucket
# $3 port

# Step 0) Make an output dir

mkdir ~/Out/"$1"

# Step 1) Launch the run

genomekey bam -n "$1" -il $1

# Step 2) Launch the web-server

cosmos runweb -p $3

# Step 3) Dump the DB (the username and the password are hard-coded here)

mysqldump -u usernam -p password –no-create-info dbName > ~/Out/"$1".sql

# Step 4) Reset cosmos DB

cosmos resetdb

# Step 5) Copy files to S3

  #cp the DB
  aws s3 cp ~/Out/"$1"/ $2

  #cp the VCF
  aws s3 cp /cosmos/output/directory

# Step 6) Clean-up

rm ~/Out/*

rm /cosmos/output/directory/*

rm /cosmos/temp/directory/*
