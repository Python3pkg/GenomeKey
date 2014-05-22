#!/bin/bash


LOG_FILE=$1

###############Download

DOWNLOAD=$($pwd/timediff.sh ${LOG_FILE} Download)

DOWNLOAD_DURATION=$(echo $DOWNLOAD|awk '{printf "%d:%02d:%02d", $1/3600, ($1/60)%60, $1%60}')

echo "Download: $DOWNLOAD_DURATION"

###############Indexing (samtools)

INDEXING=$($pwd/timediff.sh ${LOG_FILE} indexes)

INDEXING_DURATION=$(echo $INDEXING|awk '{printf "%d:%02d:%02d", $1/3600, ($1/60)%60, $1%60}')

echo "Indexing: $INDEXING_DURATION"

###############Run

RUN=$($pwd/timediff.sh ${LOG_FILE} GenomeKey)

RUN_DURATION=$(echo $RUN|awk '{printf "%d:%02d:%02d", $1/3600, ($1/60)%60, $1%60}')

echo "GK Run: $RUN_DURATION"

###############Backup (aws s3)

BACKUP=$($pwd/timediff.sh ${LOG_FILE} Push)

BACKUP_DURATION=$(echo $BACKUP|awk '{printf "%d:%02d:%02d", $1/3600, ($1/60)%60, $1%60}')

echo "Backup: $BACKUP_DURATION"

###############Total

TOTAL=$((DOWNLOAD+INDEXING+RUN+BACKUP))

TOTAL_DURATION=$(echo $TOTAL|awk '{printf "%d:%02d:%02d", $1/3600, ($1/60)%60, $1%60}')

echo "Total: $TOTAL_DURATION"

