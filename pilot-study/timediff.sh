#!/bin/bash


LOG_FILE=$1   #Path to the log file
STAGE=$2        #String: name of the stage

START=$(grep "${STAGE}" ${LOG_FILE} | grep "Beginning" | cut -d ":" -f5)
END=$(grep "${STAGE}" ${LOG_FILE} | grep "End" | cut -d ":" -f5)

echo $((END-START))
