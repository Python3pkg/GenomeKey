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

# log file name
# make unique by using timestamp in name
LOG_FILE=${HOME}/"${RUNNAME}"-$(date +"%Y-%m-%d-%s").log

DATE=$(date)
STARTDATE=$(date +%s)
echo "log: $DATE : $STARTDATE : Beginning :  Run $RUNNAME"
echo "log: $DATE : $STARTDATE : Beginning :  Run $RUNNAME" >  ${LOG_FILE}

echo $COSMOS_DEFAULT_ROOT_OUTPUT_DIR
echo $COSMOS_WORKING_DIRECTORY

##################
# Download the data from S3
# first make sure the directory is empty

DATE=$(date)
STARTDATE=$(date +%s)
echo "log: $DATE : $STARTDATE : Beginning : Download from S3"
echo "log: $DATE : $STARTDATE : Beginning : Download from S3" >>  ${LOG_FILE}

rm -rf $COSMOS_DEFAULT_ROOT_OUTPUT_DIR/"${RUNNAME}"/Inputs/
mkdir -pv $COSMOS_DEFAULT_ROOT_OUTPUT_DIR/"${RUNNAME}"/Inputs/ # now recreate directory
mkdir -pv $COSMOS_DEFAULT_ROOT_OUTPUT_DIR/Trio/


while read F
do
	BASENAME=$(basename $F .bam)
	if  [ "${BASENAME%.*.*.*.*.*}" = "CEUTrio.HiSeq.WEx.b37_decoy" ]
	then
		if [ ! -f $COSMOS_DEFAULT_ROOT_OUTPUT_DIR/Trio/${BASENAME}.bam ] 
		then
			aws s3 cp $F $COSMOS_DEFAULT_ROOT_OUTPUT_DIR/Trio/
		fi

	else
		aws s3 cp $F $COSMOS_DEFAULT_ROOT_OUTPUT_DIR/${RUNNAME}/Inputs/
	fi    

    #aws s3 cp "$F".bai $COSMOS_WORKING_DIRECTORY/${RUNNAME}/Inputs/
done <$S3LIST

DATE=$(date)
ENDDATE=$(date +%s)
echo "log: $DATE : $ENDDATE : End : Download from S3"
echo "log: $DATE : $ENDDATE : End : Download from S3" >>  ${LOG_FILE}

###################
# Getting the local list of files
# ls $COSMOS_WORKING_DIRECTORY/"${RUNNAME}"/Inputs/*.bam > ${COSMOS_WORKING_DIRECTORY}/"${RUNNAME}"/Inputs/"${RUNNAME}".idx# creating the local files index

DATE=$(date)
STARTDATE=$(date +%s)
echo "log: $DATE : $STARTDATE : Beginning : Creating local files list"
echo "log: $DATE : $STARTDATE : Beginning : Creating local files list" >>  ${LOG_FILE}

while read F
do
    BASENAME=$(basename $F .bam)

        if  [ "${BASENAME%.*.*.*.*.*}" = "CEUTrio.HiSeq.WEx.b37_decoy" ]
        then
                echo $COSMOS_DEFAULT_ROOT_OUTPUT_DIR/Trio/$BASENAME'.bam' >> ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${RUNNAME}"/Inputs/"${RUNNAME}".idx

	else
		echo $COSMOS_DEFAULT_ROOT_OUTPUT_DIR/"${RUNNAME}"/Inputs/$BASENAME'.bam' >> ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${RUNNAME}"/Inputs/"${RUNNAME}".idx
	fi

done <$S3LIST

DATE=$(date)
ENDDATE=$(date +%s)
echo "log: $DATE : $ENDDATE : End : Creating local files list"
echo "log: $DATE : $ENDDATE : End : Creating local files list" >>  ${LOG_FILE}

##################
# Generating the index files

DATE=$(date)
STARTDATE=$(date +%s)
echo "log: $DATE : $STARTDATE : Beginning : Creating bams indexes"
echo "log: $DATE : $STARTDATE : Beginning : Creating bams indexes" >>  ${LOG_FILE}

while read F
do
	
	cmd="${TOOLS_PATH}/samtools.v0.1.19 index ${F}"
	echo $cmd
	eval $cmd
	sleep 1
done <${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${RUNNAME}"/Inputs/"${RUNNAME}".idx

DATE=$(date)
ENDDATE=$(date +%s)
echo "log: $DATE : $ENDDATE : End : Creating bams indexes"
echo "log: $DATE : $ENDDATE : End : Creating bams indexes" >>  ${LOG_FILE}
########################
# Notify the user of the start
#echo "GenomeKey run \"${RUNNAME}\" started" | mail -s "GenomeKey \"${RUNNAME}\" run Started" "${EMAIL}"

# Step 2) Launch the run
# give '-y' option which assumes "yes" answers to re-running/deleting workflows
# also '-r' to restart workflow from scratch by deleting existing files

DATE=$(date)
STARTDATE=$(date +%s)
echo "log: $DATE : $STARTDATE : Beginning : GenomeKey run"
echo "log: $DATE : $STARTDATE : Beginning : GenomeKey run" >>  ${LOG_FILE}

GK_OUTPUT="${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/GK${RUNNAME}.out"

cmd="${GK_PATH}/bin/genomekey bam -n \"${RUNNAME}\" -r -y -di -il ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/${RUNNAME}/Inputs/${RUNNAME}.idx ${GK_ARGS} &> ${GK_OUTPUT}"
echo $cmd
eval $cmd
GK_EVAL=$?

DATE=$(date)
ENDDATE=$(date +%s)
echo "log: $DATE : $ENDDATE : End : GenomeKey run"
echo "log: $DATE : $ENDDATE : End : GenomeKey run" >>  ${LOG_FILE}


if [ $GK_EVAL -eq 0 ]; then

    #echo "GenomeKey run \"${RUNNAME}\" was successful" | mail -s "GenomeKey run Successful" "${EMAIL}"

    DATE=$(date)
    STARTDATE=$(date +%s)
    echo "log: $DATE : $STARTDATE : Beginning : COSMOS sqldump"
    echo "log: $DATE : $STARTDATE : Beginning : COSMOS sqldump" >>  ${LOG_FILE}

    # Dump the DB
    SQL_OUTPUT="${COSMOS_WORKING_DIRECTORY}/${RUNNAME}.sql"
    cmd="mysqldump -u ${DBUSER} -p${DBPASSWD} ${DBNAME} > ${SQL_OUTPUT}"
    echo $cmd
    eval $cmd

    DATE=$(date)
    ENDDATE=$(date +%s)
    echo "log: $DATE : $ENDDATE : End : COSMOS sqldump"
    echo "log: $DATE : $ENDDATE : End : COSMOS sqldump" >>  ${LOG_FILE}

    # Copy files to S3

    DATE=$(date)
    STARTDATE=$(date +%s)
    echo "log: $DATE : $STARTDATE : Beginning : Push results to S3"
    echo "log: $DATE : $STARTDATE : Beginning : Push results to S3" >>  ${LOG_FILE}    

    RUNNAME_OUTPUT="${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/${RUNNAME}"
    S3_OUTPUT="${OUTBUCKET}Out/${RUNNAME}"
    S3_PERMS="--grants read=uri=http://acs.amazonaws.com/groups/global/AllUsers"

    # rm the .bai and .idx files 
    find ${RUNNAME_OUTPUT} -name "*.bai" -type f -delete
    find ${RUNNAME_OUTPUT} -name "*.idx" -type f -delete
	
    # rm input files
    rm -rf $COSMOS_DEFAULT_ROOT_OUTPUT_DIR/"${RUNNAME}"/Inputs/

    #cp everything    
    aws s3 cp ${RUNNAME_OUTPUT} ${S3_OUTPUT}/ --recursive ${S3_PERMS}
    
    #cp the MySQL DB
    aws s3 cp ${SQL_OUTPUT} ${S3_OUTPUT}/ ${S3_PERMS}

    #cp the BAM after BQSR
    #aws s3 cp ${RUNNAME_OUTPUT}/BQSR/ ${S3_OUTPUT}/BQSR/ --recursive ${S3_PERMS}

    #cp the gVCF
    #aws s3 cp ${RUNNAME_OUTPUT}/HaplotypeCaller/ ${S3_OUTPUT}/HaplotypeCaller/ --recursive ${S3_PERMS}

    # both stdout stderr for GenomeKey are in the one file
    aws s3 cp ${GK_OUTPUT} ${S3_OUTPUT}/ ${S3_PERMS}
   
    DATE=$(date)
    ENDDATE=$(date +%s)
    echo "log: $DATE : $ENDDATE : End :  Push results to S3"
    echo "log: $DATE : $ENDDATE : End :  Push results to S3" >>  ${LOG_FILE}

    # Clean-up

    DATE=$(date)
    STARTDATE=$(date +%s)
    echo "log: $DATE : $STARTDATE : Beginning :  Run Data wipe"
    echo "log: $DATE : $STARTDATE : Beginning :  Run Data wipe" >>  ${LOG_FILE}

    rm -R -f ${RUNNAME_OUTPUT}
    rm -R -f ${COSMOS_WORKING_DIRECTORY}/*
    
    # Reset cosmos DB
    echo "yes" | cosmos resetdb
 
    DATE=$(date)
    ENDDATE=$(date +%s)
    echo "log: $DATE : $ENDDATE : End :  Run Data wipe"
    echo "log: $DATE : $ENDDATE : End :  Run Data wipe" >>  ${LOG_FILE}


    #echo "Genomekey run \"${S3LIST}\" data successfully backup on S3" | mail -s "GenomeKey Backup" "${EMAIL}"
#else
    #echo "Genomekey run \"${S3LIST}\" failed" | mail -s "GenomeKey run Failure" "${EMAIL}"
fi

#####################End
DATE=$(date)
ENDDATE=$(date +%s)
echo "log: $DATE : $ENDDATE : End :  Run $RUNNAME"
echo "log: $DATE : $ENDDATE : End :  Run $RUNNAME" >>  ${LOG_FILE}

#cp the run log file to S3
aws s3 cp  ${LOG_FILE} ${S3_OUTPUT}/ ${S3_PERMS}
