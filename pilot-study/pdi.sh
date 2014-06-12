#!/bin/bash
#Author: Yassine Souilmi

S3LIST=$1                           # $1: the list of the input bams paths on S3
COSMOS_DEFAULT_ROOT_OUTPUT_DIR=$2   # $2 : the path to the gluster volume


RUNNAME=$(basename "$S3LIST")
echo $RUNNAME

INPUT_DIR=$COSMOS_DEFAULT_ROOT_OUTPUT_DIR/Inputs/$RUNNAME
TRIO_DIR=$COSMOS_DEFAULT_ROOT_OUTPUT_DIR/Trio/

mkdir -pv $TRIO_DIR
mkdir -pv $INPUT_DIR

rm -f ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${RUNNAME}".idx

while read F
do
        BASENAME=$(basename $F .bam)
        if  [[ "${BASENAME%_*}" = "CEUTrioWEx" ]]
        then
                if [[ ! -f "${TRIO_DIR}/${BASENAME}.bam" ]]
                then
                        cmd="qsub -V -b y -cwd aws s3 cp $F $TRIO_DIR"
                        echo $cmd
                        DOWNLOAD_DEP_JOB_ID=$(eval $cmd | cut -d' ' -f3)

                        #generating the local list of files
                        echo ${TRIO_DIR}/${BASENAME}'.bam' >> ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${RUNNAME}".idx

                        #Indexing
                    if [[ ! -f "${TRIO_DIR}/${BASENAME}.bam.bai" ]] && [[ ! -f "${TRIO_DIR}/${BASENAME}.bai" ]]
                    then
                        cmd="qsub -hold_jid $DOWNLOAD_DEP_JOB_ID -V -b y -cwd /WGA/tools/samtools.v0.1.19 index ${TRIO_DIR}/${BASENAME}'.bam'"
                        echo $cmd
                        INDEX_DEP_JOB_ID=$(eval $cmd | cut -d' ' -f3)
                        sleep 1
                    else
                        echo "$(basename $F .bam) index file exists"
                    fi
                else
                    echo "${TRIO_DIR}/${BASENAME}.bam already downloaded"

                    #Indexing
                    if [[ ! -f "${TRIO_DIR}/${BASENAME}.bam.bai" ]] && [[ ! -f "${TRIO_DIR}/${BASENAME}.bai" ]]
                    then
                        cmd="qsub -V -b y -cwd /WGA/tools/samtools.v0.1.19 index ${TRIO_DIR}/${BASENAME}'.bam'"
                        echo $cmd
                        INDEX_DEP_JOB_ID=$(eval $cmd | cut -d' ' -f3)
                        sleep 1
                    else
                        echo "$(basename $F .bam) index file exists"
                    fi
                fi


        else
                 if [[ ! -f "${INPUT_DIR}/${BASENAME}.bam" ]]
                then
                        cmd="qsub -V -b y -cwd aws s3 cp $F $INPUT_DIR"
                        echo $cmd
                        DOWNLOAD_DEP_JOB_ID=$(eval $cmd | cut -d' ' -f3)

                        #generating the local list of files
                        echo ${$INPUT_DIR}/${BASENAME}'.bam' >> ${COSMOS_DEFAULT_ROOT_OUTPUT_DIR}/"${RUNNAME}".idx

                        #Indexing
                    if [[ ! -f "${INPUT_DIR}/${BASENAME}.bam.bai" ]] && [[ ! -f "${INPUT_DIR}/${BASENAME}.bai" ]]
                    then
                        cmd="qsub -hold_jid $DOWNLOAD_DEP_JOB_ID -V -b y -cwd /WGA/tools/samtools.v0.1.19 index ${$INPUT_DIR}/${BASENAME}'.bam'"
                        echo $cmd
                        INDEX_DEP_JOB_ID=$(eval $cmd | cut -d' ' -f3)
                        sleep 1
                    else
                        echo "$(basename $F .bam) index file exists"
                    fi
                else
                    echo "${INPUT_DIR}/${BASENAME}.bam already downloaded"

                    #Indexing
                    if [[ ! -f "${INPUT_DIR}/${BASENAME}.bam.bai" ]] && [[ ! -f "${INPUT_DIR}/${BASENAME}.bai" ]]
                    then
                        cmd="qsub -V -b y -cwd /WGA/tools/samtools.v0.1.19 index ${INPUT_DIR}/${BASENAME}'.bam'"
                        echo $cmd
                        INDEX_DEP_JOB_ID=$(eval $cmd | cut -d' ' -f3)
                        sleep 1
                    else
                        echo "$(basename $F .bam) index file exists"
                    fi
                fi

        fi
done <$S3LIST

#t=1
#while t==1
#do
#		n=$(qstat | wc -l)
#
#		if [[ ! n==2 ]]
#		then
#			t=0
#		else
#			sleep 1
#
#done
