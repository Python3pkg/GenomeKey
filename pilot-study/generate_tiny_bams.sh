#!/bin/sh
# generate an arbitrary number BAM files from an input BAM
# with different RG (read group) and SM (sample tags)
# Usage:
# generate_tiny_bams.sh <num-bams> <source.bam>


END=$1
INPUT_BAM=$2

OUTPUT_BAM_BASENAME=$(basename ${INPUT_BAM})
OUTPUT_BAM_PREFIX=${OUTPUT_BAM_BASENAME%.bam}

# get unique "SM" (sample) tag to rewrite
ID=$(samtools view -H ${INPUT_BAM} |grep "SM:"|cut -f3|cut -d':' -f2|sort|uniq)

for i in $(seq 1 $END)
do 
    # zero-pad output
    printf -v j "%02d" $i
    cmd="samtools view -H ${INPUT_BAM} | sed \"s/${ID}/${j}.${ID}/g\" | samtools reheader - ${INPUT_BAM} > ${OUTPUT_BAM_PREFIX}-${j}.bam"
    echo $cmd
    eval $cmd
done
