#!/bin/bash
#Author: Yassine Souilmi
#		Yassine_Souilmi@hms.harvard.edu

#this script could alternatively be used to monitor Genomekey's runs
#Use: watch -n 10 '/path/to/this/script/monitorGK.sh /path/to/GK/loggile.log'
#the number given after the argument -n is the interval in secends watch will wait before re-executing

LOGFILE=$1 #$1: path to the Genomekey log file

total_submitted=$(grep "Submitted" $LOGFILE|wc -l)
total_successful=$(grep "successful" $LOGFILE|wc -l)
total_failed=$(grep "failed" $LOGFILE|wc -l)
echo "total submitted jobs: $total_submitted"
echo "including $total_successful succesful jobs and $total_failed failed jobs"

echo ""
echo "***********"
echo "*** Stage 1: BAM to BWA"
successful=$(grep "BWA" $LOGFILE|grep "successful"|wc -l)
echo "Number of successful Jobs: $successful"
queuing=$(qstat|grep "BWA"|wc -l)
running=$(qstat -s r|grep "BWA"|wc -l)
echo "Number of Jobs in the queue ${queuing}, whith $queuing running"
echo ""
echo "***********"
echo ""

echo "*** Stage 2: IndelRealigner"
successful=$(grep "Indel" $LOGFILE|grep "successful"|wc -l)
echo "Number of successful Jobs: $successful"
queuing=$(qstat|grep "Indel"|wc -l)
running=$(qstat -s r|grep "Indel"|wc -l)
echo "Number of Jobs in the queue ${queuing}, whith $queuing running"
echo ""
echo "***********"
echo ""


echo "*** Stage 3: MarkDuplicates"
successful=$(grep "Mark" $LOGFILE|grep "successful"|wc -l)
echo "Number of successful Jobs: $successful"
queuing=$(qstat|grep "Mark"|wc -l)
running=$(qstat -s r|grep "Mark"|wc -l)
echo "Number of Jobs in the queue ${queuing}, whith $queuing running"
echo ""
echo "***********"
echo ""


echo "*** Stage 4: HaplotypeCaller"
successful=$(grep "Hap" $LOGFILE|grep "successful"|wc -l)
echo "Number of successful Jobs: $successful"
queuing=$(qstat|grep "Hap"|wc -l)
running=$(qstat -s r|grep "Hap"|wc -l)
echo "Number of Jobs in the queue ${queuing}, whith $queuing running"
echo ""
echo "***********"
echo ""

echo "*** Stage 4: GenotypeGVF"
successful=$(grep "Genotype" $LOGFILE|grep "successful"|wc -l)
echo "Number of successful Jobs: $successful"
queuing=$(qstat|grep "Genotype"|wc -l)
running=$(qstat -s r|grep "Genotype"|wc -l)
echo "Number of Jobs in the queue ${queuing}, whith $queuing running"
echo ""
echo "***********"
echo ""

echo "*** Stage 4: VQSR"
successful=$(grep "VQSR" $LOGFILE|grep "successful"|wc -l)
echo "Number of successful Jobs: $successful"
queuing=$(qstat|grep "VQSR"|wc -l)
running=$(qstat -s r|grep "VQSR"|wc -l)
echo "Number of Jobs in the queue ${queuing}, whith $queuing running"
echo ""
echo "***********"
echo ""



