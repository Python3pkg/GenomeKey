#!/usr/bin/env Rscript
## This script reads the information about the 1000 genomes project Exomes from phase I, 
## give an overview about the MEAN_BAIT_COVERAGE which is the mean coverage of all baits 
## in the experiment. more: http://picard.sourceforge.net/picard-metric-definitions.shtml#HsMetrics

# this script needs: ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/alignment_indices/20130502.exome.alignment.index.HsMetrics.gz
# the file should be in the working directory, otherwise replace with the absolute PATH

library(lattice)

# loading the data into cvr table
cvr=read.table("20130502.exome.alignment.index.HsMetrics", header=TRUE, fill=TRUE)

# get some overview and visualization of the distribution of the "mean coverage"
summary(cvr$MEAN_BAIT_COVERAGE)
histogram(~MEAN_BAIT_COVERAGE, data=cvr)
densityplot(~MEAN_BAIT_COVERAGE, data=cvr)

# filter by populations
# European populations info from: ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/README.populations
sbt = subset(cvr, MEAN_BAIT_COVERAGE > 100 & MEAN_BAIT_COVERAGE < 120 & grepl("CEU|FIN|IBS|GBR|TSI", File_name))
length(sbt$MEAN_BAIT_COVERAGE)

# get the file names and write to 127_exomes.index
head(sbt)
ex=(sbt$File_name)
write.table((sbt$File_name), file="127_exomes.index", sep='\n', row.names=FALSE, col.names=FALSE, quote=FALSE)
