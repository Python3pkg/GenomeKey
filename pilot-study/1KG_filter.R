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
stats = summary(cvr$MEAN_BAIT_COVERAGE)
li = quantile(cvr$MEAN_BAIT_COVERAGE)
# extract the quartile between 25-75% around the mean
lower = li["25%"][[1]]
higher = li["75%"][[1]]

histogram(~MEAN_BAIT_COVERAGE, data=cvr)
densityplot(~MEAN_BAIT_COVERAGE, data=cvr)

# filter by populations
# European populations info from: ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/README.populations
sbt = subset(cvr, MEAN_BAIT_COVERAGE > lower & MEAN_BAIT_COVERAGE < higher & grepl("CEU|FIN|IBS|GBR|TSI", File_name))
num.exomes = length(sbt$MEAN_BAIT_COVERAGE)

# get the file names and write to <num.exomes>_exomes.index
ex=(sbt$File_name)
write.table((sbt$File_name), file=sprintf("%d_exomes.index", num.exomes), sep='\n', row.names=FALSE, col.names=FALSE, quote=FALSE)
