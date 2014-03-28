#!/usr/bin/env Rscript
## This script reads the information about the 1000 genomes project Exomes from phase I, 
## give an overview about the MEAN_BAIT_COVERAGE which is the mean coverage of all baits 
## in the experiment. more: http://picard.sourceforge.net/picard-metric-definitions.shtml#HsMetrics

# this script needs: ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/alignment_indices/20130502.exome.alignment.index.HsMetrics.gz
# the file should be in the working directory, otherwise replace with the absolute PATH

library(lattice)

# loading the data into cvr table
cvr=read.table("20130502.exome.alignment.index.HsMetrics", header=TRUE, fill=TRUE)

# extract the individual ID
cvr=transform(cvr, Individual.ID=sub("[.].*", "", cvr$File_name))

# get some overview and visualization of the distribution of the "mean coverage"
stats = summary(cvr$MEAN_BAIT_COVERAGE)
li = quantile(cvr$MEAN_BAIT_COVERAGE)
# extract the quartile between 25-75% around the mean
lower = li["25%"][[1]]
higher = li["75%"][[1]]

histogram(~MEAN_BAIT_COVERAGE, data=cvr)
densityplot(~MEAN_BAIT_COVERAGE, data=cvr)

# load pedigree data
pedigree=read.table("20130606_g1k.ped", header=T, fill=T, sep="\t")

# merge the datasets
merged = merge(cvr, pedigree, by="Individual.ID")

# filter by populations
# European populations info from: ftp://ftp-trace.ncbi.nih.gov/1000genomes/ftp/README.populations
pop.regex="CEU|FIN|IBS|GBR|TSI"

# get all individuals with minimum coverage
min.cov = subset(merged, MEAN_BAIT_COVERAGE > lower & MEAN_BAIT_COVERAGE < higher & grepl(pop.regex, Population))

# now get trios only in the specified population(s)
# FIXME: disable for the moment
#trios = subset(min.cov, (Paternal.ID != 0) & (Maternal.ID !=0))
#min.cov.trios = subset(min.cov, (Individual.ID %in% trios$Paternal.ID) & (Individual.ID %in% trios$Maternal.ID))
#head(min.cov.trios[,c("Individual.ID", "Paternal.ID", "Maternal.ID", "MEAN_BAIT_COVERAGE")])

num.exomes = length(min.cov$MEAN_BAIT_COVERAGE)
# get the file names and write to <num.exomes>_exomes.index
#write.table(min.cov[,c("File_name", "MEAN_BAIT_COVERAGE", "Paternal.ID", "Maternal.ID")], file=sprintf("%d_exomes.index", num.exomes), row.names=FALSE, col.names=TRUE, quote=FALSE)
write.table(min.cov[,c("File_name")], file=sprintf("%d_exomes.index", num.exomes), row.names=FALSE, col.names=FALSE, quote=FALSE)

