#!/usr/bin/env Rscript

## Author: Alex Lancaster

## Generates PDF and PNG plots of CPU usage across the entire cluster
## in a 5 minute window by extracting all jobs within each window and
## then estimating their contribution to that window by summing the
## percent CPU.  Jobs not completely contained within the window are
## pro-rated according to the amount they contribute to the window

library(lattice);
library(directlabels);
library(IRanges)

all.jobs=NULL;
args <- commandArgs(TRUE)
numargs = length(args)

f = args[1]

all.jobs = rbind(all.jobs, read.csv(f, header=T));
all.jobs = transform(all.jobs, true_start=stop-wall_time)

## skip Load_BAMs
subset.jobs=subset(all.jobs, stage!='Load_BAMs')

ranged.data=RangedData(IRanges(start=subset.jobs$true_start, end=subset.jobs$stop),
  cpu = subset.jobs$percent_cpu, stage = as.character(subset.jobs$stage))

## get CPU usage across 5 minute window
window = 60*5
job.density = data.frame()

for (window_start in seq(1, max(subset.jobs$stop), by=window)) {
  window_end =  window_start + window
  overlaps = subsetByOverlaps(ranged.data, RangedData(IRanges(window_start, window_end)))
  df = as.data.frame(overlaps)
  df$space = NULL
  df = transform(df, subset_start=ifelse(start > window_start, start, window_start))
  df = transform(df, subset_end=ifelse(end < window_end, end, window_end))
  df = transform(df, cpu_frac = cpu*(subset_end - subset_start)/width)
  if (length(df$cpu_frac) > 0) {
    ## first get the total before aggregation
    total_cpu_sum = sum(df$cpu_frac)
    total_df = data.frame(window_start=window_start, stage="Total", cpu_sum=as.numeric(total_cpu_sum))
    df = aggregate(list(cpu_sum=df$cpu_frac), by=list(stage=df$stage), sum)
    new_df = cbind(window_start, df)
    job.density = rbind(job.density, new_df)
    ## add the total
    job.density = rbind(job.density, total_df)
  } 
}

print((job.density))

## assumes a 20 node cluster, with 32 cores per node
p1 = xyplot((cpu_sum/(32*20))~(window_start/60), group=stage, auto.key=list(corner = c(0.5, 0.95), points = FALSE, lines = TRUE),
  type='l', ylim=c(-1, 21), xlab="wall time (min)", ylab="total CPU percent usage across cluster", data=job.density)

pdf(sprintf("density-%s.pdf", f))
print(p1)
dev.off()

png(sprintf("density-%s.png", f))
print(p1)
dev.off()
