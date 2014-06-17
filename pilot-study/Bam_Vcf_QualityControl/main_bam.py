import os
from os.path import join
import re
from functions import functions_bam
from vcf_stat import Stat

# Require Samtools installed
# Once you define the BQSR folder and the name of the sample, it merges all the bam related to that sample and creates the total bam

bqsr_cosmos_folder="/media/86_Passport/Dott_work/Harvard_Pilot/Out/Batch_1/1_Exome/BQSR/BQSR"       # Directory downloaded from s3 bucket
out_dir_Bam="/media/Elements/Trio/1_Exome/Bam"                                                      # Directory where to save the merged Bam
out_dir_Stat="/media/Elements/Trio/1_Exome/Stat"                                                    # Directory where to save Bam Statistic
sample="NA12878"                                                                                    # Name of the sample to analyze

bam_file=sample+".bam"
stat_file=sample+".stat"

functions_bam.Copy_BQSR_bam(bqsr_cosmos_folder,out_dir_Bam,bam_file,sample)
functions_bam.Stat_Bam(out_dir_Stat,join(out_dir_Bam,bam_file),stat_file)
