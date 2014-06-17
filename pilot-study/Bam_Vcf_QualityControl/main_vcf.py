import os
from os.path import join
import re
from functions import functions_vcf

# Require tabix installed
# Require new bcftools installed

vqsr_cosmos_folder="/media/Elements/Trio/10_Exome/VCF/VQSR"     # Directory downloaded from s3 bucket (VQSR)
temp_dir="/media/Elements/Trio/10_Exome/VCF/Temp"               # Temp directory
out_dir="/media/Elements/Trio/10_Exome/VCF/VCF_10_Exome"        # Directory of total vcf
vcf_name="10_Exome"                                             # Name of vcf file
sample="NA12878"                                                # Sample to analyze

functions_vcf.Copy_VQSR_SNP(vqsr_cosmos_folder, temp_dir, out_dir, vcf_name)

file_vcf=join(out_dir,vcf_name+'.vcf')
file_out=join(out_dir,vcf_name+"_"+sample+".vcf")
file_stat=join(out_dir,vcf_name+"_"+sample+".stat")

functions_vcf.Stat(file_vcf,sample, file_out, file_stat, out_dir)
