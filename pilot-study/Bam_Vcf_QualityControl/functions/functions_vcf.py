import os
from os.path import join
from settings import settings
import shutil
import re

def Copy_VQSR_SNP(vqsr_cosmos_folder, temp_dir, out_dir, vcf_name):
    count_file=0
    # Create temp_dir and out_dir if don't exist
    if not os.path.exists(temp_dir):
            os.makedirs(temp_dir)
    if not os.path.exists(out_dir):
            os.makedirs(out_dir)
    
    # Search for .vcf files in the VQSR folder generated by Cosmos and saved on the s3 bucket 
    for root, subFolders, files in os.walk(vqsr_cosmos_folder):
        for file in files:
            filePath = root + '/' + file
            if filePath.endswith(".vcf"):
                with open(filePath) as f:
                    for line in f:
                        # Check if the file.vcf has been recalibrated using mode=SNP
                        if "ApplyRecalibration" in line:
                            if "mode=SNP" in line:
                                count_file+=1
                                # If yes copy to temp directory
                                cmd="cp "+filePath+" "+temp_dir+'/'+str(count_file)+".vcf"
                                os.system(cmd)
                                break 
                            else:
                                break
    
    # For each vcf file create the .gz and index it with tabix
    concatenate_vcf=""
    for file in os.listdir(temp_dir):
        cmd_bgzip="bgzip "+os.path.join(temp_dir,file)
        os.system(cmd_bgzip)
        cmd_tabix="tabix -p vcf "+os.path.join(temp_dir,file+".gz")
        os.system(cmd_tabix)
        concatenate_vcf=concatenate_vcf+os.path.join(temp_dir,file+".gz")+" "
    
    # Concatenate all the vcf files, create .gz and index it
    vcf_concat=settings.setting['vcf_concat']
    out_vcf=out_dir+"/"+vcf_name+".vcf"
    cmd="perl "+vcf_concat+" "+concatenate_vcf+"> "+out_vcf
    print cmd
    os.system(cmd)
    cmd_bgzip="bgzip "+out_vcf
    os.system(cmd_bgzip)
    cmd_tabix="tabix -p vcf "+out_vcf+".gz"
    print cmd
    os.system(cmd_tabix)
    
    # Sort the vcf file generated
    vcf_sort=settings.setting['vcf_sort']
    cmd="perl "+vcf_sort+" -c "+out_vcf+".gz > "+out_dir+"/"+vcf_name+"_sorted.vcf"
    print cmd
    os.system(cmd)
      
    # Delete temp folder and unsorted vcf
    shutil.rmtree(temp_dir)
    try:
        os.remove(out_vcf+".gz") 
        os.remove(out_vcf+".gz.tbi") 
    except:
        pass
    
    # Rename vcf
    cmd="mv "+out_dir+"/"+vcf_name+"_sorted.vcf "+out_vcf
    os.system(cmd)
    
    
def Stat(vcf, sample, out, file_stat, out_dir):
    # VCF = File.vcf
    # Sample = ID in the vcf
    # OUT = File.vcf
    if not os.path.exists(out_dir):
            os.makedirs(out_dir)
    Stat_file=open(file_stat,'w')
    gatk=settings.setting['gatk']
    reference=settings.setting['reference']
    vcf_tools=settings.setting['vcf_toots']
          
    #Select variants for the sample under investigation 
    os.system("java -Xmx2g -jar "+gatk+" -R "+reference+" -T SelectVariants --variant "+vcf+" -o "+out+" -sn "+sample+" -env")
    
    # Remove indel
    os.system(vcf_tools+" --vcf "+out+" --remove-indels --recode --recode-INFO-all --out "+out.replace(".vcf",""))
    os.system("mv "+out.replace(".vcf","")+".recode.vcf "+out)
    
    # Split sites on multiple lines because otherwise they will be not taken into account in the count of Ti and Tv 
    out_splitted=out.replace(".vcf","")+"_splitted.vcf"
    os.system("bgzip "+out)
    os.system("bcftools norm -c x -m - -f "+reference+" "+out+".gz > "+out_splitted)
    os.system("gzip -d "+out+".gz")
 
    # VariantEval total vcf
    out_eval=out.replace(".vcf","")
    out_eval=out_eval+"_eval_all"
    os.system("java -Xmx2g -jar "+gatk+" -R "+reference+" -T VariantEval --eval "+out_splitted+" -o "+out_eval)
    ti_tv=[]
    with open(out_eval) as f:
        for line in f:
            if "TiTvVariantEvaluator" in line and "all" in line:
                line=re.sub(' +',',',line)
                line=line.rstrip()
                split=line.split(',')
                ti_tv=split[7]
    Stat_file.write("Ti/Tv ALL VARIANTS\t"+ti_tv+"\n")
    os.system("rm "+out_eval)
    
    # Overall number of high-quality SNP
    out_high_gq_qual=out_splitted.replace(".vcf","")+"_high_gq.vcf"
    os.system(vcf_tools+" --vcf "+out_splitted+" --recode --recode-INFO-all --minQ 30 --out "+out_high_gq_qual.replace(".vcf",""))
    os.system("mv "+out_high_gq_qual.replace(".vcf","")+".recode.vcf "+out_high_gq_qual)
    
    # Count total SNP
    os.system("sed /#/d "+out_splitted+"> "+out_splitted+"_no_header.vcf") 
    with open(out_splitted+"_no_header.vcf") as f:
        for i, l in enumerate(f):
            pass
    num_snp = i + 1
    Stat_file.write("NUM TOTAL SNP\t"+str(num_snp)+"\n")
    os.system("rm "+out_splitted+"_no_header.vcf")
    
    # Count high-quality SNP
    os.system("sed /#/d "+out_high_gq_qual+"> "+out_high_gq_qual+"_no_header.vcf") 
    with open(out_high_gq_qual+"_no_header.vcf") as f:
        for i, l in enumerate(f):
            pass
    num_high_qual = i + 1
    Stat_file.write("NUM HIGH QUALITY SNP\t"+str(num_high_qual)+"\n")
    os.system("rm "+out_high_gq_qual+"_no_header.vcf")
    
    # Extract biallelic sites
    out_biallelic=out.replace(".vcf","")+"_biallelic.vcf"
    os.system(vcf_tools+" --vcf "+out+" --remove-indels --remove-filtered . --recode --recode-INFO-all --min-alleles 3 --max-alleles 3 --out "+out_biallelic.replace(".vcf",""))
    os.system("mv "+out_biallelic.replace(".vcf","")+".recode.vcf "+out_biallelic)
    
    # Count biallelic site
    os.system("sed /#/d "+out_biallelic+"> "+out_biallelic+"_no_header.vcf") 
    if os.stat(out_biallelic+"_no_header.vcf")[6]==0:
        num_biallelic_variants = 0
    else:
        with open(out_biallelic+"_no_header.vcf") as f:
            for i, l in enumerate(f):
                pass
        num_biallelic_variants = i + 1
        num_biallelic_variants = num_biallelic_variants*2
    Stat_file.write("NUM BIALLELIC VARIANTS\t"+str(num_biallelic_variants)+"\n")
    os.system("rm "+out_biallelic+"_no_header.vcf")
    print num_biallelic_variants
    
    # Split biallelic sites on multiple lines because otherwise they will be not taken into account in the count of Ti and Tv 
    out_biallelic_splitted=out_biallelic.replace(".vcf","")+"_splitted.vcf"
    os.system("bgzip "+out_biallelic)
    os.system("bcftools norm -m - -f "+reference+" "+out_biallelic+".gz > "+out_biallelic_splitted)
    os.system("gzip -d "+out_biallelic+".gz")

    # VariantEval total biallelic sites   
    out_eval=out.replace(".vcf","")
    out_eval=out_eval+"_eval_biallelic"
    os.system("java -Xmx2g -jar "+gatk+" -R "+reference+" -T VariantEval --eval "+out_biallelic_splitted+" -o "+out_eval)
    ti_tv=[]
    with open(out_eval) as f:
        for line in f:
            if "TiTvVariantEvaluator" in line and "all" in line:
                line=re.sub(' +',',',line)
                line=line.rstrip()
                split=line.split(',')
                ti_tv=split[7]
    Stat_file.write("Ti/Tv BIALLELIC VARIANTS\t"+ti_tv+"\n")
    os.system("rm "+out_eval)

    # Extract sites without dbSNP rs
    out_without_rs=out_biallelic.replace(".vcf","")+"_without_rs.vcf"
    os.system(vcf_tools+" --vcf "+out_biallelic+" --snp . --recode --recode-INFO-all --out "+out_without_rs.replace(".vcf",""))
    os.system("mv "+out_without_rs.replace(".vcf","")+".recode.vcf "+out_without_rs)
        
    # Count biallelic site not reported in dbSNP
    os.system("sed /#/d "+out_without_rs+" > "+out_without_rs+"_no_header.vcf") 
    if os.stat(out_without_rs+"_no_header.vcf")[6]==0:
        num_biallelic_variants = 0
    else:
        with open(out_without_rs+"_no_header.vcf") as f:
            for i, l in enumerate(f):
                pass
        num_biallelic_variants = i + 1
        num_biallelic_variants = num_biallelic_variants*2
    Stat_file.write("NUM BIALLELIC VARIANTS NOT REPORTED IN DBSNP\t"+str(num_biallelic_variants)+"\n")
    os.system("rm "+out_without_rs+"_no_header.vcf")
    print num_biallelic_variants  
     
    os.system("rm "+join(out_dir,"*log"))
    os.system("rm "+join(out_dir,"*idx"))          
    Stat_file.close()
