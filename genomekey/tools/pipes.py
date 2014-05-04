from cosmos.lib.ezflow.tool import Tool, TaskFile

def _list2input(l, opt):
    return opt + ("\n"+opt).join(map(lambda x: str(x), l))

cmd_init = r"""
            set -e -o pipefail && tmpDir=$(mktemp -d --tmpdir={s[scratch]}) && export TMPDIR=$tmpDir;
            printf "%s %s at %s\n" "{s[date]}" "$(hostname)" "$tmpDir" | tee /dev/stderr;
            """
cmd_out  = r"""

            echo "{s[date]} Moving files to Storage";
            [[ -a $tmpDir/out.bam ]] && mv -f $tmpDir/out.bam $OUT.bam;
            [[ -a $tmpDir/out.bai ]] && mv -f $tmpDir/out.bai $OUT.bai;
            echo "{s[date]} Moving done";
            /bin/rm -rf $tmpDir;
            """
cmd_out_vcf = r"""

            echo "{s[date]} Moving files to Storage"; 
            [[ -a $tmpDir/out.vcf     ]] && mv -f $tmpDir/out.vcf     $OUT.vcf;
            [[ -a $tmpDir/out.vcf.idx ]] && mv -f $tmpDir/out.vcf.idx $OUT.vcf.idx;
            echo "{s[date]} Moving done";
            /bin/rm -rf $tmpDir;
            """
           
class Bam_To_BWA(Tool):
    name     = "BAM to BWA"
    cpu_req  = 4           # orchestra: 4
    mem_req  = 12*1024     # max mem used (RSS): 12.4 Gb?
    time_req = 2*60
    inputs   = ['bam']
    outputs  = ['bam', 'bai']

    def cmd(self,i,s,p):
        # removed -m MEM option in samtools sort
        cmd_main = r"""

            rg=$({s[samtools]} view -H {i[bam][0]} | grep {p[rgId]} | uniq | sed 's/\t/\\t/g') && echo "RG= $rg";

            {s[samtools]} view -hur {p[rgId]} {i[bam][0]} 11111111111 > $tmpDir/empty.ubam 2> /dev/null;
            {s[samtools]} view -hur {p[rgId]} {i[bam][0]} {p[prevSn]} > $tmpDir/tmpIn.ubam;

            sizeEmpty=$(du -b $tmpDir/empty.ubam | cut -f 1);
            sizeTmpIn=$(du -b $tmpDir/tmpIn.ubam | cut -f 1);

            [[ "$sizeTmpIn" -gt "$sizeEmpty" ]] &&
            {s[samtools]} sort -n -o -l 0 -@ {self.cpu_req} $tmpDir/tmpIn.ubam $tmpDir/_shuf |
            {s[bamUtil]} bam2FastQ --in -.ubam --readname --noeof --firstOut /dev/stdout --merge --unpairedout $tmpDir/un.fq 2> /dev/null |
            {s[bwa]} mem -p -M -t {self.cpu_req} -R "$rg" {s[reference_fasta]} - |
            {s[samtools]} view -Shu - |
            {s[samtools]} sort    -o -l 0 -@ {self.cpu_req} - $tmpDir/_sort > $tmpDir/out.bam;
            
            # put tmpIn.ubam as output if there's no out.bam available;
            [[ ! -a $tmpDir/out.bam ]] && mv $tmpDir/tmpIn.ubam $tmpDir/out.bam;

            [[   -a $tmpDir/out.bam ]] && {s[samtools]} index $tmpDir/out.bam $tmpDir/out.bai;
    
            """
        return (cmd_init + cmd_main + cmd_out)


class IndelRealigner(Tool):
    name     = "IndelRealigner"
    cpu_req  = 4       
    mem_req  = 12*1024  
    time_req = 4*60
    inputs   = ['bam']
    outputs  = ['bam','bai']

    # RealignerTargetCreator: no -nct available, -nt = 24 recommended
    # IndelRealigner: no -nt/-nct available

    # see: http://gatkforums.broadinstitute.org/discussion/1975/recommendations-for-parallelizing-gatk-tools
    # will replace ; with CR/LF at process_cmd() in cosmos/utils/helper.py

    def cmd(self,i,s,p):
        cmd_main = r"""
            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T RealignerTargetCreator
            -R {s[reference_fasta]}
            -o $tmpDir/{p[chrom]}.intervals
            --known {s[1kindel_vcf]}
            --known {s[mills_vcf]}
            --num_threads {self.cpu_req}
            -L {p[chrom]}
            {inputs};

            printf "\n%s RealignerTargetCreator ended.\n" "{s[date]}" | tee -a /dev/stderr;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T IndelRealigner
            -R {s[reference_fasta]}
            -o $tmpDir/out.bam
            -targetIntervals $tmpDir/{p[chrom]}.intervals
            -known {s[1kindel_vcf]}
            -known {s[mills_vcf]}
            -model USE_READS
            -compress 0
            -L {p[chrom]}
            {inputs};

        """
        return (cmd_init + cmd_main + cmd_out),{'inputs': _list2input(i['bam'], "-I ")}


class MarkDuplicates(Tool):
    name     = "MarkDuplicates"
    cpu_req  = 2        
    mem_req  = 4*1024   
    time_req = 2*60
    inputs   = ['bam']
    outputs  = ['bam','bai','metrics']
        
    def cmd(self,i,s,p):
        cmd_main = r"""

            {s[java]} -Xmx{self.mem_req}M -jar {s[picard_dir]}/MarkDuplicates.jar
            TMP_DIR=$tmpDir
            OUTPUT=$tmpDir/out.bam
            METRICS_FILE=$tmpDir/out.metrics
            ASSUME_SORTED=True
            CREATE_INDEX=True
            COMPRESSION_LEVEL=0
            MAX_RECORDS_IN_RAM=1000000
            VALIDATION_STRINGENCY=SILENT
            VERBOSITY=INFO
            {inputs};

            mv -f $tmpDir/out.metrics $OUT.metrics;
        """
        return (cmd_init + cmd_main + cmd_out),{'inputs': _list2input(i['bam'], " INPUT=")}


class BaseQualityScoreRecalibration(Tool):
    name     = "BQSR"
    cpu_req  = 4
    mem_req  = 12*1024
    time_req = 4*60 
    inputs   = ['bam']
    outputs  = [TaskFile(name='bam',persist=True),TaskFile(name='bai',persist=True)]

    # no -nt, -nct = 4
    def cmd(self,i,s,p):
        cmd_main = r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T BaseRecalibrator
            -R {s[reference_fasta]}
            -o $tmpDir/{p[chrom]}.grp
            -knownSites {s[dbsnp_vcf]}
            -knownSites {s[1komni_vcf]}
            -knownSites {s[1kindel_vcf]}
            -knownSites {s[mills_vcf]}
            -nct {self.cpu_req}
            -L {p[chrom]}
            {inputs};

            printf "\n%s BaseRecalibrator ended\n" "{s[date]}" | tee -a /dev/stderr;
            
            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T PrintReads
            -R {s[reference_fasta]}
            -o $tmpDir/out.bam
            -compress 0
            -BQSR $tmpDir/{p[chrom]}.grp
            -nct {self.cpu_req}
            -L {p[chrom]}
            {inputs};

            """
        return (cmd_init + cmd_main + cmd_out),{'inputs' : _list2input(i['bam'],"-I ")}

# Mean to be used per sample
class HaplotypeCaller(Tool):
    name     = "HaplotypeCaller"
    cpu_req  = 4
    mem_req  = 12*1024
    time_req = 12*60
    inputs   = ['bam']
    outputs  = [TaskFile(name='vcf',persist=True),TaskFile(name='vcf.idx',persist=True)]

    # -nct available
    def cmd(self,i,s,p):
        cmd_main = r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T HaplotypeCaller
            -R {s[reference_fasta]}
            -D {s[dbsnp_vcf]}
            -o $tmpDir/out.vcf
            -pairHMM VECTOR_LOGLESS_CACHING
            -L {p[chrom]}
            -nct {self.cpu_req}
            --emitRefConfidence GVCF --variant_index_type LINEAR --variant_index_parameter 128000
            -A DepthPerAlleleBySample
            -stand_call_conf 30
            -stand_emit_conf 10
            {inputs};

            """
        return (cmd_init + cmd_main + cmd_out_vcf), {'inputs': _list2input(i['bam'],"-I ")}


# Joint Genotyping
class GenotypeGVCFs(Tool):
    name = "GenotypeGVCFs"
    cpu_req  = 4        
    mem_req  = 12*1024  
    time_req = 12*60
    inputs   = ['vcf']
    outputs  = ['vcf','vcf.idx']

    # -nt available
    def cmd(self,i,s,p):
        cmd_main = r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T GenotypeGVCFs
            -R {s[reference_fasta]}
            -D {s[dbsnp_vcf]}
            -o $tmpDir/out.vcf
            -L {p[chrom]}
            -nt {self.cpu_req}
            -A Coverage
            -A GCContent
            -A HaplotypeScore
            -A MappingQualityRankSumTest
            -A InbreedingCoeff -A FisherStrand -A QualByDepth -A ChromosomeCounts
            {inputs};

            """
        return (cmd_init + cmd_main + cmd_out_vcf), {'inputs': _list2input(i['vcf'],"-V ")}

    
class VariantQualityScoreRecalibration(Tool):
    """
    VQSR
    Note that HaplotypeScore is no longer applicable to indels
    see http://gatkforums.broadinstitute.org/discussion/2463/unified-genotyper-no-haplotype-score-annotated-for-indels

    """
    name     = "VQSR"
    cpu_req  = 4        
    mem_req  = 12*1024  
    time_req = 12*60
    inputs   = ['vcf']
    outputs  = ['vcf','vcf.idx','R']


    # -nt available, -nct not available
    def cmd(self,i,s,p):
        """
        Check gatk forum: http://gatkforums.broadinstitute.org/discussion/1259/what-vqsr-training-sets-arguments-should-i-use-for-my-specific-project
        --maxGaussians         8 (default), set    1 for small-scale test
        --minNumBadVariants 1000 (default), set 3000 for small-scale test
        """
        cmd_VQSR = r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T VariantRecalibrator
            -R {s[reference_fasta]}
            -recalFile    $tmpDir/out.recal
            -tranchesFile $tmpDir/out.tranches
            -rscriptFile  $tmpDir/out.R
            -nt {self.cpu_req}
            -an MQRankSum -an ReadPosRankSum -an DP -an FS -an QD
            -mode {p[glm]}
            -L {p[chrom]}
            --maxGaussians 1
            --minNumBadVariants 3000
            {inputs}
            """
        cmd_SNP = r"""
            -resource:hapmap,known=false,training=true,truth=true,prior=15.0 {s[hapmap_vcf]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0  {s[dbsnp_vcf]}
            -resource:omni,known=false,training=true,truth=true,prior=12.0   {s[1komni_vcf]}
            -resource:1000G,known=false,training=true,truth=false,prior=10.0 {s[1ksnp_vcf]};
            """
        cmd_INDEL = r"""
            -resource:mills,known=false,training=true,truth=true,prior=12.0 {s[mills_vcf]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0 {s[dbsnp_vcf]};
            """
        cmd_apply_VQSR = r"""

            printf "\n%s\n" "{s[date]}" | tee -a /dev/stderr;
            
            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T ApplyRecalibration
            -R {s[reference_fasta]}
            -recalFile    $tmpDir/out.recal
            -tranchesFile $tmpDir/out.tranches
            -o            $tmpDir/out.vcf
            --ts_filter_level 99.9
            -mode {p[glm]}
            -nt {self.cpu_req}
            -L {p[chrom]}
            {inputs};

            mv -f $tmpDir/out.R $OUT.R;            

            """
        if p['glm'] == 'SNP':
            cmd_rc = cmd_SNP
        else:
            cmd_rc = cmd_INDEL

        if p['skip_VQSR']:
            return " cp {i[vcf][0]} $OUT.vcf; cp {i[vcf][0]}.idx $OUT.vcf.idx; touch $OUT.R"
        else:
            return (cmd_init + cmd_VQSR + cmd_rc + cmd_apply_VQSR + cmd_out_vcf), {'inputs' : _list2input(i['vcf'],"-input ")}



class CombineVariants(Tool):
    name     = "CombineVariants"
    cpu_req  = 4                # max CPU here
    mem_req  = 12*1024
    time_req = 2*60
    inputs   = ['vcf']
    outputs  = ['vcf','vcf.idx']

    # -nt available, -nct not available
    # Too many -nt (20?) will cause write error
    def cmd(self,i,s,p):
        """
        :param genotypemergeoptions: select from the following:
            UNIQUIFY       - Make all sample genotypes unique by file. Each sample shared across RODs gets named sample.ROD.
            PRIORITIZE     - Take genotypes in priority order (see the priority argument).
            UNSORTED       - Take the genotypes in any order.
            REQUIRE_UNIQUE - Require that all samples/genotypes be unique between all inputs.
        """
        cmd_main = r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T CombineVariants
            -R {s[reference_fasta]}
            -o $tmpDir/out.vcf
            -genotypeMergeOptions UNSORTED
            -nt {self.cpu_req}
            {inputs};

        """
        return (cmd_init + cmd_main + cmd_out_vcf),{'inputs' : _list2input(i['vcf'],"-V ")}


#######################
## Annotation
#######################

class Vcf2Anno_in(Tool):
    name = "Convert VCF to Annovar"
    inputs = ['vcf']
    outputs = ['anno_in']
    forward_input=True
    time_req = 12*60

    def cmd(self,i,s,p):
        return cmd_init + r"""

             {s[annovarext]} vcf2anno '{i[vcf][0]}' > $OUT.anno_in


              """

class Annotate(Tool):
    name = "Annotate"
    inputs = ['anno_in']
    outputs = ['dir']
    forward_input=True
    time_req = 12*60
    mem_req = 12*1024

    def cmd(self,i,s,p):
        return cmd_init + r"""

             {s[annovarext]} anno {p[build]} {p[dbname]} {i[anno_in][0]} $OUT.dir
        
              """

class MergeAnnotations(Tool):
    name = "Merge Annotations"
    inputs = ['anno_in','dir']
    outputs = ['dir']
    mem_req = 40*1024
    time_req = 12*60
    forward_input=True
    
    def cmd(self,i,s,p):
        return cmd_init + r"""
      
              {s[annovarext]} merge {i[anno_in][0]} $OUT.dir {annotated_dir_output}

              """, { 'annotated_dir_output' : ' '.join(map(str,i['dir'])) }


############################
### Depricated from GATK 3.0
############################

class ReduceReads(Tool):
    name     = "ReduceReads"
    cpu_req  = 2
    mem_req  = 4*1024
    time_req = 4*60
    inputs   = ['bam']
    outputs  = ['bam','bai']

    # no -nt, no -nct available
    # -known should be SNPs, not indels: non SNP variants will be ignored.

    def cmd(self,i,s,p):
        cmd_main = r"""

           {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
           -jar {s[gatk]}
           -T ReduceReads           
           -R {s[reference_fasta]}
           -known {s[dbsnp_vcf]}
           -known {s[1ksnp_vcf]}
           -o $tmpDir/out.bam
           -L {p[chrom]}
           {inputs};

        """
        return (cmd_init + cmd_main + cmd_out),{'inputs' : _list2input(i['bam'],"-I ")}

class UnifiedGenotyper(Tool):
    name     = "UnifiedGenotyper"
    cpu_req  = 4
    mem_req  = 8*1024    
    time_req = 12*60
    inputs   = ['bam']
    outputs  = ['vcf','vcf.idx']

    # -nt, -nct available
    def cmd(self,i,s,p):
        cmd_main = r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{self.mem_req}M
            -jar {s[gatk]}
            -T UnifiedGenotyper
            -R {s[reference_fasta]}
            --dbsnp {s[dbsnp_vcf]}
            -glm {p[glm]}
            -o $tmpDir/out.vcf
            -L {p[chrom]}
            -nt {self.cpu_req}
            -nct 2
            -A Coverage
            -A AlleleBalance
            -A AlleleBalanceBySample
            -A DepthPerAlleleBySample
            -A HaplotypeScore
            -A InbreedingCoeff
            -A QualByDepth
            -A FisherStrand
            -A MappingQualityRankSumTest
            -baq CALCULATE_AS_NECESSARY
            {inputs};
            
        """, {'inputs' : _list2input_gatk(i['bam']), 'max':int(self.mem_req)}
    
