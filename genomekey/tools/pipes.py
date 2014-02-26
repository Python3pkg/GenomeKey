from cosmos.lib.ezflow.tool import Tool

cmd_init = r"""
            echo "$({s[date]}) $(hostname)" && set -e -o pipefail && tmpDir=$(mktemp -d --tmpdir={s[scratch]}) && cd $tmpDir;
            """

class Bam_To_BWA(Tool):
    name = "BAM to BWA"
    cpu_req = 4           # default 8, but max 16 cpus for GCE
    mem_req = 8*1024     
    time_req = 2*60

    inputs  = ['bam']
    outputs = ['bam', 'bai']

    def cmd(self,i,s,p):
        return cmd_init + r"""

            rg=$({s[samtools]} view -H {i[bam][0]} | grep {p[rgId]} | uniq | sed 's/\t/\\t/g') && echo "RG= $rg";

            {s[samtools]} view -h -u -r {p[rgId]} {i[bam][0]} {p[prevSn]}         |
            {s[htscmd]} bamshuf -Oun 128 - _tmp                                   |
            {s[htscmd]} bam2fq -a  -                                              |
            {s[bwa]} mem -p -M -t {self.cpu_req} -R "$rg" {s[reference_fasta]} -  |
            {s[samtools]} view -Shu -                                             |
            {s[samtools]} sort -o -l 0 -@ {self.cpu_req} -m 1500M - _tmp > $tmpDir/out.bam;

            {s[samtools]} index $tmpDir/out.bam $tmpDir/out.bai;

            echo "$({s[date]}) Moving files to main";
            mv -f $tmpDir/out.bam $OUT.bam; mv -f $tmpDir/out.bai $OUT.bai;
            echo "$({s[date]}) Moving done"

            """

def _list2input_markdup(l):
    return " ".join(map(lambda x: 'INPUT='+str(x)+'\n', l))

class MarkDuplicates(Tool):
    name     = "MarkDuplicates"
    cpu_req  = 2        # will allow  8 jobs in a node (max=16)
    mem_req  = 5*1024   # will allow 11 jobs in a node, as mem_total = 59.3G
    time_req = 2*60
    inputs   = ['bam']
    outputs  = ['bam','bai','metrics']
    #persist  = True
        
    def cmd(self,i,s,p):
        return cmd_init + r"""

            {s[java]} -Xmx{max}M -jar {s[picard_dir]}/MarkDuplicates.jar
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

            echo "$({s[date]}) Moving files to main";
            mv -f $tmpDir/out.bam $OUT.bam; mv -f $tmpDir/out.bai $OUT.bai; mv -f $tmpDir/out.metrics $OUT.metrics;
            echo "$({s[date]}) Moving done"; /bin/rm -rf $tmpDir;

        """, {'inputs': _list2input_markdup(i['bam']), 'max':int(self.mem_req)}


def _list2input_gatk(l):
    return "-I " +"\n-I ".join(map(lambda x: str(x), l))

class IndelRealigner(Tool):
    name    = "IndelRealigner"
    cpu_req = 4       # will allow 4 realign jobs in a node
    mem_req = 7*1024  # will allow 8 realign jobs in a node
    time_req = 4*60
    inputs  = ['bam']
    outputs = ['bam','bai']

    # RealignerTargetCreator: no -nct available, -nt = 24 recommended
    # IndelRealigner: no -nt/-nct available

    # see: http://gatkforums.broadinstitute.org/discussion/1975/recommendations-for-parallelizing-gatk-tools
    # will replace ; with CR/LF at process_cmd() in cosmos/utils/helper.py

    def cmd(self,i,s,p):
        return cmd_init + r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
            -T RealignerTargetCreator
            -R {s[reference_fasta]}
            -o $tmpDir/{p[chrom]}.intervals
            --known {s[1kindel_vcf]}
            --known {s[mills_vcf]}
            --num_threads {self.cpu_req}
            -L {p[chrom]}
            {inputs};

            echo "";

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
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

            echo "$({s[date]}) Moving files to main";
            mv -f $tmpDir/out.bam $OUT.bam; mv -f $tmpDir/out.bai $OUT.bai;
            echo "$({s[date]}) Moving done."; /bin/rm -rf $tmpDir;

        """,{'inputs': _list2input_gatk(i['bam']), 'max':int(self.mem_req)}


class BaseQualityScoreRecalibration(Tool):
    name    = "BQSR"
    cpu_req = 4         # will allow 4 bqsr jobs in a node.
    mem_req = 5*1024
    time_req = 4*60 
    inputs  = ['bam']
    outputs = ['bam','bai']

    # no -nt, -nct = 4
    def cmd(self,i,s,p):
        return cmd_init + r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
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

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
            -T PrintReads
            -R {s[reference_fasta]}
            -o $tmpDir/out.bam
            -compress 0
            -BQSR $tmpDir/{p[chrom]}.grp
            -nct {self.cpu_req}
            -L {p[chrom]}
            {inputs};

            echo "$({s[date]}) Moving files to main";
            mv -f $tmpDir/out.bam $OUT.bam; mv -f $tmpDir/out.bai $OUT.bai;
            echo "$({s[date]}) Moving done"; /bin/rm -rf $tmpDir;


        """, {'inputs' : _list2input_gatk(i['bam']), 'max':int(self.mem_req)}

class ReduceReads(Tool):
    name     = "ReduceReads"
    cpu_req  = 2       # will allow  8 reducedRead jobs in a node
    mem_req  = 5*1024  # will allow 11 reducedRead jobs in a node.
    time_req = 4*60
    inputs   = ['bam']
    outputs  = ['bam','bai']

    # no -nt, no -nct available
    # -known should be SNPs, not indels: non SNP variants will be ignored.


    # do fastqc before reducing it
    # removed -known {s[1ksnp_vcf]} for now
    def cmd(self,i,s,p):
        return cmd_init + r"""

           {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
           -T ReduceReads           
           -R {s[reference_fasta]}
           -known {s[dbsnp_vcf]}
           -o $tmpDir/out.bam
           -L {p[chrom]}
           {inputs};

            echo "$({s[date]}) Moving files to main";
            mv -f $tmpDir/out.bam $OUT.bam; mv -f $tmpDir/out.bai $OUT.bai;
            echo "$({s[date]}) Moving done"; /bin/rm -rf $tmpDir;

        """, {'inputs' : _list2input_gatk(i['bam']), 'max':int(self.mem_req)}

class UnifiedGenotyper(Tool):
    name     = "UnifiedGenotyper"
    cpu_req  = 4         # will allow 4 unifiedGenotype jobs in a node
    mem_req  = 7*1024    
    time_req = 12*60
    inputs   = ['bam']
    outputs  = ['vcf','vcf.idx']

    # -nt, -nct available
    def cmd(self,i,s,p):
        return cmd_init + r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
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
            
            echo "$({s[date]}) Moving files to main"; 
            mv -f $tmpDir/out.vcf $OUT.vcf; mv -f $tmpDir/out.vcf.idx $OUT.vcf.idx;
            echo "$({s[date]}) Moving done"; /bin/rm -rf $tmpDir;

        """, {'inputs' : _list2input_gatk(i['bam']), 'max':int(self.mem_req)}
    

class VariantQualityScoreRecalibration(Tool):
    """
    VQSR
    Note that HaplotypeScore is no longer applicable to indels
    see http://gatkforums.broadinstitute.org/discussion/2463/unified-genotyper-no-haplotype-score-annotated-for-indels

    """
    name     = "VQSR"
    cpu_req  = 16          # max CPU here
    mem_req  = 50*1024
    time_req = 12*60
    inputs   = ['vcf']
    outputs  = ['vcf','vcf.idx','R']

#   persist  = True
#   forward_input = True
    
    default_params = { 'inbreeding_coeff' : False}

    # -nt available, -nct not available
    def cmd(self,i,s,p):

        ## copied from gatk forum: http://gatkforums.broadinstitute.org/discussion/1259/what-vqsr-training-sets-arguments-should-i-use-for-my-specific-project
        ##
        ## --maxGaussians: default 10, default for INDEL 4, single sample for testing 1
        ## 
        ## removed -an QD for 'NaN LOD value assigned' error

        cmd_VQSR = r"""
            set -e -o pipefail && tmpDir=$(mktemp -d --tmpdir={s[scratch]}) && cd $tmpDir;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
            -T VariantRecalibrator
            -R {s[reference_fasta]}
            -recalFile    $tmpDir/out.recal
            -tranchesFile $tmpDir/out.tranches
            -rscriptFile  $tmpDir/out.R
            -nt {self.cpu_req}
            -an DP -an FS -an ReadPosRankSum -an MQRankSum -an QD
            -mode {p[glm]}                       
            {inputs}
            """

        # temporarily removed:             -resource:1000G,known=false,training=true,truth=false,prior=10.0 {s[1ksnp_vcf]};
        cmd_SNP = r"""
            -resource:hapmap,known=false,training=true,truth=true,prior=15.0 {s[hapmap_vcf]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0  {s[dbsnp_vcf]}
            -resource:omni,known=false,training=true,truth=true,prior=12.0   {s[1komni_vcf]};
            """

        cmd_INDEL = r"""
            -resource:mills,known=false,training=true,truth=true,prior=12.0 {s[mills_vcf]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0 {s[dbsnp_vcf]};
            """

        cmd_apply_VQSR = r"""
            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
            -T ApplyRecalibration
            -R {s[reference_fasta]}
            -recalFile    $tmpDir/out.recal
            -tranchesFile $tmpDir/out.tranches
            -o            $tmpDir/out.vcf
            --ts_filter_level 99.9
            -mode {p[glm]}
            -nt {self.cpu_req}
            {inputs}

            # gluster is really slow on appending small chunks, like making an index file.;
            echo "$({s[date]}) Moving files to main";
            mv -f $tmpDir/out.vcf     $OUT.vcf; mv -f $tmpDir/out.vcf.idx $OUT.vcf.idx; mv -f $tmpDir/out.R       $OUT.R;
            echo "$({s[date]}) Moving done"; /bin/rm -rf $tmpDir;

            """

        if p['glm'] == 'SNP': 
            cmd = cmd_init + cmd_VQSR + cmd_SNP   + cmd_apply_VQSR
        else:
            cmd = cmd_init + cmd_VQSR + cmd_INDEL + cmd_apply_VQSR

        return cmd, {'inputs' : "\n".join(["-input {0}".format(vcf) for vcf in i['vcf']]), 'max':int(self.mem_req)}

class CombineVariants(Tool):
    name     = "CombineVariants"
    cpu_req  = 16                 # max CPU here
    mem_req  = 50*1024
    time_req = 2*60
    inputs   = ['vcf']
    outputs  = ['vcf','vcf.idx']
    logging_level ='INFO'

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
        return cmd_init + r"""

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
            -T CombineVariants
            -R {s[reference_fasta]}
            -o $tmpDir/out.vcf
            -genotypeMergeOptions UNSORTED
            -nt {self.cpu_req}
            {inputs};

            echo "$({s[date]}) Moving files to main";
            mv -f $tmpDir/out.vcf     $OUT.vcf;
            mv -f $tmpDir/out.vcf.idx $OUT.vcf.idx;
            /bin/rm -rf $tmpDir;
            echo "$({s[date]}) Moving done"

        """, {'inputs' : "\n".join(["-V {0}".format(vcf) for vcf in i['vcf']]), 'max':int(self.mem_req)}


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

