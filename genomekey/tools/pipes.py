from cosmos.lib.ezflow.tool import Tool


class Bam_To_BWA(Tool):
    name = "BAM to BWA"
    cpu_req = 8
    mem_req = 14*1024
    time_req = 12*60

    inputs  = ['bam']
    outputs = ['bam', 'bai']

    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;
            cd $tmpDir;

            rg=`{s[samtools]} view -H {i[bam][0]} | grep "{p[rgid]}"`;
            echo "RG = $rg";

            set -o pipefail && 
            LD_LIBRARY=/usr/local/lib64 LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes HUGETLB_ELFMAP=RW
            {s[samtools]} view -h -u -r {p[rgid]} {i[bam][0]} {p[sn]}
            |
            LD_LIBRARY=/usr/local/lib64 LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes HUGETLB_ELFMAP=RW
            /WGA/tools/htscmd.huge bamshuf -Oun 128 - _tmp
            |
            LD_LIBRARY=/usr/local/lib64 LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes HUGETLB_ELFMAP=RW
            /WGA/tools/htscmd.huge bam2fq -a -
            |
            LD_LIBRARY=/usr/local/lib64 LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes HUGETLB_ELFMAP=RW
            {s[bwa]} mem -p -M -t {self.cpu_req} -v 1
            -R "$rg"
            {s[reference_fasta]}
            - 
            |
            LD_LIBRARY=/usr/local/lib64 LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes HUGETLB_ELFMAP=RW
            {s[samtools]} view -Shu -
            |
            LD_LIBRARY=/usr/local/lib64 LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes HUGETLB_ELFMAP=RW
            {s[samtools]} sort -o -l 0 -@ {self.cpu_req} -m 1500M - _tmp > $tmpDir/out.bam;

            LD_LIBRARY=/usr/local/lib64 LD_PRELOAD=libhugetlbfs.so HUGETLB_MORECORE=yes HUGETLB_ELFMAP=RW
            {s[samtools]} index $tmpDir/out.bam $tmpDir/out.bai;
            
       
            mv $tmpDir/out.bam $OUT.bam;
            mv $tmpDir/out.bai $OUT.bai;
            #/bin/rm -rf $tmpDir;
            """

def _list2input_markdup(l):
    return " ".join(map(lambda x: 'INPUT='+str(x)+'\n', l))

class MarkDuplicates(Tool):
    name     = "MarkDuplicates"
    cpu_req  = 2
    mem_req  = 5*1024   # will allow 11 jobs in a node, as mem_total = 59.3G
    time_req = 12*60
    inputs   = ['bam']
    outputs  = ['bam','bai','metrics']
    #persist  = True
        
    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Xms{min}M -Xmx{max}M -jar {s[picard_dir]}/MarkDuplicates.jar
            TMP_DIR=$tmpDir
            OUTPUT=$OUT.bam
            METRICS_FILE=$OUT.metrics
            ASSUME_SORTED=True
            CREATE_INDEX=True
            COMPRESSION_LEVEL=0
            MAX_RECORDS_IN_RAM=1000000
            VALIDATION_STRINGENCY=SILENT
            VERBOSITY=WARNING
            QUIET=TRUE
            {inputs};

            #mv $tmpDir/out.baa     $OUT.bam;
            #mv $tmpDir/out.bai     $OUT.bai;
            #mv $tmpDir/out.metrics $OUT.metrics;
            /bin/rm -rf $tmpDir;
        """, {'inputs': _list2input_markdup(i['bam']), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}


def _list2input_gatk(l):
    return "-I " +"\n-I ".join(map(lambda x: str(x), l))

class IndelRealigner(Tool):
    name    = "IndelRealigner"
    cpu_req = 4
    mem_req = 7*1024  # will allow 8 realign jobs in a node
    inputs  = ['bam']
    outputs = ['bam','bai']
    logging_level ='INFO'

    # RealignerTargetCreator: no -nct available, -nt = 24 recommended
    # IndelRealigner: no -nt/-nct available

    # see: http://gatkforums.broadinstitute.org/discussion/1975/recommendations-for-parallelizing-gatk-tools
    # will replace ; with CR/LF at process_cmd() in cosmos/utils/helper.py

    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[gatk]}
            -T RealignerTargetCreator
            -R {s[reference_fasta]}
            -o $tmpDir/{p[interval]}.intervals
            --known {s[1kindel_vcf]}
            --known {s[mills_vcf]}
            --num_threads {self.cpu_req}
            -L {p[interval]}
            --logging_level {self.logging_level}
            {inputs};


            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[gatk]}
            -T IndelRealigner
            -R {s[reference_fasta]}
            -o $OUT.bam
            -targetIntervals $tmpDir/{p[interval]}.intervals
            -known {s[1kindel_vcf]}
            -known {s[mills_vcf]}
            -model USE_READS
            -compress 0
            -L {p[interval]}
            --logging_level {self.logging_level}
            {inputs};


            #mv $tmpDir/out.bam $OUT.bam;
            #mv $tmpDir/out.bai $OUT.bai;
            /bin/rm -rf $tmpDir;
        """,{'inputs': _list2input_gatk(i['bam']), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}


class BaseQualityScoreRecalibration(Tool):
    name    = "BQSR"
    cpu_req = 3         # will allow 10 bqsr jobes in a node.
    mem_req = 5*1024 
    inputs  = ['bam']
    outputs = ['bam','bai']
    logging_level ='INFO'

    # no -nt, -nct = 4
    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[gatk]}
            -T BaseRecalibrator
            -R {s[reference_fasta]}
            -o $tmpDir/{p[interval]}.grp
            -knownSites {s[dbsnp_vcf]}
            -knownSites {s[1komni_vcf]}
            -knownSites {s[1kindel_vcf]}
            -knownSites {s[mills_vcf]}
            -nct {self.cpu_req}
            -L {p[interval]}
            --logging_level {self.logging_level}
            {inputs};

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[gatk]}
            -T PrintReads
            -R {s[reference_fasta]}
            -o $OUT.bam
            -compress 0
            -BQSR $tmpDir/{p[interval]}.grp
            -nct {self.cpu_req}
            -L {p[interval]}
            --logging_level {self.logging_level}
            {inputs};

            #mv $tmpDir/out.bam $OUT.bam;
            #mv $tmpDir/out.bai $OUT.bai;
            /bin/rm -rf $tmpDir;
        """, {'inputs' : _list2input_gatk(i['bam']), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}

class ReduceReads(Tool):
    name     = "ReduceReads"
    cpu_req  = 2
    mem_req  = 5*1024  # will allow 11 reducedRead jobs in a node.
    inputs   = ['bam']
    outputs  = ['bam','bai']
    logging_level ='INFO'

    # no -nt, no -nct available
    # -known should be SNPs, not indels: non SNP variants will be ignored.
    def cmd(self,i,s,p):
        return r"""
           tmpDir=`mktemp -d --tmpdir=/mnt`;

           {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[gatk]}
           -T ReduceReads           
           -R {s[reference_fasta]}
           -known {s[dbsnp_vcf]}
           -known {s[1ksnp_vcf]}
           -o $OUT.bam
           -L {p[interval]}
           --logging_level {self.logging_level}
           {inputs};

           #mv $tmpDir/out.bam $OUT.bam;
           #mv $tmpDir/out.bai $OUT.bai;
           /bin/rm -rf $tmpDir;
        """, {'inputs' : _list2input_gatk(i['bam']), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}

class UnifiedGenotyper(Tool):
    name     = "UnifiedGenotyper"
    cpu_req  = 4         # allow 8 ug jobs in a node
    mem_req  = 7*1024
    inputs   = ['bam']
    outputs  = ['vcf','vcf.idx']
    logging_level ='INFO'
    
    # -nt, -nct available
    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[gatk]}
            -T UnifiedGenotyper
            -R {s[reference_fasta]}
            --dbsnp {s[dbsnp_vcf]}
            -glm {p[glm]}
            -o $tmpDir/out.vcf
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
            -L {p[interval]}
            -nt {self.cpu_req}
            -nct 2
            --logging_level {self.logging_level}
            {inputs};
            
            mv $tmpDir/out.vcf      $OUT.vcf;
            mv $tmpDir/out.vcf.idx  $OUT.vcf.idx;
            /bin/rm -rf $tmpDir;

        """, {'inputs' : _list2input_gatk(i['bam']), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}
    

class VariantQualityScoreRecalibration(Tool):
    """
    VQSR

    Might want to set different values for capture vs whole genome

    Note that HaplotypeScore is no longer applicable to indels
    see http://gatkforums.broadinstitute.org/discussion/2463/unified-genotyper-no-haplotype-score-annotated-for-indels

    """
    name     = "VQSR"
    cpu_req  = 30
    mem_req  = 50*1024
    inputs   = ['vcf']
    outputs  = ['vcf','vcf.idx','R']
    logging_level ='INFO'

#    persist  = True
#    forward_input = True
    
    default_params = { 'inbreeding_coeff' : False}

    # -nt available, -nct not available
    def cmd(self,i,s,p):

        ## copied from gatk forum: http://gatkforums.broadinstitute.org/discussion/1259/what-vqsr-training-sets-arguments-should-i-use-for-my-specific-project
        ##
        ## --maxGaussians: default 10, default for INDEL 4, single sample for testing 1
        ## 
        ## removed -an QD for 'NaN LOD value assigned' error

        cmd_VQSR = r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[gatk]}
            -T VariantRecalibrator
            -R {s[reference_fasta]}
            -recalFile    $tmpDir/out.recal
            -tranchesFile $tmpDir/out.tranches
            -rscriptFile  $tmpDir/out.R
            -nt {self.cpu_req}
            -an DP -an FS -an ReadPosRankSum -an MQRankSum
            -mode {p[glm]}                       
            {inputs}
            --logging_level {self.logging_level}
            """
        cmd_SNP = r"""
            --numBadVariants 3000
            -resource:hapmap,known=false,training=true,truth=true,prior=15.0 {s[hapmap_vcf]}
            -resource:omni,known=false,training=true,truth=true,prior=12.0   {s[1komni_vcf]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0  {s[dbsnp_vcf]}
            -resource:1000G,known=false,training=true,truth=false,prior=10.0 {s[1ksnp_vcf]};

            """

        cmd_INDEL = r"""
            --numBadVariants 3000
            --maxGaussians   1
            -resource:mills,known=false,training=true,truth=true,prior=12.0 {s[mills_vcf]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0 {s[dbsnp_vcf]};

            """

        cmd_apply_VQSR = r"""
            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[gatk]}
            -T ApplyRecalibration
            -R {s[reference_fasta]}
            -recalFile    $tmpDir/out.recal
            -tranchesFile $tmpDir/out.tranches
            -o            $tmpDir/out.vcf
            --ts_filter_level 99.9
            -mode {p[glm]}
            -nt {self.cpu_req}
            {inputs}
            --logging_level {self.logging_level};

            # gluster is really slow on appending small chunks, like making an index file.;
            mv $tmpDir/out.vcf     $OUT.vcf;
            mv $tmpDir/out.vcf.idx $OUT.vcf.idx;
            mv $tmpDir/out.R       $OUT.R;

            #/bin/rm -rf $tmpDir;
            """

        if p['glm'] == 'SNP': 
            cmd = cmd_VQSR + cmd_SNP   + cmd_apply_VQSR
        else:
            cmd = cmd_VQSR + cmd_INDEL + cmd_apply_VQSR

        return cmd, {'inputs' : "\n".join(["-input {0}".format(vcf) for vcf in i['vcf']]),'min':int(self.mem_req *.5), 'max':int(self.mem_req)}

class CombineVariants(Tool):
    name     = "CombineVariants"
    cpu_req  = 30
    mem_req  = 50*1024
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
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            ulimit -n 65535;
            echo "`whoami`@`hostname`: ulimit -n = `ulimit -n`";

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[gatk]}
            -T CombineVariants
            -R {s[reference_fasta]}
            -o $tmpDir/out.vcf
            -genotypeMergeOptions UNSORTED
            -nt {self.cpu_req}
            --logging_level {self.logging_level}
            {inputs};

            mv $tmpDir/out.vcf     $OUT.vcf;
            mv $tmpDir/out.vcf.idx $OUT.vcf.idx;
            /bin/rm -rf $tmpDir;

        """, {'inputs' : "\n".join(["-V {0}".format(vcf) for vcf in i['vcf']]), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}
