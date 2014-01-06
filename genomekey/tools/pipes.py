from cosmos.lib.ezflow.tool import Tool


class Bam_To_BWA(Tool):
    name = "BAM to BWA"
    cpu_req = 8
    mem_req = 14*1024
    time_req = 2*60

    inputs  = ['bam']
    outputs = ['bam', 'bai']

    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;
            cd $tmpDir;

            rg=`{s[samtools]} view -H {i[bam][0]} | grep "{p[rgId]}"`;
            echo "RG = $rg";

            set -o pipefail && 
            {s[samtools]} view -h -u -r {p[rgId]} {i[bam][0]} {p[prevSn]}
            |
            {s[htscmd]} bamshuf -Oun 128 - _tmp
            |
            {s[htscmd]} bam2fq -a -
            |
            {s[bwa]} mem -p -M -t {self.cpu_req} -v 1
            -R "$rg"
            {s[reference_fasta]}
            - 
            |
            {s[samtools]} view -Shu -
            |
            {s[samtools]} sort -o -l 0 -@ {self.cpu_req} -m 1500M - _tmp > $tmpDir/out.bam;

            {s[samtools]} index $tmpDir/out.bam $tmpDir/out.bai;

            mv -f $tmpDir/out.bam $OUT.bam;
            mv -f $tmpDir/out.bai $OUT.bai;
            /bin/rm -rf $tmpDir;
            """

def _list2input_markdup(l):
    return " ".join(map(lambda x: 'INPUT='+str(x)+'\n', l))

class MarkDuplicates(Tool):
    name     = "MarkDuplicates"
    cpu_req  = 2
    mem_req  = 5*1024   # will allow 11 jobs in a node, as mem_total = 59.3G
    time_req = 2*60
    inputs   = ['bam']
    outputs  = ['bam','metrics']
    #persist  = True
        
    def cmd(self,i,s,p):
        return r"""
            export LD_PRELOAD=/usr/local/lib64/libhugetlbfs.so;
            export HUGETLB_SHM=yes;
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Xmx{max}M -jar {s[picard_dir]}/MarkDuplicates.jar
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

            /bin/rm -rf $tmpDir;
        """, {'inputs': _list2input_markdup(i['bam']), 'max':int(self.mem_req)}


def _list2input_gatk(l):
    return "-I " +"\n-I ".join(map(lambda x: str(x), l))

class IndelRealigner(Tool):
    name    = "IndelRealigner"
    cpu_req = 4
    mem_req = 7*1024  # will allow 8 realign jobs in a node
    time_req = 4*60
    inputs  = ['bam']
    outputs = ['bam']

    # RealignerTargetCreator: no -nct available, -nt = 24 recommended
    # IndelRealigner: no -nt/-nct available

    # see: http://gatkforums.broadinstitute.org/discussion/1975/recommendations-for-parallelizing-gatk-tools
    # will replace ; with CR/LF at process_cmd() in cosmos/utils/helper.py

    def cmd(self,i,s,p):
        return r"""
            export LD_PRELOAD=/usr/local/lib64/libhugetlbfs.so;
            export HUGETLB_SHM=yes;
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
            -T RealignerTargetCreator
            -R {s[reference_fasta]}
            -o $tmpDir/{p[chrom]}.intervals
            --known {s[1kindel_vcf]}
            --known {s[mills_vcf]}
            --num_threads {self.cpu_req}
            -L {p[chrom]}
            {inputs};

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
            -T IndelRealigner
            -R {s[reference_fasta]}
            -o $OUT.bam
            -targetIntervals $tmpDir/{p[chrom]}.intervals
            -known {s[1kindel_vcf]}
            -known {s[mills_vcf]}
            -model USE_READS
            -compress 0
            -L {p[chrom]}
            {inputs};

            /bin/rm -rf $tmpDir;

        """,{'inputs': _list2input_gatk(i['bam']), 'max':int(self.mem_req)}


class BaseQualityScoreRecalibration(Tool):
    name    = "BQSR"
    cpu_req = 3         # will allow 10 bqsr jobes in a node.
    mem_req = 5*1024
    time_req = 4*60 
    inputs  = ['bam']
    outputs = ['bam']

    # no -nt, -nct = 4
    def cmd(self,i,s,p):
        return r"""
            export LD_PRELOAD=/usr/local/lib64/libhugetlbfs.so;
            export HUGETLB_SHM=yes;
            tmpDir=`mktemp -d --tmpdir=/mnt`;

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
            -o $OUT.bam
            -compress 0
            -BQSR $tmpDir/{p[chrom]}.grp
            -nct {self.cpu_req}
            -L {p[chrom]}
            {inputs};

            /bin/rm -rf $tmpDir;

        """, {'inputs' : _list2input_gatk(i['bam']), 'max':int(self.mem_req)}

class ReduceReads(Tool):
    name     = "ReduceReads"
    cpu_req  = 2
    mem_req  = 5*1024  # will allow 11 reducedRead jobs in a node.
    time_req = 4*60
    inputs   = ['bam']
    outputs  = ['bam','zip']

    # no -nt, no -nct available
    # -known should be SNPs, not indels: non SNP variants will be ignored.

    # do fastqc before reducing it
    def cmd(self,i,s,p):
        return r"""
           export LD_PRELOAD=/usr/local/lib64/libhugetlbfs.so;
           export HUGETLB_SHM=yes;
           tmpDir=`mktemp -d --tmpdir=/mnt`;

           {s[fastqc]} -t {self.cpu_req} --noextract {i['bam']} --outdir $tmpDir

           {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
           -T ReduceReads           
           -R {s[reference_fasta]}
           -known {s[dbsnp_vcf]}
           -known {s[1ksnp_vcf]}
           -o $OUT.bam
           -L {p[chrom]}
           {inputs};

           mv $tmpDir/*.zip $OUT.zip
           /bin/rm -rf $tmpDir;
        """, {'inputs' : _list2input_gatk(i['bam']), 'max':int(self.mem_req)}

class UnifiedGenotyper(Tool):
    name     = "UnifiedGenotyper"
    cpu_req  = 4         # allow 8 ug jobs in a node
    mem_req  = 7*1024
    time_req = 12*60
    inputs   = ['bam']
    outputs  = ['vcf','vcf.idx']

    # -nt, -nct available
    def cmd(self,i,s,p):
        return r"""
            export LD_PRELOAD=/usr/local/lib64/libhugetlbfs.so;
            export HUGETLB_SHM=yes;
            tmpDir=`mktemp -d --tmpdir=/mnt`;

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
            
            mv -f $tmpDir/out.vcf     $OUT.vcf;
            mv -f $tmpDir/out.vcf.idx $OUT.vcf.idx;
            /bin/rm -rf $tmpDir;

        """, {'inputs' : _list2input_gatk(i['bam']), 'max':int(self.mem_req)}
    

class VariantQualityScoreRecalibration(Tool):
    """
    VQSR
    Note that HaplotypeScore is no longer applicable to indels
    see http://gatkforums.broadinstitute.org/discussion/2463/unified-genotyper-no-haplotype-score-annotated-for-indels

    """
    name     = "VQSR"
    cpu_req  = 30
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
            export LD_PRELOAD=/usr/local/lib64/libhugetlbfs.so;
            export HUGETLB_SHM=yes;
            tmpDir=`mktemp -d --tmpdir=/mnt`;

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

        cmd_SNP = r"""
            --numBadVariants 1000
            -resource:hapmap,known=false,training=true,truth=true,prior=15.0 {s[hapmap_vcf]}
            -resource:omni,known=false,training=true,truth=true,prior=12.0   {s[1komni_vcf]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0  {s[dbsnp_vcf]}
            -resource:1000G,known=false,training=true,truth=false,prior=10.0 {s[1ksnp_vcf]};

            """

        cmd_INDEL = r"""
            --numBadVariants 1000
            -resource:mills,known=false,training=true,truth=true,prior=12.0 {s[mills_vcf]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0 {s[dbsnp_vcf]};

            """

        cmd_apply_VQSR = r"""
            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
            -T ApplyRecalibration
            -R {s[reference_fasta]}
            -recalFile    $OUT.recal
            -tranchesFile $OUT.tranches
            -o            $OUT.vcf
            --ts_filter_level 99.9
            -mode {p[glm]}
            -nt {self.cpu_req}
            {inputs}

            # gluster is really slow on appending small chunks, like making an index file.;
            mv -f $tmpDir/out.vcf     $OUT.vcf;
            mv -f $tmpDir/out.vcf.idx $OUT.vcf.idx;
            mv -f $tmpDir/out.R       $OUT.R;

            /bin/rm -rf $tmpDir;
            """

        if p['glm'] == 'SNP': 
            cmd = cmd_VQSR + cmd_SNP   + cmd_apply_VQSR
        else:
            cmd = cmd_VQSR + cmd_INDEL + cmd_apply_VQSR

        return cmd, {'inputs' : "\n".join(["-input {0}".format(vcf) for vcf in i['vcf']]), 'max':int(self.mem_req)}

class CombineVariants(Tool):
    name     = "CombineVariants"
    cpu_req  = 30
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
        return r"""
            export LD_PRELOAD=/usr/local/lib64/libhugetlbfs.so;
            export HUGETLB_SHM=yes;
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xmx{max}M -jar {s[gatk]}
            -T CombineVariants
            -R {s[reference_fasta]}
            -o $tmpDir/out.vcf
            -genotypeMergeOptions UNSORTED
            -nt {self.cpu_req}
            {inputs};

            mv -f $tmpDir/out.vcf     $OUT.vcf;
            mv -f $tmpDir/out.vcf.idx $OUT.vcf.idx;
            /bin/rm -rf $tmpDir;

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
        return "{s[annovarext]} vcf2anno '{i[vcf][0]}' > $OUT.anno_in"

class Annotate(Tool):
    name = "Annotate"
    inputs = ['anno_in']
    outputs = ['dir']
    forward_input=True
    time_req = 12*60
    mem_req = 12*1024

    def cmd(self,i,s,p):
        return r"""
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
        return ('{s[annovarext]} merge {i[anno_in][0]} $OUT.dir {annotated_dir_output}',
                { 'annotated_dir_output' : ' '.join(map(str,i['dir'])) }
        )

