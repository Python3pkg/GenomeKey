from cosmos.lib.ezflow.tool import Tool
from cosmos.Workflow.models import TaskFile
from cosmos.session import settings as cosmos_settings
import os



def _list2input(l):
    """
    :param l: list of input files
    :return:  added "-I" and "\n" for each input file
    """
    # sorted(l) didn't work
    return "-I " +"\n-I ".join(map(lambda x: str(x), l))

def get_interval(param_dict):
    """
    :param param_dict: parameter dictionary
    :return: '' if param_dict does not have 'interval' in it, otherwise -L p['interval']
    """
    if 'interval' in param_dict: return '--intervals {0}'.format(param_dict['interval'])
    else:                        return ''

def get_sleep(settings_dict):
    """
    Some tools can't be submitted to short queue because orchestra gets mad if they finish before 10 minutes.

    This is especially troublesome because some jobs for exome analysis take about 10 minutes.  It is a catch-22,
    if you submit to the mini queue, the jobs that take longer than 10 minutes get killed, if you submit to the short
    queue, your jobs finish too quickly and your jobs get automatically suspended!

    :param settings_dict:
    :return: a sleep command
    """
    return ' && sleep 480' if settings_dict['capture'] and cosmos_settings['server_name'] == 'orchestra' else ''

def get_pedigree(settings_dict):
    """
    :param settings_dict: parameter dictionary
    """
    ped_path = settings_dict['pedigree']
    if ped_path: return ' --pedigree {0}'.format(ped_path)
    else:        return ''


class GATK(Tool):
    cpu_req  = 2
    mem_req  = 5*1024
    time_req = 12*60

    logging_level = 'INFO' #ERROR

    @property
    def bin(self):
        return '{s[java]} -Xms{min}M -Xmx{max}M -jar {s[GATK_path]}'.format(self=self, s=self.settings, min=int(self.mem_req *.5), max=int(self.mem_req))

    def post_cmd(self,cmd_str,format_dict):
        new_cmd_str = cmd_str + ' ' + get_pedigree(format_dict['s'])
        #import ipdb; ipdb.set_trace()
        return new_cmd_str,format_dict
    
class IndelRealigner(GATK):
    name    = "IndelRealigner"
    cpu_req = 4
    mem_req = 7*1024  # will allow 8 realign jobs in a node
    inputs  = ['bam']
    outputs = ['bam','bai']
    
    # RealignerTargetCreator: no -nct available, -nt = 24 recommended
    # IndelRealigner: no -nt/-nct available

    # see: http://gatkforums.broadinstitute.org/discussion/1975/recommendations-for-parallelizing-gatk-tools
    # will replace ; with CR/LF at process_cmd() in cosmos/utils/helper.py

    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[GATK_path]}
            -T RealignerTargetCreator
            -R {s[reference_fasta_path]}
            -o $tmpDir/{p[interval]}.intervals
            --known {s[indels_1000g_phase1_path]}
            --known {s[mills_path]}
            --num_threads {self.cpu_req}
            -L {p[interval]}
            --logging_level {self.logging_level}
            {inputs};


            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[GATK_path]}
            -T IndelRealigner
            -R {s[reference_fasta_path]}
            -o $OUT.bam
            -targetIntervals $tmpDir/{p[interval]}.intervals
            -known {s[indels_1000g_phase1_path]}
            -known {s[mills_path]}
            -model USE_READS
            -compress 0
            -L {p[interval]}
            --logging_level {self.logging_level}
            {inputs};


            #mv $tmpDir/out.bam $OUT.bam;
            #mv $tmpDir/out.bai $OUT.bai;
            /bin/rm -rf $tmpDir;
        """,{'inputs': _list2input(i['bam']), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}


class BaseQualityScoreRecalibration(GATK):
    name    = "BQSR"
    cpu_req = 3         # will allow 10 bqsr jobes in a node.
    mem_req = 5*1024 
    inputs  = ['bam']
    outputs = ['bam','bai']

    # no -nt, -nct = 4
    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[GATK_path]}
            -T BaseRecalibrator
            -R {s[reference_fasta_path]}
            -o $tmpDir/{p[interval]}.grp
            -knownSites {s[dbsnp_path]}
            -knownSites {s[omni_path]}
            -knownSites {s[indels_1000g_phase1_path]}
            -knownSites {s[mills_path]}
            -nct {self.cpu_req}
            -L {p[interval]}
            --logging_level {self.logging_level}
            {inputs};

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[GATK_path]}
            -T PrintReads
            -R {s[reference_fasta_path]}
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
        """, {'inputs' : _list2input(i['bam']), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}

class ReduceReads(GATK):
    name     = "ReduceReads"
    cpu_req  = 2
    mem_req  = 5*1024  # will allow 11 reducedRead jobs in a node.
    inputs   = ['bam']
    outputs  = ['bam','bai']

    # no -nt, no -nct available
    # -known should be SNPs, not indels: non SNP variants will be ignored.
    def cmd(self,i,s,p):
        return r"""
           tmpDir=`mktemp -d --tmpdir=/mnt`;

           {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[GATK_path]}
           -T ReduceReads           
           -R {s[reference_fasta_path]}
           -known {s[dbsnp_path]}
           -known {s[1ksnp_path]}
           -o $OUT.bam
           -L {p[interval]}
           --logging_level {self.logging_level}
           {inputs};

           #mv $tmpDir/out.bam $OUT.bam;
           #mv $tmpDir/out.bai $OUT.bai;
           /bin/rm -rf $tmpDir;
        """, {'inputs' : _list2input(i['bam']), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}

class HaplotypeCaller(GATK):
    name     = "HaplotypeCaller"
    cpu_req  = 3
    mem_req  = 5*1024
    inputs   = ['bam']
    outputs  = ['vcf']


    def cmd(self,i,s,p):
        return r"""
            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{self.mem_req}M -Xmx{self.mem_req}M -jar {s[GATK_path]}
            -T HaplotypeCaller
            -R {s[reference_fasta_path]}
            --dbsnp {s[dbsnp_path]}
            {inputs}
            -minPruning 3
            -o $OUT.vcf
            -A Coverage
            -A AlleleBalance
            -A AlleleBalanceBySample
            -A DepthPerAlleleBySample
            -A HaplotypeScore
            -A InbreedingCoeff
            -A QualByDepth
            -A FisherStrand
            -A MappingQualityRankSumTest
            -L {p[interval]}
        """, {'inputs' : _list2input(i['bam'])}

class UnifiedGenotyper(GATK):
    name     = "UnifiedGenotyper"
    cpu_req  = 4         # allow 8 ug jobs in a node
    mem_req  = 7*1024
    inputs   = ['bam']
    outputs  = ['vcf','vcf.idx']
    
    # -nt, -nct available
    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[GATK_path]}
            -T UnifiedGenotyper
            -R {s[reference_fasta_path]}
            --dbsnp {s[dbsnp_path]}
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

        """, {'inputs' : _list2input(i['bam']), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}
    

class VariantQualityScoreRecalibration(GATK):
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

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[GATK_path]}
            -T VariantRecalibrator
            -R {s[reference_fasta_path]}
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
            -resource:hapmap,known=false,training=true,truth=true,prior=15.0 {s[hapmap_path]}
            -resource:omni,known=false,training=true,truth=true,prior=12.0   {s[omni_path]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0  {s[dbsnp_path]}
            -resource:1000G,known=false,training=true,truth=false,prior=10.0 {s[1ksnp_path]};

            """

        cmd_INDEL = r"""
            --numBadVariants 3000
            --maxGaussians   1
            -resource:mills,known=false,training=true,truth=true,prior=12.0 {s[mills_path]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0 {s[dbsnp_path]};

            """

        cmd_apply_VQSR = r"""
            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[GATK_path]}
            -T ApplyRecalibration
            -R {s[reference_fasta_path]}
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

class CombineVariants(GATK):
    name     = "CombineVariants"
    cpu_req  = 30
    mem_req  = 50*1024
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
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            ulimit -n 65535;
            echo "`whoami`@`hostname`: ulimit -n = `ulimit -n`";

            {s[java]} -Djava.io.tmpdir=$tmpDir -Xms{min}M -Xmx{max}M -jar {s[GATK_path]}
            -T CombineVariants
            -R {s[reference_fasta_path]}
            -o $tmpDir/out.vcf
            -genotypeMergeOptions UNSORTED
            -nt {self.cpu_req}
            --logging_level {self.logging_level}
            {inputs};

            mv $tmpDir/out.vcf     $OUT.vcf;
            mv $tmpDir/out.vcf.idx $OUT.vcf.idx;
            /bin/rm -rf $tmpDir;

        """, {'inputs' : "\n".join(["-V {0}".format(vcf) for vcf in i['vcf']]), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}
    
