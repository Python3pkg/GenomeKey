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

    @property
    def bin(self):
        return '{s[java]} -Xms{min}M -Xmx{max}M -Djava.io.tmpdir={s[tmp_dir]}/{self.name} -jar {s[GATK_path]}'.format(self=self, s=self.settings, min=int(self.mem_req*.5), max=int(self.mem_req))

    def post_cmd(self,cmd_str,format_dict):
        new_cmd_str = cmd_str + ' ' + get_pedigree(format_dict['s'])
        #import ipdb; ipdb.set_trace()
        return new_cmd_str,format_dict
    
class IndelRealigner(GATK):
    name    = "IndelRealigner"
    cpu_req = 3
    mem_req = 6*1024 
    inputs  = ['bam']
    outputs = ['bam']
    
    # RealignerTargetCreator: no -nct available, -nt = 24 recommended
    # IndelRealigner: no -nt/-nct available

    # see: http://gatkforums.broadinstitute.org/discussion/1975/recommendations-for-parallelizing-gatk-tools
    # will replace ; with CR/LF at process_cmd() in cosmos/utils/helper.py

    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {self.bin}
            -T RealignerTargetCreator
            -R {s[reference_fasta_path]}
            -o $tmpDir/{p[interval]}.intervals
            --known {s[indels_1000g_phase1_path]}
            --known {s[mills_path]}
            --num_threads {self.cpu_req}
            -L {p[interval]}
            {inputs};

            ;

            {self.bin}
            -T IndelRealigner
            -R {s[reference_fasta_path]}
            -o $OUT.bam
            -targetIntervals $tmpDir/{p[interval]}.intervals
            -known {s[indels_1000g_phase1_path]}
            -known {s[mills_path]}
            -model USE_READS
            -compress 0
            -L {p[interval]}
            {inputs}
  
        """,{'inputs': _list2input(i['bam'])}


class BaseQualityScoreRecalibration(GATK):
    name    = "BQSR"
    cpu_req = 3
    mem_req = 5*1024   
    inputs  = ['bam']
    outputs = ['bam']

    # no -nt, -nct = 4
    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {self.bin}
            -T BaseRecalibrator
            -R {s[reference_fasta_path]}
            {inputs}
            -o $tmpDir/{p[interval]}.grp
            -knownSites {s[dbsnp_path]}
            -knownSites {s[omni_path]}
            -knownSites {s[indels_1000g_phase1_path]}
            -knownSites {s[mills_path]}
            -nct {self.cpu_req}
            -L {p[interval]} ;

            ;

            {self.bin}
            -T PrintReads
            -R {s[reference_fasta_path]}
            {inputs}
            -o $OUT.bam
            -compress 0
            -BQSR $tmpDir/{p[interval]}.grp
            -nct {self.cpu_req}
            -L {p[interval]}

        """, {'inputs' : _list2input(i['bam'])}

class ReduceReads(GATK):
    name     = "ReduceReads"
    cpu_req  = 2
    mem_req  = 5*1024
    inputs   = ['bam']
    outputs  = ['bam']

    # no -nt, no -nct available
    # -known should be SNPs, not indels: non SNP variants will be ignored.
    def cmd(self,i,s,p):
        return r"""
           {self.bin}
           -T ReduceReads           
           -R {s[reference_fasta_path]}
           -known {s[dbsnp_path]}
           -known {s[1ksnp_path]}
           -o $OUT.bam
           -L {p[interval]}
           {inputs}           
        """, {'inputs' : _list2input(i['bam'])}

class HaplotypeCaller(GATK):
    name     = "HaplotypeCaller"
    cpu_req  = 1
    mem_req  = 5.5*1024
    inputs   = ['bam']
    outputs  = ['vcf']


    def cmd(self,i,s,p):
        return r"""
            {self.bin}
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
        """, {
            'inputs' : _list2input(i['bam'])
        }

class UnifiedGenotyper(GATK):
    name     = "UnifiedGenotyper"
    cpu_req  = 4         
    mem_req  = 7*1024
    inputs   = ['bam']
    outputs  = ['vcf']
    
    # -nt, -nct available
    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {self.bin}
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
            {inputs}

            mv $tmpDir/out* $OUT;

        """, {'inputs' : _list2input(i['bam'])}
    
class CombineVariants(GATK):
    name     = "CombineVariants"
    cpu_req  = 32
    mem_req  = 55*1024
    inputs   = ['vcf']
    outputs  = [TaskFile(name='vcf',basename='master.vcf')]

    persist  = True
    
    default_params = {'genotypeMergeOptions':'UNSORTED'}
    
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

            {self.bin}
            -T CombineVariants
            -R {s[reference_fasta_path]}
            -o $tmpDir/out.vcf
            -genotypeMergeOptions UNSORTED
            -nt {self.cpu_req}
            {inputs}

            mv $tmpDir/out* $OUT;

        """, {'inputs' : "\n".join(["-V {0}".format(vcf) for vcf in i['vcf']])}
    

class VariantQualityScoreRecalibration(GATK):
    """
    VQSR

    Might want to set different values for capture vs whole genome

    Note that HaplotypeScore is no longer applicable to indels
    see http://gatkforums.broadinstitute.org/discussion/2463/unified-genotyper-no-haplotype-score-annotated-for-indels

    """
    name     = "VQSR"
    cpu_req  = 32
    mem_req  = 50*1024
    inputs   = ['vcf']
    outputs  = ['recal','tranches','R']

    persist  = True
    forward_input = True
    
    default_params = { 'inbreeding_coeff' : False}

    # -nt available, -nct not available
    def cmd(self,i,s,p):

        ## copied from gatk forum: http://gatkforums.broadinstitute.org/discussion/1259/what-vqsr-training-sets-arguments-should-i-use-for-my-specific-project
        ##
        ## --maxGaussians: default 10, default for INDEL 4, single sample for testing 1
        ## 
        ## removed -an QD for 'NaN LOD value assigned' error

        if p['glm'] == 'SNP':
            return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {self.bin}
            -T VariantRecalibrator
            -R {s[reference_fasta_path]}
            -input {i[vcf][0]}
            -recalFile    $tmpDir/out.recal
            -tranchesFile $tmpDir/out.tranches
            -rscriptFile  $tmpDir/out.R
            -nt {self.cpu_req}
            --numBadVariants 3000
            -resource:hapmap,known=false,training=true,truth=true,prior=15.0 {s[hapmap_path]}
            -resource:omni,known=false,training=true,truth=true,prior=12.0   {s[omni_path]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0  {s[dbsnp_path]}
            -resource:1000G,known=false,training=true,truth=false,prior=10.0 {s[1ksnp_path]}
            -an DP -an FS -an ReadPosRankSum -an MQRankSum
            -mode SNP 

            mv $tmpDir/out* $OUT;
            """
        else:
            return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {self.bin}
            -T VariantRecalibrator
            -R {s[reference_fasta_path]}
            -input {i[vcf][0]}
            -recalFile    $tmpDir/out.recal
            -tranchesFile $tmpDir/out.tranches
            -rscriptFile  $tmpDir/out.R
            -nt {self.cpu_req}
            --numBadVariants 1000
            --maxGaussians 1 
            -resource:mills,known=false,training=true,truth=true,prior=12.0 {s[mills_path]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0 {s[dbsnp_path]}
            -an DP -an FS -an ReadPosRankSum -an MQRankSum
            -mode INDEL

            mv $tmpDir/out* $OUT;
            """
    
class Apply_VQSR(GATK):
    name     = "Apply_VQSR"
    cpu_req  = 32
    mem_req  = 50*1024
    inputs   = ['vcf','recal','tranches']
    outputs  = [TaskFile(name='vcf',persist=True)]
    
    persist  = True    
    
    # -nt available, -nct not available
    # too many threads (-nt 20?) may create IO lag issues ('Failure working with the tmp directory ... Unable to create temporary file for stub')
    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {self.bin}
            -T ApplyRecalibration
            -R {s[reference_fasta_path]}
            -input {i[vcf][0]}
            -tranchesFile {i[tranches][0]}
            -recalFile {i[recal][0]}
            -o $tmpDir/out.vcf
            --ts_filter_level 99.9
            -mode {p[glm]}
            -nt {self.cpu_req}

            mv $tmpDir/out* $OUT
            """    

#######################################
### OBSOLETE
#######################################

# class BQSRGatherer(Tool):
#     name="BQSR Gatherer"
#     cpu_req  = 2
#     mem_req  = 3*1024
#     time_req = 10
#     inputs   = ['bam','recal']
#     outputs  = ['recal']
#
#     persist  = True
#     forward_input = True
#
#     def cmd(self,i, s, p):
#         return r"""
#             "{s[java]}" -Dlog4j.configuration="file://{log4j}"
#             -cp "{s[queue_path]}:{s[bqsr_gatherer_path]}"
#             BQSRGathererMain
#             $OUT.recal
#             {input}
#         """, {'input': '\n'.join(map(str,i['recal'])), 'log4j': os.path.join(s['bqsr_gatherer_path'],'log4j.properties')}


# class RealignerTargetCreator(GATK):
#     name          = "Realigner Target Creator"
#     cpu_req       = 2
#     mem_req       = 4*1024 
#     time_req      = 12*60
#     analysis_type = 'RealignerTargetCreator'
#     inputs        = ['bam']
#     outputs       = ['intervals']
#
#     persist = True
#     forward_input = True
#   
#     # no -nct available, -nt = 24 recommended
#     # see: http://gatkforums.broadinstitute.org/discussion/1975/recommendations-for-parallelizing-gatk-tools
#     def cmd(self,i,s,p):
#         return r"""
#             {s[java]} -Xms2G -Xmx3G -jar {s[GATK_path]}
#             -T {T}
#             -R {s[reference_fasta_path]}
#             {inputs}
#             -o $OUT.intervals
#             --known {s[indels_1000g_phase1_path]}
#             --known {s[mills_path]}
#             --num_threads 2
#             {interval}
#             {sleep}
#         """,{T: self.analysis_type, 'inputs': _list2input(i['bam']), 'interval': get_interval(p), 'sleep': get_sleep(s)}

    
# class IndelRealigner(GATK):
#     name    = "Indel Realigner"
#     cpu_req = 2
#     mem_req = 5*1024
#     inputs  = ['bam','intervals']
#     outputs = ['bam']
#    
#     # no -nt or -nct available
#     # if fixed target intervals: -targetIntervals {s[reference]}/known.realign.target.{intv}.intervals
#     def cmd(self,i,s,p):
#         return r"""
#             {s[java]} -Xms2G -Xmx5G -jar {s[GATK_path]}
#             -T IndelRealigner
#             -R {s[reference_fasta_path]}
#             -o $OUT.bam
#             -targetIntervals {i[intervals][0]}  
#             -known {s[indels_1000g_phase1_path]}
#             -known {s[mills_path]}
#             -model USE_READS
#             -compress 0
#             --intervals {intv}
#             {inputs}
#             {sleep}
#         """,{'intv': p['interval'], 'inputs': _list2input(i['bam']), 'sleep': get_sleep(s)}

# class BQSR(GATK):
#     name    = "Base Quality Score Recalibration"
#     cpu_req = 3
#     mem_req = 6*1024   
#     inputs  = ['bam']
#     outputs = ['grp']
#
#     persist = True
#     forward_input = True
#
#     # no -nt, -nct = 3
#     def cmd(self,i,s,p):
#         return r"""
#             {s[java]} -Xms4G -Xmx5G -jar {s[GATK_path]}
#             -T BaseRecalibrator
#             -R {s[reference_fasta_path]}
#             {inputs}
#             -o $OUT.grp
#             -knownSites {s[dbsnp_path]}
#             -knownSites {s[omni_path]}
#             -knownSites {s[indels_1000g_phase1_path]}
#             -knownSites {s[mills_path]}
#             --num_cpu_threads_per_data_thread {nct}
#             {sleep}
#         """, {'inputs' : _list2input(i['bam']), 'sleep': get_sleep(s), 'nct': self.cpu_req}
    

# class ApplyBQSR(GATK):
#     name    = "Apply BQSR"
#     cpu_req = 3
#     mem_req = 5*1024
#     inputs  = ['bam','grp']
#     outputs = ['bam']
#
#     # def map_inputs(self):
#     #     d= dict([ ('bam',[p.get_output('bam')]) for p in self.parent.parents ])
#     #     # d['recal'] = [bqsrG_tool.get_output('recal')]
#     #     return d
#
#     added_edge = False
#
#     # PrintReads: no -nt available, -nct = 4 recommended
#     def cmd(self,i,s,p):
#         #if not self.added_edge:
#             #TODO fix this hack.  Also there might be duplicate edges being added on reload which doesn't matter but is ugly.
#             #TODO this also forces ApplyBQSR to expect a ReduceBQSR
#             #bqsrG_tool = self.dag.get_tools_by([BQSRGatherer.name],tags={'sample_name':self.tags['sample_name']})[0]
#             #self.dag.G.add_edge(bqsrG_tool, self)
#             #self.added_edge = True
#
#         return r"""
#             {s[java]} -Xms4G -Xmx5G -jar {s[GATK_path]}
#             -T PrintReads
#             -R {s[reference_fasta_path]}
#             {inputs}
#             -o $OUT.bam
#             -compress 0
#             -BQSR {i[grp][0]}
#             --num_cpu_threads_per_data_thread {nct}
#             {sleep}
#         """, {'inputs' : _list2input(i['bam']), 'sleep': get_sleep(s), 'nct': self.cpu_req}
