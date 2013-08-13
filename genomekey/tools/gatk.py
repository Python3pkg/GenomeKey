from cosmos.lib.ezflow.tool import Tool
from cosmos.Workflow.models import TaskFile
from cosmos.session import settings as cosmos_settings
import os



def _list2input(l):
    return "-I " +"\n-I ".join(map(lambda x: str(x),l))

def get_interval(param_dict):
    """
    :param param_dict: parameter dictionary
    :return: '' if param_dict does not have 'interval' in it, otherwise -L p['interval']
    """
    if 'interval' in param_dict: return '--intervals {0}'.format(param_dict['interval'])
    else:                        return ''

def get_sleep(settings_dict):
    """
    Some tools can't be submitted to short because orchestra gets mad if they finish before 10 minutes.

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
    cpu_req  = 1
    mem_req  = 5*1024
    time_req = 12*60

    @property
    def bin(self):
        return '{s[java]} -Xmx{M}m -Djava.io.tmpdir={s[tmp_dir]} -jar {s[GATK_path]}'.format(self=self, s=self.settings, M=int(self.mem_req*.9))

    def post_cmd(self,cmd_str,format_dict):
        new_cmd_str = cmd_str + ' ' + get_pedigree(format_dict['s'])
        #import ipdb; ipdb.set_trace()
        return new_cmd_str,format_dict


class BQSRGatherer(Tool):
    name="BQSR Gatherer"
    cpu_req  = 1
    mem_req  = 3*1024
    time_req = 10
    inputs   = ['bam','recal']
    outputs  = ['recal']

    persist  = True
    forward_input = True

    def cmd(self,i, s, p):
        return r"""
            "{s[java]}" -Dlog4j.configuration="file://{log4j}"
            -cp "{s[queue_path]}:{s[bqsr_gatherer_path]}"
            BQSRGathererMain
            $OUT.recal
            {input}
        """, {'input': '\n'.join(map(str,i['recal'])), 'log4j': os.path.join(s['bqsr_gatherer_path'],'log4j.properties')}

class RealignerTargetCreator(GATK):
    name    = "Realigner Target Creator"
    cpu_req = 1
    mem_req = 20*1024
    inputs  = ['bam']
    outputs = ['intervals']

    persist = True
    forward_input = True
   
    # no -nct available, -nt = 24 recommended
    # see: http://gatkforums.broadinstitute.org/discussion/1975/recommendations-for-parallelizing-gatk-tools
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            -T RealignerTargetCreator
            -R {s[reference_fasta_path]}
            -I {i[bam][0]}
            -o $OUT.intervals
            --known {s[indels_1000g_phase1_path]}
            --known {s[mills_path]}
            --num_threads 4
            {interval}
            {sleep}
        """,{'interval':get_interval(p), 'sleep': get_sleep(s)}
    
class IndelRealigner(GATK):
    name    = "Indel Realigner"
    cpu_req = 1
    mem_req = 8*1024
    inputs  = ['bam']
    outputs = ['bam']
    
    # no -nt or -nct available
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            -T IndelRealigner
            -R {s[reference_fasta_path]}
            -o $OUT.bam
            -targetIntervals /gluster/gv0/known.realign.target.{intv}.intervals
            -known {s[indels_1000g_phase1_path]}
            -known {s[mills_path]}
            -model USE_READS
            -compress 0
            --intervals {intv}
            {inputs}
            {sleep}
        """,{'intv': p['interval'], 'inputs': _list2input(i['bam']), 'sleep': get_sleep(s)}
    

class BQSR(GATK):
    name    = "Base Quality Score Recalibration"
    cpu_req = 4
    mem_req = 25*1024
    inputs  = ['bam']
    outputs = ['grp']

    persist = True
    forward_input = True

    # no -nt, -nct = 4
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            -T BaseRecalibrator
            -R {s[reference_fasta_path]}
            {inputs}
            -o $OUT.grp
            -knownSites {s[dbsnp_path]}
            -knownSites {s[omni_path]}
            -knownSites {s[indels_1000g_phase1_path]}
            -knownSites {s[mills_path]}
            --num_cpu_threads_per_data_thread {nct}
            {sleep}
        """, {'inputs' : _list2input(i['bam']), 'sleep': get_sleep(s), 'nct': self.cpu_req}
    

class ApplyBQSR(GATK):
    name    = "Apply BQSR"
    cpu_req = 2
    mem_req = 15*1024
    inputs  = ['bam','grp']
    outputs = ['bam']

    # def map_inputs(self):
    #     d= dict([ ('bam',[p.get_output('bam')]) for p in self.parent.parents ])
    #     # d['recal'] = [bqsrG_tool.get_output('recal')]
    #     return d

    added_edge = False

    # PrintReads: no -nt available, -nct = 4 recommended
    def cmd(self,i,s,p):
        #if not self.added_edge:
            #TODO fix this hack.  Also there might be duplicate edges being added on reload which doesn't matter but is ugly.
            #TODO this also forces ApplyBQSR to expect a ReduceBQSR
            #bqsrG_tool = self.dag.get_tools_by([BQSRGatherer.name],tags={'sample_name':self.tags['sample_name']})[0]
            #self.dag.G.add_edge(bqsrG_tool, self)
            #self.added_edge = True

        return r"""
            {self.bin}
            -T PrintReads
            -R {s[reference_fasta_path]}
            {inputs}
            -o $OUT.bam
            -compress 0
            -BQSR {i[grp][0]}
            --num_cpu_threads_per_data_thread {nct}
            {sleep}
        """, {'inputs' : _list2input(i['bam']), 'sleep': get_sleep(s), 'nct': self.cpu_req}

class ReduceReads(GATK):
    name     = "Reduce Reads"
    cpu_req  = 1
    mem_req  = 8*1024
    time_req = 12*60
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
           {interval}
           {inputs}           
        """, {'inputs' : _list2input(i['bam']), 'interval': get_interval(p)}

class HaplotypeCaller(GATK):
    name     = "Haplotype Caller"
    cpu_req  = 1
    mem_req  = 5.5*1024
    time_req = 12*60
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
    name     = "Unified Genotyper"
    cpu_req  = 4
    mem_req  = 25*1024
    time_req = 12*60
    inputs   = ['bam']
    outputs  = ['vcf']
    
    # -nt, -nct available
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            -T UnifiedGenotyper
            -R {s[reference_fasta_path]}
            --dbsnp {s[dbsnp_path]}
            -glm {p[glm]}
            {inputs}
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
            -baq CALCULATE_AS_NECESSARY
            -L {p[interval]}
            --num_threads 8
            --num_cpu_threads_per_data_thread {nct}
        """, {'inputs' : _list2input(i['bam']), 'nct': self.cpu_req}
    
class CombineVariants(GATK):
    name     = "Combine Variants"
    cpu_req  = 1
    mem_req  = 8*1024
    time_req = 12*60    
    inputs   = ['vcf']
    outputs  = [TaskFile(name='vcf',basename='master.vcf')]

    persist  = True
    
    default_params = {'genotypeMergeOptions':'UNSORTED'}
    
    # -nt available, -nct not available
    def cmd(self,i,s,p):
        """
        :param genotypemergeoptions: select from the following:
            UNIQUIFY       - Make all sample genotypes unique by file. Each sample shared across RODs gets named sample.ROD.
            PRIORITIZE     - Take genotypes in priority order (see the priority argument).
            UNSORTED       - Take the genotypes in any order.
            REQUIRE_UNIQUE - Require that all samples/genotypes be unique between all inputs.
        """
        return r"""
            {self.bin}
            -T CombineVariants
            -R {s[reference_fasta_path]}
            -o $OUT.vcf
            -genotypeMergeOptions {p[genotypeMergeOptions]}
            --num_threads 4
            {inputs}
        """, {'inputs' : "\n".join(["-V {0}".format(vcf) for vcf in i['vcf']])}
    
class VQSR(GATK):
    """
    VQSR

    Might want to set different values for capture vs whole genome

    Note that HaplotypeScore is no longer applicable to indels
    see http://gatkforums.broadinstitute.org/discussion/2463/unified-genotyper-no-haplotype-score-annotated-for-indels

    """
    name     = "Variant Quality Score Recalibration"
    cpu_req  = 1
    mem_req  = 8*1024
    time_req = 12*60
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
   
        if p['glm'] == 'SNP':
            return r"""
            {self.bin}
            -T VariantRecalibrator
            -R {s[reference_fasta_path]}
            -input {i[vcf][0]}
            -recalFile $OUT.recal
            -tranchesFile $OUT.tranches
            -rscriptFile $OUT.R
            --num_threads 4
            -percentBad 0.01 -minNumBad 1000	
            -resource:hapmap,known=false,training=true,truth=true,prior=15.0 {s[hapmap_path]}
            -resource:omni,known=false,training=true,truth=true,prior=12.0   {s[omni_path]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0  {s[dbsnp_path]}
            -resource:1000G,known=false,training=true,truth=false,prior=10.0 {s[1ksnp_path]}
            -an DP -an FS -an QD -an ReadPosRankSum -an MQRankSum
            -mode SNP 
            """
        else:
            return r"""
            {self.bin}
            -T VariantRecalibrator
            -R {s[reference_fasta_path]}
            -input {i[vcf][0]}
            -recalFile $OUT.recal
            -tranchesFile $OUT.tranches
            -rscriptFile $OUT.R
            --num_threads 4
            -percentBad 0.01 -minNumBad 1000
            --maxGaussians 1 
            -resource:mills,known=false,training=true,truth=true,prior=12.0 {s[mills_path]}
            -resource:dbsnp,known=true,training=false,truth=false,prior=2.0 {s[dbsnp_path]}
            -an DP -an FS -an ReadPosRankSum -an MQRankSum
            -mode INDEL
            """
    
class Apply_VQSR(GATK):
    name     = "Apply VQSR"
    cpu_req  = 1
    mem_req  = 8*1024
    time_req = 12*60
    inputs   = ['vcf','recal','tranches']
    outputs  = [TaskFile(name='vcf',persist=True)]
    
    persist  = True    
    
    # -nt available, -nct not available
    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            -T ApplyRecalibration
            -R {s[reference_fasta_path]}
            -input {i[vcf][0]}
            -tranchesFile {i[tranches][0]}
            -recalFile {i[recal][0]}
            -o $OUT.vcf
            --ts_filter_level 99.9
            -mode {p[glm]}
            --num_threads 4
            """    
