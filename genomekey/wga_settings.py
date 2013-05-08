import os
from cosmos import session
from cosmos.config import settings
import sys
opj = os.path.join

if settings['server_name'] == 'starcluster':
    WGA_path = '/gluster/gv0/WGA'
else:
    WGA_path = '/scratch/esg21/WGA'


resource_bundle_path = opj(WGA_path, 'bundle/2.3/b37/')
tools_dir = opj(WGA_path, 'tools')

os.environ['ANNOVAREXT_DATA']=opj(WGA_path,'annovarext_data')
av_path = opj(tools_dir,'AnnovarExtensions/')
sys.path.append(av_path)
os.environ['PYTHONPATH'] = av_path+':'+os.environ.get('PYTHONPATH','')
extern_tools_dir = opj(tools_dir,'extern/')

wga_settings = {
    'tmp_dir': settings['working_directory'],
    'GATK_path': opj(extern_tools_dir, 'GenomeAnalysisTK-2.5-2-gf57256b/GenomeAnalysisTK.jar'),
    'queue_path': opj(extern_tools_dir,'Queue-2.4-9-g532efad/Queue.jar'), #necessary for BQSRGatherer.java
    'Picard_dir': opj(extern_tools_dir, 'picard-tools-1.78'),
    'bwa_path': opj(extern_tools_dir, 'bwa-0.7.4/bwa'),
    'fastqc_path': opj(extern_tools_dir, 'FastQC-0.10.1/fastqc'),
    'bamUtil_path': opj(extern_tools_dir, 'bamUtil/bin/bam'),
    'bqsr_gatherer_path': opj(tools_dir,'BQSRGathererMain'),
    'bwa_reference_fasta_path': opj(WGA_path, 'bwa_reference/human_g1k_v37.fasta'),
    'samtools_path': opj(extern_tools_dir, 'samtools-0.1.18/samtools'),
    'annovarext_path':opj(tools_dir,'AnnovarExtensions/bin/annovarext'),
    'fastqstats_path':opj(extern_tools_dir,'ea-utils.1.1.2-537/fastq-stats'),
    'get_drmaa_native_specification': session.default_get_drmaa_native_specification,

    'resource_bundle_path': resource_bundle_path,
    'reference_fasta_path': opj(resource_bundle_path, 'human_g1k_v37.fasta'),
    'dbsnp_path': opj(resource_bundle_path, 'dbsnp_137.b37.vcf'),
    'hapmap_path': opj(resource_bundle_path, 'hapmap_3.3.b37.vcf'),
    'omni_path': opj(resource_bundle_path, '1000G_omni2.5.b37.vcf'),
    '1000G_phase1_highconfidence_path':opj(resource_bundle_path,'1000G_phase1.snps.high_confidence.vcf'),
    'mills_path': opj(resource_bundle_path, 'Mills_and_1000G_gold_standard.indels.b37.vcf'),
    'indels_1000g_phase1_path': opj(resource_bundle_path, '1000G_phase1.indels.b37.vcf'),
    'genomekey_library_path': os.path.dirname(os.path.realpath(__file__))
}

if settings['server_name'] in ['orchestra', 'orchestra2']:
    def get_drmaa_native_specification(jobAttempt):
        task = jobAttempt.task
        DRM = settings['DRM']

        cpu_req = task.cpu_requirement
        mem_req = task.memory_requirement
        time_req = task.time_requirement
        queue = task.workflow.default_queue

        if wga_settings['test'] == True:
            if jobAttempt.task.stage.name != 'Unified_Genotyper':
                time_req=10
                cpu_req=1

        if time_req <= 10:
            queue = 'mini'
        elif time_req <= 12 * 60:
            queue = 'short'
        else:
            queue = 'long'


        if DRM == 'LSF':
            s = '-R "rusage[mem={0}] span[hosts=1]" -n {1}'.format(mem_req/cpu_req, cpu_req)
            if time_req:
                s += ' -W 0:{0}'.format(time_req)
            if queue:
                s += ' -q {0}'.format(queue)
            return s
        else:
            raise Exception('DRM not supported')

    wga_settings['get_drmaa_native_specification'] = get_drmaa_native_specification