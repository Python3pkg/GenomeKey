import os
from cosmos import session
from cosmos.config import settings

opj = os.path.join

if settings['server_name'] == 'orchestra2':
    WGA_path = '/groups/lpm/erik/WGA'
    resource_bundle_path = opj(WGA_path, 'bundle/2.2/b37/')
    tools_dir = opj(WGA_path, 'tools')
elif settings['server_name'] == 'orchestra':
    WGA_path = '/groups/cbi/WGA'
    resource_bundle_path = opj(WGA_path, 'bundle/2.2/b37/')
    tools_dir = opj(WGA_path, 'tools')

wga_settings = {
    'tmp_dir': settings['working_directory'],
    'GATK_path': opj(tools_dir, 'GenomeAnalysisTK-2.4-9-g532efad/GenomeAnalysisTK.jar'),
    'queue_path': opj(tools_dir,'Queue-2.4-9-g532efad/Queue.jar'), #necessary for BQSRGatherer.java
    'Picard_dir': opj(tools_dir, 'picard-tools-1.78'),
    'bwa_path': opj(tools_dir, 'bwa-0.7.4/bwa'),
    'fastqc_path': opj(tools_dir, 'FastQC-0.10.1/fastqc'),
    'bqsr_gatherer_path': opj(tools_dir,'BQSRGathererMain'),
    'bwa_reference_fasta_path': opj(WGA_path, 'bwa_reference/human_g1k_v37.fasta'),
    'samtools_path': opj(tools_dir, 'samtools-0.1.18/samtools'),
    'get_drmaa_native_specification': session.default_get_drmaa_native_specification,

    'resource_bundle_path': resource_bundle_path,
    'reference_fasta_path': opj(resource_bundle_path, 'human_g1k_v37.fasta'),
    'dbsnp_path': opj(resource_bundle_path, 'dbsnp_137.b37.vcf'),
    'hapmap_path': opj(resource_bundle_path, 'hapmap_3.3.b37.vcf'),
    'omni_path': opj(resource_bundle_path, '1000G_omni2.5.b37.vcf'),
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