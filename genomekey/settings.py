import os

from cosmos.config import settings as cosmos_settings


def _get_drmaa_native_specification(jobAttempt):
    task = jobAttempt.task
    DRM  = cosmos_settings['DRM']

    # did other attempts fail?
    # is_reattempt = task.jobAttempts.count() >1
    
    cpu_req  = task.cpu_requirement
    mem_req  = task.memory_requirement
    time_req = task.time_requirement
    queue    = task.workflow.default_queue

    # orchestra-specific option
    if cosmos_settings['server_name'] == 'orchestra':
        if   time_req <= 10:        queue = 'mini'
        elif time_req <= 12 * 60:   queue = 'short'
        else:                       queue = 'long'
                            
    if DRM == 'LSF':
        s = '-R "rusage[mem={0}] span[hosts=1]" -n {1}'.format(mem_req/cpu_req, cpu_req)

        if time_req:  s += ' -W 0:{0}'.format(time_req)
        if queue:     s += '   -q {0}'.format(queue)
        return s
    elif DRM == 'GE':
        return '-l spock_mem={mem_req}M,num_proc={cpu_req}'.format(mem_req=mem_req, cpu_req=cpu_req)
    else:
        raise Exception('DRM not supported')

    
if cosmos_settings['server_name'] == 'orchestra':
    ref_path   = '/groups/cbi/WGA/reference'   # Orchestra
    tools_path = '/groups/cbi/WGA/tools'
else:
    ref_path   = '/WGA/reference'              # AWS
    tools_path = '/WGA/tools'

opj = os.path.join

settings = {
    'java'                  : opj(tools_path, 'java -d64 -XX:ParallelGCThreads=2 -XX:+UseParallelOldGC -XX:+AggressiveOpts'),

    'bwa'                   : opj(tools_path, 'bwa'),              
    'gatk'                  : opj(tools_path, 'gatk.jar'),
    'picard_dir'            : opj(tools_path, 'picard'),  
    'samtools'              : opj(tools_path, 'samtools'),

    'annovarext'            : opj(tools_path, 'annovarext'),

    'reference_fasta'       : opj(ref_path,   'human_g1k_v37.fasta'),
    'dbsnp_vcf'             : opj(ref_path,   'dbsnp_137.b37.excluding_sites_after_129.vcf'),
    'hapmap_vcf'            : opj(ref_path,   'hapmap_3.3.b37.vcf'),
    'mills_vcf'             : opj(ref_path,   'Mills_and_1000G_gold_standard.indels.b37.vcf'),
    '1ksnp_vcf'             : opj(ref_path,   '1000G_phase1.snps.high_confidence.b37.vcf'),
    '1komni_vcf'            : opj(ref_path,   '1000G_omni2.5.b37.vcf'),
    '1kindel_vcf'           : opj(ref_path,   '1000G_phase1.indels.b37.vcf'),

    'get_drmaa_native_specification'  : _get_drmaa_native_specification
}
