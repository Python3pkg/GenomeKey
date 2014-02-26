import os

from cosmos.config import settings as cosmos_settings


def _get_drmaa_native_specification(jobAttempt):
    task = jobAttempt.task

    # did other attempts fail?
    # is_reattempt = task.jobAttempts.count() >1
    
    cpu_req  = task.cpu_requirement
    mem_req  = task.memory_requirement
    time_req = task.time_requirement
    queue    = task.workflow.default_queue

    # GridEngine specific option
    return '-l spock_mem={mem_req}M,num_proc={cpu_req}'.format(mem_req=mem_req, cpu_req=cpu_req)
    

opj = os.path.join


ref_path = '/pseq/WGA/ref'  # In shared disk
tools_path='/tools/'        # In boot   disk

settings = {
    'date'                  : '/bin/date "+%Y-%m-%d %H:%M:%S"',  
    'java'                  : 'java -d64 -XX:ParallelGCThreads=2 -XX:+UseParallelOldGC -XX:+AggressiveOpts',
    'scratch'               : '/scratch',
    'htscmd'                : opj(tools_path, 'htscmd'),
    'bwa'                   : opj(tools_path, 'bwa'),              
    'gatk'                  : opj(tools_path, 'gatk.jar'),
    'picard_dir'            : opj(tools_path, 'picard'),  
    'samtools'              : opj(tools_path, 'samtools'),
    'fastqc'                : opj(tools_path, 'fastqc'),

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
