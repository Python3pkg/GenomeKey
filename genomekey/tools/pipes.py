from cosmos.Workflow.models import TaskFile
from . import picard, bamUtil, samtools,bwa
import os
opj = os.path.join
    

    
class FilterBamByRG_To_FastQ(samtools.FilterBamByRG,picard.REVERTSAM,bamUtil.Bam2FastQ):
    name     = "BAM to FASTQ"
    cpu_req  = 1 # if it's split by sqsn
    mem_req  = 7*1024
    time_req = 12*60
    inputs   = ['bam']
    outputs  = ['1.fastq','2.fastq']

    # def cmd(self,i,s,p):
    #     return r"""
    #         set -o pipefail &&
    #         {s[samtools_path]} view -h -u -r {p[rgid]} {i[bam][0]}
    #         |
    #         {self.bin}
    #         INPUT=/dev/stdin
    #         OUTPUT=/dev/stdout
    #         VALIDATION_STRINGENCY=SILENT
    #         MAX_RECORDS_IN_RAM=4000000
    #         COMPRESSION_LEVEL=0
    #         |
    #         {s[bamUtil_path]} bam2FastQ
    #         --in -.ubam
    #         --firstOut $OUT.1.fastq.gz
    #         --secondOut $OUT.2.fastq.gz
    #         --unpairedOut $OUT.unpaired.fastq.gz
    #     """

    # samtools option
    # -f 0x1 : the read is paired in sequencing
    # -h     : Include the header in the output
    # -u     : Output uncompressed BAM
    # -r STR : Only output reads in read group STR

    def cmd(self,i,s,p):
        return r"""
            set -o pipefail && {s[samtools_path]} view -h -u -r {p[rgid]} {i[bam][0]} {p[sqsn]}
            |
            {self.bin} INPUT=/dev/stdin OUTPUT=/dev/stdout
            VALIDATION_STRINGENCY=SILENT
            MAX_RECORDS_IN_RAM=4000000
            COMPRESSION_LEVEL=0
            |
            {s[bamUtil_path]} bam2FastQ --in -.ubam
            --firstOut    $OUT.1.fastq
            --secondOut   $OUT.2.fastq
            --unpairedOut /dev/null
        """

class AlignAndClean(bwa.MEM,picard.AddOrReplaceReadGroups,picard.CollectMultipleMetrics):
    name     = "BWA Alignment"
    cpu_req  = 1             # if it's all required
    mem_req  = 7*1024        # otherwise can't use all 8 cores
    time_req = 12*60
    inputs   = ['fastq']
    outputs  = ['bam']

    def cmd(self,i,s,p):
        # -v 3 : Show all normal messages
        # -M   : Mark shorter split hits as secondary (for Picard compatibility)
        # -t   : Number of threads [1] 

        # return r"""
        #     set -o pipefail && {s[bwa_path]} mem -v 3 -M -t {self.cpu_req}
        #     -R "@RG\tID:{p[platform_unit]}\tLB:{p[library]}\tSM:{p[sample_name]}\tPL:{p[platform]}\tPU:{p[platform_unit]}"
        #     {s[reference_fasta_path]}
        #     {i[fastq][0]}
        #     {i[fastq][1]}
        #     |
        #     {self.picard_bin} -jar {AddOrReplaceReadGroups}
        #     INPUT=/dev/stdin
        #     OUTPUT=/dev/stdout
        #     RGID={p[platform_unit]}
        #     RGLB={p[library]}
        #     RGSM={p[sample_name]}
        #     RGPL={p[platform]}
        #     RGPU={p[platform_unit]}
        #     COMPRESSION_LEVEL=0
        #     |
        #     {self.picard_bin} -jar {CleanSam}
        #     INPUT=/dev/stdin
        #     OUTPUT=/dev/stdout
        #     VALIDATION_STRINGENCY=SILENT
        #     COMPRESSION_LEVEL=0
        #     |
        #     {self.picard_bin} -jar {SortSam}
        #     INPUT=/dev/stdin
        #     OUTPUT=$OUT.bam
        #     SORT_ORDER=coordinate
        #     CREATE_INDEX=True
        #     COMPRESSION_LEVEL=0
        #     """, dict (
        #     AddOrReplaceReadGroups = opj(s['Picard_dir'],'AddOrReplaceReadGroups.jar'),
        #     CleanSam               = opj(s['Picard_dir'],'CleanSam.jar'),
        #     SortSam                = opj(s['Picard_dir'],'SortSam.jar')
        # )

        # -t: set as 8 in single cpu, otherwise 2 (for HT)
        return r"""
            set -o pipefail && {s[bwa_path]} mem -M -t 2
            -R "@RG\tID:{p[rgid]}\tLB:{p[library]}\tSM:{p[sample_name]}\tPL:{p[platform]}"
            {s[reference_fasta_path]}
            {i[fastq][0]}
            {i[fastq][1]}
            |
            {self.picard_bin} -jar {SortSam}
            INPUT=/dev/stdin
            OUTPUT=$OUT.bam
            SORT_ORDER=coordinate
            CREATE_INDEX=True
            COMPRESSION_LEVEL=0
            """, dict (SortSam = opj(s['Picard_dir'],'SortSam.jar'))


    
class Bam_To_FastQ(picard.REVERTSAM):
    name     = "BAM to FASTQ"
    cpu_req  = 2          # set as 1 (to use all 8 cores) makes intermittent failures
    mem_req  = 10*1024 
    time_req = 12*60
    inputs   = ['bam']
    outputs  = [TaskFile(name='dir',persist=True)]

    # samtools option
    # -f 0x1 : the read is paired in sequencing
    # -h     : Include the header in the output
    # -u     : Output uncompressed BAM
    # -r STR : Only output reads in read group STR
    
    # picard option
    # MAX_RECORDS_IN_RAM: default 500000 at 2GB memory.

    def cmd(self,i,s,p):
        return r"""
            set -o pipefail && {s[samtools_path]} view -h -u -r {p[rgid]} {i[bam][0]} {p[region]}
            |
            {self.bin} INPUT=/dev/stdin OUTPUT=/dev/stdout
            VALIDATION_STRINGENCY=SILENT
            MAX_RECORDS_IN_RAM=4000000 
            COMPRESSION_LEVEL=0
            |
            {s[bamUtil_path]} bam2FastQ --in -.ubam
            --firstOut    $OUT.dir/1.fastq
            --secondOut   $OUT.dir/2.fastq
            --unpairedOut /dev/null
        """
