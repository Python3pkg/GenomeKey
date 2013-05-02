from . import picard, bamUtil, samtools

class FilterBamByRG_PIPE_RevertSam_PIPE_Bam2FastQ(samtools.FilterBamByRG,picard.REVERTSAM,bamUtil.Bam2FastQ):
    name = "FilterBamByRG -- RevertSam --Bam2FastQ"
    inputs = ['bam']
    outputs = ['1.fastq.gz','2.fastq.gz','unpaired.fastq.gz']
    time_req = 12*60
    mem_req = 8*1024

    def cmd(self,i,s,p):
        return r"""
            set -o pipefail &&
            {s[samtools_path]} view -h -b -r {p[rgid]} {i[bam][0]}
            | {self.bin}
            INPUT=/dev/stdin
            OUTPUT=/dev/stdout
            VALIDATION_STRINGENCY=SILENT
            MAX_RECORDS_IN_RAM=4000000
            COMPRESSION_LEVEL=1
        """


            # | {s[bamUtil_path]} bam2FastQ
            # --in /dev/stdin
            # --firstOut $OUT.1.fastq.gz
            # --secondOut $OUT.2.fastq.gz
            # --unpairedOut $OUT.unpaired.fastq.gz