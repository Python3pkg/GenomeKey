from cosmos.contrib.ezflow.tool import Tool
from .picard import REVERTSAM

class Bam2FastQ(Tool):
    name = "BamUtil bam2FastQ"
    inputs = ['bam']
    outputs = ['1.fastq.gz','2.fastq.gz','unpaired.fastq.gz']
    time_req = 12*60
    persist=True
    mem_req = 5*1024

    def cmd(self,i,s,p):
        return r"""
            {s[bamUtil_path]} bam2FastQ
            --in {i[bam][0]}
            --firstOut $OUT.1.fastq.gz
            --secondOut $OUT.2.fastq.gz
            --unpairedOut $OUT.unpaired.fastq.gz
        """

