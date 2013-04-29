from cosmos.contrib.ezflow.tool import Tool

class FastQC(Tool):
    name = "FastQC"
    mem_req = 4*1024
    cpu_req = 1 #>1 is causing error messages
    time_req = 120
    inputs = ['fastq.gz']
    outputs = ['dir']

    def cmd(self,i,s,p):
        return r"""
            perl {s[fastqc_path]}
            -t {self.cpu_req}
            -o $OUT.dir
            {inputs}
            """, {
                'inputs':' '.join(map(str,i['fastq.gz']))
            }