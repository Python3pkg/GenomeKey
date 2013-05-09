from cosmos.contrib.ezflow.tool import Tool

class FastQC(Tool):
    name = "FastQC"
    mem_req = 4*1024
    cpu_req = 1 #>1 is causing error messages
    time_req = 60*12
    inputs = ['fastq.gz']
    outputs = ['dir']

    def cmd(self,i,s,p):
        return r"""
            zcat {inputs} |
            perl {s[fastqc_path]}
            -t {self.cpu_req}
            -o $OUT.dir
            /dev/stdin
            """, {
                'inputs':' '.join(map(str,i['fastq.gz']))
            }

class FastqStats(Tool):
    name = "FastqStats"
    mem_req = 1*1024
    cpu_req = 1 #>1 is causing error messages
    time_req = 60*12
    inputs = ['fastq.gz']
    outputs = ['qstats','stats']

    def cmd(self,i,s,p):
        return r"""
            zcat {inputs}
            |
            {s[fastqstats_path]} -D -b $OUT.qstats > $OUT.stats
            """, {
                'inputs':' '.join(map(str,i['fastq.gz']))
            }
