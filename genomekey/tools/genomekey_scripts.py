from cosmos.Workflow.models import TaskFile
from cosmos.lib.ezflow.tool import Tool

class SplitFastq(Tool):
    name = 'Split FASTQ'
    #inputs = ['1.fastq.gz','2.fastq.gz']
    inputs = ['1.fastq','2.fastq']
    outputs = [TaskFile(name='dir',persist=True)]
    time_req = 12*60
    mem_req = 1000
    persist=True

    def cmd(self,i,s,p):
#        input = i['1.fastq.gz'][0] if p['pair'] == 1 else i['2.fastq.gz'][0]
        input = i['1.fastq'][0] if p['pair'] == 1 else i['2.fastq'][0]
        return "python {s[genomekey_library_path]}/scripts/splitfastq.py {input} $OUT.dir", { 'input': input}
