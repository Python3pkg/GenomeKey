from cosmos.contrib.ezflow.tool import Tool

class SplitFastq(Tool):
    inputs = ['1.fastq','2.fastq']
    outputs = ['dir']
    time_req = 12*60
    mem_req = 1000

    def cmd(self,i,s,p):
        input = i['1.fastq'][0] if p['pair'] == 1 else i['2.fastq'][0]
        return "python {s[genomekey_library_path]}/scripts/splitfastq.py {input} $OUT.dir", { 'input': input}
