from cosmos.i import Tool, TaskFile

class SplitFastq(Tool):
    inputs = ['1.fastq.gz','2.fastq.gz']
    outputs = [TaskFile(name='dir',persist=True)]
    time_req = 12*60
    mem_req = 1000

    def cmd(self,i,s,p):
        input = i['1.fastq.gz'][0] if p['pair'] == 1 else i['2.fastq.gz'][0]
        return "python {s[genomekey_library_path]}/scripts/splitfastq.py {input} $OUT.dir", { 'input': input}
