
class FilterBamByRG(Tool):
    inputs = ['bam']
    outputs = ['bam']
    time_req = 12*60
    mem_req = 3000

    def cmd(self,i,s,p):
        return "{s[samtools_path]} view -h -b -r {p[rgid]} {i[bam][0]} -o $OUT.bam"
