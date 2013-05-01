from cosmos.contrib.ezflow.tool import Tool



class MEM(Tool):
    name = "BWA MEM Paired End Mapping"
    mem_req = 10*1024
    cpu_req = 1
    time_req = 120
    inputs = ['fastq.gz']
    outputs = ['sam']

    def cmd(self,i,s,p):
        """
        Expects tags: chunk, library, sample_name, platform, platform_unit, pair
        """
        return r"""
            {s[bwa_path]} mem
            -M
            -R "@RG\tID:{p[platform_unit]}\tLB:{p[library]}\tSM:{p[sample_name]}\tPL:{p[platform]}\tPU:{p[platform_unit]}"
            {s[bwa_reference_fasta_path]}
            {i[fastq.gz][0]}
            {i[fastq.gz][1]}
            > $OUT.sam
            """


# class ALN(Tool):
#     name = "Reference Alignment"
#     mem_req = 4*1024
#     cpu_req = 2
#     time_req = 100
#     forward_input = True
#     inputs = ['fastq.gz']
#     outputs = ['sai']
#     default_params = { 'q': 5 }
#
#     def cmd(self,i,s,p):
#         """
#         Expects tags: chunk, library, sample_name, platform, platform_unit, pair
#         """
#         return '{s[bwa_path]} aln -q {p[q]} -t {self.cpu_req} {s[bwa_reference_fasta_path]} {i[fastq.gz][0]} > $OUT.sai'
#
# class SAMPE(Tool):
#     name = "Paired End Mapping"
#     mem_req = 5*1024
#     cpu_req = 1
#     time_req = 120
#     inputs = ['fastq.gz','sai']
#     outputs = ['sam']
#
#     def cmd(self,i,s,p):
#         """
#         Expects tags: chunk, library, sample_name, platform, platform_unit, pair
#         """
#         #todo assert correct fastq and sai are paired
#         return r"""
#             {s[bwa_path]} sampe
#             -f $OUT.sam
#             -r "@RG\tID:{p[platform_unit]}\tLB:{p[library]}\tSM:{p[sample_name]}\tPL:{p[platform]}"
#             {s[bwa_reference_fasta_path]}
#             {i[sai][0]}
#             {i[sai][1]}
#             {i[fastq.gz][0]}
#             {i[fastq.gz][1]}
#             """
#