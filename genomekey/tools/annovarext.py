from cosmos.contrib.ezflow.tool import Tool

class DownDB(Tool):
    time_req = 10
    name = "Download Annotation Database"

    def cmd(self,i,s,p):
        return 'annovarext downdb {p[build]} {p[dbname]}'


# class SetID(Tool):
#     name = "Set VCF ID"
#     inputs = ['vcf']
#     outputs = ['vcf_id']
#     forward_input=True
#     time_req = 10
#
#     def cmd(self,i,s,p):
#         return "annovarext setid '{i[vcf][0]}' > $OUT.vcf_id"


class Vcf2Anno_in(Tool):
    name = "Convert VCF to Annovar"
    inputs = ['vcf']
    outputs = ['anno_in']
    forward_input=True
    time_req = 10

    def cmd(self,i,s,p):
        return "annovarext vcf2anno '{i[vcf][0]}' > $OUT.anno_in"

class Annotate(Tool):
    name = "Annotate"
    inputs = ['anno_in']
    outputs = ['dir']
    forward_input=True
    time_req = 20
    mem_req = 8*1024

    def cmd(self,i,s,p):
        return 'annovarext anno {p[build]} {p[dbname]} {i[anno_in][0]} $OUT.dir'

class MergeAnnotations(Tool):
    name = "Merge Annotations"
    inputs = ['anno_in','dir']
    outputs = ['dir']
    mem_req = 40*1024
    time_req = 120
    forward_input=True
    
    def cmd(self,i,s,p):
        return ('annovarext merge {i[anno_in][0]} $OUT.dir {annotated_dir_output}',
                { 'annotated_dir_output' : ' '.join(map(str,i['dir'])) }
        )

class SQL_DUMP(Tool):
    name = "SQL Dump"
    inputs = ['tsv']
    outputs = ['sql']
    
    def cmd(self,i,t,s,p):
        return 'sql dump {i[tsv]}'

class ANALYSIS(Tool):
    name = "Filtration And Analysis"
    inputs = ['sql']
    outputs = ['analysis']
    
    def cmd(self,i,t,s,p):
        return 'analyze {i[sql]}'
