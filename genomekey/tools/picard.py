from cosmos.lib.ezflow.tool import Tool
from cosmos.Workflow.models import TaskFile

import os
opj = os.path.join

def _list2input(l):
    return " ".join(map(lambda x: 'INPUT='+str(x)+'\n', l))


class Picard(Tool):
    time_req        = 12*60
    mem_req         = 3*1024
    cpu_req         = 2
    extra_java_args = ''
    
    @property
    def picard_bin(self):
        return 'java{self.extra_java_args} -Xmx{mem_req}m -Djava.io.tmpdir={s[tmp_dir]} -Dsnappy.loader.verbosity=true'.format(
            self=self,
            mem_req=int(self.mem_req*.8),
            s=self.settings
            )

    @property
    def bin(self):
        return self.picard_bin+' -jar {0}'.format(opj(self.settings['Picard_dir'], self.jar))

class MarkDuplicates(Picard):
    name     = "MarkDuplicates"
    cpu_req  = 3
    mem_req  = 6*1024   # will allow 9 jobs in a node, as mem_total = 59.3G
    time_req = 12*60
    inputs   = ['bam']
    outputs  = ['bam','bai','metrics']
    #persist  = True
        
    def cmd(self,i,s,p):
        return r"""
            tmpDir=`mktemp -d --tmpdir=/mnt`;

            {s[java]} -Xms{min}M -Xmn{min}M -Xmx{max}M -jar {s[Picard_dir]}/MarkDuplicates.jar
            TMP_DIR=$tmpDir
            OUTPUT=$OUT.bam
            METRICS_FILE=$OUT.metrics
            ASSUME_SORTED=True
            CREATE_INDEX=True
            COMPRESSION_LEVEL=0
            MAX_RECORDS_IN_RAM=1000000
            VALIDATION_STRINGENCY=SILENT
            VERBOSITY=WARNING
            QUIET=TRUE
            {inputs};

            #mv $tmpDir/out.baa     $OUT.bam;
            #mv $tmpDir/out.bai     $OUT.bai;
            #mv $tmpDir/out.metrics $OUT.metrics;
            /bin/rm -rf $tmpDir;
        """, {'inputs': _list2input(i['bam']), 'min':int(self.mem_req *.5), 'max':int(self.mem_req)}


class CollectMultipleMetrics(Picard):
    name    = "Collect Multiple Metrics"
    inputs  = ['bam']
    outputs = ['metrics']
    # time_req = 4*60
    mem_req = 3*1024

    jar = 'CollectMultipleMetrics.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            REFERENCE_SEQUENCE={s[reference_fasta_path]}
            OUTPUT=$OUT.metrics
            INPUT={i[bam][0]}
        """

class AddOrReplaceReadGroups(Picard):
    name = "Add or Replace ReadGroups"
    inputs = ['sam']
    outputs = ['bam']
    # time_req = 4*60
    mem_req = 3*1024

    jar = 'AddOrReplaceReadGroups.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            INPUT={i[sam][0]}
            OUTPUT=$OUT.bam
            RGID={p[platform_unit]}
            RGLB={p[library]}
            RGSM={p[sample_name]}
            RGPL={p[platform]}
            RGPU={p[platform_unit]}
        """

class REVERTSAM(Picard):
    inputs  = ['bam']
    outputs = ['bam']
    mem_req = 12*1024
    cpu_req=2
    #succeed_on_failure = False

    extra_java_args =' -XX:ParallelGCThreads={0}'.format(cpu_req+1)

    jar = 'RevertSam.jar'

    def cmd(self,i,s,p):
        return r"""
            {self.bin}
            INPUT={i[bam][0]}
            OUTPUT=$OUT.bam
            VALIDATION_STRINGENCY=SILENT
            MAX_RECORDS_IN_RAM=4000000
        """

# class FIXMATE(Picard):
#     name = "Fix Mate Information"
#     inputs = ['bam']
#     outputs = ['bam']
#     # time_req = 4*60
#     mem_req = 3*1024

#     jar = 'FixMateInformation.jar'

#     def cmd(self,i,s,p):
#         return r"""
#             {self.bin}
#             INPUT={i[bam][0]}
#             OUTPUT=$OUT.bam
#             VALIDATION_STRINGENCY=LENIENT
#         """

# class SAM2FASTQ_byrg(Picard):
#     inputs = ['bam']
#     outputs = ['dir']
#     # time_req = 180
#     mem_req = 12*1024
#     #succeed_on_failure = True

#     jar = 'SamToFastq.jar'

#     def cmd(self,i,s,p):
#         return r"""
#             {self.bin}
#             INPUT={i[bam][0]}
#             OUTPUT_DIR=$OUT.dir
#             OUTPUT_PER_RG=true
#             VALIDATION_STRINGENCY=LENIENT
#         """


# class SAM2FASTQ(Picard):
#     """
#     Assumes sorted
#     """
#     inputs = ['bam']
#     outputs = ['1.fastq','2.fastq']
#     mem_req = 3*1024
#     succeed_on_failure = True

#     jar = 'SamToFastq.jar'

#     def cmd(self,i,s,p):
#         import re
#         return r"""
#             {self.bin}
#             INPUT={i[bam][0]}
#             FASTQ=$OUT.1.fastq
#             SECOND_END_FASTQ=$OUT.2.fastq
#             VALIDATION_STRINGENCY=SILENT
#         """

# class MERGE_SAMS(Picard):
#     name = "Merge Sam Files"
#     mem_req = 3*1024
#     inputs = ['bam']
#     outputs = ['bam']
#     default_params = { 'assume_sorted': False}
    
#     jar = 'MergeSamFiles.jar'
    
    
#     def cmd(self,i,s,p):
#         return r"""
#             {self.bin}
#             {inputs}
#             OUTPUT=$OUT.bam
#             SORT_ORDER=coordinate
#             MERGE_SEQUENCE_DICTIONARIES=True
#             ASSUME_SORTED={p[assume_sorted]}
#         """, {
#         'inputs' : "\n".join(["INPUT={0}".format(n) for n in i['bam']])
#         }
                
# class CLEAN_SAM(Picard):
#     name = "Clean Sams"
#     mem_req = 4*1024
#     inputs = ['bam']
#     outputs = ['bam']
        
#     jar = 'CleanSam.jar'
    
#     def cmd(self,i,s,p):
#         return r"""
#             {self.bin}
#             I={i[bam][0]}
#             O=$OUT.bam
#             VALIDATION_STRINGENCY=SILENT
#         """

# class SORT_BAM(Picard):
#     name = "Sort BAM"
#     mem_req = 4*1024
#     inputs = ['bam']
#     outputs = ['bam']

#     jar = 'SortSam.jar'

#     def cmd(self,i,s,p):
#         return r"""
#             {self.bin}
#             I={i[bam][0]}
#             O=$OUT.bam
#             SORT_ORDER=coordinate
#         """



# class INDEX_BAM(Picard):
#     name = "Index Bam Files"
#     mem_req = 4*1024
#     forward_input = True
#     inputs = ['bam']
        
#     jar = 'BuildBamIndex.jar'
    
#     def cmd(self,i,s,p):
#         return r"""
#             {self.bin}
#             INPUT={i[bam][0]}
#             OUTPUT={i[bam][0]}.bai
#         """
