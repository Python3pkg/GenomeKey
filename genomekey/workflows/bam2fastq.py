__author__ = 'erik'

"""
Convert a Bam to Fastq
"""

from cosmos.contrib.ezflow.dag import DAG, Map, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT,Tool
from cosmos.Workflow.models import TaskFile
from genomekey.tools import picard,samtools,scripts
import os
import re

####################
# Tools
####################

class BamException(Exception):pass

def Bam2Fastq(workflow,dag,settings,input_bams):

    import pysam
    inputs = []
    for input_bam in input_bams:
        sf = pysam.Samfile(input_bam)
        RG = sf.header['RG']
        rgids = [ tags['ID'] for tags in RG ]


        (dag
            |Add| [ INPUT(input_bam,tags={
                                          'input':os.path.basename(input_bam),
                                         }
                   ) ]
            |Split| ([('rgid',rgids)],samtools.FilterBamByRG)
            |Map| picard.REVERTSAM
            |Map| picard.SAM2FASTQ
            |Split| ([('pair',[1,2])],scripts.SplitFastq)
        )

    # Have to run the workflow here, because there's no way to tell how many files SplitFastq will output
    dag.configure(settings)
    dag.add_to_workflow(workflow)
    workflow.run(finish=False)

    #Load Fastq Chunks for processing
    input_chunks = []
    for split_fastq_tool in dag.last_tools:
        tags = split_fastq_tool.tags.copy()

        ####
        # Get The RG info and place into a dictionary for tags
        # note: filterbambyrg_tool has the right RG information
        ####

        filterbambyrg_tool = split_fastq_tool.parent.parent.parent
        bam_path=filterbambyrg_tool.get_output('bam').path
        sf = pysam.Samfile(bam_path,'rb')
        RGs = sf.header['RG']
        #FilterBamByRG does not remove the non-filtered RGs from the new header!
        RG = [ d for d in RGs if d['ID'] == split_fastq_tool.tags['rgid']][0]
        tags['sample_name'] = RG['SM']
        tags['library'] = RG['LB']
        tags['platform'] = RG['PL']
        tags['platform_unit'] = RG['PU']
        ####
        # Add fastq chucks as input files
        ####
        for f in os.listdir(split_fastq_tool._task_instance.output_files[0].path):
            path = os.path.join(TaskFile.objects.get(id=split_fastq_tool._task_instance.output_files[0].id).path,f)
            tags2 = tags.copy()
            tags2['chunk'] = re.search("(\d+)\.fastq",f).group(1)
            new_tool = INPUT(path,tags=tags2,stage_name='Load FASTQ Chunks')
            dag.G.add_edge(split_fastq_tool,new_tool)
            input_chunks.append(new_tool)

    dag.last_tools = input_chunks