__author__ = 'erik'

"""
Convert a Bam to Fastq
"""

from cosmos.contrib.ezflow.dag import DAG, Map, Reduce, Split, ReduceSplit, Add
from cosmos.contrib.ezflow.tool import INPUT,Tool
from genomekey.tools import picard,samtools,scripts
import os
import re

####################
# Tools
####################


def Bam2Fastq(workflow,dag,settings,rgids):

    (  dag
        |Split| ([('rgid',rgids)],samtools.FilterBamByRG)
        |Map| picard.REVERTSAM
        |Map| picard.SAM2FASTQ
        |Split| ([('pair',[1,2])],scripts.SplitFastq)
    )
    dag.configure(settings)
    # if workflow.stages.filter(name='SplitFastq',successful=True).count() == 0:
    dag.add_to_workflow(workflow)
    workflow.run(finish=False) # this updates the taskfile paths

    #Load Fastq Chunks for processing
    input_chunks = []
    for input_tool in dag.last_tools:
        d = input_tool.tags.copy()
        #TODO tags should be set and inherited by the original bam
        d['sample'] = 'NA12878'
        d['library'] = 'LIB-NA12878'
        d['platform'] = 'ILLUMINA'

        d['flowcell'] = d['rgid'][:5]
        d['lane'] = d['rgid'][6:]
        for f in os.listdir(input_tool._task_instance.output_files[0].path):
            path = os.path.join(input_tool._task_instance.output_files[0].path,f)
            d2 = d.copy()
            d2['chunk'] = re.search("(\d+)\.fastq",f).group(1)
            new_tool = INPUT(path,tags=d2,stage_name='Load FASTQ Chunks')
            dag.G.add_edge(input_tool,new_tool)
            input_chunks.append(new_tool)
    dag.last_tools = input_chunks