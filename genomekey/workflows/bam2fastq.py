__author__ = 'erik'

"""
Convert a Bam to Fastq
"""

from cosmos.contrib.ezflow.dag import DAG, Map, Reduce, Split, ReduceSplit, Add, StageNameCollision
from cosmos.contrib.ezflow.tool import INPUT,Tool
from cosmos.Workflow.models import TaskFile
from genomekey.tools import picard,samtools,genomekey_scripts
from genomekey import log
import os
import re


####################
# Tools
####################

class BamException(Exception):pass
class WorkflowException(Exception):pass

def Bam2Fastq(workflow, dag,settings, input_bams):
    if len(input_bams) == 0:
        raise WorkflowException, 'At least 1 BAM input required'
    dag.ignore_stage_name_collisions=True
    import pysam

    filters = []
    for input_bam in input_bams:
        RG = pysam.Samfile(input_bam).header['RG']
        rgids = [ tags['ID'] for tags in RG ]
        (dag |Add| [INPUT(input_bam,
                            tags={
                                'input':os.path.basename(input_bam)
                            })]
            |Split| ([('rgid',rgids)],samtools.FilterBamByRG)
        )
        filters.extend(dag.last_tools)

    (dag.branch_from_tools(filters)
        |Map| picard.REVERTSAM
        |Map| picard.SAM2FASTQ
        |Split| ([('pair',[1,2])],genomekey_scripts.SplitFastq)
    )

    # I have to run the workflow here, because there's no way to tell how many files SplitFastq will output until
    # the workflow has executed
    dag.configure(settings)
    dag.add_to_workflow(workflow)
    workflow.run(finish=False)

    # Load Fastq Chunks for processing
    input_chunks = []
    for split_fastq_tool in dag.last_tools:
        tags = split_fastq_tool.tags.copy()

        # Get The RG info and place into a dictionary for tags
        # note: FilterBamByRG's output bam has the right RG information
        filterbambyrg_tool = split_fastq_tool.parent.parent.parent
        bam_path = TaskFile.objects.get(id=filterbambyrg_tool.get_output('bam').id).path
        RGs = pysam.Samfile(bam_path,'rb').header['RG']

        # FilterBamByRG does not remove the non-filtered RGs from the new header
        RG = [ d for d in RGs if d['ID'] == split_fastq_tool.tags['rgid']][0]
        tags['sample_name'] = RG['SM']
        tags['library'] = RG['LB']
        tags['platform'] = RG['PL']
        tags['platform_unit'] = RG.get('PU',RG['ID']) # use 'ID' if 'PU' does not exist

        # Add fastq chucks as input files
        fastq_output_dir = TaskFile.objects.get(id=split_fastq_tool.get_output('dir').id).path
        for f in os.listdir(fastq_output_dir):
            fastq_path = os.path.join(fastq_output_dir,f)
            tags2 = tags.copy()
            tags2['chunk'] = re.search("(\d+)\.fastq",f).group(1)
            new_tool = INPUT(fastq_path,tags=tags2,stage_name='Load FASTQ Chunks')
            dag.G.add_edge(split_fastq_tool,new_tool)
            input_chunks.append(new_tool)

    dag.last_tools = input_chunks