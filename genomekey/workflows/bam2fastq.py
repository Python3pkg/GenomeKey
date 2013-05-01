__author__ = 'erik'

"""
Convert a Bam to Fastq
"""

from cosmos.contrib.ezflow.dag import DAG, add_,map_,reduce_,split_,reduceSplit_,combine_,sequence_,branch_
from cosmos.contrib.ezflow.tool import INPUT,Tool
from cosmos.Workflow.models import TaskFile
from genomekey.tools import picard,samtools,genomekey_scripts
from genomekey import log
import os
import re
import pysam

####################
# Tools
####################

class BamException(Exception):pass
class WorkflowException(Exception):pass

def _inputbam2rgids(input_bam):
    """
    Returns the rgids in an input file
    :param input_bam: (file)
    :return: (list) a list of rgids
    """
    if input_bam[-3:] == 'bam':
        RG = pysam.Samfile(input_bam,'rb').header['RG']
    elif input_bam[-3:] == 'sam':
        RG = pysam.Samfile(input_bam,'r').header['RG']
    else:
        raise TypeError, 'input file is not a bam or sam'

    return [ tags['ID'] for tags in RG ]

def Bam2Fastq(workflow, dag, settings, input_bams):
    dag.ignore_stage_name_collisions=True
    if len(input_bams) == 0:
        raise WorkflowException, 'At least 1 BAM input required'
    dag.sequence_(
        combine_(*[
            sequence_(
                add_([ INPUT(input_bam, tags={'input':os.path.basename(input_bam)})],stage_name="Load Input Bams"),
                split_([('rgid',_inputbam2rgids(input_bam))],samtools.FilterBamByRG)
            )
            for input_bam in input_bams
        ]),
        map_(picard.REVERTSAM),
        map_(picard.SAM2FASTQ),
        split_([('pair',[1,2])],genomekey_scripts.SplitFastq)
    )

    # I have to run the workflow here, because there's no way to tell how many files SplitFastq will output until
    # the workflow has executed
    dag.configure(settings)
    dag.add_to_workflow(workflow)
    workflow.run(finish=False)

    # Load Fastq Chunks for processing
    add_s = []
    for split_fastq_tool in dag.active_tools:
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
        tags['platform_unit'] = RG.get('PU', RG['ID']) # use 'ID' if 'PU' does not exist

        # Add fastq chucks as input files
        fastq_output_dir = TaskFile.objects.get(id=split_fastq_tool.get_output('dir').id).path
        for f in os.listdir(fastq_output_dir):
            fastq_path = os.path.join(fastq_output_dir,f)
            tags2 = tags.copy()
            tags2['chunk'] = re.search("(\d+)\.fastq",f).group(1)

            i = INPUT(name='fastq.gz',path=fastq_path,tags=tags2,stage_name='Load Input Fastqs')
            dag.add_edge(split_fastq_tool,i)
            add_s.append(add_([i]))
    dag.combine_(*add_s)

    dag.ignore_stage_name_collisions=False
    return dag