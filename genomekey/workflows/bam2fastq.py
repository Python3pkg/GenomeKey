__author__ = 'erik'

"""
Convert a Bam to Fastq
"""
import os
import re
import pysam

from cosmos.lib.ezflow.dag  import DAG, add_,map_,reduce_,split_,reduce_split_,sequence_,branch_,configure,add_run
from cosmos.lib.ezflow.tool import INPUT,Tool
from cosmos.Workflow.models import TaskFile

from genomekey.tools import picard,samtools,genomekey_scripts,bamUtil,pipes
from genomekey import log


####################
# Tools
####################

class BamException(Exception):pass
class WorkflowException(Exception):pass

def _getRgids(input_bam):
    """
    Returns the rgids in an input file
    :param input_bam: (file)
    :return: (list) a list of rgids
    """
    if   input_bam[-3:] == 'bam': RG = pysam.Samfile(input_bam,'rb').header['RG']
    elif input_bam[-3:] == 'sam': RG = pysam.Samfile(input_bam,'r' ).header['RG']
    else:
        raise TypeError, 'input file is not a bam or sam'

    return [ tags['ID'] for tags in RG ]

def _getSeqNames(input_bam):
    if   input_bam[-3:] == 'bam': SQ = pysam.Samfile(input_bam,'rb').header['SQ']
    elif input_bam[-3:] == 'sam': SQ = pysam.Samfile(input_bam,'r' ).header['SQ']
    else:
        raise TypeError, 'input file is not a bam or sam'

    return [ tags['SN'] for tags in SQ ]

def _getHeaderInfo(input_bam):
    if   input_bam[-3:] == 'bam': header = pysam.Samfile(input_bam,'rb').header
    elif input_bam[-3:] == 'sam': header = pysam.Samfile(input_bam,'r' ).header
    else:
        raise TypeError, 'input file is not a bam or sam'

    return {'rgid': [ tags['ID'] for tags in header['RG']], 'sqsn': [ tags['SN'] for tags in header['SQ']] }


def _splitfastq2inputs(dag):
    """
    Assumes dag's active tools are from SplitFastq. Traverses their output for the fastq files, and
    yields new INPUTs properly annotated with dags, and children of their right SplitFastq parent.
    """
    for split_fastq_tool in dag.active_tools:
        tags = split_fastq_tool.tags.copy()

        # Get The RG info and place into a dictionary for tags
        # note: FilterBamByRG's output bam has the right RG information
        input_tool = split_fastq_tool.parent.parent
        bam_path = TaskFile.objects.get(id=input_tool.get_output('bam').id).path
        RGs = pysam.Samfile(bam_path,'rb').header['RG']

        # FilterBamByRG does not remove the non-filtered RGs from the new header
        RG = [ d for d in RGs if d['ID'] == split_fastq_tool.tags['rgid']][0]
        tags['sample_name']   = RG['SM']
        tags['library']       = RG['LB']
        tags['platform']      = RG['PL']
        tags['platform_unit'] = RG.get('PU', RG['ID']) # use 'ID' if 'PU' does not exist

        # Add fastq chucks as input files
        fastq_output_dir = TaskFile.objects.get(id=split_fastq_tool.get_output('dir').id).path
        for f in os.listdir(fastq_output_dir):
            fastq_path = os.path.realpath(os.path.join(fastq_output_dir,f))
            tags2 = tags.copy()
            tags2['chunk'] = re.search("(\d+)\.fastq",f).group(1)

            i = INPUT(name='fastq',path=fastq_path,tags=tags2,stage_name='Load FASTQ')
            dag.add_edge(split_fastq_tool,i)
            yield i


opb  = os.path.basename
seq_ = sequence_

def Bam2Fastq(workflow, dag, settings, bams):


    bam_seq = None
    for b in bams:
        header = _getHeaderInfo(b)
        s = seq_( add_([INPUT(b, tags={'bam':opb(b)})], stage_name="Load BAMs"), split_([('rgid', header['rgid']), ('sqsn', header['sqsn'])], pipes.FilterBamByRG_To_FastQ))
        if bam_seq is None:   bam_seq = s
        else:                 bam_seq = seq_(bam_seq, s, combine=True)


    # Add "Split FASTQ" stage
    dag.sequence_(bam_seq, split_([('pair',[1,2])], genomekey_scripts.SplitFastq), configure(settings), add_run(workflow, finish=False))

    # Set "Load FASTQ" stage
    dag.sequence_(add_(list(_splitfastq2inputs(dag))))
