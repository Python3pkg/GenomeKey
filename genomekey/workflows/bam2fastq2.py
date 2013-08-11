import os
import re
import pysam

from cosmos.lib.ezflow.dag  import DAG, add_, split_, sequence_, configure, add_run, add_to_workflow
from cosmos.lib.ezflow.tool import INPUT
from cosmos.Workflow.models import TaskFile

from genomekey.tools import picard, samtools, bamUtil, pipes
from genomekey import log


class BamException(Exception):pass
class WorkflowException(Exception):pass


def _getHeaderInfo(input_bam):
    if   input_bam[-3:] == 'bam': header = pysam.Samfile(input_bam,'rb').header
    elif input_bam[-3:] == 'sam': header = pysam.Samfile(input_bam,'r' ).header
    else:
        raise TypeError, 'input file is not a bam or sam'

    return {'rg': [ [tags['ID'], tags['SM'], tags['LB'], tags['PL'] ] for tags in header['RG']],
            'sq': [ [tags['SN'], tags['LN']]                          for tags in header['SQ']]
           }

def _getRegions(header):

#   for s in sq:
#       log.info('SN= {0}\tLN= {1}'.format(s[0], s[1]))

    totalLen = sum(sn[1] for sn in header['sq'])
    log.info('Total LN sum = {0}'.format(totalLen))

    regions = []

    TOTAL_RGID = len([tags for tags in header['rg']])

    TOTAL_CPUS_IN_THE_CLUSTER = 8*4
    
    targetTaskNum   = TOTAL_CPUS_IN_THE_CLUSTER*2/TOTAL_RGID
    targetBlockSize = int(totalLen/targetTaskNum)

    log.info("\n TargetBlockSize = {0}\n".format(targetBlockSize))

    currBlockSize = 0
    currRegion    = ""

    for sn in header['sq']:
        seqname     = sn[0]
        size        = sn[1]
        chunkRemain = size
        chunkStart  = 0     # start index of the current chunk

        if (currBlockSize + size < targetBlockSize):
            currRegion    += ' {0}'.format(seqname)     # include all seq in the current region, and move next
            currBlockSize += size

        else:
            chunkNeeded  = targetBlockSize - currBlockSize
            chunkRemain -= chunkNeeded

            if chunkRemain == 0:  currRegion += ' {0}'.format(seqname)
            else:                 currRegion += ' {0}:1-{1}'.format(seqname, chunkNeeded)

            regions.append(currRegion)

            currRegion    = ""
            currBlockSize = 0
            chunkStart    = chunkNeeded +1

            while (chunkRemain >0):   # when targetBlockSize is small
                if (chunkRemain < targetBlockSize):
                    currRegion    += ' {0}:{1}-{2}'.format(seqname, chunkStart, size)
                    currBlockSize += chunkRemain
                    break
                else:
                    chunkRemain   -= targetBlockSize
                    currRegion    += ' {0}:{1}-{2}'.format(seqname, chunkStart, chunkStart+targetBlockSize-1)
                    regions.append(currRegion)

                    currRegion    = ""
                    currBlockSize = 0
                    chunkStart    += targetBlockSize

    # last element
    if (currRegion != ""):
        regions.append(currRegion)

#    for r in regions:
#        log.info('region: {0}'.format(r))

    return regions

    
def _fastq2input(dag):
    """
    Traverses their output for the fastq files, and yields new INPUTs properly annotated with dags, and children of their right parent.
    """
    fastq_input = list()

    
    for tool in dag.active_tools:
        #log.info('dag.active_tool= {0}, output= {1}'.format(tool, tool.get_output('1.fastq')))
        tags = tool.tags.copy()

        # Get The RG info and place into a dictionary for tags
        bam_path = tool.parent.get_output('bam').path
        RGs = pysam.Samfile(bam_path,'rb').header['RG']

        # FilterBamByRG does not remove the non-filtered RGs from the new header
        RG = [ d for d in RGs if d['ID'] == tool.tags['rgid']][0]
        tags['sample_name']   = RG['SM']
        tags['library']       = RG['LB']
        tags['platform']      = RG['PL']

        #log.info('tags= {0}'.format(tags))

        # Add each fastq as input file
        for idx in [1,2]:
            fastq = TaskFile.objects.get(id=tool.get_output('{0}.fastq'.format(idx)).id).path

            if os.stat(fastq).st_size == 0: continue   # somtimes output fastq file can be empty

            newTag = tags.copy()
            newTag['pair'] = idx
            i = INPUT(name='fastq',path=fastq,tags=newTag)
            dag.add_edge(tool, i)
            
            fastq_input.append(i)

    return list(fastq_input)

opb  = os.path.basename
seq_ = sequence_

def Bam2Fastq(workflow, dag, settings, bams):

    bam_seq = None
    
    for b in bams:
        header = _getHeaderInfo(b)
        region = _getRegions(header)

        rgid = [ h[0] for h in header['rg']]

        s = seq_( add_([INPUT(b, tags={'bam':opb(b)})], stage_name="Load BAMs"), split_([ ('rgid',rgid),('region', region)], pipes.Bam_To_FastQ))

        if bam_seq is None:   bam_seq = s
        else:                 bam_seq = seq_(bam_seq, s, combine=True)


    dag.sequence_(bam_seq, configure(settings), add_run(workflow,finish=False)).add_(_fastq2input(dag), stage_name="Input FASTQ")

