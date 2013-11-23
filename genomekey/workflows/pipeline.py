import os
import pysam
#import re

from cosmos.lib.ezflow.dag  import DAG, add_, split_, sequence_, configure, add_run, map_, reduce_, reduce_split_, apply_
from cosmos.lib.ezflow.tool import INPUT
from cosmos.Workflow.models import TaskFile

from genomekey.tools              import gatk, picard, pipes
from genomekey.wga_settings       import wga_settings

from genomekey       import log


class BamException(Exception):pass
class WorkflowException(Exception):pass


def _getHeaderInfo(input_bam):
    if   input_bam[-3:] == 'bam': header = pysam.Samfile(input_bam,'rb', check_sq = False).header
    elif input_bam[-3:] == 'sam': header = pysam.Samfile(input_bam,'r' , check_sq = False).header
    else:
        raise TypeError, 'input file is not a bam or sam'

    return {'rg': [ [tags['ID'], tags['SM'], tags.get('LB','noLBinfo'), tags.get('PL','noPLinfo') ] for tags in header['RG']],
            'sq': [ [tags['SN'], tags['LN']]                                                        for tags in header['SQ']]
           }

def _getSeqName(header):
    """Return sequence names (@SQ SN in header)
    """

    seqNameList = []
    unMapped=''
    for sn in header['sq']:
        if (sn[0].startswith('GL')) or (sn[0].startswith('chrUn')):
            unMapped += " %s" % sn[0]
        else:
            seqNameList.append(sn[0])  # first column is seqName

    seqNameList.append(unMapped)
    return seqNameList

    
opb  = os.path.basename
seq_ = sequence_


def Pipeline(workflow, dag, settings, bams):

    # split_ tuples
    interval = ('interval', range(1,23) + ['X', 'Y'])

    glm = ('glm', ['SNP', 'INDEL'])


    bam_seq = None
    
    for b in bams:
        header = _getHeaderInfo(b)
        sn     = _getSeqName(header)

        rgid = [ h[0] for h in header['rg']]

        # if seqName is empty, then let's assume that the input is unaligned bam
        sample_name = opb(b).partition('.')[0]
        s = seq_( add_([INPUT(b, tags={'bam':sample_name})], stage_name="Load BAMs"), split_([ ('rgid', rgid), ('sn', sn) ], pipes.Bam_To_BWA))

        if bam_seq is None:   bam_seq = s
        else:                 bam_seq = seq_(bam_seq, s, combine=True)


    return sequence_(
        bam_seq,

        reduce_split_(['bam','rgid'], [interval], gatk.IndelRealigner),

        map_(picard.MarkDuplicates),

        map_(gatk.BaseQualityScoreRecalibration),        

        reduce_split_(['bam'],      [interval], gatk.ReduceReads),

        reduce_split_(['interval'], [glm],      gatk.UnifiedGenotyper),

        reduce_(['glm'],                        gatk.VariantQualityScoreRecalibration, tag={'vcf':'master'}),

        reduce_(['vcf'], gatk.CombineVariants, "Combine into Master VCFs")
        )
