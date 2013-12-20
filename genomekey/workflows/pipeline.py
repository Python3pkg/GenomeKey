import os
import pysam

from cosmos.lib.ezflow.dag  import DAG, add_, split_, sequence_, map_, reduce_, reduce_split_, apply_
from cosmos.lib.ezflow.tool import INPUT

from genomekey.tools        import pipes


def _getHeaderInfo(input_bam):
    if   input_bam[-3:] == 'bam': 
        header = pysam.Samfile(input_bam,'rb', check_sq = False).header
    elif input_bam[-3:] == 'sam': 
        header = pysam.Samfile(input_bam,'r' , check_sq = False).header
    else:
        raise TypeError, 'input file is not a bam or sam'

    return {'rg': [ [tags['ID'], tags['SM'], tags.get('LB','noLBinfo'), tags.get('PL','noPLinfo') ] for tags in header['RG']],
            'sq': [ [tags['SN']                                                                   ] for tags in header['SQ']]
           }

def _getSeqName(header):
    """
    Return sequence names (@SQ SN in header)
    """
    seqNameList = []
    unMapped=''
    for sn in header['sq']:
        if (sn[0].startswith('GL')) or (sn[0].startswith('chrUn')):
            unMapped += " %s" % sn[0]
        else:
            seqNameList.append(sn[0])  # first column is seqName

    if unMapped != '': 
        seqNameList.append(unMapped)

    return seqNameList

    
def pipeline(bams):

    # split_ tuples
    #interval = ('interval', range(1,23) + ['X', 'Y'])
    chrom = ('chrom', ['1', '2'])
    glm = ('glm', ['SNP', 'INDEL'])

    dbnames = ('dbname', ['dbSNP135','CytoBand','Target_Scan','mirBase','Self_Chain'])

    bam_seq = None
    
    for b in bams:
        header = _getHeaderInfo(b)
        sn     = _getSeqName(header)

        rgid = [ h[0] for h in header['rg']]

        # if seqName is empty, then let's assume that the input is unaligned bam
        sample_name = os.path.basename(b).partition('.')[0]
        s = sequence_( add_([INPUT(b, tags={'bam':sample_name})], stage_name="Load BAMs"), 
                       split_([ ('rgId', rgid), ('prevSn', sn) ], pipes.Bam_To_BWA))

        if bam_seq is None:   bam_seq = s
        else:                 bam_seq = sequence_(bam_seq, s, combine=True)


    return sequence_(
        bam_seq,

        reduce_split_(['bam','rgId'], [chrom], pipes.IndelRealigner),

        map_(pipes.MarkDuplicates),

        reduce_(['bam','chrom'], pipes.BaseQualityScoreRecalibration),

        map_(pipes.ReduceReads),

        reduce_split_(['chrom'], [glm], pipes.UnifiedGenotyper),

        reduce_(['glm'], pipes.VariantQualityScoreRecalibration, tag={'vcf':'master'}),

        reduce_(['vcf'],  pipes.CombineVariants, "Merge VCF"),

        map_(pipes.Vcf2Anno_in),
        
        split_([dbnames], pipes.Annotate, tag={'build':'hg19'}),
        
        reduce_(['vcf'],  pipes.MergeAnnotations)
        )
