from cosmos.lib.ezflow.dag        import add_, map_, reduce_, split_, reduce_split_, sequence_, apply_

from genomekey.tools              import gatk, picard, bwa, misc, bamUtil, pipes
from genomekey.workflows.annotate import massive_annotation
from genomekey.wga_settings       import wga_settings


def Pipeline():
    is_capture = wga_settings['capture']

    # split_ tuples
    interval = ('interval', range(1,23) + ['X', 'Y'])

    glm = ('glm', ['SNP', 'INDEL'])

    align = reduce_(['bam','sample_name','rgid','platform','library','sn'], pipes.AlignAndClean) # sn is seqName, not sample_name

    post_align1 = sequence_(
        reduce_(['sample_name', 'library'], picard.MarkDuplicates),
        split_([interval],gatk.RealignerTargetCreator),
        map_(gatk.IndelRealigner),
        map_(gatk.BQSR),
        map_(gatk.ApplyBQSR) 
        )
    
    post_align2 = sequence_(
        reduce_(['sample_name', 'library'], picard.MarkDuplicates),
        split_([interval], gatk.IndelRealigner),
        map_(gatk.BQSR),
        map_(gatk.ApplyBQSR) 
        )

    post_align3 = sequence_(
        reduce_split_(['bam','sample_name','library','rgid'],[interval],  gatk.IndelRealigner),
        reduce_(      ['bam','sample_name','library',        'interval'], picard.MarkDuplicates),
        map_(gatk.BQSR),
        map_(gatk.ApplyBQSR)
        )

    call_variants = sequence_(
#        apply_(
            #reduce_(['interval'],gatk.HaplotypeCaller,tag={'vcf':'HaplotypeCaller'}),
#            reduce_split_(['interval'], [glm], gatk.UnifiedGenotyper, tag={'vcf': 'UnifiedGenotyper'}),
#            combine=True
#        ),
        reduce_split_(['interval'], [glm], gatk.UnifiedGenotyper, tag={'vcf': 'UnifiedGenotyper'}),
        reduce_(['vcf'], gatk.CombineVariants, 'Combine Into Raw VCFs'),
        split_([glm], gatk.VQSR),
        map_(gatk.Apply_VQSR),
        reduce_(['vcf'], gatk.CombineVariants, "Combine into Master VCFs")
    )

    if is_capture:
        return sequence_(
            align,
            post_align3,
            call_variants,
            massive_annotation
        )
    else:
        return sequence_(
            align,
            post_align3,
            reduce_split_(['sample_name'], [interval], gatk.ReduceReads),
            call_variants,
            massive_annotation
        )
