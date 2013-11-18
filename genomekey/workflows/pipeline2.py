from cosmos.lib.ezflow.dag        import add_, map_, reduce_, split_, reduce_split_, sequence_, apply_

from genomekey.tools              import gatk, picard, bwa, misc, bamUtil, pipes
from genomekey.workflows.annotate import massive_annotation
from genomekey.wga_settings       import wga_settings

def Pipeline2():
    #is_capture = wga_settings['capture'] => will not reduceReads

    # split_ tuples
    interval = ('interval', range(1,23) + ['X', 'Y'])

    glm = ('glm', ['SNP', 'INDEL'])

    post_align = sequence_(
        reduce_split_(['bam','rgid'], [interval], gatk.IndelRealigner),   
        map_(picard.MarkDuplicates),
        map_(gatk.BaseQualityScoreRecalibration)
        )

    call_variants = sequence_(
        reduce_split_(['interval'], [glm], gatk.UnifiedGenotyper),
        reduce_([glm], gatk.VariantQualityScoreRecalibration),
        map_(gatk.Apply_VQSR,tag={'vcf':'master'}),
        reduce_(['vcf'], gatk.CombineVariants, "Combine into Master VCFs")
    )

    
    return sequence_(
        post_align,
        reduce_split_(['bam'], [interval], gatk.ReduceReads),
        call_variants,
        massive_annotation
        )

def Pipeline():
    is_capture = wga_settings['capture']

    # split_ tuples
    interval = ('interval', range(1,23) + ['X', 'Y'])

    glm = ('glm', ['SNP', 'INDEL'])

    # sn is seqName, not sample_name
    # assuming that each data file is unique with rgid+sn, without 'bam' file tag.
    align = reduce_(['sample_name','rgid','platform','library','sn'], pipes.AlignAndClean) 

    # post_align1 = sequence_(
    #     reduce_(['sample_name', 'library'], picard.MarkDuplicates),
    #     split_([interval],gatk.RealignerTargetCreator),
    #     map_(gatk.IndelRealigner),
    #     map_(gatk.BQSR),
    #     map_(gatk.ApplyBQSR) 
    #     )
    
    # post_align2 = sequence_(
    #     reduce_(['sample_name', 'library'], picard.MarkDuplicates),
    #     split_([interval], gatk.IndelRealigner),
    #     map_(gatk.BQSR),
    #     map_(gatk.ApplyBQSR) 
    #     )

    # post_align3 = sequence_(
    #     reduce_split_(['bam','sample_name','library','rgid'],[interval],  gatk.IndelRealigner),
    #     reduce_(      ['bam','sample_name','library',        'interval'], picard.MarkDuplicates),
    #     map_(gatk.BQSR),
    #     map_(gatk.ApplyBQSR)
    #     )

    post_align = sequence_(
        reduce_split_(['sample_name', 'library', 'rgid'], [interval], gatk.IndelRealigner),   
        map_(picard.MarkDuplicates),
        map_(gatk.BaseQualityScoreRecalibration)
        )

    call_variants = sequence_(
#        apply_(
            #reduce_(['interval'],gatk.HaplotypeCaller,tag={'vcf':'HaplotypeCaller'}),
#            reduce_split_(['interval'], [glm], gatk.UnifiedGenotyper, tag={'vcf': 'UnifiedGenotyper'}),
#            combine=True
#        ),
        reduce_split_(['interval'], [glm], gatk.UnifiedGenotyper, tag={'vcf': 'UnifiedGenotyper'}),
        reduce_(['vcf'], gatk.CombineVariants, 'Combine Into Raw VCFs'),
        split_([glm], gatk.VariantQualityScoreRecalibration),
        map_(gatk.Apply_VQSR),
        reduce_(['vcf'], gatk.CombineVariants, "Combine into Master VCFs")
    )

    if is_capture:
        return sequence_(
            align,
            post_align,
            call_variants,
            massive_annotation
        )
    else:
        return sequence_(
            align,
            post_align,
            reduce_split_(['sample_name'], [interval], gatk.ReduceReads),
            call_variants,
            massive_annotation
        )
