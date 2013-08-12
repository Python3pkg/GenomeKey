from cosmos.lib.ezflow.dag        import add_, map_, reduce_, split_, reduce_split_, sequence_, apply_

from genomekey.tools              import gatk, picard, bwa, misc, bamUtil, pipes
from genomekey.workflows.annotate import massive_annotation
from genomekey.wga_settings       import wga_settings


def Pipeline():
    is_capture = wga_settings['capture']

    # split_ tuples
    intervals = ('interval', range(1,23) + ['X', 'Y'])

    glm = ('glm', ['SNP', 'INDEL'])


    align = reduce_(['sample_name', 'library', 'region', 'bam', 'rgid','platform'], pipes.AlignAndClean)

    post_align = sequence_(
        reduce_(['sample_name', 'library'], picard.MarkDuplicates),
#       split_([intervals],gatk.RealignerTargetCreator),
#       map_(gatk.IndelRealigner),
        split_([intervals], gatk.IndelRealigner),
        map_(gatk.BQSR),
        map_(gatk.ApplyBQSR) #TODO I add BQSRGatherer as a parent with a hack inside ApplyBQSR.cmd    
    )

    call_variants = sequence_(
        apply_(
            #reduce_(['interval'],gatk.HaplotypeCaller,tag={'vcf':'HaplotypeCaller'}),
            reduce_split_(['interval'], [glm], gatk.UnifiedGenotyper, tag={'vcf': 'UnifiedGenotyper'}),
            combine=True
        ),
        reduce_(['vcf'], gatk.CombineVariants, 'Combine Into Raw VCFs'),
        split_([glm],gatk.VQSR),
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
            reduce_split_(['sample_name'], [intervals], gatk.ReduceReads),
            call_variants,
            massive_annotation
        )
