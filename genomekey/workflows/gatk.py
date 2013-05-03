from genomekey.tools import gatk, picard, bwa, misc, bamUtil,pipes
from cosmos.contrib.ezflow.dag import add_,map_,reduce_,split_,reduce_split_,combine_,sequence_,branch_,apply_
from genomekey.workflows.annotate import massive_annotation

# Split Tags
intervals = ('interval',range(1,23) + ['X','Y']) #if not settings['test'] else ('interval',[20])
glm = ('glm',['SNP','INDEL'])

align_to_reference = sequence_(
    apply_(
        reduce_(['sample_name','library'], misc.FastQC),
        reduce_(['sample_name','library','platform','platform_unit','chunk'],pipes.AlignAndClean)
    ),
)

preprocess_alignment = sequence_(
    reduce_(['sample_name'], picard.MarkDuplicates),
    apply_(
        map_(picard.CollectMultipleMetrics),
        split_([intervals],gatk.RealignerTargetCreator),
    ),
    map_(gatk.IR),
    map_(gatk.BQSR),
    apply_(
        reduce_(['sample_name'], gatk.BQSRGatherer),
        map_(gatk.ApplyBQSR) #this is weird and I add BQSRGatherer as a parent with a hack inside ApplyBQSR.cmd
    )
)

call_variants = sequence_(
    combine_(
        sequence_(
            reduce_split_([],[intervals,glm],gatk.HaplotypeCaller,tag={'vcf':'HaplotypeCaller'}),
            reduce_([], gatk.CombineVariants, 'Combine HaplotypeCaller Raw vcfs',tag={'vcf':'UnifiedGenotyper'})
        ),
        sequence_(
            reduce_split_([],[intervals,glm], gatk.UnifiedGenotyper),
            reduce_([], gatk.CombineVariants, 'Combine UnifiedGenotyper Raw vcfs',tag={'vcf':'UnifiedGenotyper'}),
        )
    ),
    reduce_split_(['vcf'],[glm],gatk.VQSR),
    map_(gatk.Apply_VQSR),
    reduce_(['vcf'], gatk.CombineVariants, "Combine into Master VCFs")
)


ThePipeline = sequence_(
    align_to_reference,
    preprocess_alignment,
    call_variants,
    massive_annotation
)