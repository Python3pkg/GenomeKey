from genomekey.tools import gatk, picard, bwa, misc
from cosmos.contrib.ezflow.dag import add_,map_,reduce_,split_,reduce_split_,combine_,sequence_,branch_,apply_
from genomekey.workflows.annotate import massive_annotation

# Split Tags
intervals = ('interval',range(1,23) + ['X','Y']) #if not settings['test'] else ('interval',[20])
glm = ('glm',['SNP','INDEL'])

##############################################
# Map
#   Input must be correctly tagged
##############################################

alignment = sequence_(
    apply_(
        reduce_(['sample_name','library'], misc.FastQC),
        reduce_(['sample_name','library','platform','platform_unit','chunk'],bwa.MEM)
    ),
    map_(picard.AddOrReplaceReadGroups),
    map_(picard.CLEAN_SAM),
)


##############################################
# Mark Duplicates
#   Input parallelized by
#   sample_name/library/platform
#   /platform_unit/chunk
##############################################

sort_and_mark_duplicates = sequence_(
    map_(picard.SORT_BAM),
    map_(picard.INDEX_BAM, 'Index Cleaned BAMs'),
    reduce_(['sample_name'], picard.MARK_DUPES),
    map_(picard.INDEX_BAM, 'Index Deduped')
)


##############################################
# PreProcess Alignment
#   Input parallelized by sample_name
##############################################

preprocess_alignment = sequence_(
    apply_(
        reduce_(['sample_name'], picard.CollectMultipleMetrics),
        split_([intervals], gatk.BQSR)
    ),
    reduce_(['sample_name'], gatk.BQSRGatherer),
    branch_([gatk.BQSR.name]),
    map_(gatk.ApplyBQSR),
    map_(gatk.RealignerTargetCreator),
    map_(gatk.IR),
)

##############################################
# Call Variants
#   Input parallelized by sample_name/interval
##############################################

call_variants = sequence_(
    combine_(
        sequence_(
            map_(gatk.HaplotypeCaller,tag={'input_vcf':'HaplotypeCaller'}),
        ),
        sequence_(
            split_([glm], gatk.UnifiedGenotyper),
            reduce_([], gatk.CV, 'Combine UG Results into Raw vcfs',tag={'input_vcf':'UnifiedGenotyper'}),
        )
    ),
    reduce_split_(['input_vcf'],[glm],gatk.VQSR),
    map_(gatk.Apply_VQSR),
    reduce_(['input_vcf'], gatk.CV, "Combine into Recalibrated Master HC and UG vcfs")
)

##############################################
# The Pipeline
#   Combine all subworkflows
##############################################

ThePipeline = sequence_(
    alignment,
    sort_and_mark_duplicates,
    preprocess_alignment,
    call_variants,
    massive_annotation
)