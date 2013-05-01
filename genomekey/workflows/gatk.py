from genomekey.tools import gatk, picard, bwa, misc
from cosmos.contrib.ezflow.dag import add_,map_,reduce_,split_,reduceSplit_,combine_,sequence_,branch_,apply_
from genomekey.wga_settings import settings

# Split Tags
intervals = ('interval',range(1,23) + ['X','Y']) #if not settings['test'] else ('interval',[20])
glm = ('glm',['SNP','INDEL'])

map_and_align = sequence_(
    apply_(
        reduce_(['sample_name'], misc.FastQC),
        reduce_(['sample_name','library','platform','platform_unit','chunk'],bwa.MEM)
    ),
    map_(picard.AddOrReplaceReadGroups),
    map_(picard.CLEAN_SAM),
)

sort_and_mark_duplicates = sequence_(
    map_(picard.SORT_BAM),
    map_(picard.INDEX_BAM, 'Index Cleaned BAMs'),
    reduce_(['sample_name'], picard.MARK_DUPES),
    map_(picard.INDEX_BAM, 'Index Deduped')
)

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


call_variants = sequence_(
    combine_(
        sequence_(
            split_([glm], gatk.UnifiedGenotyper,tag={'input_vcf':'UnifiedGenotyper'}),
            reduce_(['input_vcf','glm'], gatk.CV, 'Combine into SNP and INDEL vcfs'),
            map_(gatk.VQSR)
        ),sequence_(
            map_(gatk.HaplotypeCaller,tag={'input_vcf':'HaplotypeCaller'}),
            reduce_(['input_vcf','glm'], gatk.CV, 'Combine into SNP and INDEL vcfs'),
            split_([glm],gatk.VQSR),
        )
    ),
    map_(gatk.Apply_VQSR),
    reduce_(['input_vcf'], gatk.CV, "Combine into Master vcfs")
)
