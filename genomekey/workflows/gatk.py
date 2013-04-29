from genomekey.tools import gatk, picard, bwa, misc

def GATK_Best_Practices(dag,wga_settings,parameters):
    """
    maps GATK best practices to dag's active_tools
    """

    # Split Tags
    intervals = ('interval',range(1,23)+['X','Y']) if not wga_settings['test'] else ('interval',[20])
    glm = ('glm',['SNP','INDEL'])

    (dag.
        reduce(['sample_name','library','platform','platform_unit','chunk'],bwa.MEM).
        map(picard.AddOrReplaceReadGroups).
        map(picard.CLEAN_SAM).
        map(picard.SORT_BAM).
        map(picard.INDEX_BAM, 'Index Cleaned BAMs').
        reduce(['sample_name'], picard.MARK_DUPES).
        map(picard.INDEX_BAM, 'Index Deduped').
        split([intervals], gatk.BQSR).
        reduce(['sample_name'], gatk.BQSRGatherer).
     branch([gatk.BQSR.name]). # note: back at sample_name/interval level
        map(gatk.ApplyBQSR).
        map(gatk.RTC).
        map(gatk.IR).
        reduce(['interval'], gatk.ReduceReads).
        split([glm], gatk.UG).
        reduce(['glm'], gatk.CV, 'Combine into SNP and INDEL vcfs').
        map(gatk.VQSR).
        map(gatk.Apply_VQSR).
        reduce([], gatk.CV, "Combine into Master vcf").
     branch(['Load Input Fastqs']).reduce(['sample_name'], misc.FastQC).
     branch([picard.MARK_DUPES.name]).reduce(['sample_name'], picard.CollectMultipleMetrics).
     branch(["Combine into Master vcf"])

    )
    dag.configure(wga_settings,parameters)
