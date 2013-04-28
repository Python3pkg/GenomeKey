from genomekey.tools import gatk,picard,bwa

def GATK_Best_Practices(dag,wga_settings):
    """
    maps GATK best practices to dag's last_tools
    """

    parameters = {
        # 'ALN': { 'q': 5 },
    }

    # Tags
    intervals = ('interval',range(1,23)+['X','Y'])
    glm = ('glm',['SNP','INDEL'])
    dbs = ('database',['1000G','PolyPhen2','COSMIC','ENCODE'])

    (dag.
        Reduce(['sample_name','library','platform','platform_unit','chunk'],bwa.MEM).
        Map(picard.AddOrReplaceReadGroups).
        Map(picard.CLEAN_SAM).
        Map(picard.SORT_BAM).
        Map(picard.INDEX_BAM,'Index Cleaned BAMs').
        Map(gatk.BQSR).
        Reduce(['sample_name'],gatk.BQSRGatherer).
        branch([gatk.BQSR.name]).
        Map(gatk.PR).
        Map(picard.MARK_DUPES).
        Map(picard.INDEX_BAM,'Index Deduped').
        Split([intervals],gatk.RTC).
        Map(gatk.IR).
        ReduceSplit([],[glm,intervals], gatk.UG).
        Reduce(['glm'],gatk.CV,'Combine into SNP and INDEL vcfs').
        Map(gatk.VQSR).
        Map(gatk.Apply_VQSR).
        Reduce([],gatk.CV,"Combine into Master vcf")
    )
    dag.configure(wga_settings,parameters)
