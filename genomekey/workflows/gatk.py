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
        reduce(['sample_name','library','platform','platform_unit','chunk'],bwa.MEM).
        map(picard.AddOrReplaceReadGroups).
        map(picard.CLEAN_SAM).
        map(picard.SORT_BAM).
        map(picard.INDEX_BAM,'Index Cleaned BAMs').
        map(gatk.BQSR).
        reduce(['sample_name'],gatk.BQSRGatherer).
        branch([gatk.BQSR.name]).
        map(gatk.PR).
        map(picard.MARK_DUPES).
        map(picard.INDEX_BAM,'Index Deduped').
        reduce_split(['sample_name'],[intervals],gatk.RTC).
        map(gatk.IR).
        reduce_split([],[glm,intervals], gatk.UG).
        reduce(['glm'],gatk.CV,'Combine into SNP and INDEL vcfs').
        map(gatk.VQSR).
        map(gatk.Apply_VQSR).
        reduce([],gatk.CV,"Combine into Master vcf")
    )
    dag.configure(wga_settings,parameters)
