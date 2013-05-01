from cosmos.contrib.ezflow.dag import add_,map_,reduce_,split_,reduceSplit_,combine_,sequence_,branch_
from subprocess import Popen,PIPE
from genomekey.tools import annovarext

def get_db_names():
    cmd = 'annovarext listdbs'
    dbs = Popen(cmd.split(' '),stdout=PIPE).communicate()[0]
    if len(dbs) < 10:
        raise Exception, "could not list databases"
    return [ db for db in dbs.split('\n') if db != '' ]



massive_annotation = sequence_(
    map_(annovarext.Vcf2Anno_in),
    split_( [('build',['hg19']),('dbname',get_db_names()) ], annovarext.Annotate ),
    reduce_(['input_vcf'],annovarext.MergeAnnotations)
)


