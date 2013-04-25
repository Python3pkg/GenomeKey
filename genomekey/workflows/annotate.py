from cosmos.contrib.ezflow.dag import DAG, Map, Reduce, Split, ReduceSplit, Add
from subprocess import Popen,PIPE
from genomekey.tools import annotation
import sys

def get_db_names():
    cmd = 'annovarext listdbs'
    dbs = Popen(cmd.split(' '),stdout=PIPE).communicate()[0]
    if len(dbs) < 10:
        raise Exception, "could not list databases"
    return [ db for db in dbs.split('\n') if db != '' ]


def anno(dag,file_format):
    """
    Annotates with all databases in annovar extensions
    """
    if not file_format in ['vcf','tsv']:
        print >> sys.stderr, 'file_format "{0}" not supported'.format(file_format)
        sys.exit(1)

    if file_format == 'vcf':
        dag |Map| annotation.Vcf2Anno_in

    (dag
      |Split| ( [('build',['hg19']),('dbname',get_db_names()) ],
                annotation.Anno )
      |Reduce| (['input'],annotation.MergeAnnotations)
    )

def downdbs(dag):
    dag |Add| [ annotation.DownDB(tags={'build':'hg19','dbname':db}) for db in get_db_names() ]

