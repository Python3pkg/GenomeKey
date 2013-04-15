from cosmos.contrib.ezflow.dag import DAG, Map, Reduce, Split, ReduceSplit, Add
from subprocess import Popen,PIPE
from genomekey.tools import annotation

def get_db_names():
    cmd = 'annovarext listdbs'
    dbs = Popen(cmd.split(' '),stdout=PIPE).communicate()[0]
    if len(dbs) < 10:
        raise Exception, "could not list databases"
    return [ db for db in dbs.split('\n') if db != '' ]


def anno(dag):
    """
    Annotates with all databases in annovar extensions
    """
    ( dag
      |Split| ( [('build',['hg19']),('dbname',get_db_names()) ],
                annotation.Anno )
      |Reduce| (['input'],annotation.MergeAnno)
    )

def downdbs(dag):
    dag |Add| [ annotation.DownDB(tags={'build':'hg19','dbname':db}) for db in get_db_names() ]

