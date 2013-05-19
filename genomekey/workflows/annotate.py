from cosmos.contrib.ezflow.dag import add_,map_,reduce_,split_,reduce_split_,sequence_,branch_
from subprocess import Popen,PIPE
from genomekey.tools import annovarext
from genomekey.wga_settings import wga_settings
import sys
import os

def get_db_names():
    cmd = '{0} listdbs'.format(wga_settings['annovarext_path'])
    if not os.path.exists(wga_settings['annovarext_path']):
        raise Exception, 'AnnovarExtensions is not installed at {0}'.format(wga_settings['annovarext_path'])
    dbs = Popen(cmd.split(' '),stdout=PIPE).communicate()[0]
    if len(dbs) < 10:
        raise Exception, "could not list databases, command was {0}".format(cmd)
    return [ db for db in dbs.split('\n') if db != '' ]

massive_annotation = sequence_(
    map_(annovarext.Vcf2Anno_in),
    split_( [('build',['hg19']),('dbname',get_db_names()) ], annovarext.Annotate ),
    reduce_(['vcf'],annovarext.MergeAnnotations)
)


