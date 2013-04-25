import argparse
import json
import sys

from cosmos.contrib.ezflow.dag import DAG,Add,Map
from cosmos.contrib.ezflow.tool import INPUT
from genomekey.workflows.gatk import GATK_Best_Practices
from genomekey.workflows import annotate
from genomekey.workflows.bam2fastq import Bam2Fastq
from cosmos.Workflow.cli import CLI
from cosmos.Workflow.models import TaskFile, Workflow
from wga_settings import wga_settings
from cosmos import session

session.get_drmaa_native_specification = wga_settings['get_drmaa_native_specification']

###############################
# Alignment and Variant Calling
###############################

def json_(workflow,input_dict,**kwargs):
    """
    Input file is a json of the following format:

    [
        {
            'chunk': 001,
            'library': 'LIB-1216301779A',
            'sample_name': '1216301779A',
            'platform': 'ILLUMINA',
            'platform_unit': 'C0MR3ACXX.001'
            'pair': 0, #0 or 1
            'path': '/path/to/fastq'
        },
        {..}
    ]
    """
    input_json = json.load(open(input_dict,'r'))
    inputs = [ INPUT(name='fastq.gz',path=i['path'],fmt='fastq.gz',tags=i) for i in input_json ]

    #Create DAG
    dag = DAG(mem_req_factor=1) |Add| inputs
    GATK_Best_Practices(dag,wga_settings)
    dag.create_dag_img('/tmp/graph.svg')

    dag.add_run(workflow)

def bam(workflow,input_bam,input_bams,**kwargs):
    """
    Input file is a bam with properly annotated readgroups.

    *** Note that this workflow assumes the bam header is ******
    *** also properly annotated with the correct readgroups! ***

    Example usage:
    genomekey bam -n 'Bam to VCF Workflow 1' input_bam.bam input_bam2.bam input_bam3.bam

    """
    input_bams.append(input_bam)
    dag = DAG()
    Bam2Fastq(workflow,dag,wga_settings,input_bams)
    GATK_Best_Practices(dag,wga_settings)
    dag.add_run(workflow)


###############################
# Annotation
###############################


def downdbs(workflow,**kwargs):
    """
    Download all annotation databases
    """
    dag = DAG()
    annotate.downdbs(dag)
    dag.add_run(workflow)


def anno(workflow,input_file,input_files,file_format='vcf',**kwargs):
    """
    Annotates all files in input_Files

    $ genomekey anno -n 'My Annotation Workflow #1' file1.vcf file2.vcf
    """
    input_files.append(input_file)
    print >> sys.stderr, 'annotating {0}'.format(', '.join(input_files))

    dag = DAG() |Add| [ INPUT(input_file,tags={'input':i}) for i,input_file in enumerate(input_files) ]
    annotate.anno(dag,file_format=file_format)
    dag.add_run(workflow)


###############################
# CLI Configuration
###############################


def main():
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(description='WGA')
    subparsers = parser.add_subparsers(title="Commands", metavar="<command>")

    json_sp = subparsers.add_parser('json',help="Input is FASTQs, encoded as a json file",description=json_.__doc__,formatter_class=RawTextHelpFormatter)
    CLI.add_default_args(json_sp)
    json_sp.set_defaults(func=json_)
    json_sp.add_argument('-i','--input_dict',type=str,help='Inputs, see script comments for format.',required=True)

    bam_sp = subparsers.add_parser('bam',help="",description=bam.__doc__,formatter_class=RawTextHelpFormatter)
    CLI.add_default_args(bam_sp)
    bam_sp.add_argument('input_bam')
    bam_sp.add_argument('input_bams',type=str,help="Any number of input files",nargs=argparse.REMAINDER)
    bam_sp.set_defaults(func=bam)

    downdbs_sp = subparsers.add_parser('downdbs',help=downdbs.__doc__)
    CLI.add_default_args(downdbs_sp)
    downdbs_sp.set_defaults(func=downdbs)

    anno_sp = subparsers.add_parser('anno',help="Annotate",description=anno.__doc__,formatter_class=RawTextHelpFormatter)
    CLI.add_default_args(anno_sp)
    anno_sp.add_argument('-f','--file_format',type=str,default='vcf',help='vcf or tsv.  If tsv: Input file is already a tsv file with ID as the 5th column')
    anno_sp.add_argument('input_file',type=str,help='An input file')
    anno_sp.add_argument('input_files',type=str,help="Any number of input files",nargs=argparse.REMAINDER)
    anno_sp.set_defaults(func=anno)

    a = parser.parse_args()
    kwargs = dict(a._get_kwargs())
    del kwargs['func']
    wf = Workflow.start(**kwargs)
    a.func(workflow=wf,**kwargs)

if __name__ == '__main__':
    main()
