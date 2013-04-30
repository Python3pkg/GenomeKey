import argparse
import json
import sys
import glob, os

from cosmos.contrib.ezflow.dag import DAG
from cosmos.contrib.ezflow.tool import INPUT
from cosmos.Workflow.cli import CLI

from genomekey.workflows.gatk import GATK_Best_Practices
from genomekey.workflows.annotate import DownDBs,AnnovarExtensions
from genomekey.workflows.bam2fastq import Bam2Fastq
from genomekey.tools import unix
from wga_settings import wga_settings
from cosmos import session

session.get_drmaa_native_specification = wga_settings['get_drmaa_native_specification']

###############################
# Alignment and Variant Calling
###############################

def json_(workflow,input_dict,capture,**kwargs):
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
    wga_settings['capture']=capture
    input_json = json.load(open(input_dict,'r'))
    inputs = [ INPUT(name='fastq.gz',path=i['path'],fmt='fastq.gz',tags=i,stage_name='Load Input Fastqs') for i in input_json ]

    #Create DAG
    dag = DAG(mem_req_factor=1).add(inputs)
    GATK_Best_Practices(dag,wga_settings,{})
    AnnovarExtensions(dag,file_format='vcf')
    dag.create_dag_img('/tmp/graph.svg')

    dag.add_run(workflow)

def bam(workflow,input_bam,input_bam_list,capture,**kwargs):
    """
    Input file is a bam with properly annotated readgroups.

    *** Note that this workflow assumes the bam header is ******
    *** also properly annotated with the correct readgroups! ***

    Example usage:
    $ genomekey bam -n 'Bam to VCF Workflow 1' input_bam.bam

    $ echo "dir/sample1.bam" > /tmp/bam.list
    $ echo "dir/sample2.bam" >> /tmp/bam.list
    $ genomekey bam -n 'Bam to VCF 2' -li /tmp/bam.list

    """
    wga_settings['capture'] = capture

    input_bams = input_bam_list.read().strip().split('\n') if input_bam_list else []
    if input_bam:
        input_bams.append(input_bam.name)
    dag = DAG()

    Bam2Fastq(workflow,dag,wga_settings,input_bams)
    GATK_Best_Practices(dag,wga_settings,{})
    AnnovarExtensions(dag,file_format='vcf',multi_input=False)

    dag.add_run(workflow)


###############################
# Annotation
###############################


def downdbs(workflow,**kwargs):
    """
    Download all annotation databases
    """
    dag = DAG()
    DownDBs(dag)
    dag.add_run(workflow)


def anno(workflow,input_file,input_file_list,file_format='vcf',**kwargs):
    """
    Annotates all files in input_Files

    $ genomekey anno -n 'My Annotation Workflow #1' file1.vcf file2.vcf
    """
    input_files = input_file_list.read().strip().split('\n') if input_file_list else []
    if input_file:
        input_files.append(input_file.name)
    print >> sys.stderr, 'annotating {0}'.format(', '.join(input_files))

    dag = DAG().add([ INPUT(input_file,tags={'input':i}) for i,input_file in enumerate(input_files) ])
    AnnovarExtensions(dag,file_format=file_format)
    dag.add_run(workflow)


###############################
# Utils
###############################

def gunzip(workflow,input_dir,**kwargs):
    """
    Gunzips all gz files in directory

    $ genomekey gunzip -n 'Gunzip' /path/to/dir
    """
    (DAG().
         add([ INPUT(f,tags={'i':i}) for i,f in enumerate(glob.glob(os.path.join(input_dir,'*.gz'))) ]).
         map(unix.Gunzip).
         add_run(workflow)
    )

###############################
# CLI Configuration
###############################


def main():
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(description='WGA')
    parser.add_argument('-test',action='store_true',default=False,help='signifies this as a test run')
    parser.add_argument('-test2',action='store_true',default=False,help='signifies this as a test2 run')
    subparsers = parser.add_subparsers(title="Commands", metavar="<command>")

    json_sp = subparsers.add_parser('json',help="Input is FASTQs, encoded as a json file",description=json_.__doc__,formatter_class=RawTextHelpFormatter)

    json_sp.set_defaults(func=json_)
    CLI.add_workflow_args(json_sp)
    json_sp.add_argument('-i','--input_dict',type=str,help='Inputs, see script comments for format.',required=True)
    json_sp.add_argument('-capture','--capture',action="store_true",default=False,help='Signifies that a capture technology was used')

    bam_sp = subparsers.add_parser('bam',help="Input is a BAM or list of BAMs",description=bam.__doc__,formatter_class=RawTextHelpFormatter)
    CLI.add_workflow_args(bam_sp)
    bam_sp.add_argument('-i','--input_bam',type=file,help='A path to a BAM with RGs properly annotated')
    bam_sp.add_argument('-il','--input_bam_list',type=file,help='A path to a file containing a list of paths to BAMs, separated by newlines')
    bam_sp.add_argument('-capture','--capture',action="store_true",default=False,help='Signifies that a capture technology was used')
    bam_sp.set_defaults(func=bam)

    downdbs_sp = subparsers.add_parser('downdbs',help=downdbs.__doc__)
    CLI.add_workflow_args(downdbs_sp)
    downdbs_sp.set_defaults(func=downdbs)

    anno_sp = subparsers.add_parser('anno',help="Annotate",description=anno.__doc__,formatter_class=RawTextHelpFormatter)
    CLI.add_workflow_args(anno_sp)
    anno_sp.add_argument('-f','--file_format',type=str,default='vcf',help='vcf or tsv.  If tsv: Input file is already a tsv file with ID as the 5th column')
    anno_sp.add_argument('-i','--input_file',type=file,help='An input file')
    anno_sp.add_argument('-il','--input_file_list',type=file,help="A file with a list of input_files, separated by newlines")
    anno_sp.set_defaults(func=anno)

    sp = subparsers.add_parser('gunzip',help="Gunzips all files in a dir",description=gunzip.__doc__,formatter_class=RawTextHelpFormatter)
    CLI.add_workflow_args(sp)
    sp.add_argument('input_dir',type=str,help='An input directory')
    sp.set_defaults(func=gunzip)

    wf,kwargs = CLI.parse_args(parser)
    wga_settings['test'] = kwargs['test']
    wga_settings['test2'] = kwargs['test2']

    kwargs['func'](wf,**kwargs)

if __name__ == '__main__':
    main()
