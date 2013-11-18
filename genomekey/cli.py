import argparse
import json
import sys
import glob, os
import pprint
from cosmos.lib.ezflow.dag import DAG,add_,configure,add_run, map_
from cosmos.lib.ezflow.tool import INPUT
from cosmos.Workflow import cli
from cosmos.config import settings
from cosmos import session

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
    inputs = [ INPUT(name='fastq.gz',path=i['path'],fmt='fastq.gz',tags=i,stage_name='Load Input Fastqs') for i in input_json ]

    DAG(ignore_stage_name_collisions=True).sequence_(
         add_(inputs),
         Pipeline(),
         configure(wga_settings),
         add_run(workflow)
    )


def bam(workflow,input_bam,input_bam_list,**kwargs):
    """
    Input file is a bam with properly annotated readgroups.

    *** Note that this workflow assumes the bam header is    ***
    *** also properly annotated with the correct readgroups! ***

    Example usage:
    $ genomekey bam -n 'Bam to VCF Workflow 1' input_bam.bam

    $ echo "dir/sample1.bam" > /tmp/bam.list
    $ echo "dir/sample2.bam" >> /tmp/bam.list
    $ genomekey bam -n 'Bam to VCF 2' -li /tmp/bam.list

    """
    # capture and pedigree_file are used in main()

    input_bams = input_bam_list.read().strip().split('\n') if input_bam_list else []
    if input_bam:
        input_bams.append(input_bam.name)

    if len(input_bams) == 0:
        raise WorkflowException, 'At least 1 BAM input required'

    dag = DAG(ignore_stage_name_collisions=True)
    Bam2BWA(workflow,dag,wga_settings,input_bams)
    #exit
    dag.sequence_(
        Pipeline2(),
        configure(wga_settings),
        add_run(workflow)
    )
    


###############################
# Annotation
###############################


def downdbs(workflow,**kwargs):
    """
    Download all annotation databases
    """
    DAG().sequence_(
        add_([ annovarext.DownDB(tags={'build':'hg19','dbname':db}) for db in get_db_names() ]),
        configure(wga_settings),
    ).add_to_workflow(workflow)

    workflow.run(terminate_on_fail=False)


def anno(workflow,input_file,input_file_list,file_format='vcf',**kwargs):
    """
    Annotates all files in input_Files

    $ genomekey anno -n 'My Annotation Workflow #1' file1.vcf file2.vcf
    """
    input_files = input_file_list.read().strip().split('\n') if input_file_list else []
    if input_file:
        input_files.append(input_file.name)
    print >> sys.stderr, 'annotating {0}'.format(', '.join(input_files))

    DAG().sequence_(
        add_([ INPUT(input_file,tags={'vcf':i}) for i,input_file in enumerate(input_files) ]),
        massive_annotation,
        configure(wga_settings),
        add_run(workflow)
    )


###############################
# Utils
###############################

def gunzip(workflow,input_dir,**kwargs):
    """
    Gunzips all gz files in directory

    $ genomekey gunzip -n 'Gunzip' /path/to/dir
    """
    DAG().sequence_(
         add_([ INPUT(f,tags={'i':i}) for i,f in enumerate(glob.glob(os.path.join(input_dir,'*.gz'))) ]),
         map_(unix.Gunzip),
         add_run(workflow)
    )

###############################
# CLI Configuration
###############################

def main():
    from argparse import RawTextHelpFormatter
    parser = argparse.ArgumentParser(description='WGA')
    parser.add_argument('-test',action='store_true',default=False,help='Signifies this as a test run')
    # parser.add_argument('-cp','--cProfile',type=str,default=None,help='output cprofile information to a file')
    parser.add_argument('-lustre',action='store_true',default=False,help='submits to erik\'s special orchestra cluster')
    parser.add_argument('-tmp','--temp_directory',type=str,default=settings['working_directory'],
                        help='Specify a wga_settings[tmp_dir].  Defaults to working_directory specified in cosmos.ini')
    subparsers = parser.add_subparsers(title="Commands", metavar="<command>")

    json_sp = subparsers.add_parser('json',help="Input is FASTQs, encoded as a json file",description=json_.__doc__,formatter_class=RawTextHelpFormatter)

    json_sp.set_defaults(func=json_)
    cli.add_workflow_args(json_sp)
    json_sp.add_argument('-il','--input_dict',type=str,help='Inputs, see script comments for format.',required=True)
    json_sp.add_argument('-ped','--pedigree_file',type=file,help='A Pedigree File to pass to all GATK tools')
    json_sp.add_argument('-capture','--capture',action="store_true",default=False,help='Signifies that a capture technology was used.  Currently'
                                                                                       'all this does is remove -an DP to VQSR')

    bam_sp = subparsers.add_parser('bam',help="Input is a BAM or list of BAMs",description=bam.__doc__,formatter_class=RawTextHelpFormatter)
    cli.add_workflow_args(bam_sp)
    bam_sp.add_argument('-i', '--input_bam',type=file,help='A path to a BAM with RGs properly annotated')
    bam_sp.add_argument('-il','--input_bam_list',type=file,help='A path to a file containing a list of paths to BAMs, separated by newlines')
    bam_sp.add_argument('-ped','--pedigree_file',type=file,help='A Pedigree File to pass to all GATK tools')
    bam_sp.add_argument('-capture','--capture',action="store_true",default=False,help='Signifies that a capture technology was used.  Currently all this does is remove -an DP to VQSR')
    bam_sp.set_defaults(func=bam)

    downdbs_sp = subparsers.add_parser('downdbs',help=downdbs.__doc__)
    cli.add_workflow_args(downdbs_sp)
    downdbs_sp.set_defaults(func=downdbs)

    anno_sp = subparsers.add_parser('anno',help="Annotate",description=anno.__doc__,formatter_class=RawTextHelpFormatter)
    cli.add_workflow_args(anno_sp)
    anno_sp.add_argument('-f','--file_format',type=str,default='vcf',help='vcf or tsv.  If tsv: Input file is already a tsv file with ID as the 5th column')
    anno_sp.add_argument('-i','--input_file',type=file,help='An input file')
    anno_sp.add_argument('-il','--input_file_list',type=file,help="A file with a list of input_files, separated by newlines")
    anno_sp.set_defaults(func=anno)

    sp = subparsers.add_parser('gunzip',help="Gunzips all files in a dir",description=gunzip.__doc__,formatter_class=RawTextHelpFormatter)
    cli.add_workflow_args(sp)
    sp.add_argument('input_dir',type=str,help='An input directory')
    sp.set_defaults(func=gunzip)

    wf,kwargs = cli.parse_args(parser)
    wga_settings['test']    = kwargs['test']
    wga_settings['lustre']  = kwargs['lustre']
    wga_settings['tmp_dir'] = kwargs.get('temp_directory')
    wga_settings['capture'] = kwargs.get('capture',None)

    ped_file = kwargs.get('pedigree',None)

    wga_settings['pedigree'] = ped_file.name if ped_file else None

    wf.log.info('wga_settings =\n{0}'.format(pprint.pformat(wga_settings,indent=2)))

    # cp_path = kwargs.pop('cProfile',None)
    # if False and cp_path:
    #     import cProfile
    #     cProfile.run("kwargs['func'](wf,**kwargs)",cp_path)
    # else:
    kwargs['func'](wf,**kwargs)

if __name__ == '__main__':
    main()

from genomekey.workflows.pipeline2 import Pipeline,Pipeline2
from genomekey.workflows.annotate import massive_annotation, get_db_names
from genomekey.workflows.bam2fastq2 import Bam2Fastq, Bam2BWA
from genomekey.tools import annovarext
from genomekey.tools import unix
from genomekey.wga_settings import wga_settings

session.get_drmaa_native_specification = wga_settings['get_drmaa_native_specification']
