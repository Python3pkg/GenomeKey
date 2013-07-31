#!/usr/bin/env python
"""
Chunks a fastq file

TODO implement a producer/consumer pattern

.. note:: This is nicer than the unix split command because everything is gzipped which means less scratch space and i/o (at the cost of CPU)

.. note:: The closest unix split command equivalent is `split -a 3 -l 2 -d input.txt /tmp/input_`
"""

import re
import os
import logging as log
import gzip
import argparse

from itertools import islice
from cosmos.utils.helpers import confirm

def splitFastq(input_fastq,output_dir,chunksize,buffersize):
    """
    Chunks a large fastq file into smaller pieces.
    """
    chunk = 0
    log.info('Opening {0}'.format(input_fastq))

    ## output file type will be decided based on input format
    if input_fastq.endswith('.gz'):
        infile  = gzip.open(input_fastq)
        outType = '.fastq.gz'
    else:
        infile  = open(input_fastq, 'r')
        outType = '.fastq'

    output_prefix = os.path.basename(input_fastq)
    output_prefix = re.search("(.+?)(_001)*\.(fastq|fq)(\.gz)*",output_prefix).group(1)

    #write chunks
    while True:
        chunk += 1

        # generate output paths
        new_filename = '{0}_{1:0>3}'.format(output_prefix,chunk)
        output_path = os.path.join(output_dir, new_filename + outType)
        
        if os.path.exists(output_path) and not confirm('{0} already exists!  Are you sure you want to overwrite the file?'.format(output_path), timeout=0): return

        log.info('Reading {0} lines and writing to: {1}'.format(chunksize*4,output_path))
        if  outType == 'fastq.gz': outfile = gzip.open(output_path,'wb')
        else:                      outfile =      open(output_path,'wb')
        
        #Read/Write
        total_read=0
        while total_read < chunksize*4:
            data = list(islice(infile,buffersize*4)) #read data
            if len(data) == 0:
                log.info('Done')
                return

            # Write what was read
            outfile.writelines(data)
            log.info('wrote {0} lines'.format(len(data)))
            del(data)
            total_read += buffersize*4
        outfile.close()
            
    infile.close()


if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser(description='SplitFastq')

    parser.add_argument('input_fastq'      , type=str, help='')
    parser.add_argument('output_dir'       , type=str, help='')
    parser.add_argument('-c','--chunksize' , type=int, help='Number of reads per fastq chunk, default is 1.5 million', default=15000000)
    parser.add_argument('-b','--buffersize', type=int, help='Number of reads to keep in RAM, default is 1M',           default= 1000000)

    parsed_args = parser.parse_args()
    kwargs      = dict(parsed_args._get_kwargs())
    splitFastq(**kwargs)


