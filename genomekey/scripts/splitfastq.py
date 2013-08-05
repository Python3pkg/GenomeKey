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
    log.info('Opening {0}'.format(input_fastq))

    ## output file type will be decided based on input format
    if input_fastq.endswith('.gz'):
        infile  = gzip.open(input_fastq, 'rb')
        outType = '.fastq.gz'
    else:
        infile  =      open(input_fastq, 'rb')
        outType = '.fastq'

    output_prefix = re.search("(.+?)(_001)*\.(fastq|fq)(\.gz)*", os.path.basename(input_fastq)).group(1)

    #write chunks
    chunk = 0
    eof   = False
    while not eof:
        chunk += 1

        # generate output paths
        new_filename = '{0}_{1:0>3}'.format(output_prefix,chunk)
        output_path = os.path.join(output_dir, new_filename + outType)
        
        if os.path.exists(output_path) and not confirm('{0} already exists!  Are you sure you want to overwrite the file?'.format(output_path), timeout=0): return

        log.info('Reading {0} lines and writing to: {1}'.format(chunksize, output_path))
        if  outType == 'fastq.gz': outfile = gzip.open(output_path,'wb')
        else:                      outfile =      open(output_path,'wb')
        
        #Read/Write
        total_read = 0
        while total_read < chunksize and not eof:
            data    = list(islice(infile, buffersize)) # read data
            dataLen = len(data)
            if dataLen != buffersize: eof = True       # last chunk of input file or 0
            if dataLen == 0: break
            
            # Write what was read
            outfile.writelines(data)
            log.info('wrote {0} lines'.format(dataLen))
            total_read += dataLen # might not be the same as bufferesize
            del(data)

        outfile.close()
            
    infile.close()
    log.info('Done')


if __name__ == '__main__':
    log.basicConfig(level=log.INFO, format='%(asctime)s %(levelname)s: %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

    parser = argparse.ArgumentParser(description='SplitFastq')

    parser.add_argument('input_fastq'      , type=str, help='')
    parser.add_argument('output_dir'       , type=str, help='')

    # Default chunk option will make about 10 split files from a given RG/Paired fastq of a full WG BAM.                
    parser.add_argument('-c','--chunksize' , type=int, help='Number of reads per fastq chunk, default is 32M', default=32000000) # keep with Erik's original setting
    parser.add_argument('-b','--buffersize', type=int, help='Number of reads to keep in RAM,  default is  4M', default= 4000000)

    parsed_args = parser.parse_args()
    kwargs      = dict(parsed_args._get_kwargs())
    splitFastq(**kwargs)


