Whole Genome Analysis Pipeline
===============================

* (BWA + GATK Best Practices v4) Cosmos workflow
* (AnnovarExtensions annotation) Cosmos workflow
* VarDB - Variant Database Warehouse

Install
=======

1) Install Cosmos using virtualenvwrapper

2) Clone git@github.com:egafni/GenomeKey.git

3) Activate Cosmos vrtualenv

    $ workon cosmos

4) Add GenomeKey to your PYTHONPATH when you're in the cosmos virtualenv

    add2virtualenv /path/to/GenomeKey

Usage
======

GenomeKey/bin/genomekey -h

*Examples*:

    genomekey bam -n "My workflow from bam" -i '/path/to/bam'

    genomekey json -n "My workflow from a json file" -i '/path/to/json'

.. code-block:: json

    [
        {
            'lane': 001,
            'chunk': 001,
            'library': 'LIB-1216301779A',
            'sample': '1216301779A',
            'platform': 'ILLUMINA',
            'flowcell': 'C0MR3ACXX'
            'pair': 0, #0 or 1
            'path': '/path/to/fastq'
        },
        {..}
    ]