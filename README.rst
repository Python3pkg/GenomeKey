GenomeKey
===============================

GenomeKey is a Whole Genome Analysis pipeline, that can call variants from FASTQ or BAM files, as well as massively
annotate VCF files.  It is implemented and made possible by the Cosmos workflow management system.

Components include:

* *BWA + GATK Best Practices v4* Cosmos workflow
* *AnnovarExtensions annotation* Cosmos workflow
* VarDB - Variant Database Warehouse.  Integration coming soon.

Install
=======

1) Install Cosmos using virtualenvwrapper

2) Clone git@github.com:egafni/GenomeKey.git

3) Activate Cosmos virtualenv

    $ workon cosmos

4) Add GenomeKey to your PYTHONPATH when you're in the cosmos virtualenv

    add2virtualenv /path/to/GenomeKey

Configuration
=============

After Cosmos is properly configured, edit GenomeKey/genomekey/wga_settings.py and make sure
it is pointing to the correct paths to the GATK bundle, reference genome, and binaries.

Usage
======

Inside the GenomeKey directory, execute:

$ bin/genomekey -h

*Examples*:

    genomekey bam -n "My Workflow from BAM" -i /path/to/bam1
    genomekey bam -n "My Multi-BAM Workflow" -il /path/to/bam.list

    genomekey json -n "My workflow from a JSON file" '/path/to/json'

.. code-block:: json

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

.. note::
    I have GenomeKey set to launch you into an ipdb post mortem debugging session on any exceptions.  That behavior is
    set in bin/genomekey.

    To quit type 'q' then enter

Testing
========

-test will inform genomekey you are running a test dataset.  drmaa_native_specification() will be adjusted
accordingly automatically for Orchestra, so that requests are sent to the mini queue with a cpu_requirement of 1

.. code-block:: bash

    $ genomekey -test -n 'Test GK' bam -il genomekey/test/bams.list
