from distutils.core import setup
from setuptools import find_packages

__version__ = '0.9'

README = open('README.rst').read()

setup(name='GenomeKey',
      version=__version__,
      description = "Next-generation Sequencing Analysis Pipeline",
      license='Non-commercial',
      long_description=README,
      packages=find_packages(),
      scripts=['bin/genomekey'],
      install_requires=['pysam','ipdb']
)
