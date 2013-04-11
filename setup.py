from distutils.core import setup
from setuptools import find_packages
from genomekey import __version__

README = open('README.rst').read()

setup(name='GenomeKey',
      version=__version__,
      description = "Whole Genome Analysis Pipeline",
      author='Erik Gafni',
      license='Non-commercial',
      long_description=README,
      packages=find_packages(),
      scripts=['bin/genomekey'],
      install_requires=[
          'cosmos',
      ]
)