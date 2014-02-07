from distutils.core import setup
from setuptools import find_packages
from genomekey import __version__

README = open('README.rst').read()

setup(name='CteamKey',
      version=__version__,
      description = "Next-generation Sequencing Analysis Pipeline",
      license='Non-commercial',
      long_description=README,
      packages=find_packages(),
      scripts=['bin/cteamkey'],
      install_requires=['pysam','ipdb']
)
