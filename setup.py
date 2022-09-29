import os
from setuptools import setup

setup(name='pvm_processor',
      version='1.0',
      description='A module to translate PVM data files into timeseries and wave spectra datasets',
      url='https://github.com/cioos-atlantic/pvm_processor',
      author='Scott Bruce',
      author_email='scott.bruce@cioosatlantic.ca',
      license='GPL3',
      packages=[''],
      zip_safe=False,
      install_requires=open("requirements.txt").read().splitlines()
      )
