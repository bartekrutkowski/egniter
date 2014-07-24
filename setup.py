#!/usr/bin/env python

from os.path import join
from setuptools import setup, find_packages

setup(name='egniter',
      version='0.1',
      description='Egniter is a command line tool for easy launching VMWare' +
                  'ESX virtual machines using ESX API',
      author='Bartek Rutkowski',
      author_email='contact+egniter@robakdesign.com',
      license='BSD3',
      url='https://github.com/bartekrutkowski/egniter',
      packages=find_packages(),
      entry_points = {
          'console_scripts': ['egniter = egniter:main',]
      },
      install_requires=['configparser>=3.3.0', 'pysphere>=0.1.8'],
)
