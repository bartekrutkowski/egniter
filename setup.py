#!/usr/bin/env python

from os.path import join
from setuptools import setup, find_packages

setup(name='egniter',
      version='1.0',
      description='Egniter is a command line tool for easy launching VMWare' +
                  'ESX virtual machines using ESX API',
      author='',
      author_email='',
      license='BSD3',
      url='https://github.com/bartekrutkowski/egniter',
      packages=find_packages(),
      entry_points = {
          'console_scripts': ['egniter = egniter:main',]
      },
      install_requires=['configparser', 'pysphere'],
)

