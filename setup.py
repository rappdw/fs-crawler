#!/usr/bin/env python3

from os import path
from setuptools import setup

package = 'fs-rgb-crawler'
version = '0.1'

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name=package,
      version=version,
      description='FamilySearch Crawler - extracts files for RedBlackGraph ingestion',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='Daniel Rapp',
      author_email='rappdw@gmail.com',
      url='https://github.com/rappdw/fs-rgb-crawler',
      install_requires=[
            'diskcache',
            'requests'
      ],
      entry_points={
            'console_scripts': ['getmyancestors=fsb_rgb_crawler.crawler:main'],
      }
)