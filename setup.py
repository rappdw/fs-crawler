#!/usr/bin/env python3

from os import path
from setuptools import setup
import versioneer

package = 'fs-crawler'

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(name=package,
      version=versioneer.get_version(),
      cmdclass=versioneer.get_cmdclass(),
      description='FamilySearch Crawler - extracts files for RedBlackGraph ingestion',
      long_description=long_description,
      long_description_content_type="text/markdown",
      author='Daniel Rapp',
      author_email='rappdw@gmail.com',
      url='https://github.com/rappdw/fs-rgb-crawler',
      license='GPLv3+',
      classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'Topic :: Software Development :: Version Control :: Git',
            'Topic :: Software Development :: Libraries :: Python Modules',
            'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
      ],
      platforms=["Windows", "Linux", "Mac OS-X"],
      install_requires=[
            'requests'
      ],
      entry_points={
            'console_scripts': [
                  'crawl-fs = fscrawler.crawler:main'
            ],
      }
)