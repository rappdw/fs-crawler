#!/usr/bin/env python3

from os import path
from setuptools import setup, find_packages
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
      url='https://github.com/rappdw/fs-crawler',
      license='GPLv3+',
      packages=find_packages(exclude=['tests*']),
      classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'Topic :: Software Development :: Version Control :: Git',
            'Topic :: Software Development :: Libraries :: Python Modules',
            'License :: OSI Approved :: GNU General Public License v3 or later (GPLv3+)',
            'Programming Language :: Python :: 3.5',
            'Programming Language :: Python :: 3.6',
            'Programming Language :: Python :: 3.7',
            'Programming Language :: Python :: 3.8',
      ],
      platforms=["Windows", "Linux", "Mac OS-X"],
      install_requires=[
            'httpx',
            'keyring',
            'tqdm',
            'iteration_utilities'
      ],
      extras_require={
            'dev': [
                  'wheel>=0.29'
            ],
            'test': [
                  'asynctest>=0.13',
                  'pytest>=6.2',
                  'pytest-asyncio>=0.12',
                  'pytest-cov>=2.10',
                  'pytest-httpx>=0.8',
            ],
      },
      entry_points={
            'console_scripts': [
                  'crawl-fs = fscrawler.crawler:main',
                  'validate-fs = fscrawler.validator:main'
            ],
      }
)
