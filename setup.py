#!/usr/bin/env python

from setuptools import setup, find_packages
import versioneer


requires = open('requirements.txt').read().strip().split('\n')

setup(
    name='intake-accumulo',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    description='Intake Accumulo plugin',
    url='https://github.com/ContinuumIO/intake-accumulo',
    maintainer='Joseph Crail',
    maintainer_email='jbcrail@gmail.com',
    license='BSD',
    packages=find_packages(),
    entry_points={
        'intake.drivers':
            ['accumulo = intake_accumulo.source:AccumuloSource',
        ]},
    package_data={'': ['*.yml', '*.html']},
    include_package_data=True,
    install_requires=requires,
    long_description=open('README.md').read(),
    zip_safe=False,
)
