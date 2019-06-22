# -*- coding: utf-8 -*-
from setuptools import find_packages
from setuptools import setup

setup(
    name='guillotina_s3storage',
    description='s3 guillotina storage support',
    version=open('VERSION').read().strip(),
    long_description=(open('README.rst').read() + '\n' +
                      open('CHANGELOG.rst').read()),
    classifiers=[
        'Programming Language :: Python :: 3.6',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],
    author='Ramon Navarro Bosch',
    author_email='ramon@plone.org',
    keywords='async aiohttp guillotina s3',
    url='https://pypi.python.org/pypi/guillotina_s3storage',
    license='GPL version 3',
    setup_requires=[
        'pytest-runner',
    ],
    zip_safe=True,
    include_package_data=True,
    packages=find_packages(exclude=['ez_setup']),
    install_requires=[
        'setuptools',
        'guillotina>=5.0.0a7',
        'aiohttp>3.0.0,<4.0.0',
        'ujson',
        'aiobotocore==0.9.4',
        'botocore==1.10.58',
        'backoff'
    ],
    extras_require={
        'test': [
            'pytest',
            'pytest-aiohttp',
            'pytest-docker-fixtures'
        ]
    }
)
