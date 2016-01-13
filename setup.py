#!/usr/bin/env python

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

from datablimp import __version__ as version
from datablimp import __doc__ as long_description


setup(
    name='datablimp',
    version=version,

    author='Andrew Grigorev',
    author_email='andrew@ei-grad.ru',

    description='ETL framework with bolder approach',
    long_description=long_description,
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers'
    ],

    install_requires=[
        'pytz',
        'sqlalchemy',
    ],
)
