# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import setuptools
import sys


if sys.version_info < (3, 5):
    raise RuntimeError('asyncpg requires Python 3.5 or greater')


setuptools.setup(
    name='asyncpg',
    version='0.5.2',
    description='An asyncio PosgtreSQL driver',
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Development Status :: 4 - Beta',
    ],
    platforms=['POSIX'],
    author='MagicStack Inc',
    author_email='hello@magic.io',
    license='Apache License, Version 2.0',
    packages=['asyncpg'],
    provides=['asyncpg'],
    include_package_data=True,
    ext_modules=[
        setuptools.Extension(
            "asyncpg.protocol.protocol",
            ["asyncpg/protocol/record/recordobj.c",
             "asyncpg/protocol/protocol.c"],
            extra_compile_args=['-O2'])
    ]
)
