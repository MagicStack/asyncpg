# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


from setuptools import setup, Extension


setup(
    name='asyncpg',
    version='0.0.1',
    description='An asyncio PosgtreSQL driver',
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Development Status :: 4 - Beta',
    ],
    platforms=['POSIX'],
    author='MagicStack Inc',
    author_email='hello@magic.io',
    license='MIT',
    packages=['asyncpg'],
    provides=['asyncpg'],
    include_package_data=True,
    ext_modules=[
        Extension("asyncpg.protocol.protocol",
                  ["asyncpg/protocol/record/recordobj.c",
                   "asyncpg/protocol/protocol.c"],
                  extra_compile_args=['-O2'])
    ]
)
