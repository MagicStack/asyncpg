# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import os
import os.path
import platform
import sys

import setuptools
from setuptools.command import build_ext as _build_ext


if sys.version_info < (3, 5):
    raise RuntimeError('asyncpg requires Python 3.5 or greater')

CFLAGS = ['-O2']
LDFLAGS = []

if platform.uname().system != 'Windows':
    CFLAGS.extend(['-fsigned-char', '-Wall', '-Wsign-compare', '-Wconversion'])


class build_ext(_build_ext.build_ext):
    user_options = _build_ext.build_ext.user_options + [
        ('cython-always', None,
            'run cythonize() even if .c files are present'),
        ('cython-annotate', None,
            'Produce a colorized HTML version of the Cython source.'),
        ('cython-directives=', None,
            'Cython compiler directives'),
    ]

    def initialize_options(self):
        # initialize_options() may be called multiple times on the
        # same command object, so make sure not to override previously
        # set options.
        if getattr(self, '_initialized', False):
            return

        super(build_ext, self).initialize_options()
        self.cython_always = False
        self.cython_annotate = None
        self.cython_directives = None

    def finalize_options(self):
        # finalize_options() may be called multiple times on the
        # same command object, so make sure not to override previously
        # set options.
        if getattr(self, '_initialized', False):
            return

        need_cythonize = self.cython_always
        cfiles = {}

        for extension in self.distribution.ext_modules:
            for i, sfile in enumerate(extension.sources):
                if sfile.endswith('.pyx'):
                    prefix, ext = os.path.splitext(sfile)
                    cfile = prefix + '.c'

                    if os.path.exists(cfile) and not self.cython_always:
                        extension.sources[i] = cfile
                    else:
                        if os.path.exists(cfile):
                            cfiles[cfile] = os.path.getmtime(cfile)
                        else:
                            cfiles[cfile] = 0
                        need_cythonize = True

        if need_cythonize:
            try:
                import Cython
            except ImportError:
                raise RuntimeError(
                    'please install Cython to compile asyncpg from source')

            if Cython.__version__ < '0.24':
                raise RuntimeError(
                    'asyncpg requires Cython version 0.24 or greater')

            from Cython.Build import cythonize

            directives = {}
            if self.cython_directives:
                for directive in self.cython_directives.split(','):
                    k, _, v = directive.partition('=')
                    if v.lower() == 'false':
                        v = False
                    if v.lower() == 'true':
                        v = True

                    directives[k] = v

            self.distribution.ext_modules[:] = cythonize(
                self.distribution.ext_modules,
                compiler_directives=directives,
                annotate=self.cython_annotate)

        super(build_ext, self).finalize_options()


with open(os.path.join(os.path.dirname(__file__), 'README.rst')) as f:
    readme = f.read()


with open(os.path.join(
        os.path.dirname(__file__), 'asyncpg', '__init__.py')) as f:
    for line in f:
        if line.startswith('__version__ ='):
            _, _, version = line.partition('=')
            VERSION = version.strip(" \n'\"")
            break
    else:
        raise RuntimeError(
            'unable to read the version from asyncpg/__init__.py')


setuptools.setup(
    name='asyncpg',
    version=VERSION,
    description='An asyncio PosgtreSQL driver',
    long_description=readme,
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Framework :: AsyncIO',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Development Status :: 5 - Production/Stable',
    ],
    platforms=['POSIX'],
    author='MagicStack Inc',
    author_email='hello@magic.io',
    url='https://github.com/MagicStack/asyncpg',
    license='Apache License, Version 2.0',
    packages=['asyncpg'],
    provides=['asyncpg'],
    include_package_data=True,
    ext_modules=[
        setuptools.Extension(
            "asyncpg.protocol.protocol",
            ["asyncpg/protocol/record/recordobj.c",
             "asyncpg/protocol/protocol.pyx"],
            extra_compile_args=CFLAGS,
            extra_link_args=LDFLAGS)
    ],
    cmdclass={'build_ext': build_ext},
    test_suite='tests.suite',
)
