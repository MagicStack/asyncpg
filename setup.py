# Copyright (C) 2016-present the ayncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import os
import os.path
import platform
import re
import sys

import setuptools
from setuptools.command import build_ext as _build_ext


if sys.version_info < (3, 5):
    raise RuntimeError('asyncpg requires Python 3.5 or greater')


CFLAGS = ['-O2']
LDFLAGS = []

if platform.uname().system == 'Windows':
    LDFLAGS.append('ws2_32.lib')


class build_ext(_build_ext.build_ext):
    user_options = _build_ext.build_ext.user_options + [
        ('cython-always', None,
            'run cythonize() even if .c files are present'),
        ('cython-annotate', None,
            'Produce a colorized HTML version of the Cython source.'),
    ]

    def initialize_options(self):
        super(build_ext, self).initialize_options()
        self.cython_always = False
        self.cython_annotate = None

    def finalize_options(self):
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

            for cfile, timestamp in cfiles.items():
                if os.path.getmtime(cfile) != timestamp:
                    # The file was recompiled, patch
                    self._patch_cfile(cfile)

        super(build_ext, self).finalize_options()

    def _patch_cfile(self, cfile):
        # Script to patch Cython 'async def' coroutines to have a 'tp_iter'
        # slot, which makes them compatible with 'yield from' without the
        # `asyncio.coroutine` decorator.

        with open(cfile, 'rt') as f:
            src = f.read()

        src = re.sub(
            r'''
            \s* offsetof\(__pyx_CoroutineObject,\s*gi_weakreflist\),
            \s* 0,
            \s* 0,
            \s* __pyx_Coroutine_methods,
            \s* __pyx_Coroutine_memberlist,
            \s* __pyx_Coroutine_getsets,
            ''',

            r'''
            offsetof(__pyx_CoroutineObject, gi_weakreflist),
            __Pyx_Coroutine_await, /* tp_iter */
            0,
            __pyx_Coroutine_methods,
            __pyx_Coroutine_memberlist,
            __pyx_Coroutine_getsets,
            ''',

            src, flags=re.X)

        # Fix a segfault in Cython.
        src = re.sub(
            r'''
            \s* __Pyx_Coroutine_get_qualname\(__pyx_CoroutineObject\s+\*self\)
            \s* {
            \s* Py_INCREF\(self->gi_qualname\);
            ''',

            r'''
            __Pyx_Coroutine_get_qualname(__pyx_CoroutineObject *self)
            {
                if (self->gi_qualname == NULL) { return __pyx_empty_unicode; }
                Py_INCREF(self->gi_qualname);
            ''',

            src, flags=re.X)

        src = re.sub(
            r'''
            \s* __Pyx_Coroutine_get_name\(__pyx_CoroutineObject\s+\*self\)
            \s* {
            \s* Py_INCREF\(self->gi_name\);
            ''',

            r'''
            __Pyx_Coroutine_get_name(__pyx_CoroutineObject *self)
            {
                if (self->gi_name == NULL) { return __pyx_empty_unicode; }
                Py_INCREF(self->gi_name);
            ''',

            src, flags=re.X)

        with open(cfile, 'wt') as f:
            f.write(src)


setuptools.setup(
    name='asyncpg',
    version='0.6.3',
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
             "asyncpg/protocol/protocol.pyx"],
            extra_compile_args=CFLAGS,
            extra_link_args=LDFLAGS)
    ],
    cmdclass={'build_ext': build_ext},
)
