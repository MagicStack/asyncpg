# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import sys

if sys.version_info < (3, 5):
    raise RuntimeError('asyncpg requires Python 3.5 or greater')

import os
import os.path
import pathlib
import platform
import re
import subprocess

# We use vanilla build_ext, to avoid importing Cython via
# the setuptools version.
from distutils import extension as distutils_extension
from distutils.command import build_ext as distutils_build_ext

import setuptools
from setuptools.command import build_py as setuptools_build_py
from setuptools.command import sdist as setuptools_sdist


CYTHON_DEPENDENCY = 'Cython==0.28.4'

# Minimal dependencies required to test asyncpg.
TEST_DEPENDENCIES = [
    'flake8~=3.5.0',
    'uvloop>=0.8.0;platform_system!="Windows"',
]

# Dependencies required to build documentation.
DOC_DEPENDENCIES = [
    'Sphinx~=1.7.3',
    'sphinxcontrib-asyncio~=0.2.0',
    'sphinx_rtd_theme~=0.2.4',
]

EXTRA_DEPENDENCIES = {
    'docs': DOC_DEPENDENCIES,
    'test': TEST_DEPENDENCIES,
    # Dependencies required to develop asyncpg.
    'dev': [
        CYTHON_DEPENDENCY,
        'pytest>=3.6.0',
    ] + DOC_DEPENDENCIES + TEST_DEPENDENCIES
}


CFLAGS = ['-O2']
LDFLAGS = []

if platform.uname().system != 'Windows':
    CFLAGS.extend(['-fsigned-char', '-Wall', '-Wsign-compare', '-Wconversion'])


_ROOT = pathlib.Path(__file__).parent


with open(str(_ROOT / 'README.rst')) as f:
    readme = f.read()


with open(str(_ROOT / 'asyncpg' / '__init__.py')) as f:
    for line in f:
        if line.startswith('__version__ ='):
            _, _, version = line.partition('=')
            VERSION = version.strip(" \n'\"")
            break
    else:
        raise RuntimeError(
            'unable to read the version from asyncpg/__init__.py')


if (_ROOT / '.git').is_dir() and 'dev' in VERSION:
    # This is a git checkout, use git to
    # generate a precise version.
    def git_commitish():
        env = {}
        v = os.environ.get('PATH')
        if v is not None:
            env['PATH'] = v

        git = subprocess.run(['git', 'rev-parse', 'HEAD'], env=env,
                             cwd=str(_ROOT), stdout=subprocess.PIPE)
        if git.returncode == 0:
            commitish = git.stdout.strip().decode('ascii')
        else:
            commitish = 'unknown'

        return commitish

    VERSION += '+' + git_commitish()[:7]


class VersionMixin:

    def _fix_version(self, filename):
        # Replace asyncpg.__version__ with the actual version
        # of the distribution (possibly inferred from git).

        with open(str(filename)) as f:
            content = f.read()

        version_re = r"(.*__version__\s*=\s*)'[^']+'(.*)"
        repl = r"\1'{}'\2".format(self.distribution.metadata.version)
        content = re.sub(version_re, repl, content)

        with open(str(filename), 'w') as f:
            f.write(content)


class sdist(setuptools_sdist.sdist, VersionMixin):

    def make_release_tree(self, base_dir, files):
        super().make_release_tree(base_dir, files)
        self._fix_version(pathlib.Path(base_dir) / 'asyncpg' / '__init__.py')


class build_py(setuptools_build_py.build_py, VersionMixin):

    def build_module(self, module, module_file, package):
        outfile, copied = super().build_module(module, module_file, package)

        if module == '__init__' and package == 'asyncpg':
            self._fix_version(outfile)

        return outfile, copied


class build_ext(distutils_build_ext.build_ext):

    user_options = distutils_build_ext.build_ext.user_options + [
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

        if os.environ.get('ASYNCPG_DEBUG'):
            self.cython_always = True
            self.cython_annotate = True
            self.cython_directives = "linetrace=True"
            self.define = 'ASYNCPG_DEBUG,CYTHON_TRACE,CYTHON_TRACE_NOGIL'
            self.debug = True
        else:
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
            import pkg_resources

            # Double check Cython presence in case setup_requires
            # didn't go into effect (most likely because someone
            # imported Cython before setup_requires injected the
            # correct egg into sys.path.
            try:
                import Cython
            except ImportError:
                raise RuntimeError(
                    'please install {} to compile asyncpg from source'.format(
                        CYTHON_DEPENDENCY))

            cython_dep = pkg_resources.Requirement.parse(CYTHON_DEPENDENCY)
            if Cython.__version__ not in cython_dep:
                raise RuntimeError(
                    'asyncpg requires {}, got Cython=={}'.format(
                        CYTHON_DEPENDENCY, Cython.__version__
                    ))

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


setup_requires = []

if (not (_ROOT / 'asyncpg' / 'protocol' / 'protocol.c').exists() or
        '--cython-always' in sys.argv):
    # No Cython output, require Cython to build.
    setup_requires.append(CYTHON_DEPENDENCY)


setuptools.setup(
    name='asyncpg',
    version=VERSION,
    description='An asyncio PosgtreSQL driver',
    long_description=readme,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Framework :: AsyncIO',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python :: 3 :: Only',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: Implementation :: CPython',
        'Topic :: Database :: Front-Ends',
    ],
    platforms=['macOS', 'POSIX', 'Windows'],
    python_requires='>=3.5.0',
    zip_safe=False,
    author='MagicStack Inc',
    author_email='hello@magic.io',
    url='https://github.com/MagicStack/asyncpg',
    license='Apache License, Version 2.0',
    packages=['asyncpg'],
    provides=['asyncpg'],
    include_package_data=True,
    ext_modules=[
        distutils_extension.Extension(
            "asyncpg.protocol.protocol",
            ["asyncpg/protocol/record/recordobj.c",
             "asyncpg/protocol/protocol.pyx"],
            extra_compile_args=CFLAGS,
            extra_link_args=LDFLAGS)
    ],
    cmdclass={'build_ext': build_ext, 'build_py': build_py, 'sdist': sdist},
    test_suite='tests.suite',
    extras_require=EXTRA_DEPENDENCIES,
    setup_requires=setup_requires,
)
