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
