#!/usr/bin/env python3


import os.path
import sys


def main():
    version_file = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), 'asyncpg', '__init__.py')

    with open(version_file, 'r') as f:
        for line in f:
            if line.startswith('__version__ ='):
                _, _, version = line.partition('=')
                print(version.strip(" \n'\""))
                return 0

    print('could not find package version in asyncpg/__init__.py',
          file=sys.stderr)
    return 1


if __name__ == '__main__':
    sys.exit(main())
