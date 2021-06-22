# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

import os
import subprocess
import sys
import unittest


def find_root():
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestFlake8(unittest.TestCase):

    def test_flake8(self):
        try:
            import flake8  # NoQA
        except ImportError:
            raise unittest.SkipTest('flake8 module is missing')

        root_path = find_root()
        config_path = os.path.join(root_path, '.flake8')
        if not os.path.exists(config_path):
            raise RuntimeError('could not locate .flake8 file')

        try:
            subprocess.run(
                [sys.executable, '-m', 'flake8', '--config', config_path],
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                cwd=root_path)
        except subprocess.CalledProcessError as ex:
            output = ex.output.decode()
            raise AssertionError(
                'flake8 validation failed:\n{}'.format(output)) from None
