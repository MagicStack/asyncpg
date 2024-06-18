#!/usr/bin/env python3
#
# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import json
import requests
import re
import sys


BASE_URL = 'https://api.github.com/repos/magicstack/asyncpg/compare'


def main():
    if len(sys.argv) < 2:
        print('pass a sha1 hash as a first argument')
        sys.exit(1)

    from_hash = sys.argv[1]
    if len(sys.argv) > 2:
        to_hash = sys.argv[2]

    r = requests.get(f'{BASE_URL}/{from_hash}...{to_hash}')
    data = json.loads(r.text)

    for commit in data['commits']:
        message = commit['commit']['message']
        first_line = message.partition('\n\n')[0]
        if commit.get('author'):
            username = '@{}'.format(commit['author']['login'])
        else:
            username = commit['commit']['author']['name']
        sha = commit["sha"][:8]

        m = re.search(r'\#(?P<num>\d+)\b', message)
        if m:
            issue_num = m.group('num')
        else:
            issue_num = None

        print(f'* {first_line}')
        print(f'  (by {username} in {sha}', end='')
        print(')')
        print()


if __name__ == '__main__':
    main()
