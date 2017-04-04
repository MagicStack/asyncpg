# Copyright (C) 2016-present the ayncpg authors and contributors
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

    r = requests.get(f'{BASE_URL}/{from_hash}...master')
    data = json.loads(r.text)

    for commit in data['commits']:
        message = commit['commit']['message']
        first_line = message.partition('\n\n')[0]
        gh_username = commit['author']['login']
        sha = commit["sha"][:8]

        m = re.search(r'\#(?P<num>\d+)\b', message)
        if m:
            issue_num = m.group('num')
        else:
            issue_num = None

        print(f'* {first_line}')
        print(f'  (by @{gh_username} in {sha}', end='')
        if issue_num:
            print(f' for #{issue_num})')
        else:
            print(')')
        print()


if __name__ == '__main__':
    main()
