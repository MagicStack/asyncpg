#!/usr/bin/env python3
#
# Copyright (c) 2016 MagicStack Inc.
# All rights reserved.
#
# See LICENSE for details.
##


import argparse
import asyncio
import json
import math
import time

import numpy as np
import uvloop

import aiopg
import asyncpg


QUERY = '''select typname, typnamespace, typowner, typlen, typbyval,
typcategory, typispreferred, typisdefined,
typdelim, typrelid, typelem, typarray from pg_type'''


def weighted_quantile(values, quantiles, weights):
    """Very close to np.percentile, but supports weights.

    :param values: np.array with data
    :param quantiles: array-like with many quantiles needed,
           quantiles should be in [0, 1]!
    :param weights: array-like of the same length as `array`
    :return: np.array with computed quantiles.
    """
    values = np.array(values)
    quantiles = np.array(quantiles)
    weights = np.array(weights)
    assert np.all(quantiles >= 0) and np.all(quantiles <= 1), \
                 'quantiles should be in [0, 1]'

    weighted_quantiles = np.cumsum(weights) - 0.5 * weights
    weighted_quantiles -= weighted_quantiles[0]
    weighted_quantiles /= weighted_quantiles[-1]

    return np.interp(quantiles, weighted_quantiles, values)


async def aiopg_connect(args):
    conn = await aiopg.connect(user=args.pguser, host=args.pghost,
                               port=args.pgport)
    return conn


async def aiopg_execute(conn, query):
    cur = await conn.cursor()
    await cur.execute(query)
    await cur.fetchall()


async def asyncpg_connect(args):
    conn = await asyncpg.connect(user=args.pguser, host=args.pghost,
                                 port=args.pgport)
    return conn


async def asyncpg_execute(conn, query):
    stmt = await conn.prepare(query)
    await stmt.fetch()


async def worker(executor, eargs, start, duration, timeout):
    n = 0
    latency_stats = np.zeros((timeout * 100,))
    min_latency = float('inf')
    max_latency = 0.0

    while time.monotonic() - start < duration:
        req_start = time.monotonic()
        await executor(*eargs)
        req_time = round((time.monotonic() - req_start) * 100000)

        if req_time > max_latency:
            max_latency = req_time
        if req_time < min_latency:
            min_latency = req_time
        latency_stats[req_time] += 1
        n += 1

    return n, latency_stats, min_latency, max_latency


async def runner(args, connector, executor):

    timeout = args.timeout * 1000
    concurrency = args.concurrency

    conns = []

    for i in range(concurrency):
        conn = await connector(args)
        conns.append(conn)

    start = time.monotonic()
    tasks = []

    for i in range(concurrency):
        task = worker(executor, [conns[i], QUERY],
                      start, args.duration, timeout)
        tasks.append(task)

    results = await asyncio.gather(*tasks)
    end = time.monotonic()

    for conn in conns:
        await conn.close()

    duration = end - start

    min_latency = float('inf')
    max_latency = 0.0
    messages = 0
    latency_stats = None

    for result in results:
        t_messages, t_latency_stats, t_min_latency, t_max_latency = result
        messages += t_messages
        if latency_stats is None:
            latency_stats = t_latency_stats
        else:
            latency_stats = np.add(latency_stats, t_latency_stats)
        if t_max_latency > max_latency:
            max_latency = t_max_latency
        if t_min_latency < min_latency:
            min_latency = t_min_latency

    arange = np.arange(len(latency_stats))

    mean_latency = np.average(arange, weights=latency_stats)
    variance = np.average((arange - mean_latency) ** 2, weights=latency_stats)
    latency_std = math.sqrt(variance)
    latency_cv = latency_std / mean_latency

    percentiles = [25, 50, 75, 90, 99, 99.99]
    percentile_data = []

    quantiles = weighted_quantile(arange, [p / 100 for p in percentiles],
                                  weights=latency_stats)

    for i, percentile in enumerate(percentiles):
        percentile_data.append((percentile, round(quantiles[i] / 100, 3)))

    data = dict(
        duration=round(duration, 2),
        messages=messages,
        rps=round(messages / duration, 2),
        latency_min=round(min_latency / 100, 3),
        latency_mean=round(mean_latency / 100, 3),
        latency_max=round(max_latency / 100, 3),
        latency_std=round(latency_std / 100, 3),
        latency_cv=round(latency_cv * 100, 2),
        latency_percentiles=percentile_data
    )

    if args.output_format == 'json':
        data['latency_percentiles'] = json.dumps(percentile_data)

        output = '''\
{{
    "duration": {duration},
    "messages": {messages},
    "rps": {rps},
    "latency_min": {latency_min},
    "latency_mean": {latency_mean},
    "latency_max": {latency_max},
    "latency_std": {latency_std},
    "latency_cv": {latency_cv},
    "latency_percentiles": {latency_percentiles}
}}'''.format(**data)
    else:
        data['latency_percentiles'] = '; '.join(
            '{}% under {}ms'.format(*v) for v in percentile_data)

        output = '''\
{messages} {size}-row results in {duration} seconds
Latency: min {latency_min}ms; max {latency_max}ms; mean {latency_mean}ms; \
std: {latency_std}ms ({latency_cv}%)
Latency distribution: {latency_percentiles}
Requests/sec: {rps}
'''.format(size=args.setsize, **data)

    print(output)


if __name__ == '__main__':
    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    parser = argparse.ArgumentParser(
        description='async pg driver benchmark [concurrent]')
    parser.add_argument(
        '-C', '--concurrency', type=int, default=50,
        help='number of concurrent connections')
    parser.add_argument(
        '-S', '--setsize', type=int, default=1000,
        help='size of dataset returned from the server')
    parser.add_argument(
        '-D', '--duration', type=int, default=30,
        help='duration of test in seconds')
    parser.add_argument(
        '--timeout', default=2, type=int,
        help='server timeout in seconds')
    parser.add_argument(
        '--output-format', default='text', type=str,
        help='output format', choices=['text', 'json'])
    parser.add_argument(
        '--pghost', type=str, default='127.0.0.1',
        help='PostgreSQL server host')
    parser.add_argument(
        '--pgport', type=int, default=5432,
        help='PostgreSQL server port')
    parser.add_argument(
        '--pguser', type=str, default='postgres',
        help='PostgreSQL server user')
    parser.add_argument(
        'driver', help='driver implementation to use',
        choices=['aiopg', 'asyncpg'])

    args = parser.parse_args()

    if args.driver == 'aiopg':
        connector, executor = aiopg_connect, aiopg_execute
    elif args.driver == 'asyncpg':
        connector, executor = asyncpg_connect, asyncpg_execute
    else:
        raise ValueError('unexpected driver: {!r}'.format(args.driver))

    loop.run_until_complete(runner(args, connector, executor))
