import asyncio
import aiopg
import asyncpg
import time
import uvloop


async def bench_asyncpg(iters, size, query):
    con = await asyncpg.connect(user='postgres', host='127.0.0.1',
                                command_timeout=60)

    started = time.monotonic()
    for _ in range(iters):
        await con.fetch(query)
    print('-> asyncpg: {:.3} seconds'.format(time.monotonic() - started))


async def bench_asyncpg_execute(iters, size, query):
    con = await asyncpg.connect(user='postgres', host='127.0.0.1')

    started = time.monotonic()
    for _ in range(iters):
        await con.execute(query)
    print('-> asyncpg execute: {:.3} seconds'.format(
        time.monotonic() - started))


async def bench_aiopg(iters, size, query):
    con = await aiopg.connect(user='postgres', host='127.0.0.1')

    started = time.monotonic()
    for _ in range(iters):
        cur = await con.cursor(timeout=60)
        await cur.execute(query)
        await cur.fetchall()

    print('-> aiopg: {:.3} seconds'.format(time.monotonic() - started))

    con.close()


async def print_debug(loop):
    while True:
        print(chr(27) + "[2J")  # clear screen
        loop.print_debug_info()
        await asyncio.sleep(0.5, loop=loop)


if __name__ == '__main__':
    iters = 10000
    size = 1000

    asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    loop = asyncio.get_event_loop()

    # loop.create_task(print_debug(loop))

    query = 'select generate_series(0,{})'.format(size)
    query = '''select typname, typnamespace, typowner, typlen, typbyval,
                      typcategory, typispreferred, typisdefined,
                      typdelim, typrelid, typelem, typarray from pg_type'''

    print(loop)
    print('iters: {}; \nquery: {}'.format(iters, query))

    loop.run_until_complete(bench_aiopg(iters, size, query))
    loop.run_until_complete(bench_asyncpg(iters, size, query))
    # loop.run_until_complete(bench_asyncpg_execute(iters, size, query))
