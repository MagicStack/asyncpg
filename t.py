import asyncio
import uvloop
import asyncpg


asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())


async def run():
    con = await asyncpg.connect(user='postgres')
    print(await con.fetchrow('''select 1 as a, 2 as b, 'aaaaa' as c'''))
    print(await con.fetchrow('''select 1 as a, 2 as b, 'aaaaa' as c'''))
    print(await con.fetchrow('''select 1 as a, 2 as b, 'aaaaa' as c'''))


asyncio.get_event_loop().run_until_complete(run())
