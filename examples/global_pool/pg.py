import asyncio

import asyncpg

DB = {}


class PG:
    def __init__(self, host, port, user, pwd, dbName):
        self.user = user
        self.pwd = pwd
        self.host = host
        self.port = port
        self.dbName = dbName
        self.db = None
        self.table = None

    def __getitem__(self, tb):
        self.table = tb
        return self

    async def db_pool(self):
        global DB
        if DB.get(self.dbName, None) is None:
            DB[self.dbName] = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.pwd,
                database=self.dbName,
            )

        self.db = DB[self.dbName]
        print("DB", self.dbName, self.db)
        return self

    # 如果测算程序不调用,切数据库会出现pgadmin看不到表格的问题.
    def terminate_pool(self):
        print("will terminate", self.db)
        if self.db is not None:
            self.db.terminate()
        DB.pop(self.dbName, None)

    async def check(self):
        if self.dbName is None:
            raise "no db name"
        if self.table is None:
            raise "no table name"
        if self.db is None:
            await self.db_pool()

    async def execute(self, sql):
        print("sql===", sql)
        await self.check()
        async with self.db.acquire() as conn:
            await conn.execute(sql)

    async def trans(self, sqls):
        await self.check()
        async with self.db.acquire() as conn:
            async with conn.transaction():
                for sql in sqls:
                    print("sql===", sql)
                    await conn.execute(sql)

    async def select(self, sql):
        await self.check()
        async with self.db.acquire() as conn:
            q = await conn.fetch(sql)
            return [dict(i) for i in q]

    def run(self, f):
        print(self.dbName, self.table)
        asyncio.get_event_loop().run_until_complete(f(self))
        self.terminate_pool()
