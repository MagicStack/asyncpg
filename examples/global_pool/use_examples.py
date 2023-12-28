from pg import PG

'''
PG class will save connection pool in global dictionary DB with database name as dic key. Each database has one and only one pool.
 Different pg instances can share db pool.
'''



async def run_many_sqls_in_transaction(table):
    sqls = [
        "sql1",
        "sql2",
        "sql3",
    ]
    await table.trans(sqls)


async def run_one_sql(table):
    sql="INSERT INTO ......"
    await table.execute(sql)
async def select(table):
    sql="select ......"
    b = await table.select(sql)
    print(11, b)

pgdb = PG("host", "port", "user", "password", "database")
table = pgdb['test']
table.run(run_many_sqls_in_transaction)
table.run(run_one_sql)
table.run(select)
