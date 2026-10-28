
This example is about how to use asyncpg conveniently in both server environment and non-server environment. Users from pymongo will be comfortable for demo provided. The demo does not require to write data type schema like norm orm do. The process is adapted for  non-async datadb users: first to get db handler,  then table handler ,and then run code.

The demo code provide PG class,which save connection pool in global dictionary  with database name as key. Each database has one and only one pool.  Different pg instances can share same db pool.

# Non Server Environment
```build
from pg import PG

# define your data process logic here
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


```

In non-server environment, data process logic is written in async function. Run this function in PG class method "run" will guarantee connection pool will be terminated.

# Server Environment
```build
# example by fastapi

from pg import PG
from fastapi import FastAPI
from fastapi.testclient import TestClient

app = FastAPI()

# Do not need init connection pool here,pg class will init automatically when pool is none.
@app.on_event("startup")
async def startup_event():
    pass
    
@app.on_event("shutdown")
async def shutdown_event():
    pgdb = PG("host", "port", "user", "password", "database")
    pgdb.terminate_pool()
    

@app.get("/xxx")
async def handlers():
    pgdb = PG("host", "port", "user", "password", "database")
    table = pgdb['test']
    data= await table.select("select ....")
    return data


```
The PG class will create pool with first call. The pool will be terminated during server lifetime shutdown phase. 