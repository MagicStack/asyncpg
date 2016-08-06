.. _asyncpg-examples:


Usage Examples
==============

Below is an example of how **asyncpg** can be used to implement a simple
Web service that computes the requested power of two.


.. code-block:: python

    import asyncio
    import asyncpg
    from aiohttp import web


    async def handle(request):
        """Handle incoming requests."""
        pool = request.app['pool']
        power = int(request.match_info.get('power', 10))

        # Take a connection from the pool.
        async with pool.acquire() as connection:
            # Open a transaction.
            async with connection.transaction():
                # Run the query passing the request argument.
                result = await connection.fetchval('select 2 ^ $1', power)
                return web.Response(
                    text="2 ^ {} is {}".format(power, result))


    async def init_app():
        """Initialize the application server."""
        app = web.Application()
        # Create a database connection pool
        app['pool'] = await asyncpg.create_pool(database='postgres',
                                                user='postgres')
        # Configure service routes
        app.router.add_route('GET', '/{power:\d+}', handle)
        app.router.add_route('GET', '/', handle)
        return app


    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init_app())
    web.run_app(app)
