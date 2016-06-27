import datetime
import decimal
import uuid

from asyncpg import _testbase as tb


def _timezone(offset):
    minutes = offset // 60
    return datetime.timezone(datetime.timedelta(minutes=minutes))


infinity_datetime = datetime.datetime(
    datetime.MAXYEAR, 12, 31, 23, 59, 59, 999999)
negative_infinity_datetime = datetime.datetime(
    datetime.MINYEAR, 1, 1, 0, 0, 0, 0)

infinity_date = datetime.date(datetime.MAXYEAR, 12, 31)
negative_infinity_date = datetime.date(datetime.MINYEAR, 1, 1)


type_samples = [
    ('smallint', 'int2', (
        -2 ** 15 + 1, 2 ** 15 - 1,
        -1, 0, 1,
    )),
    ('int', 'int4', (
        -2 ** 31 + 1, 2 ** 31 - 1,
        -1, 0, 1,
    )),
    ('bigint', 'int8', (
        -2 ** 63 + 1, 2 ** 63 - 1,
        -1, 0, 1,
    )),
    ('numeric', 'numeric', (
        -(2 ** 64),
        2 ** 64,
        -(2 ** 128),
        2 ** 128,
        -1, 0, 1,
        decimal.Decimal("0.00000000000000"),
        decimal.Decimal("1.00000000000000"),
        decimal.Decimal("-1.00000000000000"),
        decimal.Decimal("-2.00000000000000"),
        decimal.Decimal("1000000000000000.00000000000000"),
        decimal.Decimal("-0.00000000000000"),
        decimal.Decimal(1234),
        decimal.Decimal(-1234),
        decimal.Decimal("1234000000.00088883231"),
        decimal.Decimal(str(1234.00088883231)),
        decimal.Decimal("3123.23111"),
        decimal.Decimal("-3123000000.23111"),
        decimal.Decimal("3123.2311100000"),
        decimal.Decimal("-03123.0023111"),
        decimal.Decimal("3123.23111"),
        decimal.Decimal("3123.23111"),
        decimal.Decimal("10000.23111"),
        decimal.Decimal("100000.23111"),
        decimal.Decimal("1000000.23111"),
        decimal.Decimal("10000000.23111"),
        decimal.Decimal("100000000.23111"),
        decimal.Decimal("1000000000.23111"),
        decimal.Decimal("1000000000.3111"),
        decimal.Decimal("1000000000.111"),
        decimal.Decimal("1000000000.11"),
        decimal.Decimal("100000000.0"),
        decimal.Decimal("10000000.0"),
        decimal.Decimal("1000000.0"),
        decimal.Decimal("100000.0"),
        decimal.Decimal("10000.0"),
        decimal.Decimal("1000.0"),
        decimal.Decimal("100.0"),
        decimal.Decimal("100"),
        decimal.Decimal("100.1"),
        decimal.Decimal("100.12"),
        decimal.Decimal("100.123"),
        decimal.Decimal("100.1234"),
        decimal.Decimal("100.12345"),
        decimal.Decimal("100.123456"),
        decimal.Decimal("100.1234567"),
        decimal.Decimal("100.12345679"),
        decimal.Decimal("100.123456790"),
        decimal.Decimal("100.123456790000000000000000"),
        decimal.Decimal("1.0"),
        decimal.Decimal("0.0"),
        decimal.Decimal("-1.0"),
        decimal.Decimal("1.0E-1000"),
        decimal.Decimal("1.0E1000"),
        decimal.Decimal("0.000000000000000000000000001"),
        decimal.Decimal("0.000000000000010000000000001"),
        decimal.Decimal("0.00000000000000000000000001"),
        decimal.Decimal("0.00000000100000000000000001"),
        decimal.Decimal("0.0000000000000000000000001"),
        decimal.Decimal("0.000000000000000000000001"),
        decimal.Decimal("0.00000000000000000000001"),
        decimal.Decimal("0.0000000000000000000001"),
        decimal.Decimal("0.000000000000000000001"),
        decimal.Decimal("0.00000000000000000001"),
        decimal.Decimal("0.0000000000000000001"),
        decimal.Decimal("0.000000000000000001"),
        decimal.Decimal("0.00000000000000001"),
        decimal.Decimal("0.0000000000000001"),
        decimal.Decimal("0.000000000000001"),
        decimal.Decimal("0.00000000000001"),
        decimal.Decimal("0.0000000000001"),
        decimal.Decimal("0.000000000001"),
        decimal.Decimal("0.00000000001"),
        decimal.Decimal("0.0000000001"),
        decimal.Decimal("0.000000001"),
        decimal.Decimal("0.00000001"),
        decimal.Decimal("0.0000001"),
        decimal.Decimal("0.000001"),
        decimal.Decimal("0.00001"),
        decimal.Decimal("0.0001"),
        decimal.Decimal("0.001"),
        decimal.Decimal("0.01"),
        decimal.Decimal("0.1"),
    )),
    ('bytea', 'bytea', (
        bytes(range(256)),
        bytes(range(255, -1, -1)),
        b'\x00\x00',
        b'foo'
    )),
    ('"char"', 'char', (
        b'a',
        b'b',
        b'\x00'
    )),
    ('timestamp', 'timestamp', [
        datetime.datetime(3000, 5, 20, 5, 30, 10),
        datetime.datetime(2000, 1, 1, 5, 25, 10),
        datetime.datetime(500, 1, 1, 5, 25, 10),
        datetime.datetime(250, 1, 1, 5, 25, 10),
        infinity_datetime,
        negative_infinity_datetime,
    ]),
    ('date', 'date', [
        datetime.date(3000, 5, 20),
        datetime.date(2000, 1, 1),
        datetime.date(500, 1, 1),
        datetime.date(1, 1, 1),
    ]),
    ('time', 'time', [
        datetime.time(12, 15, 20),
        datetime.time(0, 1, 1),
        datetime.time(23, 59, 59),
    ]),
    ('timestamptz', 'timestamptz', [
        # It's converted to UTC. When it comes back out, it will be in UTC
        # again. The datetime comparison will take the tzinfo into account.
        datetime.datetime(1990, 5, 12, 10, 10, 0, tzinfo=_timezone(4000)),
        datetime.datetime(1982, 5, 18, 10, 10, 0, tzinfo=_timezone(6000)),
        datetime.datetime(1950, 1, 1, 10, 10, 0, tzinfo=_timezone(7000)),
        datetime.datetime(1800, 1, 1, 10, 10, 0, tzinfo=_timezone(2000)),
        datetime.datetime(2400, 1, 1, 10, 10, 0, tzinfo=_timezone(2000)),
        infinity_datetime,
        negative_infinity_datetime,
    ]),
    ('timetz', 'timetz', [
        # timetz retains the offset
        datetime.time(10, 10, 0, tzinfo=_timezone(4000)),
        datetime.time(10, 10, 0, tzinfo=_timezone(6000)),
        datetime.time(10, 10, 0, tzinfo=_timezone(7000)),
        datetime.time(10, 10, 0, tzinfo=_timezone(2000)),
        datetime.time(22, 30, 0, tzinfo=_timezone(0)),
    ]),
    ('interval', 'interval', [
        # no months :(
        datetime.timedelta(40, 10, 1234),
        datetime.timedelta(0, 0, 4321),
        datetime.timedelta(0, 0),
        datetime.timedelta(-100, 0),
        datetime.timedelta(-100, -400),
    ]),
    ('uuid', 'uuid', [
        uuid.UUID('38a4ff5a-3a56-11e6-a6c2-c8f73323c6d4'),
        uuid.UUID('00000000-0000-0000-0000-000000000000')
    ]),
    ('json', 'json', [
        '[1, 2, 3, 4]',
        '{"a": [1, 2], "b": 0}'
    ]),
    ('jsonb', 'jsonb', [
        '[1, 2, 3, 4]',
        '{"a": [1, 2], "b": 0}'
    ]),
    ('oid[]', 'oid[]', [
        [1, 2, 3, 4],
        []
    ]),
    ('smallint[]', 'int2[]', [
        [1, 2, 3, 4],
        [1, 2, 3, 4, 5, 6, 7, 8, 9, 0],
        []
    ]),
    ('bigint[]', 'int8[]', [
        [2 ** 42, -2 ** 54, 0],
        []
    ]),
    ('int[]', 'int4[]', [
        [2 ** 22, -2 ** 24, 0],
        []
    ]),
    ('time[]', 'time[]', [
        [datetime.time(12, 15, 20), datetime.time(0, 1, 1)],
        []
    ]),
    ('text[]', 'text[]', [
        ['ABCDE', 'EDCBA'],
        []
    ])
]


class TestCodecs(tb.ConnectedTestCase):

    async def test_standard_codecs(self):
        """Test encoding/decoding of standard data types and arrays thereof"""
        for (typname, intname, sample_data) in type_samples:
            st = await self.con.prepare(
                "SELECT $1::" + typname
            )

            for sample in sample_data:
                with self.subTest(sample=sample, typname=typname):
                    rsample = list(await st.execute(sample))[0][0]
                    self.assertEqual(
                        rsample, sample,
                        ("failed to return {} object data as-is; "
                         "gave {!r}, received {!r}").format(typname, sample,
                                                            rsample)
                    )

            at = st.get_attributes()
            self.assertEqual(at[0].type.name, intname)

    async def test_composites(self):
        """Test encoding/decoding of composite types"""

        st = await self.con.prepare('''
            CREATE TYPE test_composite AS (
                a int,
                b text,
                c int[]
            )
        ''')

        await st.execute()

        try:
            st = await self.con.prepare('''
                SELECT ROW(
                    NULL,
                    '5678',
                    ARRAY[9, NULL, 11]::int[]
                )::test_composite AS test
            ''')

            res = list(await st.execute())[0][0]

            self.assertIsNone(res['a'])
            self.assertEqual(res['b'], '5678')
            self.assertEqual(res['c'], [9, None, 11])

            self.assertIsNone(res[0])
            self.assertEqual(res[1], '5678')
            self.assertEqual(res[2], [9, None, 11])

            at = st.get_attributes()
            self.assertEqual(len(at), 1)
            self.assertEqual(at[0].name, 'test')
            self.assertEqual(at[0].type.name, 'test_composite')
            self.assertEqual(at[0].type.kind, 'composite')

        finally:
            st = await self.con.prepare('DROP TYPE test_composite')
            await st.execute()

    async def test_domains(self):
        """Test encoding/decoding of composite types"""

        st = await self.con.prepare('''
            CREATE DOMAIN my_dom AS int
        ''')

        await st.execute()

        st = await self.con.prepare('''
            CREATE DOMAIN my_dom2 AS my_dom
        ''')

        await st.execute()

        try:
            st = await self.con.prepare('''
                SELECT 3::my_dom2
            ''')

            res = list(await st.execute())[0][0]

            self.assertEqual(res, 3)

            st = await self.con.prepare('''
                SELECT NULL::my_dom2
            ''')

            res = list(await st.execute())[0][0]

            self.assertIsNone(res)

            at = st.get_attributes()
            self.assertEqual(len(at), 1)
            self.assertEqual(at[0].name, 'my_dom2')
            self.assertEqual(at[0].type.name, 'int4')
            self.assertEqual(at[0].type.kind, 'scalar')

        finally:
            st = await self.con.prepare('DROP DOMAIN my_dom2')
            await st.execute()
            st = await self.con.prepare('DROP DOMAIN my_dom')
            await st.execute()

    async def test_custom_codec_text(self):
        """Test encoding/decoding using a custom codec in text mode"""

        st = await self.con.prepare('''
            CREATE EXTENSION IF NOT EXISTS hstore
        ''')

        await st.execute()

        def hstore_decoder(data):
            result = {}
            items = data.split(',')
            for item in items:
                k, _, v = item.partition('=>')
                result[k.strip('"')] = v.strip('"')

            return result

        def hstore_encoder(obj):
            return ','.join('{}=>{}'.format(k, v) for k, v in obj.items())

        await self.con.set_type_codec('hstore', encoder=hstore_encoder,
                                      decoder=hstore_decoder,
                                      format=0)

        st = await self.con.prepare('''
            SELECT $1::hstore AS result
        ''')

        res = list(await st.execute({'ham': 'spam'}))[0]['result']

        self.assertEqual(res, {'ham': 'spam'})

        pt = st.get_parameters()
        self.assertTrue(isinstance(pt, tuple))
        self.assertEqual(len(pt), 1)
        self.assertEqual(pt[0].name, 'hstore')
        self.assertEqual(pt[0].kind, 'scalar')
        self.assertEqual(pt[0].schema, 'public')

        at = st.get_attributes()
        self.assertTrue(isinstance(at, tuple))
        self.assertEqual(len(at), 1)
        self.assertEqual(at[0].name, 'result')
        self.assertEqual(at[0].type, pt[0])
