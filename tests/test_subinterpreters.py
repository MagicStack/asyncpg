# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import sys
import unittest


@unittest.skipIf(
    sys.version_info < (3, 14),
    "Subinterpreter support requires Python 3.14+",
)
class TestSubinterpreters(unittest.TestCase):
    def setUp(self) -> None:
        from concurrent import interpreters
        self.interpreters = interpreters

    def test_record_module_loads_in_subinterpreter(self) -> None:
        interp = self.interpreters.create()

        try:
            code = """
import asyncpg.protocol.record as record
assert record.Record is not None
"""
            interp.exec(code)
        finally:
            interp.close()

    def test_record_module_state_isolation(self) -> None:
        import asyncpg.protocol.record

        main_record_id = id(asyncpg.protocol.record.Record)

        interp = self.interpreters.create()

        try:
            code = f"""
import asyncpg.protocol.record as record

sub_record_id = id(record.Record)
main_id = {main_record_id}

assert sub_record_id != main_id, (
    f"Record type objects are the same: {{sub_record_id}} == {{main_id}}. "
    "This indicates shared global state."
)
"""
            interp.exec(code)
        finally:
            interp.close()


if __name__ == "__main__":
    unittest.main()
