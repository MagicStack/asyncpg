# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0


import textwrap
import threading
import unittest

try:
    from concurrent import interpreters
except ImportError:
    pass
else:
    class TestSubinterpreters(unittest.TestCase):
        def test_record_module_loads_in_subinterpreter(self) -> None:
            def run_in_subinterpreter() -> None:
                interp = interpreters.create()

                try:
                    code = textwrap.dedent("""\
                    import asyncpg.protocol.record as record
                    assert record.Record is not None
                    """)
                    interp.exec(code)
                finally:
                    interp.close()

            thread = threading.Thread(target=run_in_subinterpreter)
            thread.start()
            thread.join()

        def test_record_module_state_isolation(self) -> None:
            import asyncpg.protocol.record

            main_record_id = id(asyncpg.protocol.record.Record)

            def run_in_subinterpreter() -> None:
                interp = interpreters.create()

                try:
                    code = textwrap.dedent(f"""\
                    import asyncpg.protocol.record as record

                    sub_record_id = id(record.Record)
                    main_id = {main_record_id}

                    assert sub_record_id != main_id, (
                        f"Record type objects are the same: "
                        f"{{sub_record_id}} == {{main_id}}. "
                        f"This indicates shared global state."
                    )
                    """)
                    interp.exec(code)
                finally:
                    interp.close()

            thread = threading.Thread(target=run_in_subinterpreter)
            thread.start()
            thread.join()


if __name__ == "__main__":
    _ = unittest.main()
