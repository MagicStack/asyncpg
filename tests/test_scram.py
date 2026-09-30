# Copyright (C) 2016-present the asyncpg authors and contributors
# <see AUTHORS file>
#
# This module is part of asyncpg and is released under
# the Apache 2.0 License: http://www.apache.org/licenses/LICENSE-2.0

import base64
import hashlib
import hmac
import unittest

from asyncpg.protocol.protocol import SCRAMAuthentication


# PBKDF2-HMAC-SHA-256 vectors from RFC 7914, Section 11.  SCRAM only
# needs one block, so the expected values are the first halves of the
# RFC's 64-byte outputs.
RFC7914_VECTORS = [
    (b"passwd", b"salt", 1,
     "55ac046e56e3089fec1691c22544b605"
     "f94185216dde0465e68b9d57c20dacbc"),
    (b"Password", b"NaCl", 80000,
     "4ddcd8f60b98be21830cee5ef22701f9"
     "641a4418d04c0414aeff08876b34ab56"),
]

# Salted password from the SCRAM-SHA-256 example in RFC 7677, Section 3
# (password "pencil", with the salt and iteration count from the server).
RFC7677_PASSWORD = "pencil"
RFC7677_SALT = b"W22ZaJ0SNY7soEsUEjb6gQ=="
RFC7677_ITERATIONS = 4096
RFC7677_SALTED_PASSWORD = (
    "c4a49510323ab4f952cac1fa99441939"
    "e78ea74d6be81ddf7096e87513dc615d"
)


def reference_hifi(password, salt, iterations):
    """The RFC 5802 Hi() loop that the salted password used to implement."""
    p = password.encode("utf8")
    u = hmac.new(p, salt + b"\x00\x00\x00\x01", hashlib.sha256).digest()
    result = u
    for _ in range(iterations - 1):
        u = hmac.new(p, u, hashlib.sha256).digest()
        result = bytes(a ^ b for a, b in zip(result, u))
    return result


class TestSCRAMSaltedPassword(unittest.TestCase):

    def setUp(self):
        self.scram = SCRAMAuthentication(b"SCRAM-SHA-256")

    def test_rfc7914_vectors(self):
        for password, salt, iterations, expected in RFC7914_VECTORS:
            derived = self.scram._generate_salted_password(
                password.decode(), base64.b64encode(salt), iterations)
            self.assertEqual(derived.hex(), expected)

    def test_rfc7677_salted_password(self):
        derived = self.scram._generate_salted_password(
            RFC7677_PASSWORD, RFC7677_SALT, RFC7677_ITERATIONS)
        self.assertEqual(derived.hex(), RFC7677_SALTED_PASSWORD)

    def test_matches_reference_hifi(self):
        cases = [
            ("pencil", b"W22ZaJ0SNY7soEsUEjb6gQ==", 4096),
            ("passwd", b"c2FsdA==", 1),
            ("passwd", b"c2FsdA==", 2),
            ("p\u00e4ssw\u00f6rd", b"c2FsdA==", 100),
        ]
        for password, salt, iterations in cases:
            expected = reference_hifi(
                password, base64.b64decode(salt), iterations)
            derived = self.scram._generate_salted_password(
                password, salt, iterations)
            self.assertEqual(derived, expected)

    def test_uses_hashlib_pbkdf2_hmac(self):
        calls = []
        pbkdf2_hmac = hashlib.pbkdf2_hmac

        def spy(digest, password, salt, iterations):
            calls.append((digest, password, salt, iterations))
            return pbkdf2_hmac(digest, password, salt, iterations)

        hashlib.pbkdf2_hmac = spy
        try:
            derived = self.scram._generate_salted_password(
                RFC7677_PASSWORD, RFC7677_SALT, RFC7677_ITERATIONS)
        finally:
            hashlib.pbkdf2_hmac = pbkdf2_hmac

        self.assertEqual(calls, [(
            "sha256", RFC7677_PASSWORD.encode(),
            base64.b64decode(RFC7677_SALT), RFC7677_ITERATIONS)])
        self.assertEqual(derived.hex(), RFC7677_SALTED_PASSWORD)


if __name__ == '__main__':
    unittest.main()
