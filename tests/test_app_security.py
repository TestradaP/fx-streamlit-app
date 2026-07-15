import unittest

from app_security import hash_password, verify_credentials, verify_password


class AppSecurityTests(unittest.TestCase):
    def setUp(self):
        self.password_hash = hash_password("correct horse battery staple", iterations=100_000, salt=b"fixed-test-salt")

    def test_hash_round_trip(self):
        self.assertTrue(verify_password("correct horse battery staple", self.password_hash))
        self.assertFalse(verify_password("wrong", self.password_hash))

    def test_credentials_fail_closed_when_configuration_is_missing(self):
        self.assertFalse(verify_credentials("admin", "admin", None, None))

    def test_credentials_require_both_fields(self):
        self.assertTrue(
            verify_credentials("treasury", "correct horse battery staple", "treasury", self.password_hash)
        )
        self.assertFalse(
            verify_credentials("other", "correct horse battery staple", "treasury", self.password_hash)
        )


if __name__ == "__main__":
    unittest.main()
