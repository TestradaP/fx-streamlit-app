from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import secrets


ALGORITHM = "pbkdf2_sha256"
DEFAULT_ITERATIONS = 600_000


def hash_password(password: str, *, iterations: int = DEFAULT_ITERATIONS, salt: bytes | None = None) -> str:
    """Return a portable PBKDF2-SHA256 password hash for application secrets."""
    if not password:
        raise ValueError("password must not be empty")
    if iterations < 100_000:
        raise ValueError("iterations must be at least 100000")
    salt_value = salt or secrets.token_bytes(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt_value, iterations)
    encoded_salt = base64.urlsafe_b64encode(salt_value).decode("ascii")
    encoded_digest = base64.urlsafe_b64encode(digest).decode("ascii")
    return f"{ALGORITHM}${iterations}${encoded_salt}${encoded_digest}"


def verify_password(password: str, encoded_hash: str) -> bool:
    """Verify a password without leaking comparison timing information."""
    try:
        algorithm, raw_iterations, encoded_salt, encoded_digest = encoded_hash.split("$", 3)
        if algorithm != ALGORITHM:
            return False
        iterations = int(raw_iterations)
        salt = base64.urlsafe_b64decode(encoded_salt.encode("ascii"))
        expected = base64.urlsafe_b64decode(encoded_digest.encode("ascii"))
    except (binascii.Error, TypeError, ValueError):
        return False
    actual = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, iterations)
    return hmac.compare_digest(actual, expected)


def verify_credentials(
    username: str,
    password: str,
    expected_username: str | None,
    expected_password_hash: str | None,
) -> bool:
    if not expected_username or not expected_password_hash:
        return False
    username_matches = hmac.compare_digest(username, expected_username)
    password_matches = verify_password(password, expected_password_hash)
    return username_matches and password_matches
