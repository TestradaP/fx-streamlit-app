from __future__ import annotations

import getpass
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from app_security import hash_password  # noqa: E402


if __name__ == "__main__":
    print(hash_password(getpass.getpass("Application password: ")))
