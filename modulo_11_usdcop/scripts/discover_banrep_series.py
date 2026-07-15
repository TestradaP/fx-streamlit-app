from __future__ import annotations

import argparse
import json
import sys
import unicodedata
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from usdcop.data.banrep import BanRepClient, flatten_catalog  # noqa: E402


def normalize(value: str) -> str:
    return "".join(
        character for character in unicodedata.normalize("NFKD", value.lower())
        if not unicodedata.combining(character)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("keywords", nargs="+", help="e.g. remesas deuda externa forward NDF")
    args = parser.parse_args()
    rows = flatten_catalog(BanRepClient().fetch_catalog())
    terms = [normalize(term) for term in args.keywords]
    matches = [row for row in rows if all(term in normalize(row["name"]) for term in terms)]
    print(json.dumps(matches, ensure_ascii=False, indent=2))
