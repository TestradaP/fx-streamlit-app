from __future__ import annotations

import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from usdcop.logging_config import configure_logging  # noqa: E402
from usdcop.pipeline.update_data import update_all  # noqa: E402

if __name__ == "__main__":
    configure_logging()
    result = update_all(ROOT)
    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    if result["status"] != "success":
        raise SystemExit(1)
