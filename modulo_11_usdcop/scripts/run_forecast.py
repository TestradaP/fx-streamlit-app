from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from usdcop.logging_config import configure_logging  # noqa: E402
from usdcop.pipeline.forecast import run_forecast  # noqa: E402

if __name__ == "__main__":
    configure_logging()
    print(run_forecast(ROOT).to_string(index=False))
