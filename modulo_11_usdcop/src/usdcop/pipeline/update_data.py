from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from usdcop.config import load_settings
from usdcop.data.banrep import BanRepClient
from usdcop.data.dane import DaneTradeClient
from usdcop.data.fred import FredClient
from usdcop.data.quality import assess_series
from usdcop.data.repository import SeriesRepository

LOGGER = logging.getLogger(__name__)


def update_all(project_root: str | Path | None = None) -> dict[str, Any]:
    paths, _, catalog = load_settings(project_root)
    repository = SeriesRepository(paths.storage_root)
    started = datetime.now(timezone.utc)
    details: dict[str, Any] = {"updated": [], "failed": [], "quality": []}

    banrep = BanRepClient()
    for item in catalog.get("banrep", []):
        if not item.get("enabled") or item.get("series_id") is None:
            continue
        try:
            frame = banrep.fetch_series(item["series_id"], latest_n=10000)
            quality = assess_series(frame, datetime.now().date(), int(item.get("max_staleness_days", 30)))
            details["quality"].append({"series": item["name"], **quality.__dict__})
            if not quality.passed:
                raise ValueError(f"quality check failed: {', '.join(quality.messages)}")
            repository.save_series(frame, "banrep", item["name"])
            details["updated"].append(f"banrep:{item['name']}")
        except Exception as exc:  # noqa: BLE001 - continue other sources
            LOGGER.exception("BanRep update failed for %s", item["name"])
            details["failed"].append({"series": f"banrep:{item['name']}", "error": str(exc)})

    fred = FredClient()
    for item in catalog.get("fred", []):
        if not item.get("enabled"):
            continue
        try:
            frame = fred.fetch_series(item["series_id"])
            quality = assess_series(frame, datetime.now().date(), int(item.get("max_staleness_days", 30)))
            details["quality"].append({"series": item["name"], **quality.__dict__})
            if not quality.passed:
                raise ValueError(f"quality check failed: {', '.join(quality.messages)}")
            repository.save_series(frame, "fred", item["name"])
            details["updated"].append(f"fred:{item['name']}")
        except Exception as exc:  # noqa: BLE001
            LOGGER.exception("FRED update failed for %s", item["name"])
            details["failed"].append({"series": f"fred:{item['name']}", "error": str(exc)})

    try:
        dane_url = catalog.get("dane", {}).get("trade_balance_page")
        summary = DaneTradeClient().fetch_latest_summary(dane_url)
        output = paths.storage_root / "dane_trade_balance_latest.json"
        output.write_text(json.dumps(summary.__dict__, ensure_ascii=False, default=str, indent=2), encoding="utf-8")
        details["updated"].append("dane:trade_balance")
    except Exception as exc:  # noqa: BLE001
        LOGGER.exception("DANE update failed")
        details["failed"].append({"series": "dane:trade_balance", "error": str(exc)})

    status = "success" if not details["failed"] else "partial_success"
    repository.record_run(status, details, started_at=started)
    return {"status": status, **details}
