from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from .http import build_session


@dataclass(frozen=True)
class TradeBalanceSummary:
    period_label: str
    deficit_usd_millions: float
    publication_date_label: str | None
    source_url: str
    retrieved_at: datetime


class DaneTradeClient:
    DEFAULT_URL = (
        "https://www.dane.gov.co/index.php/estadisticas-por-tema/"
        "comercio-internacional/balanza-comercial"
    )

    def __init__(self, timeout_seconds: int = 30) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = build_session()

    def fetch_latest_summary(self, url: str | None = None) -> TradeBalanceSummary:
        target = url or self.DEFAULT_URL
        response = self.session.get(target, timeout=self.timeout_seconds)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        text = " ".join(soup.stripped_strings)
        period_match = re.search(r"Informaci[oó]n\s+([A-Za-zÁÉÍÓÚáéíóú]+\s+\d{4})", text)
        deficit_match = re.search(
            r"d[eé]ficit.*?US\$\s*([\d\.,]+)\s*millones",
            text,
            flags=re.IGNORECASE,
        )
        date_match = re.search(r"actualizada?\s+el\s+([^\.]+)", text, flags=re.IGNORECASE)
        if not deficit_match:
            raise ValueError("Could not parse DANE trade-balance deficit")
        raw = deficit_match.group(1).replace(".", "").replace(",", ".")
        return TradeBalanceSummary(
            period_label=period_match.group(1) if period_match else "unknown",
            deficit_usd_millions=float(raw),
            publication_date_label=date_match.group(1).strip() if date_match else None,
            source_url=target,
            retrieved_at=datetime.now(timezone.utc),
        )

    def list_download_links(self, url: str | None = None) -> list[dict[str, str]]:
        target = url or self.DEFAULT_URL
        response = self.session.get(target, timeout=self.timeout_seconds)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, "lxml")
        links: list[dict[str, str]] = []
        for anchor in soup.find_all("a", href=True):
            label = " ".join(anchor.stripped_strings)
            href = urljoin(target, anchor["href"])
            if any(token in href.lower() for token in (".xlsx", ".xls", ".csv")):
                links.append({"label": label, "url": href})
        return links
