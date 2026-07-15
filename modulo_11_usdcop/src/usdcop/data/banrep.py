from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Iterable

import pandas as pd

from .http import build_session

LOGGER = logging.getLogger(__name__)


class BanRepClient:
    """Cliente defensivo para el portal publico SUAMECA.

    La interfaz REST utilizada por el portal es publica, pero no constituye un
    contrato documentado. Mantenga un adaptador de respaldo para SDMX o archivos
    oficiales antes de considerar el pipeline como productivo.
    """

    BASE_URL = (
        "https://suameca.banrep.gov.co/estadisticas-economicas-back/rest/"
        "estadisticaEconomicaRestService"
    )
    ORIGIN = "https://suameca.banrep.gov.co"
    REFERER = "https://suameca.banrep.gov.co/estadisticas-economicas/"

    def __init__(self, timeout_seconds: int = 30) -> None:
        self.timeout_seconds = timeout_seconds
        self.session = build_session()
        self.session.headers.update(
            {
                "Accept": "application/json, text/plain, */*",
                "Origin": self.ORIGIN,
                "Referer": self.REFERER,
            }
        )

    def _get_json(self, endpoint: str, params: dict[str, Any] | None = None) -> Any:
        url = f"{self.BASE_URL}/{endpoint}"
        response = self.session.get(url, params=params, timeout=self.timeout_seconds)
        response.raise_for_status()
        return response.json()

    def fetch_catalog(self) -> Any:
        return self._get_json("consultaMenuXopcion", {"opcion": "CATALOGO_DATOS"})

    def fetch_menu(self, menu_id: int | str) -> Any:
        return self._get_json("consultaMenuXId", {"idMenu": menu_id})

    def fetch_series(
        self,
        series_id: int | str,
        latest_n: int = 5000,
        all_history: bool = False,
    ) -> pd.DataFrame:
        payload = self._get_json(
            "consultaInformacionSerieXTipoDato",
            {
                "idSerie": str(series_id),
                "tipoDato": 1 if all_history else 0,
                "cantDatos": int(latest_n),
            },
        )
        rows = self._extract_timestamp_value_pairs(payload)
        if not rows:
            raise ValueError(f"No observations parsed for BanRep series {series_id}")
        frame = pd.DataFrame(rows, columns=["timestamp_ms", "value"])
        frame["observation_date"] = (
            pd.to_datetime(frame["timestamp_ms"], unit="ms", utc=True)
            .dt.tz_convert("America/Bogota")
            .dt.tz_localize(None)
            .dt.normalize()
        )
        frame["value"] = pd.to_numeric(frame["value"], errors="coerce")
        frame = frame.dropna(subset=["value"]).sort_values("observation_date")
        frame = frame.drop_duplicates(subset=["observation_date"], keep="last")
        frame["series_id"] = str(series_id)
        frame["source"] = "banrep_suameca"
        frame["retrieved_at"] = datetime.now(timezone.utc)
        # Where an official release timestamp is unavailable from the endpoint,
        # default to the observation date and flag this in metadata downstream.
        frame["release_timestamp"] = frame["observation_date"]
        return frame[
            [
                "series_id",
                "observation_date",
                "value",
                "release_timestamp",
                "retrieved_at",
                "source",
            ]
        ]

    def fetch_active_series(self, series_id: int | str) -> Any:
        return self._get_json("consultaSerieActiva", {"idSeries": str(series_id)})

    @classmethod
    def _extract_timestamp_value_pairs(cls, value: Any) -> list[tuple[int, float]]:
        results: list[tuple[int, float]] = []

        def visit(node: Any) -> None:
            if isinstance(node, dict):
                for key, child in node.items():
                    if key.lower() == "data" and isinstance(child, list):
                        for item in child:
                            if (
                                isinstance(item, (list, tuple))
                                and len(item) >= 2
                                and cls._looks_numeric(item[0])
                                and cls._looks_numeric(item[1])
                            ):
                                results.append((int(float(item[0])), float(item[1])))
                    visit(child)
            elif isinstance(node, list):
                for child in node:
                    visit(child)

        visit(value)
        deduped = {(ts, val) for ts, val in results}
        return sorted(deduped, key=lambda pair: pair[0])

    @staticmethod
    def _looks_numeric(value: Any) -> bool:
        try:
            float(value)
            return True
        except (TypeError, ValueError):
            return False


def flatten_catalog(payload: Any) -> list[dict[str, str]]:
    """Return probable id/name pairs from nested catalog JSON."""
    found: list[dict[str, str]] = []

    def visit(node: Any) -> None:
        if isinstance(node, dict):
            lower = {str(k).lower(): v for k, v in node.items()}
            identifier = lower.get("id") or lower.get("idserie") or lower.get("idmenu")
            name = lower.get("nombre") or lower.get("descripcion") or lower.get("label")
            if identifier is not None and name is not None:
                found.append({"id": str(identifier), "name": str(name)})
            for child in node.values():
                visit(child)
        elif isinstance(node, list):
            for child in node:
                visit(child)

    visit(payload)
    unique = {(row["id"], row["name"]): row for row in found}
    return list(unique.values())
