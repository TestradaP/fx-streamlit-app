from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


class SeriesRepository:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.series_dir = self.root / "series"
        self.vintages_dir = self.root / "vintages"
        self.series_dir.mkdir(parents=True, exist_ok=True)
        self.vintages_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = self.root / "metadata.sqlite"
        self._initialize_database()

    def _initialize_database(self) -> None:
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS ingestion_runs (
                    run_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    started_at TEXT NOT NULL,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    details_json TEXT
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS series_registry (
                    source TEXT NOT NULL,
                    series_id TEXT NOT NULL,
                    rows INTEGER NOT NULL,
                    min_date TEXT,
                    max_date TEXT,
                    updated_at TEXT NOT NULL,
                    PRIMARY KEY (source, series_id)
                )
                """
            )

    def save_series(self, frame: pd.DataFrame, source: str, series_id: str) -> Path:
        required = {"observation_date", "value"}
        missing = required.difference(frame.columns)
        if missing:
            raise ValueError(f"Series frame missing columns: {sorted(missing)}")
        path = self.series_dir / f"{source}__{series_id}.parquet"
        incoming = frame.copy()
        incoming["observation_date"] = pd.to_datetime(incoming["observation_date"])
        if "retrieved_at" not in incoming:
            incoming["retrieved_at"] = datetime.now(timezone.utc)
        incoming["retrieved_at"] = pd.to_datetime(incoming["retrieved_at"], utc=True)
        self._save_vintages(incoming, source, series_id)
        if path.exists():
            previous = pd.read_parquet(path)
            combined = pd.concat([previous, incoming], ignore_index=True)
        else:
            combined = incoming
        sort_columns = [
            column
            for column in ["observation_date", "retrieved_at"]
            if column in combined.columns
        ]
        combined = combined.sort_values(sort_columns)
        combined = combined.drop_duplicates(subset=["observation_date"], keep="last")
        combined.to_parquet(path, index=False)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                """
                INSERT INTO series_registry(source, series_id, rows, min_date, max_date, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, series_id) DO UPDATE SET
                    rows=excluded.rows,
                    min_date=excluded.min_date,
                    max_date=excluded.max_date,
                    updated_at=excluded.updated_at
                """,
                (
                    source,
                    series_id,
                    int(len(combined)),
                    str(combined["observation_date"].min().date()),
                    str(combined["observation_date"].max().date()),
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
        return path

    def _save_vintages(self, frame: pd.DataFrame, source: str, series_id: str) -> Path:
        """Append immutable values as observed by each ingestion run.

        The regular series file remains the convenient latest view.  This file
        retains revisions and allows future backtests to reconstruct only the
        values that had actually been downloaded by a given timestamp.
        """
        path = self.vintages_dir / f"{source}__{series_id}.parquet"
        incoming = frame.copy()
        if path.exists():
            previous = pd.read_parquet(path)
            previous["observation_date"] = pd.to_datetime(previous["observation_date"])
            previous_latest = (
                previous.sort_values("retrieved_at")
                .drop_duplicates("observation_date", keep="last")
                .set_index("observation_date")
            )
            incoming["_previous_value"] = incoming["observation_date"].map(
                previous_latest["value"]
            )
            previous_authoritative = previous_latest.get(
                "release_timestamp_is_authoritative",
                pd.Series(False, index=previous_latest.index),
            ).fillna(False).astype(bool)
            incoming_authoritative = incoming.get(
                "release_timestamp_is_authoritative",
                pd.Series(False, index=incoming.index),
            ).fillna(False).astype(bool)
            previous_initial_available = previous_latest.get(
                "initial_release_value",
                pd.Series(pd.NA, index=previous_latest.index),
            ).notna()
            incoming_initial_available = incoming.get(
                "initial_release_value",
                pd.Series(pd.NA, index=incoming.index),
            ).notna()
            metadata_enriched = incoming_authoritative & ~incoming[
                "observation_date"
            ].map(previous_authoritative).fillna(False)
            initial_value_enriched = incoming_initial_available & ~incoming[
                "observation_date"
            ].map(previous_initial_available).fillna(False)
            changed = (
                incoming["_previous_value"].isna()
                | incoming["value"].ne(incoming["_previous_value"])
                | metadata_enriched
                | initial_value_enriched
            )
            incoming = incoming.loc[changed].drop(columns="_previous_value")
            if incoming.empty:
                return path
            vintages = pd.concat([previous, incoming], ignore_index=True)
        else:
            vintages = incoming
        vintages["observation_date"] = pd.to_datetime(vintages["observation_date"])
        vintages["retrieved_at"] = pd.to_datetime(vintages["retrieved_at"], utc=True)
        vintages = vintages.sort_values(["retrieved_at", "observation_date"])
        vintages = vintages.drop_duplicates(
            subset=["observation_date", "retrieved_at"], keep="last"
        )
        vintages.to_parquet(path, index=False)
        return path

    def load_series(self, source: str, series_id: str) -> pd.DataFrame:
        path = self.series_dir / f"{source}__{series_id}.parquet"
        if not path.exists():
            raise FileNotFoundError(path)
        frame = pd.read_parquet(path)
        frame["observation_date"] = pd.to_datetime(frame["observation_date"])
        return frame.sort_values("observation_date")

    def load_vintages(self, source: str, series_id: str) -> pd.DataFrame:
        path = self.vintages_dir / f"{source}__{series_id}.parquet"
        if not path.exists():
            raise FileNotFoundError(path)
        frame = pd.read_parquet(path)
        frame["observation_date"] = pd.to_datetime(frame["observation_date"])
        frame["retrieved_at"] = pd.to_datetime(frame["retrieved_at"], utc=True)
        return frame.sort_values(["retrieved_at", "observation_date"])

    def load_series_as_of(
        self, source: str, series_id: str, as_of: str | pd.Timestamp
    ) -> pd.DataFrame:
        """Return the latest downloaded vintage available at ``as_of``."""
        vintages = self.load_vintages(source, series_id)
        cutoff = pd.Timestamp(as_of)
        cutoff = (
            cutoff.tz_localize("UTC")
            if cutoff.tzinfo is None
            else cutoff.tz_convert("UTC")
        )
        eligible = vintages.loc[vintages["retrieved_at"].le(cutoff)].copy()
        if eligible.empty:
            return eligible
        return (
            eligible.sort_values(["observation_date", "retrieved_at"])
            .drop_duplicates("observation_date", keep="last")
            .sort_values("observation_date")
        )

    def load_first_seen_series(self, source: str, series_id: str) -> pd.DataFrame:
        """Return the first value captured for every observation date.

        This prevents revisions observed in later ingestion runs from leaking
        backwards.  It is genuinely historical only for observations released
        after the archive began; coverage metadata keeps that limitation visible.
        """
        vintages = self.load_vintages(source, series_id)
        first_seen = (
            vintages.sort_values(["observation_date", "retrieved_at"])
            .drop_duplicates("observation_date", keep="first")
            .sort_values("observation_date")
        )
        if "release_timestamp_is_authoritative" in vintages:
            authoritative = vintages.loc[
                vintages["release_timestamp_is_authoritative"].fillna(False).astype(bool)
            ]
            if not authoritative.empty:
                release_metadata = (
                    authoritative.sort_values(["observation_date", "retrieved_at"])
                    .drop_duplicates("observation_date", keep="first")
                    .set_index("observation_date")
                )
                for column in (
                    "release_timestamp",
                    "release_timestamp_is_authoritative",
                    "release_timestamp_source",
                    "initial_release_value",
                ):
                    if column in release_metadata:
                        mapped = first_seen["observation_date"].map(
                            release_metadata[column]
                        )
                        first_seen[column] = mapped.where(
                            mapped.notna(), first_seen.get(column)
                        )
                initial_values = pd.to_numeric(
                    first_seen.get(
                        "initial_release_value",
                        pd.Series(float("nan"), index=first_seen.index),
                    ),
                    errors="coerce",
                )
                first_seen["value"] = initial_values.where(
                    initial_values.notna(), first_seen["value"]
                )
        return first_seen

    def vintage_series_diagnostics(self, source: str, series_id: str) -> dict[str, Any]:
        vintages = self.load_vintages(source, series_id)
        first_seen = (
            vintages.sort_values(["observation_date", "retrieved_at"])
            .drop_duplicates("observation_date", keep="first")
            .copy()
        )
        authoritative = first_seen.get(
            "release_timestamp_is_authoritative",
            pd.Series(False, index=first_seen.index),
        ).fillna(False).astype(bool)
        initial_values = pd.to_numeric(
            first_seen.get(
                "initial_release_value", pd.Series(float("nan"), index=first_seen.index)
            ),
            errors="coerce",
        )
        release = pd.to_datetime(
            first_seen.get(
                "release_timestamp", pd.Series(pd.NaT, index=first_seen.index)
            ),
            errors="coerce",
            utc=True,
        )
        retrieved = pd.to_datetime(first_seen["retrieved_at"], utc=True)
        captured_after_release = release.notna() & retrieved.ge(release)
        return {
            "source": source,
            "series_id": series_id,
            "observations": int(len(first_seen)),
            "authoritative_release_share": float(authoritative.mean()),
            "initial_release_value_share": float(initial_values.notna().mean()),
            "release_before_retrieval_share": float(captured_after_release.mean()),
            "archive_first_retrieval": retrieved.min().isoformat(),
            "archive_last_retrieval": retrieved.max().isoformat(),
        }

    def vintage_coverage(self) -> pd.DataFrame:
        rows: list[dict[str, Any]] = []
        for path in sorted(self.vintages_dir.glob("*.parquet")):
            frame = pd.read_parquet(path)
            retrieved = pd.to_datetime(frame["retrieved_at"], utc=True)
            observation = pd.to_datetime(frame["observation_date"])
            authoritative = frame.get(
                "release_timestamp_is_authoritative",
                pd.Series(False, index=frame.index),
            ).fillna(False).astype(bool)
            source, series_id = path.stem.split("__", 1)
            rows.append(
                {
                    "source": source,
                    "series_id": series_id,
                    "vintage_rows": int(len(frame)),
                    "snapshots": int(retrieved.nunique()),
                    "first_snapshot": retrieved.min().isoformat(),
                    "last_snapshot": retrieved.max().isoformat(),
                    "first_observation": str(observation.min().date()),
                    "last_observation": str(observation.max().date()),
                    "authoritative_release_share": float(authoritative.mean()),
                }
            )
        return pd.DataFrame(rows)

    def registry(self) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as connection:
            return pd.read_sql_query("SELECT * FROM series_registry ORDER BY source, series_id", connection)

    def record_run(
        self,
        status: str,
        details: dict[str, Any],
        started_at: datetime | None = None,
    ) -> None:
        start = started_at or datetime.now(timezone.utc)
        with sqlite3.connect(self.db_path) as connection:
            connection.execute(
                "INSERT INTO ingestion_runs(started_at, finished_at, status, details_json) VALUES (?, ?, ?, ?)",
                (
                    start.isoformat(),
                    datetime.now(timezone.utc).isoformat(),
                    status,
                    json.dumps(details, ensure_ascii=False, default=str),
                ),
            )
