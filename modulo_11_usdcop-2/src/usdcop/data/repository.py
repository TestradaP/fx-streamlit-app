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
        self.series_dir.mkdir(parents=True, exist_ok=True)
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
        if path.exists():
            previous = pd.read_parquet(path)
            combined = pd.concat([previous, incoming], ignore_index=True)
        else:
            combined = incoming
        sort_columns = [column for column in ["observation_date", "retrieved_at"] if column in combined.columns]
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

    def load_series(self, source: str, series_id: str) -> pd.DataFrame:
        path = self.series_dir / f"{source}__{series_id}.parquet"
        if not path.exists():
            raise FileNotFoundError(path)
        frame = pd.read_parquet(path)
        frame["observation_date"] = pd.to_datetime(frame["observation_date"])
        return frame.sort_values("observation_date")

    def registry(self) -> pd.DataFrame:
        with sqlite3.connect(self.db_path) as connection:
            return pd.read_sql_query("SELECT * FROM series_registry ORDER BY source, series_id", connection)

    def record_run(self, status: str, details: dict[str, Any], started_at: datetime | None = None) -> None:
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
