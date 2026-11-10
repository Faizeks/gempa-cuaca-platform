"""Ekstraksi histori gempa dari USGS FDSN Event API (format CSV).

USGS membatasi 20.000 event per query. Wilayah Indonesia hanya +-3.000 event
per tahun, jadi query per bulan sangat aman. Event USGS bisa direvisi setelah
terbit (kolom `updated`, `status`), sehingga dimuat dengan upsert berdasarkan `id`.
"""

from __future__ import annotations

import csv
import io
from datetime import datetime, timezone

import requests

from pipeline import config
from pipeline.http import get

EXPECTED_COLUMNS = {"time", "latitude", "longitude", "depth", "mag", "id", "updated", "place", "type", "status"}


class UsgsQueryTooLarge(RuntimeError):
    """Jumlah event melebihi batas satu query USGS."""


def _as_utc(value: datetime) -> datetime:
    """Samakan ke UTC; waktu tanpa zona dianggap sudah UTC (konvensi USGS)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: datetime) -> str:
    return _as_utc(value).strftime("%Y-%m-%dT%H:%M:%S")


def build_params(
    start: datetime,
    end: datetime,
    updated_after: datetime | None = None,
    bbox: dict[str, float] | None = None,
) -> dict[str, str | float]:
    """Parameter query untuk rentang [start, end) di kotak wilayah Indonesia."""
    if _as_utc(end) <= _as_utc(start):
        raise ValueError(f"end ({end}) harus setelah start ({start})")
    params: dict[str, str | float] = {
        "starttime": _iso(start),
        "endtime": _iso(end),
        "eventtype": "earthquake",
        "orderby": "time-asc",
        **(bbox or config.INDONESIA_BBOX),
    }
    if updated_after is not None:
        params["updatedafter"] = _iso(updated_after)
    return params


def fetch_count(session: requests.Session, params: dict) -> int:
    response = get(session, config.USGS_BASE_URL + "count", params=params)
    return int(response.text.strip())


def fetch_csv(session: requests.Session, params: dict) -> str:
    """Ambil event sebagai CSV setelah memastikan jumlahnya di bawah batas USGS."""
    count = fetch_count(session, params)
    if count >= config.USGS_MAX_EVENTS_PER_QUERY:
        raise UsgsQueryTooLarge(f"{count} event; perkecil rentang waktu query")
    if count == 0:
        return ""
    text = get(session, config.USGS_BASE_URL + "query", params={**params, "format": "csv"}).text
    validate_csv(text)
    return text


def validate_csv(text: str) -> int:
    """Pastikan header berisi kolom wajib; kembalikan jumlah baris data."""
    reader = csv.DictReader(io.StringIO(text))
    missing = EXPECTED_COLUMNS - set(reader.fieldnames or [])
    if missing:
        raise ValueError(f"CSV USGS tanpa kolom wajib: {sorted(missing)}")
    return sum(1 for _ in reader)
