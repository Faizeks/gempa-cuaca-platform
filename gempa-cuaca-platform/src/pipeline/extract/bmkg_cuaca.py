"""Ekstraksi prakiraan cuaca BMKG per kelurahan/desa (kode adm4).

Struktur respons (sudah diuji): {"lokasi": {...}, "data": [{"lokasi": {...}, "cuaca": [[slot, ...], ...]}]}
Jumlah slot bervariasi (maksimal 24 = 3 hari x 8) karena slot yang sudah lewat
tidak dikirim. Batas akses BMKG: 60 permintaan per menit per IP.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import requests

from pipeline import config
from pipeline.http import get

log = logging.getLogger(__name__)

MAX_SLOTS = 24


class BmkgCuacaFormatError(ValueError):
    """Respons prakiraan cuaca tidak sesuai struktur yang diharapkan."""


def fetch_forecast(session: requests.Session, adm4: str) -> dict:
    payload = get(session, config.BMKG_CUACA_URL, params={"adm4": adm4}).json()
    count_slots(payload)  # validasi
    return payload


def iter_slots(payload: dict) -> list[dict]:
    """Ratakan data[0].cuaca (list per hari berisi list slot) menjadi satu list slot."""
    try:
        days = payload["data"][0]["cuaca"]
    except (KeyError, IndexError, TypeError) as exc:
        raise BmkgCuacaFormatError("Payload tidak memiliki data[0].cuaca") from exc
    return [slot for day in days for slot in day]


def count_slots(payload: dict) -> int:
    slots = iter_slots(payload)
    if not 1 <= len(slots) <= MAX_SLOTS:
        raise BmkgCuacaFormatError(f"Jumlah slot tidak wajar: {len(slots)} (harus 1-{MAX_SLOTS})")
    for slot in slots:
        if "local_datetime" not in slot or "t" not in slot:
            raise BmkgCuacaFormatError("Slot tanpa local_datetime atau t")
    return len(slots)


@dataclass
class BatchResult:
    records: list[dict] = field(default_factory=list)
    failures: dict[str, str] = field(default_factory=dict)

    @property
    def total(self) -> int:
        return len(self.records) + len(self.failures)

    @property
    def failure_ratio(self) -> float:
        return len(self.failures) / self.total if self.total else 0.0


def fetch_many(
    session: requests.Session,
    adm4_codes: Iterable[str],
    delay_seconds: float = config.BMKG_CUACA_DELAY_SECONDS,
    sleep: Callable[[float], None] = time.sleep,
) -> BatchResult:
    """Ambil prakiraan banyak lokasi berurutan dengan jeda agar tidak melewati rate limit.

    Lokasi yang gagal dicatat (bukan menghentikan seluruh batch); pemanggil
    memutuskan apakah rasio kegagalan masih bisa diterima.
    Setiap record berisi metadata ekstraksi + respons mentah.
    """
    result = BatchResult()
    for i, adm4 in enumerate(adm4_codes):
        if i > 0:
            sleep(delay_seconds)
        try:
            payload = fetch_forecast(session, adm4)
        except (requests.RequestException, ValueError) as exc:
            log.warning("Gagal mengambil adm4=%s: %s", adm4, exc)
            result.failures[adm4] = repr(exc)
            continue
        result.records.append(
            {
                "adm4": adm4,
                "extracted_at": datetime.now(UTC).isoformat(),
                "payload": payload,
            }
        )
    return result
