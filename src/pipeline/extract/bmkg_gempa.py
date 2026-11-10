"""Ekstraksi data gempa dari feed terbuka BMKG.

Tiga feed (JSON):
  autogempa       -> 1 gempa dirasakan terbaru (objek tunggal)
  gempaterkini    -> 15 gempa M 5.0+ terakhir (list)
  gempadirasakan  -> 15 gempa dirasakan terakhir (list)

Kolom antar feed berbeda: gempaterkini tidak punya Dirasakan/Shakemap,
gempadirasakan tidak punya Potensi. BMKG tidak menyediakan ID kejadian.

Fungsi parse_* di bawah dipakai di tahap silver; diletakkan di sini supaya
semua pengetahuan tentang format BMKG ada di satu tempat.
"""

from __future__ import annotations

import hashlib
import re

import requests

from pipeline import config
from pipeline.http import get


class BmkgFormatError(ValueError):
    """Respons BMKG tidak sesuai struktur yang diharapkan."""


def feed_url(feed: str) -> str:
    if feed not in config.BMKG_GEMPA_FEEDS:
        raise ValueError(f"Feed tidak dikenal: {feed!r}. Pilihan: {config.BMKG_GEMPA_FEEDS}")
    return f"{config.BMKG_GEMPA_BASE_URL}{feed}.json"


def fetch_feed(session: requests.Session, feed: str) -> dict:
    """Ambil satu feed dan pastikan strukturnya valid sebelum disimpan."""
    payload = get(session, feed_url(feed)).json()
    extract_events(payload)  # validasi; lempar BmkgFormatError jika rusak
    return payload


def extract_events(payload: dict) -> list[dict]:
    """Ambil daftar kejadian dari payload feed; autogempa (objek) dijadikan list."""
    try:
        gempa = payload["Infogempa"]["gempa"]
    except (KeyError, TypeError) as exc:
        raise BmkgFormatError("Payload tidak memiliki Infogempa.gempa") from exc
    events = gempa if isinstance(gempa, list) else [gempa]
    if not events or not all(isinstance(e, dict) for e in events):
        raise BmkgFormatError("Infogempa.gempa kosong atau bukan objek")
    for event in events:
        missing = {"DateTime", "Coordinates", "Magnitude"} - event.keys()
        if missing:
            raise BmkgFormatError(f"Kejadian tanpa kolom wajib: {sorted(missing)}")
    return events


# ---------------------------------------------------------------------------
# Parser kolom (dipakai di silver)
# ---------------------------------------------------------------------------
_NUMBER = re.compile(r"-?\d+(?:[.,]\d+)?")


def parse_magnitude(value: str | float | None) -> float | None:
    """'5.3' -> 5.3"""
    if value is None or value == "":
        return None
    match = _NUMBER.search(str(value))
    return float(match.group().replace(",", ".")) if match else None


def parse_depth_km(value: str | float | None) -> float | None:
    """'10 km' -> 10.0"""
    return parse_magnitude(value)


def parse_coordinates(value: str) -> tuple[float, float]:
    """'-0.98,127.49' -> (lat, lon). Lebih andal daripada teks Lintang/Bujur ('0.98 LS')."""
    parts = [p.strip() for p in str(value).split(",")]
    if len(parts) != 2:
        raise BmkgFormatError(f"Format Coordinates tidak dikenal: {value!r}")
    lat, lon = float(parts[0]), float(parts[1])
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        raise BmkgFormatError(f"Koordinat di luar rentang: {value!r}")
    return lat, lon


def event_key(event: dict) -> str:
    """ID buatan untuk kejadian BMKG: hash dari DateTime + Coordinates.

    Kejadian yang sama di feed berbeda (mis. autogempa dan gempadirasakan)
    menghasilkan key yang sama, sehingga bisa dideduplikasi.
    """
    raw = f"bmkg|{event['DateTime'].strip()}|{event['Coordinates'].strip()}"
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:32]
