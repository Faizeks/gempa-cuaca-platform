"""Data quality check untuk layer silver di warehouse.

Setiap check adalah query yang menghitung baris BERMASALAH; check lolos jika hasilnya 0.
Check gagal membuat task Airflow gagal, sehingga masalah data langsung terlihat.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

import psycopg

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Check:
    name: str
    query: str  # mengembalikan satu angka: jumlah baris bermasalah


SILVER_CHECKS = [
    Check("kabkota: minimal 500 kab/kota", "SELECT CASE WHEN count(*) >= 500 THEN 0 ELSE 1 END FROM silver.kabkota"),
    Check("gempa: tabel tidak kosong", "SELECT CASE WHEN count(*) > 0 THEN 0 ELSE 1 END FROM silver.gempa"),
    Check(
        # USGS bisa mencatat magnitudo sedikit negatif untuk gempa yang sangat kecil
        "gempa: magnitudo di luar -2 s.d. 10",
        "SELECT count(*) FROM silver.gempa WHERE magnitudo IS NOT NULL AND (magnitudo < -2 OR magnitudo > 10)",
    ),
    Check(
        # Longgar: BMKG juga melaporkan gempa di negara tetangga (Filipina, PNG, Australia)
        "gempa: koordinat sangat jauh dari wilayah Indonesia",
        "SELECT count(*) FROM silver.gempa WHERE lat NOT BETWEEN -25 AND 25 OR lon NOT BETWEEN 80 AND 155",
    ),
    Check(
        "gempa: punya koordinat tapi tanpa kab/kota",
        "SELECT count(*) FROM silver.gempa WHERE lat IS NOT NULL AND kode_kabkota IS NULL",
    ),
    Check(
        "gempa: waktu kejadian di masa depan",
        "SELECT count(*) FROM silver.gempa WHERE waktu_utc > now() + interval '1 hour'",
    ),
    Check(
        "cuaca: lebih dari 24 slot per lokasi per analisis",
        """SELECT count(*) FROM (
               SELECT 1 FROM silver.prakiraan_cuaca GROUP BY adm4, analisis_utc HAVING count(*) > 24
           ) t""",
    ),
    Check(
        "cuaca: kode kab/kota tidak ada di referensi",
        """SELECT count(*) FROM silver.prakiraan_cuaca c
           LEFT JOIN silver.kabkota k USING (kode_kabkota) WHERE k.kode_kabkota IS NULL""",
    ),
    Check(
        "cuaca: kelembapan di luar 0-100",
        "SELECT count(*) FROM silver.prakiraan_cuaca WHERE kelembapan_pct NOT BETWEEN 0 AND 100",
    ),
]


class DataQualityError(RuntimeError):
    pass


def run_checks(conn: psycopg.Connection, checks: list[Check] = SILVER_CHECKS) -> dict[str, int]:
    results = {}
    with conn.cursor() as cur:
        for check in checks:
            cur.execute(check.query)
            results[check.name] = int(cur.fetchone()[0])
    for name, bad in results.items():
        log.info("[%s] %s (%d baris bermasalah)", "LOLOS" if bad == 0 else "GAGAL", name, bad)
    failed = {name: bad for name, bad in results.items() if bad}
    if failed:
        raise DataQualityError(f"{len(failed)} data quality check gagal: {failed}")
    return results
