"""
### Prakiraan cuaca BMKG

Dua kali sehari mengambil prakiraan cuaca untuk satu desa per kab/kota
(daftar dari `reference/lokasi_cuaca.csv`, dibuat oleh DAG `ref_wilayah`).

- Jeda +-1,1 detik antar permintaan agar di bawah batas BMKG (60/menit/IP);
  514 lokasi butuh +-10 menit.
- Hasil: `bronze/bmkg_cuaca/date=<YYYY-MM-DD>/run=<HHMM>.jsonl` (satu baris per lokasi, waktu UTC)
- Lokasi yang gagal dicatat di file `.failures.json`; run gagal jika lebih dari 10% lokasi gagal.
- Untuk uji cepat, set `BMKG_CUACA_MAX_LOCATIONS=20` di `.env`.

Sumber: Data Terbuka BMKG (wajib dicantumkan).
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task

from pipeline import config, storage
from pipeline.dag_defaults import DEFAULT_ARGS, TIMEZONE
from pipeline.extract import bmkg_cuaca
from pipeline.http import build_session
from pipeline.runtime import run_timestamp

log = logging.getLogger(__name__)


@dag(
    dag_id="bmkg_cuaca",
    schedule="15 6,18 * * *",  # 06:15 dan 18:15 WIB
    start_date=pendulum.datetime(2026, 11, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["bmkg", "cuaca", "bronze"],
    doc_md=__doc__,
)
def bmkg_cuaca_dag():
    @task
    def load_locations() -> list[str]:
        if not config.LOKASI_CUACA_CSV.exists():
            raise FileNotFoundError(
                f"{config.LOKASI_CUACA_CSV} belum ada. Jalankan DAG 'ref_wilayah' terlebih dahulu."
            )
        codes = [row["adm4"] for row in storage.read_csv(config.LOKASI_CUACA_CSV)]
        if config.BMKG_CUACA_MAX_LOCATIONS:
            codes = codes[: config.BMKG_CUACA_MAX_LOCATIONS]
        log.info("%d lokasi akan diambil", len(codes))
        return codes

    # Satu retry saja: setiap percobaan ulang memanggil ulang semua lokasi (+-10 menit)
    @task(retries=1, execution_timeout=timedelta(minutes=40))
    def extract_forecasts(adm4_codes: list[str]) -> dict:
        ts = run_timestamp(get_current_context())
        result = bmkg_cuaca.fetch_many(build_session(), adm4_codes)

        partitions = {"date": f"{ts:%Y-%m-%d}"}
        path = storage.bronze_path("bmkg_cuaca", partitions, f"run={ts:%H%M}.jsonl")
        if result.records:
            storage.write_jsonl(path, result.records)
        if result.failures:
            storage.write_json(
                storage.bronze_path("bmkg_cuaca", partitions, f"run={ts:%H%M}.failures.json"), result.failures
            )

        summary = {
            "lokasi_berhasil": len(result.records),
            "lokasi_gagal": len(result.failures),
            "slot_total": sum(bmkg_cuaca.count_slots(r["payload"]) for r in result.records),
            "path": str(path),
        }
        log.info("Ringkasan: %s", summary)
        if result.failure_ratio > config.BMKG_CUACA_MAX_FAILURE_RATIO:
            raise RuntimeError(
                f"{result.failure_ratio:.0%} lokasi gagal (batas {config.BMKG_CUACA_MAX_FAILURE_RATIO:.0%}); "
                "data yang berhasil tetap tersimpan"
            )
        return summary

    extract_forecasts(load_locations())


bmkg_cuaca_dag()
