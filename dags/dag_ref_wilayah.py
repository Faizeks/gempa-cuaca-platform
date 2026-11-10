"""
### Referensi wilayah

Mengunduh kode wilayah administrasi Indonesia (github.com/cahyadsn/wilayah, MIT) lalu
membuat dua file referensi yang dipakai DAG lain:

- `reference/kabkota.csv`: 514 kab/kota dengan provinsi, centroid, luas, dan penduduk
- `reference/lokasi_cuaca.csv`: satu desa (kode adm4) per kab/kota untuk prakiraan cuaca

**Jalankan DAG ini pertama kali** sebelum `bmkg_cuaca`.
"""

from __future__ import annotations

import logging

import pendulum
from airflow.sdk import dag, get_current_context, task

from pipeline import config, storage
from pipeline.dag_defaults import DEFAULT_ARGS, TIMEZONE
from pipeline.extract import wilayah
from pipeline.http import build_session, get
from pipeline.runtime import run_timestamp

log = logging.getLogger(__name__)


@dag(
    dag_id="ref_wilayah",
    schedule="@monthly",
    start_date=pendulum.datetime(2026, 1, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["referensi", "bronze"],
    doc_md=__doc__,
)
def ref_wilayah():
    @task
    def download(name: str, url: str) -> str:
        ts = run_timestamp(get_current_context())
        response = get(build_session(), url)
        path = storage.bronze_path("wilayah", {"date": f"{ts:%Y-%m-%d}"}, f"{name}.sql")
        storage.write_bytes_atomic(path, response.content)
        log.info("Tersimpan %s (%.1f MB)", path, len(response.content) / 1e6)
        return str(path)

    @task
    def build_reference(level12_path: str, full_path: str) -> dict:
        with open(level12_path, encoding="utf-8") as f:
            level12 = wilayah.parse_level12_sql(f.read())
        with open(full_path, encoding="utf-8") as f:
            semua = wilayah.parse_full_sql(f.read())

        kabkota = wilayah.kabkota_records(level12)
        lokasi = [vars(x) for x in wilayah.pick_weather_locations(level12, semua)]

        # Validasi sederhana sebelum menimpa file referensi yang dipakai DAG lain
        if len(kabkota) < 500:
            raise ValueError(f"Jumlah kab/kota tidak wajar: {len(kabkota)}")
        if len(lokasi) != len(kabkota):
            log.warning("%d kab/kota tanpa lokasi cuaca", len(kabkota) - len(lokasi))

        storage.write_csv(config.KABKOTA_CSV, kabkota)
        storage.write_csv(config.LOKASI_CUACA_CSV, lokasi)
        summary = {"kabkota": len(kabkota), "lokasi_cuaca": len(lokasi), "wilayah_semua_level": len(semua)}
        log.info("Referensi diperbarui: %s", summary)
        return summary

    level12 = download.override(task_id="download_level_1_2")("wilayah_level_1_2", config.WILAYAH_LEVEL_1_2_URL)
    full = download.override(task_id="download_semua_level")("wilayah", config.WILAYAH_FULL_URL)
    build_reference(level12, full)


ref_wilayah()
