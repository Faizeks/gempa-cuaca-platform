"""
### Gempa USGS (histori)

Dua DAG dalam satu file:

- `usgs_backfill`: satu run per bulan sejak Januari 2016 (catchup), mengambil seluruh event
  wilayah Indonesia pada bulan tersebut.
  Hasil: `bronze/usgs/mode=backfill/month=<YYYY-MM>/events.csv`
- `usgs_incremental`: harian, mengambil event yang **dibuat atau direvisi** sejak run
  sebelumnya (parameter `updatedafter`). Event USGS bisa direvisi setelah terbit,
  sehingga di tahap silver dimuat dengan upsert berdasarkan `id` dan `updated`.
  Hasil: `bronze/usgs/mode=incremental/date=<YYYY-MM-DD>/events.csv`

Setiap query dicek dulu jumlahnya (`/count`) agar tidak melewati batas 20.000 event.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, get_current_context, task

from pipeline import storage
from pipeline.dag_defaults import DEFAULT_ARGS
from pipeline.extract import usgs
from pipeline.http import build_session
from pipeline.runtime import data_interval, previous_month_interval

log = logging.getLogger(__name__)


def _save(text: str, partitions: dict[str, str]) -> dict:
    path = storage.bronze_path("usgs", partitions, "events.csv")
    # File tetap ditulis walau kosong, sebagai tanda periode itu sudah diproses
    storage.write_text(path, text)
    rows = usgs.validate_csv(text) if text else 0
    log.info("%d event -> %s", rows, path)
    return {"events": rows, "path": str(path)}


@dag(
    dag_id="usgs_backfill",
    schedule="@monthly",
    start_date=pendulum.datetime(2016, 1, 1, tz="UTC"),
    catchup=True,
    max_active_runs=3,
    default_args=DEFAULT_ARGS,
    tags=["usgs", "gempa", "bronze", "backfill"],
    doc_md=__doc__,
)
def usgs_backfill():
    @task
    def extract_month() -> dict:
        start, end = previous_month_interval(get_current_context())
        log.info("Rentang data: %s s.d. %s", start.isoformat(), end.isoformat())
        text = usgs.fetch_csv(build_session(), usgs.build_params(start, end))
        return _save(text, {"mode": "backfill", "month": f"{start:%Y-%m}"})

    extract_month()


@dag(
    dag_id="usgs_incremental",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args=DEFAULT_ARGS,
    tags=["usgs", "gempa", "bronze"],
    doc_md=__doc__,
)
def usgs_incremental():
    @task
    def extract_updates() -> dict:
        start, end = data_interval(get_current_context(), fallback_length=timedelta(days=1))
        log.info("Rentang data: %s s.d. %s", start.isoformat(), end.isoformat())
        params = usgs.build_params(
            start=end - timedelta(days=60),  # revisi biasanya terjadi pada event beberapa minggu terakhir
            end=end,
            updated_after=start - timedelta(hours=1),  # overlap 1 jam agar tidak ada yang terlewat
        )
        text = usgs.fetch_csv(build_session(), params)
        return _save(text, {"mode": "incremental", "date": f"{end:%Y-%m-%d}"})

    extract_updates()


usgs_backfill()
usgs_incremental()
