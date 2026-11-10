"""
### Gempa BMKG

Setiap 15 menit mengambil tiga feed gempa BMKG (autogempa, gempaterkini, gempadirasakan)
dan menyimpan respons mentahnya ke layer bronze:

`bronze/bmkg_gempa/feed=<feed>/date=<YYYY-MM-DD>/<HHMMSS>.json` (waktu UTC)

Feed hanya berisi kejadian terakhir, jadi deduplikasi dilakukan di tahap silver
memakai `event_key` (hash DateTime + Coordinates).

Sumber: Data Terbuka BMKG (wajib dicantumkan).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import pendulum
from airflow.sdk import dag, get_current_context, task

from pipeline import config, storage
from pipeline.dag_defaults import DEFAULT_ARGS, TIMEZONE
from pipeline.extract import bmkg_gempa
from pipeline.http import build_session
from pipeline.runtime import run_timestamp

log = logging.getLogger(__name__)


@dag(
    dag_id="bmkg_gempa",
    schedule="*/15 * * * *",
    start_date=pendulum.datetime(2026, 11, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=10),
    default_args=DEFAULT_ARGS,
    tags=["bmkg", "gempa", "bronze"],
    doc_md=__doc__,
)
def bmkg_gempa_dag():
    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract_feed(feed: str) -> dict:
        ts = run_timestamp(get_current_context())
        payload = bmkg_gempa.fetch_feed(build_session(), feed)
        events = bmkg_gempa.extract_events(payload)

        path = storage.bronze_path("bmkg_gempa", {"feed": feed, "date": f"{ts:%Y-%m-%d}"}, f"{ts:%H%M%S}.json")
        storage.write_json(
            path,
            {
                "source_url": bmkg_gempa.feed_url(feed),
                "extracted_at": datetime.now(timezone.utc).isoformat(),
                "payload": payload,
            },
        )
        latest = events[0]
        log.info("%s: %d kejadian, terbaru %s M%s -> %s", feed, len(events), latest["DateTime"], latest["Magnitude"], path)
        return {"feed": feed, "events": len(events), "path": str(path)}

    extract_feed.expand(feed=list(config.BMKG_GEMPA_FEEDS))


bmkg_gempa_dag()
