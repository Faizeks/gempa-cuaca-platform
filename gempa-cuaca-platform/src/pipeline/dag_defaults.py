"""Pengaturan bersama untuk semua DAG (tanpa import Airflow, supaya mudah diuji)."""

from __future__ import annotations

from datetime import timedelta

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=20),
}

TIMEZONE = "Asia/Jakarta"
