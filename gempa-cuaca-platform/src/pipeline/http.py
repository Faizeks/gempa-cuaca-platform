"""HTTP session dengan retry dan backoff untuk semua sumber data."""

from __future__ import annotations

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline import config


def build_session(total_retries: int = 3, backoff_factor: float = 2.0) -> requests.Session:
    """Session dengan retry otomatis untuk error sementara (429 dan 5xx).

    backoff_factor=2 -> jeda 2, 4, 8 detik antar percobaan.
    """
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff_factor,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({"User-Agent": config.HTTP_USER_AGENT})
    return session


def get(session: requests.Session, url: str, params: dict | None = None) -> requests.Response:
    """GET dengan timeout; error HTTP dilempar sebagai exception agar task Airflow gagal jelas."""
    response = session.get(url, params=params, timeout=config.HTTP_TIMEOUT_SECONDS)
    response.raise_for_status()
    return response
