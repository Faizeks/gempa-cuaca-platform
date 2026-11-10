"""Helper waktu run yang konsisten untuk penamaan file dan partisi.

Semua waktu dikembalikan sebagai `datetime` standar ber-zona UTC. Nilai dari
konteks Airflow bisa berupa objek pendulum, kadang tanpa zona waktu, jadi
selalu dinormalkan lebih dulu sebelum dibandingkan atau dihitung.
"""

from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timedelta, timezone


def run_timestamp(context: Mapping) -> datetime:
    """Waktu acuan sebuah run Airflow, dalam UTC.

    Urutan prioritas: akhir data interval -> logical_date -> dag_run.run_after -> sekarang.
    Pada Airflow 3, run manual bisa tidak punya logical_date/data interval, jadi
    selalu ada nilai cadangan.
    """
    for key in ("data_interval_end", "logical_date"):
        value = context.get(key)
        if value is not None:
            return to_utc(value)
    dag_run = context.get("dag_run")
    run_after = getattr(dag_run, "run_after", None) if dag_run is not None else None
    if run_after is not None:
        return to_utc(run_after)
    return datetime.now(timezone.utc)


def data_interval(context: Mapping, fallback_length: timedelta) -> tuple[datetime, datetime]:
    """Rentang data sebuah run dalam UTC.

    Jika run tidak punya data interval yang valid (mis. trigger manual di Airflow 3,
    yang awal dan akhirnya sama), rentang dihitung mundur sepanjang fallback_length
    dari waktu run.
    """
    start, end = context.get("data_interval_start"), context.get("data_interval_end")
    if start is not None and end is not None:
        start, end = to_utc(start), to_utc(end)
        if end > start:
            return start, end
    end = run_timestamp(context)
    return end - fallback_length, end


def previous_month_interval(context: Mapping) -> tuple[datetime, datetime]:
    """Satu bulan kalender penuh (UTC) sebelum akhir interval run.

    Run terjadwal '@monthly' untuk September berakhir 1 Oktober -> [1 Sep, 1 Okt).
    Run manual pada 15 Oktober juga menghasilkan [1 Sep, 1 Okt), bukan
    '30 hari terakhir', sehingga partisi month=YYYY-MM selalu berisi satu bulan utuh.
    """
    end = context.get("data_interval_end")
    reference = to_utc(end) if end is not None else run_timestamp(context)
    month_end = reference.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    month_start = (month_end - timedelta(days=1)).replace(day=1)
    return month_start, month_end


def to_utc(value: datetime) -> datetime:
    """Ubah datetime apa pun (termasuk pendulum) menjadi datetime standar ber-zona UTC.

    Waktu tanpa zona dianggap sudah UTC, sesuai konvensi Airflow.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        aware = datetime(
            value.year, value.month, value.day, value.hour, value.minute, value.second, value.microsecond,
            tzinfo=timezone.utc,
        )
    else:
        aware = value.astimezone(timezone.utc)
        aware = datetime(
            aware.year, aware.month, aware.day, aware.hour, aware.minute, aware.second, aware.microsecond,
            tzinfo=timezone.utc,
        )
    return aware
