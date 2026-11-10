from datetime import datetime, timedelta, timezone

import pytest
from conftest import FIXTURES, FakeResponse, FakeSession

from pipeline.extract import usgs

START = datetime(2026, 10, 1, tzinfo=timezone.utc)
END = datetime(2026, 11, 1, tzinfo=timezone.utc)
CSV_TEXT = (FIXTURES / "usgs_events.csv").read_text(encoding="utf-8")


def test_build_params_uses_indonesia_bbox_and_utc_strings():
    params = usgs.build_params(START, END)
    assert params["starttime"] == "2026-10-01T00:00:00"
    assert params["endtime"] == "2026-11-01T00:00:00"
    assert params["minlatitude"] == -11.0 and params["maxlongitude"] == 141.0
    assert "updatedafter" not in params


def test_build_params_converts_timezone_to_utc():
    wib = timezone(timedelta(hours=7))
    params = usgs.build_params(datetime(2026, 11, 1, 7, tzinfo=wib), END + timedelta(days=1))
    assert params["starttime"] == "2026-11-01T00:00:00"


def test_build_params_accepts_mixed_naive_and_aware():
    # Ditemukan saat uji end-to-end dengan Airflow: salah satu waktu bisa tanpa zona
    params = usgs.build_params(datetime(2026, 10, 1), END)
    assert params["starttime"] == "2026-10-01T00:00:00"


def test_build_params_with_updated_after():
    params = usgs.build_params(START, END, updated_after=END - timedelta(hours=1))
    assert params["updatedafter"] == "2026-10-31T23:00:00"


def test_build_params_rejects_empty_range():
    with pytest.raises(ValueError):
        usgs.build_params(END, START)


def test_validate_csv_counts_rows():
    assert usgs.validate_csv(CSV_TEXT) == 2


def test_validate_csv_rejects_missing_columns():
    with pytest.raises(ValueError):
        usgs.validate_csv("time,latitude\n2026-01-01,1.0\n")


def _session(count: int):
    def handler(url, params):
        return FakeResponse(text=str(count)) if url.endswith("count") else FakeResponse(text=CSV_TEXT)

    return FakeSession(handler)


def test_fetch_csv_happy_path():
    session = _session(2)
    assert usgs.fetch_csv(session, usgs.build_params(START, END)) == CSV_TEXT
    assert session.calls[1][1]["format"] == "csv"


def test_fetch_csv_empty_period_skips_query():
    session = _session(0)
    assert usgs.fetch_csv(session, usgs.build_params(START, END)) == ""
    assert len(session.calls) == 1


def test_fetch_csv_refuses_oversized_query():
    with pytest.raises(usgs.UsgsQueryTooLarge):
        usgs.fetch_csv(_session(20000), usgs.build_params(START, END))
