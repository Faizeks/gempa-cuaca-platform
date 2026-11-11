from datetime import UTC, datetime, timedelta, timezone
from types import SimpleNamespace

from pipeline import storage
from pipeline.runtime import data_interval, previous_month_interval, run_timestamp


def test_bronze_path_is_hive_style(tmp_path):
    path = storage.bronze_path("bmkg_gempa", {"feed": "autogempa", "date": "2026-11-09"}, "004500.json", tmp_path)
    assert path == tmp_path / "bmkg_gempa" / "feed=autogempa" / "date=2026-11-09" / "004500.json"


def test_atomic_write_overwrites_and_leaves_no_temp_files(tmp_path):
    target = tmp_path / "a" / "b.json"
    storage.write_json(target, {"v": 1})
    storage.write_json(target, {"v": 2})
    assert '"v": 2' in target.read_text(encoding="utf-8")
    assert [p.name for p in target.parent.iterdir()] == ["b.json"]


def test_csv_roundtrip(tmp_path):
    rows = [{"kode": "11.01", "nama": "Aceh Selatan"}]
    storage.write_csv(tmp_path / "x.csv", rows)
    assert storage.read_csv(tmp_path / "x.csv") == rows  # kode tetap teks


def test_run_timestamp_priority_and_utc():
    wib = timezone(timedelta(hours=7))
    ctx = {"data_interval_end": datetime(2026, 11, 9, 7, 0, tzinfo=wib), "logical_date": datetime(2020, 1, 1)}
    assert run_timestamp(ctx) == datetime(2026, 11, 9, 0, 0, tzinfo=UTC)


def test_run_timestamp_falls_back_to_run_after():
    run_after = datetime(2026, 11, 9, 1, 2, 3, tzinfo=UTC)
    assert run_timestamp({"dag_run": SimpleNamespace(run_after=run_after)}) == run_after


def test_data_interval_normalizes_naive_start_before_comparing():
    # Regresi: Airflow bisa memberi data_interval_start tanpa zona waktu
    ctx = {
        "data_interval_start": datetime(2026, 9, 1),
        "data_interval_end": datetime(2026, 10, 1, tzinfo=UTC),
    }
    start, end = data_interval(ctx, timedelta(days=30))
    assert start == datetime(2026, 9, 1, tzinfo=UTC)
    assert start.tzinfo is not None and end.tzinfo is not None


def test_previous_month_interval_scheduled_and_manual_runs_agree():
    utc = UTC
    scheduled = {"data_interval_start": datetime(2026, 9, 1), "data_interval_end": datetime(2026, 10, 1, tzinfo=utc)}
    moment = datetime(2026, 10, 15, 4, tzinfo=utc)
    manual = {"data_interval_start": moment, "data_interval_end": moment}
    expected = (datetime(2026, 9, 1, tzinfo=utc), datetime(2026, 10, 1, tzinfo=utc))
    assert previous_month_interval(scheduled) == expected
    assert previous_month_interval(manual) == expected


def test_previous_month_interval_crosses_year():
    ctx = {"data_interval_end": datetime(2026, 1, 1, tzinfo=UTC)}
    start, end = previous_month_interval(ctx)
    assert (start.year, start.month, end.year, end.month) == (2025, 12, 2026, 1)


def test_data_interval_fallback_for_manual_runs():
    run_after = datetime(2026, 11, 9, tzinfo=UTC)
    start, end = data_interval({"dag_run": SimpleNamespace(run_after=run_after)}, timedelta(days=1))
    assert (start, end) == (run_after - timedelta(days=1), run_after)
