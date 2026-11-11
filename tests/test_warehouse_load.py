"""Test integrasi loader dan quality check terhadap PostgreSQL sungguhan.

Dilewati jika database tidak bisa dihubungi. Di Docker:
    docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests/test_warehouse_load.py
Test ini membuat schema sementara dan menghapusnya kembali.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime

import pytest

pyarrow = pytest.importorskip("pyarrow")
psycopg = pytest.importorskip("psycopg")
import pyarrow.parquet as pq  # noqa: E402

from pipeline import quality  # noqa: E402
from pipeline.load import warehouse  # noqa: E402


@pytest.fixture
def conn():
    try:
        connection = warehouse.connect(connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("Warehouse PostgreSQL tidak tersedia")
    schema = f"test_{uuid.uuid4().hex[:8]}"
    connection.execute(f"CREATE SCHEMA {schema}")
    connection.execute(
        f"CREATE TABLE {schema}.gempa (sumber text, sumber_event_id text, waktu_utc timestamptz, "
        "magnitudo double precision, PRIMARY KEY (sumber, sumber_event_id))"
    )
    connection.commit()
    yield connection, schema
    connection.rollback()
    connection.execute(f"DROP SCHEMA {schema} CASCADE")
    connection.commit()
    connection.close()


def _write_partitioned(tmp_path, rows):
    """Tiru keluaran Spark: Parquet dipartisi gaya Hive (sumber=...)."""
    for sumber in {r["sumber"] for r in rows}:
        part = [{k: v for k, v in r.items() if k != "sumber"} for r in rows if r["sumber"] == sumber]
        folder = tmp_path / "gempa" / f"sumber={sumber}"
        folder.mkdir(parents=True)
        pq.write_table(pyarrow.Table.from_pylist(part), folder / "part-0.parquet")


ROWS = [
    {"sumber": "bmkg", "sumber_event_id": "a", "waktu_utc": datetime(2026, 11, 9, tzinfo=UTC), "magnitudo": 5.1},
    {"sumber": "usgs", "sumber_event_id": "us1", "waktu_utc": datetime(2026, 11, 8, tzinfo=UTC), "magnitudo": 4.2},
]


def test_replace_table_is_idempotent_and_reads_partition_column(conn, tmp_path):
    connection, schema = conn
    _write_partitioned(tmp_path, ROWS)
    target = warehouse.SilverTable(schema, "gempa", "gempa", ("sumber", "sumber_event_id", "waktu_utc", "magnitudo"))

    assert warehouse.replace_table(connection, target, tmp_path) == 2
    assert warehouse.replace_table(connection, target, tmp_path) == 2  # rerun tidak menggandakan
    rows = connection.execute(f"SELECT sumber, sumber_event_id, magnitudo FROM {schema}.gempa ORDER BY 1").fetchall()
    assert rows == [("bmkg", "a", 5.1), ("usgs", "us1", 4.2)]


def test_quality_checks_report_failures(conn):
    connection, schema = conn
    connection.execute(f"INSERT INTO {schema}.gempa VALUES ('bmkg', 'x', now(), 42)")
    checks = [quality.Check("magnitudo wajar", f"SELECT count(*) FROM {schema}.gempa WHERE magnitudo > 10")]
    with pytest.raises(quality.DataQualityError):
        quality.run_checks(connection, checks)
    connection.execute(f"UPDATE {schema}.gempa SET magnitudo = 5")
    assert quality.run_checks(connection, checks) == {"magnitudo wajar": 0}
