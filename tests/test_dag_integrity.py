"""Memastikan semua DAG ter-parse oleh Airflow tanpa error.

Butuh Airflow terpasang; jalankan di dalam container:
    docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests/test_dag_integrity.py
Di luar container (tanpa Airflow) test ini otomatis dilewati.
"""

from pathlib import Path

import pytest

pytest.importorskip("airflow")
from airflow.models import DagBag  # noqa: E402

DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"
EXPECTED_DAGS = {"ref_wilayah", "bmkg_gempa", "bmkg_cuaca", "usgs_backfill", "usgs_incremental", "silver", "gold"}


@pytest.fixture(scope="module")
def dagbag():
    return DagBag(dag_folder=str(DAGS_DIR))


def test_no_import_errors(dagbag):
    assert dagbag.import_errors == {}


def test_expected_dags_present(dagbag):
    assert EXPECTED_DAGS <= set(dagbag.dag_ids)


def test_every_dag_has_tags_docs_and_retries(dagbag):
    for dag_id in EXPECTED_DAGS:
        dag = dagbag.dags[dag_id]  # get_dag() di Airflow 3 membaca database; cukup hasil parsing
        assert dag.tags, dag_id
        assert dag.doc_md, dag_id
        for t in dag.tasks:
            if t.task_id == "quality_checks":
                continue  # sengaja tanpa retry: data buruk tidak akan membaik dengan dicoba ulang
            assert t.retries >= 1, f"{dag_id}.{t.task_id} tanpa retry"


def test_gold_dijadwalkan_dari_asset_silver(dagbag):
    silver_outlets = dagbag.dags["silver"].get_task("quality_checks").outlets
    assert [a.name for a in silver_outlets] == ["warehouse_silver"]
    gold = dagbag.dags["gold"]
    assert [t.task_id for t in gold.topological_sort()] == ["dbt_build", "dbt_docs"]
