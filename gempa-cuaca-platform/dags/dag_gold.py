"""
### Gold: model dimensional dan mart risiko dengan dbt

Berjalan otomatis setiap kali DAG `silver` selesai dan lolos quality check
(dijadwalkan pada Asset `warehouse_silver`, bukan jam tetap).

1. `dbt_build`: seed aset sintetis, membangun semua model, dan menjalankan semua test
   sesuai urutan dependensi. Jika satu test gagal, model turunannya tidak dibangun.
2. `dbt_docs`: membuat dokumentasi dan lineage graph di `dbt/target/`.

Hasil di schema `gold` (PostgreSQL `warehouse`):

| Tabel | Isi |
| --- | --- |
| `dim_wilayah`, `dim_waktu`, `dim_cuaca`, `dim_aset` | Dimensi |
| `fact_gempa`, `fact_prakiraan_cuaca` | Fakta (incremental) |
| `fact_paparan_gempa`, `fact_paparan_cuaca` | Paparan aset terhadap gempa dan cuaca |
| `mart_risiko_provinsi`, `mart_paparan_aset`, `mart_peringatan_cuaca_aset` | Mart untuk dashboard |
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from pipeline.assets import WAREHOUSE_SILVER
from pipeline.dag_defaults import DEFAULT_ARGS, TIMEZONE


@dag(
    dag_id="gold",
    schedule=[WAREHOUSE_SILVER],
    start_date=pendulum.datetime(2026, 11, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args={**DEFAULT_ARGS, "retries": 1, "execution_timeout": timedelta(minutes=30)},
    tags=["gold", "dbt", "warehouse"],
    doc_md=__doc__,
)
def gold():
    @task
    def dbt_build() -> None:
        from pipeline.dbt import run_dbt

        run_dbt(["build"])

    @task
    def dbt_docs() -> None:
        from pipeline.dbt import run_dbt

        run_dbt(["docs", "generate"])

    dbt_build() >> dbt_docs()


gold()
