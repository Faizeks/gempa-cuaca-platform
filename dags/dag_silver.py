"""
### Silver: bronze -> Parquet bersih -> PostgreSQL

Setiap jam, Spark membaca seluruh data bronze dan menghasilkan layer silver yang
bersih, bertipe benar, dan tanpa duplikat:

| Tabel | Isi | Kunci |
| --- | --- | --- |
| `silver.kabkota` | 514 kab/kota: centroid, luas, penduduk | `kode_kabkota` |
| `silver.gempa` | Gempa BMKG + USGS, dengan kab/kota terdekat | `sumber`, `sumber_event_id` |
| `silver.prakiraan_cuaca` | Satu baris per lokasi x slot x analisis BMKG | `adm4`, `analisis_utc`, `waktu_utc` |

Alur: Spark (cluster) menulis Parquet ke `data/silver/`, lalu tabel PostgreSQL diisi
ulang dengan COPY dalam satu transaksi, lalu data quality check dijalankan.
Silver dihitung ulang penuh setiap run (idempotent); volume datanya masih kecil.
Jika quality check lolos, Asset `warehouse_silver` diperbarui dan DAG `gold` berjalan.
"""

from __future__ import annotations

import logging
from datetime import timedelta

import pendulum
from airflow.sdk import dag, task

from pipeline import config
from pipeline.assets import WAREHOUSE_SILVER
from pipeline.dag_defaults import DEFAULT_ARGS, TIMEZONE

log = logging.getLogger(__name__)

SILVER_DEFAULT_ARGS = {**DEFAULT_ARGS, "retries": 1, "execution_timeout": timedelta(minutes=30)}


@dag(
    dag_id="silver",
    schedule="20 * * * *",  # setiap jam, menit ke-20
    start_date=pendulum.datetime(2026, 11, 1, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    default_args=SILVER_DEFAULT_ARGS,
    tags=["silver", "spark", "warehouse"],
    doc_md=__doc__,
)
def silver():
    @task
    def silver_kabkota() -> int:
        from pipeline.silver import wilayah
        from pipeline.spark import spark_session, write_parquet

        with spark_session("silver_kabkota") as spark:
            df = wilayah.kabkota(wilayah.read_reference(spark, config.KABKOTA_CSV))
            rows = write_parquet(df, config.SILVER_DIR / "kabkota")
        log.info("silver/kabkota: %d baris", rows)
        return rows

    @task
    def silver_gempa() -> int:
        from pipeline.silver import gempa, wilayah
        from pipeline.spark import spark_session, write_parquet

        with spark_session("silver_gempa") as spark:
            reference = wilayah.read_reference(spark, config.KABKOTA_CSV)
            df = gempa.build_silver_gempa(spark, config.BRONZE_DIR, reference)
            rows = write_parquet(df, config.SILVER_DIR / "gempa", partition_by=["sumber"])
        log.info("silver/gempa: %d baris", rows)
        return rows

    @task
    def silver_cuaca() -> int:
        from pipeline.silver import cuaca
        from pipeline.spark import spark_session, write_parquet

        with spark_session("silver_cuaca") as spark:
            df = cuaca.build_silver_cuaca(spark, config.BRONZE_DIR)
            rows = write_parquet(df, config.SILVER_DIR / "prakiraan_cuaca", partition_by=["tanggal_lokal"])
        log.info("silver/prakiraan_cuaca: %d baris", rows)
        return rows

    @task
    def load_warehouse() -> dict:
        from pipeline.load import warehouse
        from pipeline.silver import cuaca, gempa, wilayah

        tables = [
            warehouse.SilverTable("silver", "kabkota", "kabkota", tuple(wilayah.SILVER_COLUMNS)),
            warehouse.SilverTable("silver", "gempa", "gempa", tuple(gempa.SILVER_COLUMNS)),
            warehouse.SilverTable("silver", "prakiraan_cuaca", "prakiraan_cuaca", tuple(cuaca.SILVER_COLUMNS)),
        ]
        with warehouse.connect() as conn:
            warehouse.apply_ddl(conn)
            return {t.table: warehouse.replace_table(conn, t, config.SILVER_DIR) for t in tables}

    # outlets: menandai Asset warehouse_silver, yang memicu DAG gold
    @task(retries=0, outlets=[WAREHOUSE_SILVER])
    def quality_checks() -> dict:
        from pipeline import quality
        from pipeline.load import warehouse

        with warehouse.connect() as conn:
            return quality.run_checks(conn)

    # Spark task dijalankan berurutan agar tidak berebut 2 core worker
    k, g, c = silver_kabkota(), silver_gempa(), silver_cuaca()
    k >> g >> c >> load_warehouse() >> quality_checks()


silver()
