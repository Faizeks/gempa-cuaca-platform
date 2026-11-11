"""Pembuatan SparkSession untuk task Airflow (task berperan sebagai driver Spark)."""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

from pipeline import config


@contextmanager
def spark_session(app_name: str, master: str | None = None) -> Iterator[SparkSession]:  # noqa: F821
    """SparkSession yang otomatis dihentikan setelah selesai.

    - master default dari SPARK_MASTER_URL (cluster di Docker, local[2] di luar Docker)
    - zona waktu sesi UTC, agar timestamp konsisten dengan partisi bronze
    - spark.driver.host diisi jika SPARK_DRIVER_HOST ada, agar executor di container
      worker bisa menghubungi driver di container airflow-scheduler
    """
    from pyspark.sql import SparkSession  # import di sini agar DAG tetap ringan saat di-parse

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master or config.SPARK_MASTER_URL)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.executor.memory", config.SPARK_EXECUTOR_MEMORY)
        .config("spark.sql.parquet.compression.codec", "snappy")
    )
    if config.SPARK_DRIVER_HOST:
        builder = builder.config("spark.driver.host", config.SPARK_DRIVER_HOST).config(
            "spark.driver.bindAddress", "0.0.0.0"
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        yield spark
    finally:
        spark.stop()


def write_parquet(df: DataFrame, path: Path, partition_by: list[str] | None = None) -> int:  # noqa: F821
    """Tulis DataFrame ke Parquet (overwrite penuh) dan kembalikan jumlah barisnya.

    Data di-cache dulu agar perhitungan jumlah baris dan penulisan tidak
    menjalankan transformasi dua kali.
    """
    df = df.cache()
    try:
        rows = df.count()
        writer = df.write.mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        writer.parquet(str(path))
        return rows
    finally:
        df.unpersist()
