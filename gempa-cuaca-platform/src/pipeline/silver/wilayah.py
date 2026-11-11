"""Silver kab/kota: referensi wilayah bertipe benar (dari reference/kabkota.csv)."""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

SILVER_COLUMNS = [
    "kode_kabkota",
    "nama_kabkota",
    "jenis",
    "ibukota",
    "kode_provinsi",
    "nama_provinsi",
    "lat",
    "lon",
    "elevasi_m",
    "luas_km2",
    "penduduk",
]


def read_reference(spark: SparkSession, kabkota_csv: Path) -> DataFrame:
    if not kabkota_csv.exists():
        raise FileNotFoundError(f"{kabkota_csv} belum ada. Jalankan DAG 'ref_wilayah' terlebih dahulu.")
    # Semua kolom dibaca sebagai teks agar kode seperti '11.01' tidak berubah menjadi angka
    return spark.read.option("header", True).option("inferSchema", False).csv(str(kabkota_csv))


def kabkota(raw: DataFrame) -> DataFrame:
    return raw.select(
        F.col("kode").alias("kode_kabkota"),
        F.col("nama").alias("nama_kabkota"),
        F.when(F.col("nama").startswith("Kota "), F.lit("kota")).otherwise(F.lit("kabupaten")).alias("jenis"),
        F.col("ibukota"),
        F.col("kode_provinsi"),
        F.col("nama_provinsi"),
        F.col("lat").try_cast("double").alias("lat"),
        F.col("lng").try_cast("double").alias("lon"),
        F.col("elv").try_cast("double").alias("elevasi_m"),
        F.col("luas_km2").try_cast("double").alias("luas_km2"),
        F.col("penduduk").try_cast("long").alias("penduduk"),
    ).select(*SILVER_COLUMNS)
