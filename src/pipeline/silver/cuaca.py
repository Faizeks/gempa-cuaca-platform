"""Silver prakiraan cuaca: satu baris per lokasi x slot waktu x waktu analisis BMKG.

Setiap run bronze menyimpan prakiraan terbaru; prakiraan untuk slot yang sama bisa
berubah antar analisis. Kunci baris: (adm4, analysis_date, waktu_utc). Jika satu
analisis tersimpan lebih dari sekali, diambil ekstraksi terakhir.
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

_NUM = T.DoubleType()
SLOT = T.StructType(
    [
        T.StructField("utc_datetime", T.StringType()),
        T.StructField("local_datetime", T.StringType()),
        T.StructField("analysis_date", T.StringType()),
        T.StructField("t", _NUM),
        T.StructField("hu", _NUM),
        T.StructField("tp", _NUM),
        T.StructField("tcc", _NUM),
        T.StructField("ws", _NUM),
        T.StructField("wd_deg", _NUM),
        T.StructField("wd", T.StringType()),
        T.StructField("vs", _NUM),
        T.StructField("weather", _NUM),
        T.StructField("weather_desc", T.StringType()),
        T.StructField("weather_desc_en", T.StringType()),
    ]
)
LOKASI = T.StructType(
    [
        T.StructField("adm4", T.StringType()),
        T.StructField("desa", T.StringType()),
        T.StructField("kecamatan", T.StringType()),
        T.StructField("kotkab", T.StringType()),
        T.StructField("provinsi", T.StringType()),
        T.StructField("lat", _NUM),
        T.StructField("lon", _NUM),
    ]
)
BRONZE_RECORD = T.StructType(
    [
        T.StructField("adm4", T.StringType()),
        T.StructField("extracted_at", T.StringType()),
        T.StructField(
            "payload",
            T.StructType(
                [
                    T.StructField("lokasi", LOKASI),
                    T.StructField(
                        "data",
                        T.ArrayType(T.StructType([T.StructField("cuaca", T.ArrayType(T.ArrayType(SLOT)))])),
                    ),
                ]
            ),
        ),
    ]
)

SILVER_COLUMNS = [
    "adm4",
    "kode_kabkota",
    "desa",
    "lat",
    "lon",
    "analisis_utc",
    "waktu_utc",
    "tanggal_lokal",
    "jam_lokal",
    "suhu_c",
    "kelembapan_pct",
    "curah_hujan_mm",
    "tutupan_awan_pct",
    "kec_angin_kmj",
    "arah_angin_deg",
    "arah_angin",
    "jarak_pandang_m",
    "kode_cuaca",
    "cuaca",
    "cuaca_en",
    "diekstrak_utc",
]


def read_bronze(spark: SparkSession, bronze_dir: Path) -> DataFrame:
    path = bronze_dir / "bmkg_cuaca"
    if not path.exists():
        raise FileNotFoundError(f"Belum ada data bronze cuaca di {path}")
    return (
        spark.read.schema(BRONZE_RECORD)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.jsonl")
        .json(str(path))
    )


def forecast_slots(raw: DataFrame) -> DataFrame:
    """Ratakan data[0].cuaca (list hari -> list slot) menjadi satu baris per slot."""
    slots = (
        raw.select(
            "adm4",
            "extracted_at",
            F.col("payload.lokasi").alias("lokasi"),
            F.explode(F.flatten(F.col("payload.data")[0]["cuaca"])).alias("s"),
        )
        .select(
            F.col("adm4"),
            F.substring("adm4", 1, 5).alias("kode_kabkota"),  # '31.71.03.1001' -> '31.71'
            F.col("lokasi.desa").alias("desa"),
            F.col("lokasi.lat").alias("lat"),
            F.col("lokasi.lon").alias("lon"),
            F.try_to_timestamp(F.col("s.analysis_date")).alias("analisis_utc"),
            F.try_to_timestamp(F.col("s.utc_datetime"), F.lit("yyyy-MM-dd HH:mm:ss")).alias("waktu_utc"),
            F.to_date(F.try_to_timestamp(F.substring("s.local_datetime", 1, 10), F.lit("yyyy-MM-dd"))).alias(
                "tanggal_lokal"
            ),
            F.substring("s.local_datetime", 12, 2).try_cast("int").alias("jam_lokal"),
            F.col("s.t").alias("suhu_c"),
            F.col("s.hu").alias("kelembapan_pct"),
            F.col("s.tp").alias("curah_hujan_mm"),
            F.col("s.tcc").alias("tutupan_awan_pct"),
            F.col("s.ws").alias("kec_angin_kmj"),
            F.col("s.wd_deg").alias("arah_angin_deg"),
            F.col("s.wd").alias("arah_angin"),
            F.col("s.vs").alias("jarak_pandang_m"),
            F.col("s.weather").try_cast("int").alias("kode_cuaca"),
            F.col("s.weather_desc").alias("cuaca"),
            F.col("s.weather_desc_en").alias("cuaca_en"),
            F.try_to_timestamp(F.col("extracted_at")).alias("diekstrak_utc"),
        )
        .where(F.col("adm4").isNotNull() & F.col("waktu_utc").isNotNull() & F.col("analisis_utc").isNotNull())
    )
    latest = Window.partitionBy("adm4", "analisis_utc", "waktu_utc").orderBy(F.col("diekstrak_utc").desc_nulls_last())
    return slots.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn").select(*SILVER_COLUMNS)


def build_silver_cuaca(spark: SparkSession, bronze_dir: Path) -> DataFrame:
    return forecast_slots(read_bronze(spark, bronze_dir))
