"""Silver gempa: gabungan feed BMKG dan katalog USGS dalam satu skema.

Aturan (sesuai blueprint): kedua sumber TIDAK digabung jadi satu baris. Setiap baris
punya kolom `sumber`; mart memilih sumber sesuai perannya (USGS untuk histori dan
M >= 5, BMKG untuk gempa terbaru dan gempa dirasakan).
"""

from __future__ import annotations

from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

from pipeline import config

EARTH_RADIUS_KM = 6371.0
NUMBER_PATTERN = r"(-?\d+(?:[.,]\d+)?)"

# Kolom satu kejadian di feed BMKG (semua teks; feed yang tidak punya kolom tertentu -> null)
BMKG_EVENT_FIELDS = [
    "Tanggal", "Jam", "DateTime", "Coordinates", "Lintang", "Bujur",
    "Magnitude", "Kedalaman", "Wilayah", "Potensi", "Dirasakan", "Shakemap",
]
BMKG_EVENT = T.StructType([T.StructField(f, T.StringType()) for f in BMKG_EVENT_FIELDS])

USGS_COLUMNS = [
    "time", "latitude", "longitude", "depth", "mag", "magType", "nst", "gap", "dmin", "rms", "net", "id",
    "updated", "place", "type", "horizontalError", "depthError", "magError", "magNst", "status",
    "locationSource", "magSource",
]
USGS_SCHEMA = T.StructType([T.StructField(c, T.StringType()) for c in USGS_COLUMNS])

# Skema akhir silver gempa (urutan kolom = urutan tabel warehouse)
SILVER_COLUMNS = [
    "sumber", "sumber_event_id", "waktu_utc", "lat", "lon", "kedalaman_km", "magnitudo",
    "tipe_magnitudo", "keterangan_lokasi", "potensi", "dirasakan", "status_review",
    "diperbarui_utc", "feed_bmkg", "kode_kabkota", "jarak_kabkota_km", "diekstrak_utc",
]


def _number(col: F.Column) -> F.Column:
    """Ambil angka pertama dari teks ('10 km' -> 10.0, '4,8' -> 4.8); gagal -> null."""
    raw = F.regexp_extract(col, NUMBER_PATTERN, 1)
    return F.when(raw == "", F.lit(None)).otherwise(F.regexp_replace(raw, ",", ".")).try_cast("double")


def read_bmkg_bronze(spark: SparkSession, bronze_dir: Path) -> DataFrame | None:
    """Baca semua file bronze BMKG gempa; satu baris per (file, kejadian)."""
    frames = []
    for feed in config.BMKG_GEMPA_FEEDS:
        feed_dir = bronze_dir / "bmkg_gempa" / f"feed={feed}"
        if not feed_dir.exists():
            continue
        # autogempa berisi satu objek, feed lain berisi list -> skema berbeda per feed
        gempa_type = BMKG_EVENT if feed == "autogempa" else T.ArrayType(BMKG_EVENT)
        schema = T.StructType(
            [
                T.StructField("source_url", T.StringType()),
                T.StructField("extracted_at", T.StringType()),
                T.StructField(
                    "payload",
                    T.StructType([T.StructField("Infogempa", T.StructType([T.StructField("gempa", gempa_type)]))]),
                ),
            ]
        )
        df = (
            spark.read.schema(schema)
            .option("multiLine", True)
            .option("recursiveFileLookup", True)
            .option("pathGlobFilter", "*.json")
            .json(str(feed_dir))
        )
        events = F.col("payload.Infogempa.gempa")
        events = F.array(events) if feed == "autogempa" else events
        frames.append(
            df.select(
                F.lit(feed).alias("feed"),
                F.col("extracted_at"),
                F.explode(events).alias("e"),
            ).select("feed", "extracted_at", "e.*")
        )
    if not frames:
        return None
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result


def bmkg_events(raw: DataFrame) -> DataFrame:
    """Bersihkan dan deduplikasi kejadian BMKG.

    Satu kejadian muncul berkali-kali (setiap 15 menit, dan di beberapa feed). Kunci
    deduplikasi `event_key` = 32 karakter pertama sha256('bmkg|DateTime|Coordinates'),
    sama persis dengan pipeline.extract.bmkg_gempa.event_key.
    """
    coords = F.split(F.col("Coordinates"), ",")
    cleaned = raw.where(F.col("DateTime").isNotNull() & F.col("Coordinates").isNotNull()).select(
        F.substring(
            F.sha2(F.concat_ws("|", F.lit("bmkg"), F.trim("DateTime"), F.trim("Coordinates")), 256), 1, 32
        ).alias("sumber_event_id"),
        F.try_to_timestamp(F.col("DateTime")).alias("waktu_utc"),
        F.trim(F.get(coords, 0)).try_cast("double").alias("lat"),
        F.trim(F.get(coords, 1)).try_cast("double").alias("lon"),
        _number(F.col("Kedalaman")).alias("kedalaman_km"),
        _number(F.col("Magnitude")).alias("magnitudo"),
        F.col("Wilayah").alias("keterangan_lokasi"),
        F.col("Potensi").alias("potensi"),
        F.col("Dirasakan").alias("dirasakan"),
        F.col("feed"),
        F.try_to_timestamp(F.col("extracted_at")).alias("diekstrak_utc"),
    )
    # Gabungkan semua kemunculan: ambil nilai non-null (mis. 'dirasakan' hanya ada di
    # sebagian feed) dan catat feed mana saja yang memuat kejadian ini
    return cleaned.groupBy("sumber_event_id").agg(
        F.first("waktu_utc", ignorenulls=True).alias("waktu_utc"),
        F.first("lat", ignorenulls=True).alias("lat"),
        F.first("lon", ignorenulls=True).alias("lon"),
        F.first("kedalaman_km", ignorenulls=True).alias("kedalaman_km"),
        F.first("magnitudo", ignorenulls=True).alias("magnitudo"),
        F.first("keterangan_lokasi", ignorenulls=True).alias("keterangan_lokasi"),
        F.first("potensi", ignorenulls=True).alias("potensi"),
        F.first("dirasakan", ignorenulls=True).alias("dirasakan"),
        F.array_join(F.sort_array(F.collect_set("feed")), ",").alias("feed_bmkg"),
        F.max("diekstrak_utc").alias("diekstrak_utc"),
    ).select(
        F.lit("bmkg").alias("sumber"),
        "sumber_event_id",
        "waktu_utc",
        "lat",
        "lon",
        "kedalaman_km",
        "magnitudo",
        F.lit(None).cast("string").alias("tipe_magnitudo"),
        "keterangan_lokasi",
        "potensi",
        "dirasakan",
        F.lit(None).cast("string").alias("status_review"),
        F.col("diekstrak_utc").alias("diperbarui_utc"),
        "feed_bmkg",
        "diekstrak_utc",
    )


def read_usgs_bronze(spark: SparkSession, bronze_dir: Path) -> DataFrame | None:
    usgs_dir = bronze_dir / "usgs"
    if not usgs_dir.exists():
        return None
    # recursiveFileLookup: folder backfill (month=) dan incremental (date=) punya struktur
    # partisi berbeda, jadi penemuan partisi otomatis dimatikan
    return (
        spark.read.schema(USGS_SCHEMA)
        .option("header", True)
        .option("recursiveFileLookup", True)
        .option("pathGlobFilter", "*.csv")
        .csv(str(usgs_dir))
        .withColumn("diekstrak_utc", F.col("_metadata.file_modification_time"))
    )


def usgs_events(raw: DataFrame) -> DataFrame:
    """Bersihkan event USGS dan ambil versi terbaru per `id` (event bisa direvisi)."""
    typed = raw.where((F.col("type") == "earthquake") & F.col("id").isNotNull()).select(
        F.lit("usgs").alias("sumber"),
        F.col("id").alias("sumber_event_id"),
        F.try_to_timestamp(F.col("time")).alias("waktu_utc"),
        F.col("latitude").try_cast("double").alias("lat"),
        F.col("longitude").try_cast("double").alias("lon"),
        F.col("depth").try_cast("double").alias("kedalaman_km"),
        F.col("mag").try_cast("double").alias("magnitudo"),
        F.col("magType").alias("tipe_magnitudo"),
        F.col("place").alias("keterangan_lokasi"),
        F.lit(None).cast("string").alias("potensi"),
        F.lit(None).cast("string").alias("dirasakan"),
        F.col("status").alias("status_review"),
        F.try_to_timestamp(F.col("updated")).alias("diperbarui_utc"),
        F.lit(None).cast("string").alias("feed_bmkg"),
        F.col("diekstrak_utc"),
    )
    latest = Window.partitionBy("sumber_event_id").orderBy(
        F.col("diperbarui_utc").desc_nulls_last(), F.col("diekstrak_utc").desc_nulls_last()
    )
    return typed.withColumn("_rn", F.row_number().over(latest)).where("_rn = 1").drop("_rn")


def attach_nearest_kabkota(events: DataFrame, kabkota: DataFrame) -> DataFrame:
    """Beri setiap gempa kab/kota terdekat (jarak ke centroid, rumus Haversine).

    Gempa di laut tetap mendapat kab/kota terdekat; jaraknya disimpan agar analisis bisa
    memfilter (mis. hanya gempa < 100 km dari centroid).
    """
    k = kabkota.select(
        F.col("kode").alias("kode_kabkota"),
        F.col("lat").try_cast("double").alias("k_lat"),
        F.col("lng").try_cast("double").alias("k_lon"),
    ).where(F.col("k_lat").isNotNull() & F.col("k_lon").isNotNull())

    lat1, lon1 = F.radians("lat"), F.radians("lon")
    lat2, lon2 = F.radians("k_lat"), F.radians("k_lon")
    a = F.pow(F.sin((lat2 - lat1) / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin((lon2 - lon1) / 2), 2)
    distance = F.lit(2 * EARTH_RADIUS_KM) * F.asin(F.sqrt(a))

    with_coords = events.where(F.col("lat").isNotNull() & F.col("lon").isNotNull())
    nearest = Window.partitionBy("sumber", "sumber_event_id").orderBy("jarak_kabkota_km", "kode_kabkota")
    matched = (
        with_coords.crossJoin(F.broadcast(k))
        .withColumn("jarak_kabkota_km", F.round(distance, 2))
        .withColumn("_rn", F.row_number().over(nearest))
        .where("_rn = 1")
        .drop("_rn", "k_lat", "k_lon")
    )
    # Kejadian tanpa koordinat valid tetap disimpan, tanpa kab/kota
    without_coords = (
        events.where(F.col("lat").isNull() | F.col("lon").isNull())
        .withColumn("kode_kabkota", F.lit(None).cast("string"))
        .withColumn("jarak_kabkota_km", F.lit(None).cast("double"))
    )
    return matched.unionByName(without_coords)


def build_silver_gempa(spark: SparkSession, bronze_dir: Path, kabkota: DataFrame) -> DataFrame:
    parts = []
    bmkg_raw = read_bmkg_bronze(spark, bronze_dir)
    if bmkg_raw is not None:
        parts.append(bmkg_events(bmkg_raw))
    usgs_raw = read_usgs_bronze(spark, bronze_dir)
    if usgs_raw is not None:
        parts.append(usgs_events(usgs_raw))
    if not parts:
        raise FileNotFoundError(f"Belum ada data bronze gempa di {bronze_dir}")
    events = parts[0]
    for part in parts[1:]:
        events = events.unionByName(part)
    # Kejadian tanpa waktu valid tidak bisa dianalisis; dibuang di sini
    events = events.where(F.col("waktu_utc").isNotNull() & F.col("sumber_event_id").isNotNull())
    return attach_nearest_kabkota(events, kabkota).select(*SILVER_COLUMNS)
