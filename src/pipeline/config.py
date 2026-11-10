"""Konfigurasi terpusat: URL sumber, lokasi data lake, dan parameter ekstraksi."""

from __future__ import annotations

import os
from pathlib import Path


def _env(name: str, default: str) -> str:
    """Nilai environment variable; kosong atau hanya spasi dianggap tidak diisi."""
    value = os.getenv(name, "").strip()
    return value or default


def _env_int(name: str, default: int) -> int:
    return int(_env(name, str(default)))


def _env_float(name: str, default: float) -> float:
    return float(_env(name, str(default)))


# ---------------------------------------------------------------------------
# Data lake (folder lokal yang di-mount ke container Airflow)
# ---------------------------------------------------------------------------
DATA_LAKE_DIR = Path(_env("DATA_LAKE_DIR", "/opt/airflow/data"))
BRONZE_DIR = DATA_LAKE_DIR / "bronze"
REFERENCE_DIR = DATA_LAKE_DIR / "reference"
SILVER_DIR = DATA_LAKE_DIR / "silver"

# File referensi hasil dag_ref_wilayah (dibaca oleh DAG lain)
KABKOTA_CSV = REFERENCE_DIR / "kabkota.csv"
LOKASI_CUACA_CSV = REFERENCE_DIR / "lokasi_cuaca.csv"

# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------
HTTP_USER_AGENT = _env("HTTP_USER_AGENT", "gempa-cuaca-platform/0.1 (final project data engineering)")
HTTP_TIMEOUT_SECONDS = _env_int("HTTP_TIMEOUT_SECONDS", 30)

# URL sumber bisa diganti lewat environment variable (dipakai untuk pengujian
# dengan server lokal); nilai default adalah endpoint resmi.
# ---------------------------------------------------------------------------
# BMKG gempa (data terbuka BMKG, wajib mencantumkan BMKG sebagai sumber)
# ---------------------------------------------------------------------------
BMKG_GEMPA_BASE_URL = _env("BMKG_GEMPA_BASE_URL", "https://data.bmkg.go.id/DataMKG/TEWS/")
BMKG_GEMPA_FEEDS = ("autogempa", "gempaterkini", "gempadirasakan")

# ---------------------------------------------------------------------------
# BMKG prakiraan cuaca
# ---------------------------------------------------------------------------
BMKG_CUACA_URL = _env("BMKG_CUACA_URL", "https://api.bmkg.go.id/publik/prakiraan-cuaca")
# Batas BMKG: 60 permintaan per menit per IP -> jeda sedikit di atas 1 detik
BMKG_CUACA_DELAY_SECONDS = _env_float("BMKG_CUACA_DELAY_SECONDS", 1.1)
# Batasi jumlah lokasi saat development (kosong atau 0 = semua lokasi)
BMKG_CUACA_MAX_LOCATIONS = _env_int("BMKG_CUACA_MAX_LOCATIONS", 0) or None
# Run dianggap gagal jika lebih dari persentase ini lokasi gagal diambil
BMKG_CUACA_MAX_FAILURE_RATIO = _env_float("BMKG_CUACA_MAX_FAILURE_RATIO", 0.1)

# ---------------------------------------------------------------------------
# USGS FDSN Event API
# ---------------------------------------------------------------------------
USGS_BASE_URL = _env("USGS_BASE_URL", "https://earthquake.usgs.gov/fdsnws/event/1/")
USGS_MAX_EVENTS_PER_QUERY = 20000
# Kotak wilayah Indonesia (perkiraan)
INDONESIA_BBOX = {
    "minlatitude": -11.0,
    "maxlatitude": 6.0,
    "minlongitude": 95.0,
    "maxlongitude": 141.0,
}

# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------
# "spark://spark-master:7077" di Docker; "local[2]" untuk menjalankan tanpa cluster
SPARK_MASTER_URL = _env("SPARK_MASTER_URL", "local[2]")
# Nama host driver yang bisa dihubungi executor (container airflow-scheduler)
SPARK_DRIVER_HOST = _env("SPARK_DRIVER_HOST", "") or None
SPARK_EXECUTOR_MEMORY = _env("SPARK_EXECUTOR_MEMORY", "1g")

# ---------------------------------------------------------------------------
# Warehouse (PostgreSQL)
# ---------------------------------------------------------------------------
WAREHOUSE_HOST = _env("WAREHOUSE_HOST", "localhost")
WAREHOUSE_PORT = _env_int("WAREHOUSE_PORT", 5433)
WAREHOUSE_DB = _env("WAREHOUSE_DB", "warehouse")
WAREHOUSE_USER = _env("WAREHOUSE_USER", "warehouse")
WAREHOUSE_PASSWORD = _env("WAREHOUSE_PASSWORD", "warehouse")

# ---------------------------------------------------------------------------
# dbt (layer gold)
# ---------------------------------------------------------------------------
DBT_BIN = _env("DBT_BIN", "dbt")
DBT_PROJECT_DIR = _env("DBT_PROJECT_DIR", str(Path(__file__).resolve().parents[2] / "dbt"))

# ---------------------------------------------------------------------------
# Kode wilayah (github.com/cahyadsn/wilayah, lisensi MIT)
# ---------------------------------------------------------------------------
WILAYAH_LEVEL_1_2_URL = "https://raw.githubusercontent.com/cahyadsn/wilayah/master/db/wilayah_level_1_2.sql"
WILAYAH_FULL_URL = "https://raw.githubusercontent.com/cahyadsn/wilayah/master/db/wilayah.sql"
