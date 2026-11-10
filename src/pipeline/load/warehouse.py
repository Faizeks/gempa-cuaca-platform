"""Memuat Parquet layer silver ke PostgreSQL.

Setiap tabel diisi ulang penuh (TRUNCATE + COPY) dalam satu transaksi, sehingga:
- idempotent: menjalankan ulang menghasilkan isi yang sama
- atomik: pembaca tidak pernah melihat tabel setengah terisi; jika gagal, isi lama tetap ada
COPY jauh lebih cepat daripada INSERT per baris.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import psycopg
import pyarrow.dataset as ds
from psycopg import sql

from pipeline import config

log = logging.getLogger(__name__)

SILVER_DDL = Path(__file__).resolve().parents[3] / "sql" / "warehouse" / "silver.sql"


@dataclass(frozen=True)
class SilverTable:
    schema: str
    table: str
    parquet_subdir: str
    columns: tuple[str, ...]


def connect(**overrides) -> psycopg.Connection:
    params = {
        "host": config.WAREHOUSE_HOST,
        "port": config.WAREHOUSE_PORT,
        "dbname": config.WAREHOUSE_DB,
        "user": config.WAREHOUSE_USER,
        "password": config.WAREHOUSE_PASSWORD,
        **overrides,
    }
    return psycopg.connect(**params)


def apply_ddl(conn: psycopg.Connection, ddl_path: Path = SILVER_DDL) -> None:
    with conn.transaction():
        conn.execute(ddl_path.read_text(encoding="utf-8"))


def read_parquet(parquet_dir: Path, columns: tuple[str, ...]):
    """Baca Parquet hasil Spark, termasuk kolom partisi gaya Hive (mis. sumber=bmkg)."""
    if not parquet_dir.exists():
        raise FileNotFoundError(f"Parquet silver tidak ditemukan: {parquet_dir}")
    dataset = ds.dataset(str(parquet_dir), format="parquet", partitioning="hive", ignore_prefixes=[".", "_"])
    return dataset.to_table(columns=list(columns))


def replace_table(conn: psycopg.Connection, target: SilverTable, silver_dir: Path) -> int:
    data = read_parquet(silver_dir / target.parquet_subdir, target.columns)
    table = sql.Identifier(target.schema, target.table)
    cols = sql.SQL(", ").join(sql.Identifier(c) for c in target.columns)
    with conn.transaction(), conn.cursor() as cur:
        cur.execute(sql.SQL("TRUNCATE {}").format(table))
        with cur.copy(sql.SQL("COPY {} ({}) FROM STDIN").format(table, cols)) as copy:
            for batch in data.to_batches(max_chunksize=10_000):
                columns = [batch.column(c).to_pylist() for c in target.columns]
                for row in zip(*columns):
                    copy.write_row(row)
    log.info("%s.%s: %d baris dimuat", target.schema, target.table, data.num_rows)
    return data.num_rows
