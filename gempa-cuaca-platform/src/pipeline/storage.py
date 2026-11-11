"""Penulisan file ke data lake lokal (layer bronze dan reference).

Semua penulisan bersifat atomik: data ditulis ke file sementara lalu di-rename.
Dengan begitu file yang terlihat selalu lengkap, dan rerun task dengan path yang
sama cukup menimpa file lama (idempotent).
"""

from __future__ import annotations

import csv
import io
import json
import os
import tempfile
from collections.abc import Iterable
from pathlib import Path

from pipeline import config


def bronze_path(source: str, partitions: dict[str, str], filename: str, base_dir: Path | None = None) -> Path:
    """Bangun path bronze bergaya Hive: bronze/<source>/<k>=<v>/.../<filename>.

    Contoh: bronze/bmkg_gempa/feed=autogempa/date=2026-11-09/004500.json
    """
    path = (base_dir or config.BRONZE_DIR) / source
    for key, value in partitions.items():
        path = path / f"{key}={value}"
    return path / filename


def write_bytes_atomic(path: Path, data: bytes) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(dir=path.parent, prefix=f".{path.name}.", suffix=".tmp")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
        os.replace(tmp_name, path)
    except BaseException:
        Path(tmp_name).unlink(missing_ok=True)
        raise
    return path


def write_text(path: Path, text: str) -> Path:
    return write_bytes_atomic(path, text.encode("utf-8"))


def write_json(path: Path, payload: object) -> Path:
    return write_text(path, json.dumps(payload, ensure_ascii=False, indent=2))


def write_jsonl(path: Path, records: Iterable[object]) -> Path:
    lines = (json.dumps(record, ensure_ascii=False) for record in records)
    return write_text(path, "\n".join(lines) + "\n")


def write_csv(path: Path, rows: list[dict], fieldnames: list[str] | None = None) -> Path:
    if not rows and not fieldnames:
        raise ValueError("Butuh rows atau fieldnames untuk menulis header CSV")
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames or list(rows[0].keys()), lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return write_text(path, buffer.getvalue())


def read_csv(path: Path) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))
