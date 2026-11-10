"""Menjalankan perintah dbt sebagai subprocess dan meneruskan log-nya ke Airflow.

dbt dipasang di virtualenv terpisah (lihat docker/airflow/Dockerfile), jadi dbt
dipanggil sebagai program, bukan diimpor sebagai library Python.
"""

from __future__ import annotations

import logging
import subprocess
from collections.abc import Sequence

from pipeline import config

log = logging.getLogger(__name__)


class DbtError(RuntimeError):
    """dbt selesai dengan exit code bukan 0 (model gagal atau test gagal)."""


def dbt_command(args: Sequence[str], project_dir: str | None = None) -> list[str]:
    project_dir = project_dir or config.DBT_PROJECT_DIR
    return [
        config.DBT_BIN,
        *args,
        "--no-use-colors",  # log Airflow tanpa kode warna ANSI
        "--project-dir", project_dir,
        "--profiles-dir", project_dir,
    ]


def run_dbt(args: Sequence[str], project_dir: str | None = None) -> None:
    command = dbt_command(args, project_dir)
    log.info("Menjalankan: %s", " ".join(command))
    with subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    ) as proc:
        assert proc.stdout is not None
        for line in proc.stdout:
            log.info(line.rstrip())
        returncode = proc.wait()
    if returncode != 0:
        raise DbtError(f"dbt {' '.join(args)} gagal (exit code {returncode})")
