import sys

import pytest

from pipeline import config, dbt


def test_dbt_command_memakai_project_dan_profiles_dir():
    cmd = dbt.dbt_command(["build"], project_dir="/opt/airflow/dbt")
    assert cmd[1] == "build"
    assert cmd[-4:] == ["--project-dir", "/opt/airflow/dbt", "--profiles-dir", "/opt/airflow/dbt"]


def test_run_dbt_gagal_jika_exit_code_bukan_nol(monkeypatch, tmp_path):
    # Pakai interpreter Python sebagai pengganti dbt yang selalu gagal
    script = tmp_path / "fake_dbt.py"
    script.write_text("import sys; print('model gagal'); sys.exit(1)\n")
    monkeypatch.setattr(config, "DBT_BIN", sys.executable)
    with pytest.raises(dbt.DbtError):
        dbt.run_dbt([str(script)], project_dir=str(tmp_path))


def test_run_dbt_sukses(monkeypatch, tmp_path, caplog):
    script = tmp_path / "fake_dbt.py"
    script.write_text("print('Completed successfully')\n")
    monkeypatch.setattr(config, "DBT_BIN", sys.executable)
    with caplog.at_level("INFO"):
        dbt.run_dbt([str(script)], project_dir=str(tmp_path))
    assert "Completed successfully" in caplog.text
