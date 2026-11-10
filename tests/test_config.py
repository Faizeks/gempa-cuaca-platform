import importlib

from pipeline import config


def _reload(monkeypatch, **env):
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    return importlib.reload(config)


def test_empty_env_values_fall_back_to_defaults(monkeypatch):
    # Regresi: BMKG_CUACA_MAX_LOCATIONS= (kosong) di .env membuat semua DAG gagal di-import
    cfg = _reload(
        monkeypatch,
        BMKG_CUACA_MAX_LOCATIONS="",
        BMKG_CUACA_DELAY_SECONDS="  ",
        WAREHOUSE_PORT="",
        SPARK_MASTER_URL="",
        SPARK_DRIVER_HOST="",
    )
    assert cfg.BMKG_CUACA_MAX_LOCATIONS is None  # semua lokasi
    assert cfg.BMKG_CUACA_DELAY_SECONDS == 1.1
    assert cfg.WAREHOUSE_PORT == 5433
    assert cfg.SPARK_MASTER_URL == "local[2]"
    assert cfg.SPARK_DRIVER_HOST is None


def test_zero_and_numbers_are_respected(monkeypatch):
    assert _reload(monkeypatch, BMKG_CUACA_MAX_LOCATIONS="0").BMKG_CUACA_MAX_LOCATIONS is None
    assert _reload(monkeypatch, BMKG_CUACA_MAX_LOCATIONS="20").BMKG_CUACA_MAX_LOCATIONS == 20


def teardown_module():
    importlib.reload(config)
