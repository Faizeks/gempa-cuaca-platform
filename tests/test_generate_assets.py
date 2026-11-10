import importlib.util
from collections import Counter

from conftest import REPO_ROOT

spec = importlib.util.spec_from_file_location("generate_assets", REPO_ROOT / "scripts" / "generate_assets.py")
generate_assets = importlib.util.module_from_spec(spec)
spec.loader.exec_module(generate_assets)

KABKOTA = [
    {"kode": "31.71", "nama": "Kota Jakarta Pusat", "kode_provinsi": "31", "nama_provinsi": "DKI Jakarta",
     "lat": "-6.18", "lng": "106.83", "penduduk": "1000000"},
    {"kode": "11.01", "nama": "Kabupaten Aceh Selatan", "kode_provinsi": "11", "nama_provinsi": "Aceh",
     "lat": "3.25", "lng": "97.17", "penduduk": "10000"},
]


def test_generation_is_deterministic_and_weighted_by_population():
    first = generate_assets.generate(KABKOTA, 500, seed=7)
    assert first == generate_assets.generate(KABKOTA, 500, seed=7)
    per_kab = Counter(a["kode_kabkota"] for a in first)
    assert per_kab["31.71"] > per_kab["11.01"] * 10


def test_assets_stay_near_centroid_and_have_valid_values():
    for aset in generate_assets.generate(KABKOTA, 200):
        kab = next(k for k in KABKOTA if k["kode"] == aset["kode_kabkota"])
        assert abs(aset["lat"] - float(kab["lat"])) <= generate_assets.BATAS_SEBARAN_DERAJAT
        assert aset["nilai_rp"] > 0 and aset["nilai_rp"] % 1_000_000 == 0
        assert aset["jenis"] in {"gudang", "polis"}
