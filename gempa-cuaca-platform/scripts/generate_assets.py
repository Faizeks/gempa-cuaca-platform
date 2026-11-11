"""
Membuat data aset sintetis (gudang logistik dan objek polis asuransi) untuk framing bisnis.

Aset disebar ke kab/kota sebanding dengan jumlah penduduknya, dengan koordinat acak
+-5 km di sekitar centroid kab/kota. Hasil deterministik (seed tetap), jadi file yang
sama akan terbentuk setiap kali dijalankan.

Cara pakai (dari root repo):
    python scripts/generate_assets.py                         # baca data/reference/kabkota.csv
    python scripts/generate_assets.py --kabkota-csv <path>    # sumber lain
    python scripts/generate_assets.py --jumlah 2000           # jumlah aset berbeda

Output: dbt/seeds/aset.csv (dimuat ke warehouse dengan `dbt seed`).
Catatan: data ini fiktif dan hanya mewakili data internal perusahaan.
"""

from __future__ import annotations

import argparse
import csv
import math
import random
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = REPO_ROOT / "data" / "reference" / "kabkota.csv"
DEFAULT_OUTPUT = REPO_ROOT / "dbt" / "seeds" / "aset.csv"

# (jenis, kategori, proporsi, nilai median Rp, sebaran lognormal)
PROFIL_ASET = [
    ("gudang", "gudang_logistik", 0.30, 15_000_000_000, 0.6),
    ("polis", "rumah_tinggal", 0.40, 1_500_000_000, 0.7),
    ("polis", "ruko", 0.20, 4_000_000_000, 0.6),
    ("polis", "pabrik", 0.10, 40_000_000_000, 0.8),
]
SEBARAN_DERAJAT = 0.05  # +-5,5 km
BATAS_SEBARAN_DERAJAT = 0.15


def load_kabkota(path: Path) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as f:
        rows = [r for r in csv.DictReader(f) if r.get("lat") and r.get("lng") and r.get("penduduk")]
    if not rows:
        raise SystemExit(f"Tidak ada kab/kota di {path}. Jalankan DAG ref_wilayah dulu.")
    return rows


def generate(kabkota: list[dict], jumlah: int, seed: int = 42) -> list[dict]:
    rng = random.Random(seed)
    bobot_kab = [float(k["penduduk"]) for k in kabkota]
    bobot_profil = [p[2] for p in PROFIL_ASET]
    hasil = []
    for i in range(1, jumlah + 1):
        kab = rng.choices(kabkota, weights=bobot_kab)[0]
        jenis, kategori, _, median, sigma = rng.choices(PROFIL_ASET, weights=bobot_profil)[0]

        def geser() -> float:
            return max(-BATAS_SEBARAN_DERAJAT, min(BATAS_SEBARAN_DERAJAT, rng.gauss(0, SEBARAN_DERAJAT)))

        nilai = median * math.exp(rng.gauss(0, sigma))
        hasil.append(
            {
                "aset_id": f"AST-{i:05d}",
                "jenis": jenis,
                "kategori": kategori,
                "kode_provinsi": kab["kode_provinsi"],
                "nama_provinsi": kab["nama_provinsi"],
                "kode_kabkota": kab["kode"],
                "nama_kabkota": kab["nama"],
                "lat": round(float(kab["lat"]) + geser(), 6),
                "lon": round(float(kab["lng"]) + geser(), 6),
                "nilai_rp": int(round(nilai, -6)),  # dibulatkan ke juta rupiah
            }
        )
    return hasil


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--kabkota-csv", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--jumlah", type=int, default=1000)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    aset = generate(load_kabkota(args.kabkota_csv), args.jumlah, args.seed)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(aset[0].keys()), lineterminator="\n")
        writer.writeheader()
        writer.writerows(aset)
    total = sum(a["nilai_rp"] for a in aset)
    print(f"{len(aset)} aset ditulis ke {args.output} (total nilai Rp {total / 1e12:,.1f} triliun)")


if __name__ == "__main__":
    main()
