"""Menyiapkan Metabase dari kode: admin, koneksi warehouse, pertanyaan (SQL), dan dashboard.

Dijalankan otomatis oleh service `metabase-setup` (docker compose --profile bi up -d),
atau manual dari laptop:
    python scripts/setup_metabase.py --url http://localhost:3000 --warehouse-host postgres

Skrip ini idempotent: aman dijalankan berulang kali. Pertanyaan dengan nama yang sama
diperbarui (bukan diduplikasi) dan isi dashboard disusun ulang sesuai DASHBOARD di bawah.
Query SQL-nya ada di folder metabase/sql/, jadi bisa ditinjau dan diuji tanpa Metabase.
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path

import requests

log = logging.getLogger("setup_metabase")

ROOT = Path(__file__).resolve().parents[1]
SQL_DIR = ROOT / "metabase" / "sql"

DATABASE_NAME = "Warehouse"
COLLECTION_NAME = "Risiko Bencana"
DASHBOARD_NAME = "Risiko Bencana: Paparan Aset terhadap Gempa dan Cuaca"
DASHBOARD_DESCRIPTION = (
    "Paparan portofolio aset fiktif (gudang logistik dan polis asuransi) terhadap gempa "
    "(USGS, BMKG) dan prakiraan cuaca ekstrem (BMKG). Sumber: schema gold, dibangun dbt."
)


@dataclass(frozen=True)
class Card:
    """Satu pertanyaan Metabase beserta posisinya di dashboard.

    `at` = (baris, kolom, lebar, tinggi) pada grid dashboard Metabase yang lebarnya 24 kolom.
    """

    name: str
    sql_file: str
    display: str
    at: tuple[int, int, int, int]
    description: str = ""
    settings: dict = field(default_factory=dict)

    @property
    def sql(self) -> str:
        return (SQL_DIR / self.sql_file).read_text(encoding="utf-8").strip()

    @property
    def row(self) -> int:
        return self.at[0]

    @property
    def col(self) -> int:
        return self.at[1]

    @property
    def size_x(self) -> int:
        return self.at[2]

    @property
    def size_y(self) -> int:
        return self.at[3]


def _pin_map(lat: str = "lat", lon: str = "lon") -> dict:
    return {"map.type": "pin", "map.latitude_column": lat, "map.longitude_column": lon}


def _chart(dimensions: list[str], metrics: list[str], **extra) -> dict:
    return {"graph.dimensions": dimensions, "graph.metrics": metrics, **extra}


PAPARAN = "Terpapar = aset berada dalam radius dampak gempa M5+ (M5: 25 km, M6: 63 km, M7: 158 km)."

DASHBOARD: list[Card] = [
    # Baris 1: angka utama (judul pendek agar tidak terpotong)
    Card("Nilai portofolio (Rp triliun)", "01_nilai_portofolio.sql", "scalar", at=(0, 0, 6, 3)),
    Card("Aset terpapar gempa (12 bln)", "02_aset_terpapar.sql", "scalar", at=(0, 6, 6, 3), description=PAPARAN),
    Card(
        "% nilai terpapar gempa (12 bln)",
        "03_persen_nilai_terpapar.sql",
        "scalar",
        at=(0, 12, 6, 3),
        description=PAPARAN,
        settings={"column_settings": {'["name","persen_nilai_terpapar"]': {"suffix": "%"}}},
    ),
    Card("Gempa M5+ di Indonesia (12 bln)", "04_gempa_m5.sql", "scalar", at=(0, 18, 6, 3), description="Sumber: USGS."),
    # Baris 2: peta gempa dan provinsi paling terpapar
    Card(
        "Episentrum gempa M5+, 12 bulan terakhir",
        "05_peta_gempa.sql",
        "map",
        at=(3, 0, 14, 9),
        description="Sumber: USGS.",
        settings=_pin_map(),
    ),
    Card(
        "10 provinsi dengan % nilai aset terpapar tertinggi",
        "06_top_provinsi_terpapar.sql",
        "row",
        at=(3, 14, 10, 9),
        description=PAPARAN,
        settings=_chart(["Provinsi"], ["% nilai aset terpapar"], **{"graph.show_values": True}),
    ),
    # Baris 3: tren
    Card(
        "Jumlah gempa per bulan (USGS)",
        "07_tren_gempa_bulanan.sql",
        "bar",
        at=(12, 0, 12, 7),
        settings=_chart(["Bulan"], ["M 4-4.9", "M 5+"], **{"stackable.stack_type": "stacked"}),
    ),
    Card(
        "Nilai aset terpapar gempa per bulan (Rp miliar)",
        "08_nilai_terpapar_bulanan.sql",
        "bar",
        at=(12, 12, 12, 7),
        description=PAPARAN,
        settings=_chart(["Bulan", "Jenis aset"], ["Nilai terpapar (Rp miliar)"], **{"stackable.stack_type": "stacked"}),
    ),
    # Baris 4: tindakan operasional
    Card(
        "Peringatan cuaca ekstrem untuk aset (prakiraan BMKG terbaru)",
        "09_peringatan_cuaca.sql",
        "table",
        at=(19, 0, 24, 7),
        description="Aset di kab/kota yang diprakirakan hujan lebat atau petir mulai hari ini.",
    ),
    # Baris 5: ringkasan provinsi dan peta aset
    Card("Risiko per provinsi, 12 bulan terakhir", "10_risiko_provinsi.sql", "table", at=(26, 0, 12, 9)),
    Card(
        "Aset terpapar gempa, 12 bulan terakhir (peta)",
        "11_peta_aset_terpapar.sql",
        "map",
        at=(26, 12, 12, 9),
        description=PAPARAN,
        settings=_pin_map(),
    ),
]


class MetabaseError(RuntimeError):
    pass


class Metabase:
    def __init__(self, url: str, session: requests.Session | None = None):
        self.url = url.rstrip("/")
        self.http = session or requests.Session()

    def request(self, method: str, path: str, **kwargs):
        resp = self.http.request(method, f"{self.url}/api{path}", timeout=60, **kwargs)
        if resp.status_code >= 400:
            raise MetabaseError(f"{method} {path} -> {resp.status_code}: {resp.text[:500]}")
        return resp.json() if resp.content else None

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def post(self, path, body):
        return self.request("POST", path, json=body)

    def put(self, path, body):
        return self.request("PUT", path, json=body)

    # ------------------------------------------------------------------ langkah-langkah
    def wait_until_ready(self, timeout_s: int = 900, interval_s: int = 5) -> None:
        """Start pertama Metabase (migrasi database) bisa beberapa menit."""
        deadline = time.monotonic() + timeout_s
        while True:
            try:
                resp = self.http.get(f"{self.url}/api/health", timeout=10)
                if resp.status_code == 200 and resp.json().get("status") == "ok":
                    return
            except (requests.RequestException, ValueError):
                pass
            if time.monotonic() > deadline:
                raise MetabaseError(f"Metabase di {self.url} belum siap setelah {timeout_s} detik")
            log.info("Menunggu Metabase siap...")
            time.sleep(interval_s)

    def login(self, email: str, password: str) -> None:
        props = self.get("/session/properties")
        if not props.get("has-user-setup"):
            log.info("Setup awal Metabase: membuat admin %s", email)
            session = self.post(
                "/setup",
                {
                    "token": props["setup-token"],
                    "user": {"first_name": "Admin", "last_name": "Risiko", "email": email, "password": password},
                    "prefs": {"site_name": "Risiko Bencana", "allow_tracking": False},
                },
            )
        else:
            session = self.post("/session", {"username": email, "password": password})
        self.http.headers["X-Metabase-Session"] = session["id"]

    def ensure_database(self, host: str, port: int, dbname: str, user: str, password: str) -> int:
        details = {
            "host": host,
            "port": port,
            "dbname": dbname,
            "user": user,
            "password": password,
            # Hanya tampilkan schema gold di menu Browse data
            "schema-filters-type": "inclusion",
            "schema-filters-patterns": "gold",
        }
        for db in _items(self.get("/database")):
            if db.get("name") == DATABASE_NAME:
                self.put(f"/database/{db['id']}", {"details": details})
                return db["id"]
        log.info("Menambahkan koneksi database %s (%s:%s/%s)", DATABASE_NAME, host, port, dbname)
        return self.post("/database", {"name": DATABASE_NAME, "engine": "postgres", "details": details})["id"]

    def ensure_collection(self) -> int:
        for coll in _items(self.get("/collection")):
            if coll.get("name") == COLLECTION_NAME and not coll.get("archived") and isinstance(coll.get("id"), int):
                return coll["id"]
        return self.post("/collection", {"name": COLLECTION_NAME})["id"]

    def collection_items(self, collection_id: int, model: str) -> dict[str, int]:
        items = _items(self.get(f"/collection/{collection_id}/items", params={"models": model}))
        return {i["name"]: i["id"] for i in items if i.get("model") == model}

    def upsert_card(self, card: Card, database_id: int, collection_id: int, existing: dict[str, int]) -> int:
        body = {
            "name": card.name,
            "description": card.description or None,
            "type": "question",
            "display": card.display,
            "visualization_settings": card.settings,
            "collection_id": collection_id,
            "dataset_query": {"database": database_id, "type": "native", "native": {"query": card.sql}},
        }
        if card.name in existing:
            self.put(f"/card/{existing[card.name]}", body)
            return existing[card.name]
        return self.post("/card", body)["id"]

    def upsert_dashboard(self, collection_id: int, card_ids: list[int]) -> int:
        existing = self.collection_items(collection_id, "dashboard")
        if DASHBOARD_NAME in existing:
            dashboard_id = existing[DASHBOARD_NAME]
        else:
            dashboard_id = self.post(
                "/dashboard",
                {
                    "name": DASHBOARD_NAME,
                    "description": DASHBOARD_DESCRIPTION,
                    "collection_id": collection_id,
                },
            )["id"]
        # dashcards adalah daftar lengkap: id negatif = kartu baru, kartu lama yang tidak disebut dihapus
        dashcards = [
            {
                "id": -(i + 1),
                "card_id": card_id,
                "row": c.row,
                "col": c.col,
                "size_x": c.size_x,
                "size_y": c.size_y,
                "parameter_mappings": [],
                "visualization_settings": {},
            }
            for i, (c, card_id) in enumerate(zip(DASHBOARD, card_ids, strict=True))
        ]
        self.put(f"/dashboard/{dashboard_id}", {"description": DASHBOARD_DESCRIPTION, "dashcards": dashcards})
        return dashboard_id


def _items(payload) -> list[dict]:
    """Beberapa endpoint mengembalikan list, versi lain {"data": [...]}."""
    if isinstance(payload, dict):
        return payload.get("data", [])
    return payload or []


def setup(mb: Metabase, args: argparse.Namespace) -> str:
    mb.wait_until_ready(timeout_s=args.timeout)
    mb.login(args.email, args.password)
    database_id = mb.ensure_database(
        args.warehouse_host, args.warehouse_port, args.warehouse_db, args.warehouse_user, args.warehouse_password
    )
    collection_id = mb.ensure_collection()
    existing = mb.collection_items(collection_id, "card")
    card_ids = [mb.upsert_card(c, database_id, collection_id, existing) for c in DASHBOARD]
    dashboard_id = mb.upsert_dashboard(collection_id, card_ids)
    return f"{args.public_url.rstrip('/')}/dashboard/{dashboard_id}"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    env = os.environ.get
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--url", default=env("METABASE_URL", "http://localhost:3000"))
    p.add_argument(
        "--public-url",
        default=env("METABASE_PUBLIC_URL", "http://localhost:3000"),
        help="Alamat Metabase yang dibuka di browser (untuk link di akhir)",
    )
    p.add_argument("--email", default=env("MB_ADMIN_EMAIL") or "admin@example.com")
    p.add_argument("--password", default=env("MB_ADMIN_PASSWORD") or "Bencana2026!")
    # Koneksi dari Metabase ke warehouse: dilihat dari dalam jaringan Docker
    p.add_argument("--warehouse-host", default="postgres")
    p.add_argument("--warehouse-port", type=int, default=5432)
    p.add_argument("--warehouse-db", default=env("WAREHOUSE_DB") or "warehouse")
    p.add_argument("--warehouse-user", default=env("WAREHOUSE_USER") or "warehouse")
    p.add_argument("--warehouse-password", default=env("WAREHOUSE_PASSWORD") or "warehouse")
    p.add_argument("--timeout", type=int, default=900, help="Batas waktu menunggu Metabase siap (detik)")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args(argv)
    try:
        url = setup(Metabase(args.url), args)
    except MetabaseError as exc:
        log.error("%s", exc)
        return 1
    log.info("Dashboard siap: %s (login %s)", url, args.email)
    return 0


if __name__ == "__main__":
    sys.exit(main())
