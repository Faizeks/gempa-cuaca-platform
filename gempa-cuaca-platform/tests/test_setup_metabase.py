"""Uji alur skrip setup Metabase terhadap Metabase tiruan (tanpa jaringan)."""

import importlib.util
import json
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("setup_metabase", ROOT / "scripts" / "setup_metabase.py")
setup_metabase = importlib.util.module_from_spec(spec)
sys.modules["setup_metabase"] = setup_metabase  # dibutuhkan @dataclass
spec.loader.exec_module(setup_metabase)


class FakeResponse:
    def __init__(self, status_code, payload):
        self.status_code = status_code
        self._payload = payload
        self.content = b"" if payload is None else json.dumps(payload).encode()
        self.text = self.content.decode()

    def json(self):
        return self._payload


class FakeMetabase:
    """Meniru endpoint Metabase yang dipakai skrip, termasuk state antar run."""

    def __init__(self):
        self.headers = {}
        self.user = None
        self.databases, self.collections, self.cards, self.dashboards = [], [], {}, {}
        self.next_id = 1
        self.calls = []

    def _id(self):
        self.next_id += 1
        return self.next_id

    def get(self, url, timeout=None):
        return self.request("GET", url)

    def request(self, method, url, json=None, params=None, timeout=None):
        path = url.split("/api", 1)[1]
        self.calls.append((method, path))
        if path == "/health":
            return FakeResponse(200, {"status": "ok"})
        if path == "/session/properties":
            return FakeResponse(
                200, {"has-user-setup": self.user is not None, "setup-token": None if self.user else "tok"}
            )
        if path == "/setup":
            assert json["token"] == "tok"
            self.user = json["user"]
            return FakeResponse(200, {"id": "sess-1"})
        if path == "/session":
            if json["password"] != self.user["password"]:
                return FakeResponse(401, {"errors": "salah"})
            return FakeResponse(200, {"id": "sess-2"})
        assert self.headers.get("X-Metabase-Session"), f"{path} tanpa session"
        if path == "/database" and method == "GET":
            return FakeResponse(200, {"data": self.databases})
        if path == "/database" and method == "POST":
            db = {"id": self._id(), **json}
            self.databases.append(db)
            return FakeResponse(200, db)
        if path.startswith("/database/") and method == "PUT":
            return FakeResponse(200, {})
        if path == "/collection" and method == "GET":
            return FakeResponse(200, [{"id": "root", "name": "Our analytics"}, *self.collections])
        if path == "/collection" and method == "POST":
            coll = {"id": self._id(), "name": json["name"]}
            self.collections.append(coll)
            return FakeResponse(200, coll)
        if path.endswith("/items"):
            model = params["models"]
            source = self.cards if model == "card" else self.dashboards
            items = [{"id": i, "name": v["name"], "model": model} for i, v in source.items()]
            return FakeResponse(200, {"data": items})
        if path == "/card" and method == "POST":
            card_id = self._id()
            self.cards[card_id] = json
            return FakeResponse(200, {"id": card_id})
        if path.startswith("/card/") and method == "PUT":
            self.cards[int(path.split("/")[2])] = json
            return FakeResponse(200, {})
        if path == "/dashboard" and method == "POST":
            dash_id = self._id()
            self.dashboards[dash_id] = {**json, "dashcards": []}
            return FakeResponse(200, {"id": dash_id})
        if path.startswith("/dashboard/") and method == "PUT":
            self.dashboards[int(path.split("/")[2])].update(json)
            return FakeResponse(200, {})
        raise AssertionError(f"endpoint tak terduga: {method} {path}")


@pytest.fixture
def args():
    return setup_metabase.parse_args(["--email", "admin@example.com", "--password", "Rahasia123!"])


def test_semua_file_sql_ada_dan_tata_letak_tidak_bertumpuk():
    occupied = set()
    for card in setup_metabase.DASHBOARD:
        assert card.sql.lower().startswith(("select", "with", "--"))
        assert card.col + card.size_x <= 24, card.name
        for r in range(card.row, card.row + card.size_y):
            for c in range(card.col, card.col + card.size_x):
                assert (r, c) not in occupied, f"{card.name} bertumpuk"
                occupied.add((r, c))


def test_run_pertama_membuat_semuanya(args):
    fake = FakeMetabase()
    url = setup_metabase.setup(setup_metabase.Metabase("http://mb:3000", session=fake), args)

    assert fake.user["email"] == "admin@example.com"
    assert [d["engine"] for d in fake.databases] == ["postgres"]
    assert fake.databases[0]["details"]["host"] == "postgres"
    assert len(fake.cards) == len(setup_metabase.DASHBOARD)
    ((dash_id, dash),) = fake.dashboards.items()
    assert url == f"http://localhost:3000/dashboard/{dash_id}"
    assert len(dash["dashcards"]) == len(setup_metabase.DASHBOARD)
    assert all(dc["id"] < 0 and dc["card_id"] in fake.cards for dc in dash["dashcards"])
    native = next(iter(fake.cards.values()))["dataset_query"]
    assert native["type"] == "native" and "gold." in native["native"]["query"]


def test_run_kedua_tidak_membuat_duplikat(args):
    fake = FakeMetabase()
    mb = setup_metabase.Metabase("http://mb:3000", session=fake)
    setup_metabase.setup(mb, args)
    snapshot = (len(fake.databases), len(fake.collections), len(fake.cards), len(fake.dashboards))

    setup_metabase.setup(setup_metabase.Metabase("http://mb:3000", session=fake), args)

    assert (len(fake.databases), len(fake.collections), len(fake.cards), len(fake.dashboards)) == snapshot
    assert ("POST", "/session") in fake.calls  # run kedua login, bukan setup ulang


def test_password_salah_memberi_error_jelas(args):
    fake = FakeMetabase()
    setup_metabase.setup(setup_metabase.Metabase("http://mb:3000", session=fake), args)
    args.password = "salah"
    with pytest.raises(setup_metabase.MetabaseError, match="401"):
        setup_metabase.setup(setup_metabase.Metabase("http://mb:3000", session=fake), args)
