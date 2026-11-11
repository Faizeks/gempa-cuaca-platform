"""Unit test transformasi silver dengan Spark lokal.

Butuh PySpark + Java. Di luar container tanpa PySpark, test ini otomatis dilewati.
Jalankan di container:  docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests
"""

from __future__ import annotations

import json

import pytest
from conftest import FIXTURES, load_json

pytest.importorskip("pyspark")

from pipeline.extract import bmkg_gempa
from pipeline.silver import cuaca, gempa, wilayah
from pipeline.spark import spark_session

KABKOTA_CSV = (
    "kode,nama,ibukota,lat,lng,elv,tz,luas_km2,penduduk,kode_provinsi,nama_provinsi\n"
    "71.73,Kota Tomohon,Tomohon,1.3247,124.8389,700,8,147.21,100587,71,Sulawesi Utara\n"
    "82.01,Kabupaten Halmahera Barat,Jailolo,1.1171,127.4900,10,9,2612.24,140000,82,Maluku Utara\n"
    "31.71,Kota Adm. Jakarta Pusat,Jakarta Pusat,-6.18,106.83,8,7,48.13,1050000,31,DKI Jakarta\n"
)


@pytest.fixture(scope="module")
def spark():
    with spark_session("test_silver", master="local[1]") as session:
        yield session


@pytest.fixture
def lake(tmp_path):
    """Folder data lake berisi bronze buatan dari fixture."""
    bronze = tmp_path / "bronze"

    def put_json(feed, date, name, payload, extracted_at):
        path = bronze / "bmkg_gempa" / f"feed={feed}" / f"date={date}" / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({"source_url": "x", "extracted_at": extracted_at, "payload": payload}, indent=2))

    auto = load_json("bmkg_autogempa.json")
    put_json("autogempa", "2026-11-09", "004500.json", auto, "2026-11-09T00:45:05+00:00")
    put_json("autogempa", "2026-11-09", "010000.json", auto, "2026-11-09T01:00:05+00:00")  # kejadian yang sama
    dirasakan = {"Infogempa": {"gempa": [auto["Infogempa"]["gempa"]]}}
    put_json("gempadirasakan", "2026-11-09", "010000.json", dirasakan, "2026-11-09T01:00:06+00:00")
    put_json(
        "gempaterkini", "2026-11-09", "010000.json", load_json("bmkg_gempaterkini.json"), "2026-11-09T01:00:07+00:00"
    )

    csv_text = (FIXTURES / "usgs_events.csv").read_text(encoding="utf-8")
    backfill = bronze / "usgs" / "mode=backfill" / "month=2026-11" / "events.csv"
    backfill.parent.mkdir(parents=True)
    backfill.write_text(csv_text)
    # Versi revisi us7000test2 di run incremental (updated lebih baru, magnitudo berubah)
    revised = (
        csv_text.splitlines()[0]
        + "\n"
        + csv_text.splitlines()[2]
        .replace(",4.4,mb,", ",4.6,mww,")
        .replace("2026-11-01T20:15:30.000Z", "2026-11-03T08:00:00.000Z")
        .replace("automatic", "reviewed")
        + "\n"
    )
    incremental = bronze / "usgs" / "mode=incremental" / "date=2026-11-04" / "events.csv"
    incremental.parent.mkdir(parents=True)
    incremental.write_text(revised)
    (bronze / "usgs" / "mode=backfill" / "month=2026-10").mkdir(parents=True)
    (bronze / "usgs" / "mode=backfill" / "month=2026-10" / "events.csv").write_text("")  # bulan tanpa event

    cuaca_payload = load_json("bmkg_cuaca.json")
    run1 = {"adm4": "31.71.03.1001", "extracted_at": "2026-11-09T00:20:00+00:00", "payload": cuaca_payload}
    run2 = {**run1, "extracted_at": "2026-11-09T00:30:00+00:00"}  # analisis yang sama tersimpan dua kali
    run2["payload"] = json.loads(json.dumps(cuaca_payload))
    run2["payload"]["data"][0]["cuaca"][0][0]["t"] = 31  # nilai terbaru harus menang
    path = bronze / "bmkg_cuaca" / "date=2026-11-09" / "run=0020.jsonl"
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps(run1) + "\n")
    (path.parent / "run=0030.jsonl").write_text(json.dumps(run2) + "\n")

    ref = tmp_path / "kabkota.csv"
    ref.write_text(KABKOTA_CSV)
    return tmp_path


def test_bmkg_events_deduplicated_across_runs_and_feeds(spark, lake):
    events = gempa.bmkg_events(gempa.read_bmkg_bronze(spark, lake / "bronze"))
    rows = {r["sumber_event_id"]: r for r in events.collect()}
    assert len(rows) == 3  # 1 kejadian Jailolo (4 kemunculan) + 2 dari gempaterkini

    auto = load_json("bmkg_autogempa.json")["Infogempa"]["gempa"]
    jailolo = rows[bmkg_gempa.event_key(auto)]  # kunci Spark == kunci Python
    assert jailolo["feed_bmkg"] == "autogempa,gempadirasakan"
    assert jailolo["magnitudo"] == 2.5 and jailolo["kedalaman_km"] == 10.0
    assert (jailolo["lat"], jailolo["lon"]) == (1.08, 127.51)
    assert jailolo["dirasakan"] == "II Jailolo"


def test_usgs_keeps_latest_revision_and_handles_mixed_folders(spark, lake):
    events = gempa.usgs_events(gempa.read_usgs_bronze(spark, lake / "bronze"))
    rows = {r["sumber_event_id"]: r for r in events.collect()}
    assert set(rows) == {"us7000test1", "us7000test2"}
    assert rows["us7000test2"]["magnitudo"] == 4.6
    assert rows["us7000test2"]["status_review"] == "reviewed"


def test_nearest_kabkota_and_full_silver_gempa(spark, lake):
    reference = wilayah.read_reference(spark, lake / "kabkota.csv")
    df = gempa.build_silver_gempa(spark, lake / "bronze", reference)
    assert df.columns == gempa.SILVER_COLUMNS
    rows = {r["sumber_event_id"]: r for r in df.collect()}
    assert rows["us7000test1"]["kode_kabkota"] == "71.73"  # 5 km SW of Tomohon
    auto_key = bmkg_gempa.event_key(load_json("bmkg_autogempa.json")["Infogempa"]["gempa"])
    assert rows[auto_key]["kode_kabkota"] == "82.01"  # Jailolo -> Halmahera Barat
    assert rows[auto_key]["jarak_kabkota_km"] < 10


def test_kabkota_types_and_jenis(spark, lake):
    df = wilayah.kabkota(wilayah.read_reference(spark, lake / "kabkota.csv"))
    rows = {r["kode_kabkota"]: r for r in df.collect()}
    assert rows["71.73"]["jenis"] == "kota"
    assert rows["82.01"]["jenis"] == "kabupaten"
    assert rows["31.71"]["penduduk"] == 1050000


def test_cuaca_flattened_and_latest_extraction_wins(spark, lake):
    df = cuaca.build_silver_cuaca(spark, lake / "bronze")
    assert df.columns == cuaca.SILVER_COLUMNS
    rows = df.orderBy("waktu_utc").collect()
    assert len(rows) == 3  # 3 slot, duplikat dari run kedua dibuang
    first = rows[0]
    assert first["suhu_c"] == 31.0  # dari ekstraksi terakhir
    assert first["kode_kabkota"] == "31.71"
    assert first["jam_lokal"] == 13 and str(first["tanggal_lokal"]) == "2026-11-09"
    assert first["curah_hujan_mm"] == 0.4 and first["kode_cuaca"] == 61
