import pytest
from conftest import FakeResponse, FakeSession, load_json

from pipeline.extract import bmkg_cuaca as c


def test_slots_are_flattened_across_days():
    payload = load_json("bmkg_cuaca.json")
    assert c.count_slots(payload) == 3
    assert c.iter_slots(payload)[0]["tp"] == 0.4  # field curah hujan ikut terbawa


def test_invalid_payload_rejected():
    with pytest.raises(c.BmkgCuacaFormatError):
        c.count_slots({"data": []})


def test_too_many_slots_rejected():
    payload = load_json("bmkg_cuaca.json")
    slot = c.iter_slots(payload)[0]
    payload["data"][0]["cuaca"] = [[slot] * 25]
    with pytest.raises(c.BmkgCuacaFormatError):
        c.count_slots(payload)


def test_fetch_many_records_failures_and_throttles():
    good = load_json("bmkg_cuaca.json")

    def handler(url, params):
        return FakeResponse(text="error", status_code=500) if params["adm4"] == "GAGAL" else FakeResponse(good)

    sleeps = []
    result = c.fetch_many(FakeSession(handler), ["A", "GAGAL", "B"], delay_seconds=1.1, sleep=sleeps.append)

    assert [r["adm4"] for r in result.records] == ["A", "B"]
    assert list(result.failures) == ["GAGAL"]
    assert result.failure_ratio == pytest.approx(1 / 3)
    assert sleeps == [1.1, 1.1]  # jeda di antara permintaan, bukan sebelum yang pertama
    assert result.records[0]["payload"] == good
