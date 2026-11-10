import pytest
from conftest import FakeResponse, FakeSession, load_json

from pipeline.extract import bmkg_gempa as g


def test_autogempa_object_becomes_single_event_list():
    events = g.extract_events(load_json("bmkg_autogempa.json"))
    assert len(events) == 1
    assert events[0]["Magnitude"] == "2.5"


def test_gempaterkini_list_without_dirasakan_column():
    events = g.extract_events(load_json("bmkg_gempaterkini.json"))
    assert len(events) == 2
    assert "Dirasakan" not in events[0]


@pytest.mark.parametrize(
    "payload",
    [{}, {"Infogempa": {}}, {"Infogempa": {"gempa": []}}, {"Infogempa": {"gempa": [{"DateTime": "x"}]}}],
)
def test_invalid_payload_rejected(payload):
    with pytest.raises(g.BmkgFormatError):
        g.extract_events(payload)


@pytest.mark.parametrize(("raw", "expected"), [("5.3", 5.3), ("10 km", 10.0), ("4,8", 4.8), ("", None), (None, None)])
def test_parse_numbers(raw, expected):
    assert g.parse_magnitude(raw) == expected
    assert g.parse_depth_km(raw) == expected


def test_parse_coordinates_south_is_negative():
    assert g.parse_coordinates("-8.95,112.41") == (-8.95, 112.41)


@pytest.mark.parametrize("raw", ["8.95 LS", "1,2,3", "95.1,300.0"])
def test_parse_coordinates_invalid(raw):
    with pytest.raises((g.BmkgFormatError, ValueError)):
        g.parse_coordinates(raw)


def test_event_key_is_stable_and_shared_across_feeds():
    auto = load_json("bmkg_autogempa.json")["Infogempa"]["gempa"]
    same_event_other_feed = {k: v for k, v in auto.items() if k != "Potensi"}
    assert g.event_key(auto) == g.event_key(same_event_other_feed)
    other = load_json("bmkg_gempaterkini.json")["Infogempa"]["gempa"][0]
    assert g.event_key(auto) != g.event_key(other)


def test_feed_url_rejects_unknown_feed():
    with pytest.raises(ValueError):
        g.feed_url("gempa_palsu")


def test_fetch_feed_validates_response():
    session = FakeSession(lambda url, params: FakeResponse({"bukan": "gempa"}))
    with pytest.raises(g.BmkgFormatError):
        g.fetch_feed(session, "autogempa")
    assert session.calls[0][0].endswith("/autogempa.json")
