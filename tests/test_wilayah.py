from conftest import FIXTURES

from pipeline.extract import wilayah as w

LEVEL12 = w.parse_level12_sql((FIXTURES / "wilayah_level_1_2_sample.sql").read_text(encoding="utf-8"))
SEMUA = w.parse_full_sql((FIXTURES / "wilayah_sample.sql").read_text(encoding="utf-8"))


def test_parse_level12_reads_numbers_and_levels():
    assert [x.kode for x in LEVEL12] == ["11", "11.01", "11.02"]
    aceh_selatan = LEVEL12[1]
    assert aceh_selatan.level == 2 and aceh_selatan.kode_provinsi == "11"
    assert aceh_selatan.ibukota == "Tapak Tuan"
    assert aceh_selatan.penduduk == 239629 and aceh_selatan.tz == 7
    assert round(aceh_selatan.lat, 4) == 3.2548


def test_parse_full_handles_sql_escaped_apostrophe():
    assert SEMUA["11.01.02.2018"] == "Pasi Kuala Ba'u"
    assert len(SEMUA) == 15


def test_kabkota_records_include_province_name():
    records = w.kabkota_records(LEVEL12)
    assert len(records) == 2
    assert records[0]["nama_provinsi"] == "Aceh"


def test_pick_locations_prefers_kecamatan_named_like_capital():
    lokasi = {x.kode_kabkota: x for x in w.pick_weather_locations(LEVEL12, SEMUA)}
    # Ibukota 'Tapak Tuan' cocok dengan kecamatan 'Tapaktuan'; desa kode terkecil dipilih
    assert lokasi["11.01"].adm4 == "11.01.08.2001"
    assert lokasi["11.01"].metode == "kecamatan_ibukota"
    # 'Kutacane' tidak ada di sampel -> kecamatan kode terkecil
    assert lokasi["11.02"].adm4 == "11.02.01.2001"
    assert lokasi["11.02"].metode == "kode_terkecil"
