"""Kode wilayah administrasi Indonesia dari github.com/cahyadsn/wilayah (lisensi MIT).

Dua file dipakai:
  wilayah_level_1_2.sql -> provinsi & kab/kota beserta ibukota, koordinat, luas, penduduk
  wilayah.sql           -> semua level (prov, kab/kota, kecamatan, desa) berupa (kode, nama)

Dari keduanya dibuat:
  kabkota.csv        -> referensi kab/kota (centroid & penduduk) untuk dim_wilayah
  lokasi_cuaca.csv   -> satu desa (kode adm4) per kab/kota untuk diambil prakiraan cuacanya
"""

from __future__ import annotations

import re
from dataclasses import asdict, dataclass

# Baris data level 1-2:
# ('11.01','Kabupaten Aceh Selatan','Tapak Tuan', 3.25, 97.17, 19, 7, 4174.211, 239629, '[[...]]', 1);
_LEVEL12_ROW = re.compile(
    r"^\('(?P<kode>[\d.]+)',\s*'(?P<nama>(?:[^'\\]|\\.|'')*)',\s*(?:'(?P<ibukota>(?:[^'\\]|\\.|'')*)'|NULL),\s*"
    r"(?P<lat>[-\d.eE]+|NULL),\s*(?P<lng>[-\d.eE]+|NULL),\s*(?P<elv>[-\d.eE]+|NULL),\s*"
    r"(?P<tz>-?\d+|NULL),\s*(?P<luas>[-\d.eE]+|NULL),\s*(?P<penduduk>[-\d.eE]+|NULL),",
    re.MULTILINE,
)
# Baris data semua level: ('11.01.01.2001','Keude Bakongan'),
_FULL_ROW = re.compile(r"^\('(?P<kode>[\d.]+)',\s*'(?P<nama>(?:[^'\\]|\\.|'')*)'\)", re.MULTILINE)


@dataclass(frozen=True)
class Wilayah12:
    kode: str
    nama: str
    ibukota: str | None
    lat: float | None
    lng: float | None
    elv: float | None
    tz: int | None
    luas_km2: float | None
    penduduk: int | None

    @property
    def level(self) -> int:
        return self.kode.count(".") + 1  # '11' -> 1 (provinsi), '11.01' -> 2 (kab/kota)

    @property
    def kode_provinsi(self) -> str:
        return self.kode.split(".")[0]


@dataclass(frozen=True)
class LokasiCuaca:
    kode_kabkota: str
    nama_kabkota: str
    adm4: str
    nama_desa: str
    kode_kecamatan: str
    nama_kecamatan: str
    metode: str  # 'kecamatan_ibukota' atau 'kode_terkecil'


def _unescape(value: str | None) -> str | None:
    """Kembalikan apostrof dari bentuk SQL ('' atau \\') ke bentuk aslinya."""
    if value is None:
        return None
    return value.replace("''", "'").replace("\\'", "'")


def _num(value: str | None) -> float | None:
    return None if value in (None, "NULL") else float(value)


def parse_level12_sql(sql_text: str) -> list[Wilayah12]:
    rows = []
    for m in _LEVEL12_ROW.finditer(sql_text):
        penduduk = _num(m["penduduk"])
        tz = _num(m["tz"])
        rows.append(
            Wilayah12(
                kode=m["kode"],
                nama=_unescape(m["nama"]),
                ibukota=_unescape(m["ibukota"]),
                lat=_num(m["lat"]),
                lng=_num(m["lng"]),
                elv=_num(m["elv"]),
                tz=int(tz) if tz is not None else None,
                luas_km2=_num(m["luas"]),
                penduduk=int(penduduk) if penduduk is not None else None,
            )
        )
    return rows


def parse_full_sql(sql_text: str) -> dict[str, str]:
    """kode -> nama untuk semua level."""
    return {m["kode"]: _unescape(m["nama"]) for m in _FULL_ROW.finditer(sql_text)}


def kabkota_records(level12: list[Wilayah12]) -> list[dict]:
    """Baris kab/kota lengkap dengan nama provinsinya (untuk kabkota.csv)."""
    provinsi = {w.kode: w.nama for w in level12 if w.level == 1}
    records = []
    for w in level12:
        if w.level != 2:
            continue
        row = asdict(w)
        row["kode_provinsi"] = w.kode_provinsi
        row["nama_provinsi"] = provinsi.get(w.kode_provinsi)
        records.append(row)
    return records


def _normalize(name: str | None) -> str:
    return re.sub(r"[^a-z0-9]", "", (name or "").lower())


def pick_weather_locations(level12: list[Wilayah12], semua_wilayah: dict[str, str]) -> list[LokasiCuaca]:
    """Pilih satu desa (adm4) per kab/kota sebagai titik pengambilan prakiraan cuaca.

    Aturan: cari kecamatan yang namanya sama dengan ibukota kab/kota, lalu ambil desa
    dengan kode terkecil di kecamatan itu. Jika tidak ketemu, ambil desa dengan kode
    terkecil di kab/kota tersebut. Hasilnya deterministik (selalu sama).
    """
    kecamatan_by_kab: dict[str, list[str]] = {}
    desa_by_kec: dict[str, list[str]] = {}
    for kode in semua_wilayah:
        parts = kode.split(".")
        if len(parts) == 3:
            kecamatan_by_kab.setdefault(".".join(parts[:2]), []).append(kode)
        elif len(parts) == 4:
            desa_by_kec.setdefault(".".join(parts[:3]), []).append(kode)

    lokasi = []
    for kab in (w for w in level12 if w.level == 2):
        kecamatan = sorted(k for k in kecamatan_by_kab.get(kab.kode, []) if desa_by_kec.get(k))
        if not kecamatan:
            continue
        target = _normalize(kab.ibukota)
        cocok = [k for k in kecamatan if target and _normalize(semua_wilayah[k]) == target]
        kec_terpilih, metode = (cocok[0], "kecamatan_ibukota") if cocok else (kecamatan[0], "kode_terkecil")
        adm4 = min(desa_by_kec[kec_terpilih])
        lokasi.append(
            LokasiCuaca(
                kode_kabkota=kab.kode,
                nama_kabkota=kab.nama,
                adm4=adm4,
                nama_desa=semua_wilayah[adm4],
                kode_kecamatan=kec_terpilih,
                nama_kecamatan=semua_wilayah[kec_terpilih],
                metode=metode,
            )
        )
    return lokasi
