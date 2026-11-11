# Platform Data Risiko Bencana: Gempa & Cuaca Indonesia

Final project Data Engineering (dibimbing). Platform ini mengumpulkan data gempa dan
prakiraan cuaca BMKG serta histori gempa USGS secara otomatis, lalu
menghitung paparan risiko aset perusahaan (gudang logistik dan polis asuransi) per wilayah.

> **Sumber data:** Badan Meteorologi, Klimatologi, dan Geofisika (BMKG), USGS Earthquake
> Hazards Program, dan kode wilayah dari [cahyadsn/wilayah](https://github.com/cahyadsn/wilayah) (MIT).
> Data aset di `dbt/seeds/aset.csv` adalah **data fiktif**.

## Status

| Tahap | Isi | Status |
| --- | --- | --- |
| 1. Ekstraksi ke bronze | 5 DAG Airflow menarik data mentah ke `data/bronze` | ✅ selesai |
| 2. Silver | Spark cluster: parsing, deduplikasi, kab/kota terdekat → Parquet → PostgreSQL + data quality check | ✅ selesai |
| 3. Gold | dbt: star schema, fakta paparan aset, mart risiko, 34 test; dipicu Asset dari silver | ✅ selesai |
| 4. Dashboard | Metabase: dashboard 11 kartu dibuat otomatis dari kode | ✅ selesai |

## Arsitektur saat ini

```
BMKG gempa (JSON) ─┐                        bronze (file mentah)        silver (Parquet)         warehouse
BMKG cuaca (API)  ─┼─> Airflow (extract) ─> data/bronze/... ─> Spark ─> data/silver/... ─> COPY ─> PostgreSQL silver.*
USGS (CSV)        ─┤                        data/reference/   cluster                          + data quality check
Kode wilayah (SQL)─┘                                                                                  │ Asset
                                                                                                      v
                                       aset.csv (seed) ──> dbt build (Airflow DAG gold) ──> PostgreSQL gold.*
```

- **Data lake** memakai folder lokal `data/` yang di-mount ke semua container pada path yang
  sama. MinIO tidak dipakai karena proyeknya sudah diarsipkan dan image Docker-nya tidak lagi
  tersedia bebas.
- **Spark cluster** (1 master + 1 worker, 2 core, 2 GB) memakai image yang sama dengan Airflow,
  yang sudah berisi PySpark 4.0.5 dan Java 21. Dengan begitu versi Python, Spark, dan Java di
  driver (task Airflow) dan worker dijamin identik. Image Bitnami Spark yang dipakai repo contoh
  tidak lagi tersedia bebas.
- **dbt** dipasang di virtualenv terpisah di dalam image (`/home/airflow/dbt-venv`) agar
  dependensinya tidak bentrok dengan Airflow, lalu dipanggil dari task Airflow sebagai subprocess.
- **Transformasi hanya memakai fungsi bawaan Spark** (tanpa Python UDF), sehingga berjalan
  penuh di JVM executor.

## DAG

| DAG | Jadwal | Fungsi | Output |
| --- | --- | --- | --- |
| `ref_wilayah` | Bulanan | Unduh kode wilayah, buat daftar kab/kota dan 1 lokasi cuaca per kab/kota | `data/reference/*.csv` |
| `bmkg_gempa` | Tiap 15 menit | 3 feed gempa BMKG | `data/bronze/bmkg_gempa/feed=…/date=…/HHMMSS.json` |
| `bmkg_cuaca` | 06:15 & 18:15 WIB | Prakiraan cuaca 514 lokasi (jeda 1,1 detik, ±10 menit) | `data/bronze/bmkg_cuaca/date=…/run=HHMM.jsonl` |
| `usgs_backfill` | Bulanan, catchup sejak 2016 | Histori gempa per bulan kalender | `data/bronze/usgs/mode=backfill/month=…/events.csv` |
| `usgs_incremental` | Harian | Event baru/direvisi (`updatedafter`) | `data/bronze/usgs/mode=incremental/date=…/events.csv` |
| `silver` | Tiap jam (menit 20) | Spark: bronze → Parquet silver → PostgreSQL, lalu 9 data quality check | `data/silver/…`, tabel `silver.*` |
| `gold` | Setiap silver lolos quality check (Asset `warehouse_silver`) | `dbt build` (seed, model, test) lalu `dbt docs generate` | tabel `gold.*`, `referensi.aset`, `dbt/target/` |

### Tabel silver (database `warehouse`, schema `silver`)

| Tabel | Isi | Kunci |
| --- | --- | --- |
| `kabkota` | 514 kab/kota: centroid, luas, penduduk, jenis (kabupaten/kota) | `kode_kabkota` |
| `gempa` | Gempa BMKG + USGS dalam satu skema, kolom `sumber` membedakan asal data; kab/kota dengan centroid terdekat dan jaraknya | `sumber`, `sumber_event_id` |
| `prakiraan_cuaca` | Satu baris per lokasi × slot waktu × waktu analisis BMKG | `adm4`, `analisis_utc`, `waktu_utc` |

Gempa BMKG tidak punya ID, jadi `sumber_event_id` = 32 karakter pertama sha256 dari
`bmkg|DateTime|Coordinates`. Kejadian yang muncul berulang kali (setiap 15 menit dan di beberapa
feed) digabung menjadi satu baris; kolom `feed_bmkg` mencatat feed mana saja yang memuatnya.
Event USGS yang direvisi diambil versi dengan `updated` terbaru.

Semua waktu di nama partisi memakai **UTC**.

### Tabel gold (schema `gold`, dibangun dbt)

Star schema dengan aset sintetis sebagai pusat analisis paparan:

| Tabel | Jenis | Grain / isi |
| --- | --- | --- |
| `dim_wilayah` | dimensi | kab/kota + provinsi, penduduk, luas, kepadatan |
| `dim_waktu` | dimensi | satu baris per tanggal (nama bulan/hari Indonesia, musim hujan/kemarau) |
| `dim_cuaca` | dimensi | kategori cuaca BMKG + flag `is_cuaca_ekstrem` |
| `dim_aset` | dimensi | 1.000 aset fiktif (gudang & polis), nilai, kelas nilai, lokasi |
| `fact_gempa` | fakta, incremental | satu baris per gempa per sumber; kategori magnitudo & kedalaman |
| `fact_prakiraan_cuaca` | fakta, incremental | lokasi × slot 3 jam × analisis; flag hujan lebat & cuaca ekstrem |
| `fact_paparan_gempa` | fakta | aset × gempa M ≥ 5 yang radius dampaknya menjangkau aset (jarak Haversine) |
| `fact_paparan_cuaca` | fakta | aset × tanggal × analisis: hujan total, slot ekstrem |
| `mart_risiko_provinsi` | mart | provinsi × bulan: jumlah gempa, gempa dirasakan, hari cuaca ekstrem, per 1 juta penduduk |
| `mart_paparan_aset` | mart | provinsi × jenis aset × bulan: aset terpapar dan % nilai portofolio terpapar |
| `mart_peringatan_cuaca_aset` | mart | aset yang diprakirakan terkena cuaca ekstrem mulai hari ini |

**Radius dampak gempa bergantung magnitudo:** `25 km × 10^(0,4 × (M − 5))`, yaitu M5 = 25 km,
M6 = 63 km, M7 = 158 km. Radius tetap (mis. 100 km untuk semua gempa M4+) membuat hampir seluruh
portofolio di Indonesia tampak terpapar setiap tahun, sehingga tidak membedakan risiko. Ini
penyederhanaan atenuasi guncangan untuk project, bukan model rekayasa gempa.

Parameter analisis (radius dampak, magnitudo minimum, ambang hujan lebat) ada di `vars` pada
`dbt/dbt_project.yml`. Test dbt mencakup `unique`, `not_null`, `relationships`,
`accepted_values`, test generik kombinasi kunci unik, dan dua test bisnis (paparan harus di
dalam radius; persentase 0–100).

## Menjalankan (Windows + Docker Desktop)

Kebutuhan: Docker Desktop dengan memori minimal 4 GB untuk Docker.

```powershell
cd "D:\Dibimbing-Final_Project\gempa-cuaca-platform"
Copy-Item .env.example .env          # lalu edit isinya bila perlu
docker compose up -d --build         # build image + jalankan (pertama kali beberapa menit)
docker compose ps                    # tunggu semua "healthy"
```

Buka http://localhost:8080 (login `airflow` / `airflow`, sesuai `.env`). Semua DAG dalam
keadaan **paused**. Urutan menyalakannya:

1. `ref_wilayah` → unpause, lalu klik **Trigger**. Tunggu sukses (±10 detik).
2. `bmkg_gempa` → unpause; berjalan sendiri tiap 15 menit.
3. `bmkg_cuaca` → unpause dan Trigger. Dengan `BMKG_CUACA_MAX_LOCATIONS=20` di `.env`
   run uji hanya ±25 detik; isi `0` untuk semua 514 lokasi.
4. `usgs_incremental` → unpause.
5. `usgs_backfill` → unpause; akan membuat ±130 run (satu per bulan sejak 2016).
6. `silver` → unpause setelah langkah 1–5 punya data. Spark UI: http://localhost:8081
7. `gold` → unpause. Tidak perlu di-trigger: berjalan otomatis setiap `silver` selesai.

Hasilnya langsung terlihat di folder `data/` di laptop, dan tabel silver/gold bisa dibuka dengan
DBeaver/pgAdmin: host `localhost`, port `5433`, database `warehouse`, user/password dari `.env`.

**Dokumentasi dbt dan lineage graph:** setelah `gold` sukses, file-nya ada di `dbt/target/`.

```powershell
cd dbt\target
python -m http.server 8085                 # lalu buka http://localhost:8085
```

**Memori:** Docker Desktop (WSL2) secara default memakai 50% RAM laptop. Seluruh stack
(Airflow + PostgreSQL + Spark) butuh ±6 GB. Jika Docker kekurangan memori, isi
`SPARK_MASTER_URL=local[2]` di `.env` agar Spark berjalan di dalam container Airflow saja.

Perintah lain:

```powershell
docker compose logs -f airflow-scheduler   # lihat log
docker compose down                        # hentikan (data tetap ada)
```

## Dashboard (Metabase)

```powershell
docker compose --profile bi up -d
docker compose logs -f metabase-setup      # tunggu baris "Dashboard siap: ..." (start pertama ±3-5 menit)
```

Buka http://localhost:3000 dan login dengan `MB_ADMIN_EMAIL` / `MB_ADMIN_PASSWORD` dari `.env`
(default `admin@example.com` / `Bencana2026!`). Dashboard ada di koleksi **Risiko Bencana**.

Semua isi Metabase dibuat dari kode oleh `scripts/setup_metabase.py` lewat Metabase API:
akun admin, koneksi ke database warehouse (hanya schema `gold`), 11 pertanyaan SQL, dan
tata letak dashboard. Query-nya ada di `metabase/sql/` sehingga bisa ditinjau dan diuji tanpa
Metabase. Skrip ini idempotent; setelah mengubah query, jalankan ulang:

```powershell
docker compose --profile bi run --rm metabase-setup
```

Isi dashboard:

| Bagian | Kartu |
| --- | --- |
| Angka utama | nilai portofolio, aset terpapar gempa, % nilai terpapar, jumlah gempa M5+ (12 bulan) |
| Peta dan peringkat | peta episentrum gempa M5+, 10 provinsi dengan % nilai aset terpapar tertinggi |
| Tren | gempa per bulan (USGS), nilai aset terpapar per bulan per jenis aset |
| Operasional | daftar aset yang diprakirakan terkena hujan lebat/petir mulai hari ini |
| Ringkasan | risiko per provinsi, peta aset terpapar |

Metabase butuh ±1-1,5 GB memori. Jika laptop berat, hentikan setelah selesai dengan
`docker compose stop metabase`.

## Test

```powershell
python -m pip install -r requirements-dev.txt
python -m pytest                           # unit test ringan (tanpa Docker)
ruff check . ; ruff format --check .      # lint dan format kode
docker compose exec airflow-scheduler python -m pytest /opt/airflow/tests   # semua test, termasuk Spark & PostgreSQL
```

## Membuat ulang data aset sintetis

```powershell
python scripts/generate_assets.py          # butuh data/reference/kabkota.csv (dari ref_wilayah)
```

## Struktur repo

```
dags/                 DAG Airflow (tipis, hanya orkestrasi)
src/pipeline/         logika ekstraksi, penyimpanan, helper waktu
  extract/            client per sumber: bmkg_gempa, bmkg_cuaca, usgs, wilayah
  silver/             transformasi Spark: gempa, cuaca, wilayah
  load/               pemuatan Parquet ke PostgreSQL (TRUNCATE + COPY dalam satu transaksi)
  quality.py          data quality check layer silver
  dbt.py              menjalankan dbt dari task Airflow
  assets.py           Asset Airflow penghubung silver → gold
scripts/              generate_assets.py, setup_metabase.py
metabase/sql/         query SQL setiap kartu dashboard
dbt/                  project dbt (layer gold)
  models/staging/     view pembersih di atas tabel silver dan seed
  models/marts/       dimensi, fakta, dan mart
  macros/             haversine_km, generate_schema_name
  tests/              test generik dan test bisnis
  seeds/              aset.csv (data fiktif) + tipe kolom
sql/init/             script pembuatan database warehouse & metabase
sql/warehouse/        DDL tabel silver
docker/airflow/       Dockerfile + requirements image Airflow
tests/                unit test + fixture respons sumber data
data/                 data lake lokal (tidak di-commit)
```

## Keputusan desain

- **Bronze menyimpan respons mentah apa adanya** (dibungkus metadata `source_url` dan
  `extracted_at`), jadi tahap silver bisa diulang tanpa memanggil API lagi.
- **Penulisan atomik dan idempotent:** file ditulis ke file sementara lalu di-rename; rerun
  dengan waktu run yang sama menimpa file yang sama.
- **Rate limit BMKG** (60 permintaan/menit/IP) dihormati dengan jeda 1,1 detik; lokasi yang gagal
  dicatat di `*.failures.json`, dan run gagal jika lebih dari 10% lokasi gagal.
- **USGS**: jumlah event dicek dulu lewat `/count` (batas 20.000 per query). Event bisa direvisi,
  maka DAG harian memakai `updatedafter` dan tahap silver akan meng-upsert berdasarkan `id`.
- **Silver dihitung ulang penuh setiap jam** dari seluruh bronze (idempotent) dan dimuat ke
  PostgreSQL dalam satu transaksi, sehingga pembaca tidak pernah melihat tabel setengah terisi.
  Volume data masih kecil; jika membesar, langkah berikutnya adalah pemrosesan incremental.
- **Data quality check** (9 aturan, mis. magnitudo wajar, koordinat valid, maksimal 24 slot cuaca,
  integritas kode kab/kota) menggagalkan DAG jika ada baris bermasalah.
- **Gold dijadwalkan dengan Asset, bukan jam tetap:** `gold` hanya berjalan setelah silver lolos
  quality check, jadi tidak pernah membangun mart dari data yang belum tervalidasi.
- **Fakta besar incremental** (`delete+insert` berdasarkan kunci, filter `diperbarui_utc`) dengan
  index BRIN pada kolom waktu; fakta paparan memakai bounding box ±1° sebelum menghitung Haversine
  agar join aset × gempa tidak menjadi cross join penuh.
- **Lokasi cuaca**: per kab/kota dipilih desa di kecamatan yang bernama sama dengan ibukota
  kab/kota (278 dari 514); sisanya desa dengan kode terkecil.
