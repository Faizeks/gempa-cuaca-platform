-- Tabel layer silver di database "warehouse".
-- Dijalankan setiap kali loader berjalan (idempotent: hanya membuat yang belum ada).
-- Tabel diisi ulang penuh dari Parquet silver dalam satu transaksi.

CREATE SCHEMA IF NOT EXISTS silver;

CREATE TABLE IF NOT EXISTS silver.kabkota (
    kode_kabkota   varchar(5) PRIMARY KEY,
    nama_kabkota   text NOT NULL,
    jenis          text NOT NULL,          -- 'kabupaten' atau 'kota'
    ibukota        text,
    kode_provinsi  varchar(2) NOT NULL,
    nama_provinsi  text,
    lat            double precision,       -- centroid
    lon            double precision,
    elevasi_m      double precision,
    luas_km2       double precision,
    penduduk       bigint
);

CREATE TABLE IF NOT EXISTS silver.gempa (
    sumber             text NOT NULL,       -- 'bmkg' atau 'usgs'
    sumber_event_id    text NOT NULL,       -- event_key (BMKG) atau id (USGS)
    waktu_utc          timestamptz NOT NULL,
    lat                double precision,
    lon                double precision,
    kedalaman_km       double precision,
    magnitudo          double precision,
    tipe_magnitudo     text,                -- hanya USGS (mww, mb, ...)
    keterangan_lokasi  text,
    potensi            text,                -- hanya BMKG
    dirasakan          text,                -- hanya BMKG (skala MMI)
    status_review      text,                -- hanya USGS (reviewed/automatic)
    diperbarui_utc     timestamptz,
    feed_bmkg          text,                -- feed BMKG yang memuat kejadian ini
    kode_kabkota       varchar(5),          -- kab/kota dengan centroid terdekat
    jarak_kabkota_km   double precision,
    diekstrak_utc      timestamptz,
    PRIMARY KEY (sumber, sumber_event_id)
);
CREATE INDEX IF NOT EXISTS ix_silver_gempa_waktu ON silver.gempa (waktu_utc);
CREATE INDEX IF NOT EXISTS ix_silver_gempa_kabkota ON silver.gempa (kode_kabkota);

CREATE TABLE IF NOT EXISTS silver.prakiraan_cuaca (
    adm4               varchar(13) NOT NULL,
    kode_kabkota       varchar(5) NOT NULL,
    desa               text,
    lat                double precision,
    lon                double precision,
    analisis_utc       timestamptz NOT NULL, -- waktu analisis BMKG
    waktu_utc          timestamptz NOT NULL, -- waktu yang diprakirakan
    tanggal_lokal      date NOT NULL,
    jam_lokal          smallint,
    suhu_c             double precision,
    kelembapan_pct     double precision,
    curah_hujan_mm     double precision,
    tutupan_awan_pct   double precision,
    kec_angin_kmj      double precision,
    arah_angin_deg     double precision,
    arah_angin         text,
    jarak_pandang_m    double precision,
    kode_cuaca         integer,
    cuaca              text,
    cuaca_en           text,
    diekstrak_utc      timestamptz,
    PRIMARY KEY (adm4, analisis_utc, waktu_utc)
);
CREATE INDEX IF NOT EXISTS ix_silver_cuaca_tanggal ON silver.prakiraan_cuaca (tanggal_lokal);
CREATE INDEX IF NOT EXISTS ix_silver_cuaca_kabkota ON silver.prakiraan_cuaca (kode_kabkota);
