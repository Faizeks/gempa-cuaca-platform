-- Grain: satu provinsi x satu bulan x satu jenis aset.
-- "Terpapar" = ada minimal satu gempa M >= paparan_min_magnitudo dalam radius paparan.
-- Aset dihitung sekali per bulan walau terpapar beberapa gempa (atau gempa yang sama
-- tercatat di BMKG dan USGS), jadi nilai aset terpapar tidak dihitung ganda.
with portofolio as (
    select kode_provinsi, max(nama_provinsi) as nama_provinsi, jenis,
           count(*) as jumlah_aset, sum(nilai_rp) as nilai_portofolio_rp
    from {{ ref('dim_aset') }}
    group by kode_provinsi, jenis
),

bulan as (
    select distinct awal_bulan as bulan
    from {{ ref('dim_waktu') }}
    where tanggal <= current_date
),

aset_terpapar as (
    select
        kode_provinsi,
        jenis,
        date_trunc('month', tanggal_wib)::date as bulan,
        aset_id,
        max(nilai_rp) as nilai_rp,
        max(magnitudo) as magnitudo_maks,
        min(jarak_km) as jarak_terdekat_km,
        count(distinct sumber || ':' || sumber_event_id) as jumlah_gempa
    from {{ ref('fact_paparan_gempa') }}
    group by 1, 2, 3, 4
),

ringkasan as (
    select
        kode_provinsi,
        jenis,
        bulan,
        count(*) as aset_terpapar,
        sum(nilai_rp) as nilai_terpapar_rp,
        max(magnitudo_maks) as magnitudo_maks,
        min(jarak_terdekat_km) as jarak_terdekat_km
    from aset_terpapar
    group by 1, 2, 3
)

select
    p.kode_provinsi,
    p.nama_provinsi,
    p.jenis,
    b.bulan,
    p.jumlah_aset,
    p.nilai_portofolio_rp,
    coalesce(r.aset_terpapar, 0) as aset_terpapar,
    coalesce(r.nilai_terpapar_rp, 0) as nilai_terpapar_rp,
    round(coalesce(r.nilai_terpapar_rp, 0) * 100.0 / nullif(p.nilai_portofolio_rp, 0), 2) as persen_nilai_terpapar,
    r.magnitudo_maks,
    r.jarak_terdekat_km
from portofolio p
cross join bulan b
left join ringkasan r on r.kode_provinsi = p.kode_provinsi and r.jenis = p.jenis and r.bulan = b.bulan
