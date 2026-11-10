{{
    config(
        indexes=[
            {'columns': ['aset_id', 'sumber', 'sumber_event_id'], 'unique': true},
            {'columns': ['tanggal_wib']},
        ],
    )
}}

-- Grain: satu aset x satu gempa yang episentrumnya dalam radius paparan.
-- Bounding box (+-1 derajat, kira-kira 111 km) menyaring kandidat dulu agar perhitungan
-- jarak Haversine tidak dilakukan untuk setiap pasangan aset-gempa.
with gempa as (
    select sumber, sumber_event_id, waktu_utc, tanggal_wib, lat, lon, magnitudo, kedalaman_km, kategori_magnitudo
    from {{ ref('fact_gempa') }}
    where lat is not null
      and lon is not null
      and magnitudo >= {{ var('paparan_min_magnitudo') }}
),

kandidat as (
    select
        a.aset_id,
        a.jenis,
        a.kategori,
        a.nilai_rp,
        a.kode_kabkota,
        a.kode_provinsi,
        g.sumber,
        g.sumber_event_id,
        g.waktu_utc,
        g.tanggal_wib,
        g.magnitudo,
        g.kedalaman_km,
        g.kategori_magnitudo,
        {{ haversine_km('a.lat', 'a.lon', 'g.lat', 'g.lon') }} as jarak_km
    from {{ ref('dim_aset') }} a
    join gempa g
      on g.lat between a.lat - 1 and a.lat + 1
     and g.lon between a.lon - 1 and a.lon + 1
)

select
    aset_id,
    sumber,
    sumber_event_id,
    waktu_utc,
    tanggal_wib,
    jenis,
    kategori,
    nilai_rp,
    kode_kabkota,
    kode_provinsi,
    magnitudo,
    kedalaman_km,
    kategori_magnitudo,
    round(jarak_km::numeric, 2) as jarak_km
from kandidat
where jarak_km <= {{ var('paparan_radius_km') }}
