{{
    config(
        indexes=[
            {'columns': ['aset_id', 'sumber', 'sumber_event_id'], 'unique': true},
            {'columns': ['tanggal_wib']},
        ],
    )
}}

-- Grain: satu aset x satu gempa yang episentrumnya berada dalam radius dampak gempa itu.
-- Radius dampak bergantung magnitudo (macro radius_dampak_km). Bounding box seluas radius
-- (1 derajat ~ 111 km; dibagi 105 agar aman untuk lintang Indonesia) menyaring kandidat dulu,
-- sehingga jarak Haversine tidak dihitung untuk setiap pasangan aset-gempa.
with gempa as (
    select
        sumber, sumber_event_id, waktu_utc, tanggal_wib, lat, lon, magnitudo, kedalaman_km, kategori_magnitudo,
        {{ radius_dampak_km('magnitudo') }} as radius_dampak_km
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
        g.radius_dampak_km,
        {{ haversine_km('a.lat', 'a.lon', 'g.lat', 'g.lon') }} as jarak_km
    from {{ ref('dim_aset') }} a
    join gempa g
      on g.lat between a.lat - g.radius_dampak_km / 105 and a.lat + g.radius_dampak_km / 105
     and g.lon between a.lon - g.radius_dampak_km / 105 and a.lon + g.radius_dampak_km / 105
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
    round(radius_dampak_km::numeric, 1) as radius_dampak_km,
    round(jarak_km::numeric, 2) as jarak_km
from kandidat
where jarak_km <= radius_dampak_km
