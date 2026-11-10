{{
    config(
        materialized='incremental',
        unique_key=['sumber', 'sumber_event_id'],
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns',
        indexes=[
            {'columns': ['sumber', 'sumber_event_id'], 'unique': true},
            {'columns': ['waktu_utc'], 'type': 'brin'},
            {'columns': ['kode_kabkota']},
        ],
    )
}}

-- Grain: satu kejadian gempa per sumber (BMKG dan USGS tidak digabung).
-- Incremental: hanya kejadian baru atau yang direvisi (diperbarui_utc lebih baru) yang diproses.
-- BRIN index cocok untuk data yang bertambah menurut waktu: kecil dan cepat untuk filter rentang waktu.
select
    sumber,
    sumber_event_id,
    waktu_utc,
    tanggal_wib,
    lat,
    lon,
    kedalaman_km,
    magnitudo,
    tipe_magnitudo,
    case
        when magnitudo is null then 'tidak diketahui'
        when magnitudo < 4 then 'lemah (<4)'
        when magnitudo < 5 then 'ringan (4-4.9)'
        when magnitudo < 6 then 'sedang (5-5.9)'
        when magnitudo < 7 then 'kuat (6-6.9)'
        else 'sangat kuat (>=7)'
    end as kategori_magnitudo,
    case
        when kedalaman_km is null then 'tidak diketahui'
        when kedalaman_km < 70 then 'dangkal (<70 km)'
        when kedalaman_km <= 300 then 'menengah (70-300 km)'
        else 'dalam (>300 km)'
    end as kategori_kedalaman,
    dirasakan is not null as is_dirasakan,
    keterangan_lokasi,
    potensi,
    dirasakan,
    status_review,
    feed_bmkg,
    kode_kabkota,
    jarak_kabkota_km,
    diperbarui_utc
from {{ ref('stg_gempa') }}
{% if is_incremental() %}
where diperbarui_utc > (select coalesce(max(diperbarui_utc), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}
