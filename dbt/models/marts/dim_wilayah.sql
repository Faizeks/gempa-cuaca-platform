{{ config(indexes=[{'columns': ['kode_kabkota'], 'unique': true}, {'columns': ['kode_provinsi']}]) }}

select
    kode_kabkota,
    nama_kabkota,
    jenis,
    ibukota,
    kode_provinsi,
    nama_provinsi,
    lat,
    lon,
    elevasi_m,
    luas_km2,
    penduduk,
    round((penduduk / nullif(luas_km2, 0))::numeric, 1) as kepadatan_per_km2
from {{ ref('stg_kabkota') }}
