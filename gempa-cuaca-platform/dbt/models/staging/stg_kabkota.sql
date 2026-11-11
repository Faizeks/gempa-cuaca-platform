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
    penduduk
from {{ source('silver', 'kabkota') }}
