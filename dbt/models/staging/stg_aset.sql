-- Aset sintetis (seed). Data fiktif yang mewakili data internal perusahaan.
select
    aset_id,
    jenis,
    kategori,
    kode_kabkota,
    lat,
    lon,
    nilai_rp
from {{ ref('aset') }}
