-- Kategori cuaca BMKG yang pernah muncul di data. Deskripsi diambil dari data terbaru,
-- bukan dari daftar statis, sehingga kode baru dari BMKG otomatis ikut.
with kemunculan as (
    select
        kode_cuaca,
        cuaca,
        cuaca_en,
        row_number() over (partition by kode_cuaca order by diekstrak_utc desc nulls last) as urutan
    from {{ ref('stg_prakiraan_cuaca') }}
    where kode_cuaca is not null
)

select
    kode_cuaca,
    cuaca,
    cuaca_en,
    (cuaca ilike '%lebat%' or cuaca ilike '%petir%') as is_cuaca_ekstrem
from kemunculan
where urutan = 1
