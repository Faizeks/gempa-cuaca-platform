{{ config(indexes=[{'columns': ['aset_id'], 'unique': true}, {'columns': ['kode_kabkota']}]) }}

select
    a.aset_id,
    a.jenis,
    a.kategori,
    a.nilai_rp,
    case
        when a.nilai_rp >= 25000000000 then '>= Rp25 M'
        when a.nilai_rp >= 5000000000 then 'Rp5-25 M'
        when a.nilai_rp >= 1000000000 then 'Rp1-5 M'
        else '< Rp1 M'
    end as kelas_nilai,
    a.lat,
    a.lon,
    a.kode_kabkota,
    w.nama_kabkota,
    w.kode_provinsi,
    w.nama_provinsi
from {{ ref('stg_aset') }} a
left join {{ ref('dim_wilayah') }} w using (kode_kabkota)
