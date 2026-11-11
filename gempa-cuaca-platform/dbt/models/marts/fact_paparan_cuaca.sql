{{
    config(
        indexes=[
            {'columns': ['aset_id', 'analisis_utc', 'tanggal_lokal'], 'unique': true},
            {'columns': ['tanggal_lokal']},
        ],
    )
}}

-- Grain: satu aset x satu tanggal prakiraan x satu waktu analisis BMKG.
-- Aset memakai prakiraan dari titik cuaca di kab/kota tempat aset berada
-- (satu titik per kab/kota, lihat reference/lokasi_cuaca.csv).
with harian as (
    select
        kode_kabkota,
        analisis_utc,
        tanggal_lokal,
        sum(curah_hujan_mm) as total_hujan_mm,
        max(curah_hujan_mm) as hujan_maks_per_slot_mm,
        max(suhu_c) as suhu_maks_c,
        max(kec_angin_kmj) as angin_maks_kmj,
        bool_or(is_hujan_lebat) as ada_hujan_lebat,
        bool_or(is_cuaca_ekstrem) as ada_cuaca_ekstrem,
        -- Kondisi terburuk hari itu: slot ekstrem dulu, lalu curah hujan terbesar
        (array_agg(cuaca order by is_cuaca_ekstrem desc, curah_hujan_mm desc nulls last))[1] as kondisi_terburuk,
        count(*) as jumlah_slot
    from {{ ref('fact_prakiraan_cuaca') }}
    group by kode_kabkota, analisis_utc, tanggal_lokal
)

select
    a.aset_id,
    h.analisis_utc,
    h.tanggal_lokal,
    a.jenis,
    a.kategori,
    a.nilai_rp,
    a.kode_kabkota,
    a.kode_provinsi,
    h.total_hujan_mm,
    h.hujan_maks_per_slot_mm,
    h.suhu_maks_c,
    h.angin_maks_kmj,
    h.ada_hujan_lebat,
    h.ada_cuaca_ekstrem,
    h.kondisi_terburuk,
    h.jumlah_slot
from {{ ref('dim_aset') }} a
join harian h using (kode_kabkota)
