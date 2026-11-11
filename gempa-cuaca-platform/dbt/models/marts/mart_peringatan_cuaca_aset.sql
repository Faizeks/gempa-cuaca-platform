-- Daftar aset yang diprakirakan terkena cuaca ekstrem (hujan lebat/petir) mulai hari ini,
-- berdasarkan analisis BMKG terbaru untuk setiap kab/kota.
-- Menjawab pertanyaan bisnis: "Gudang mana yang perlu bersiap dalam 3 hari ke depan?"
with analisis_terbaru as (
    select kode_kabkota, max(analisis_utc) as analisis_utc
    from {{ ref('fact_paparan_cuaca') }}
    group by kode_kabkota
)

select
    f.aset_id,
    a.jenis,
    a.kategori,
    a.nilai_rp,
    a.nama_kabkota,
    a.nama_provinsi,
    f.tanggal_lokal,
    f.kondisi_terburuk,
    f.total_hujan_mm,
    f.hujan_maks_per_slot_mm,
    f.angin_maks_kmj,
    f.analisis_utc
from {{ ref('fact_paparan_cuaca') }} f
join analisis_terbaru t using (kode_kabkota, analisis_utc)
join {{ ref('dim_aset') }} a using (aset_id)
where f.ada_cuaca_ekstrem
  and f.tanggal_lokal >= (now() at time zone 'Asia/Jakarta')::date
