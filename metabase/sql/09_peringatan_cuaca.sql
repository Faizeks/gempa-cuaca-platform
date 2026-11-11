-- Aset yang diprakirakan terkena hujan lebat atau petir mulai hari ini (analisis BMKG terbaru)
select
    tanggal_lokal as "Tanggal",
    nama_provinsi as "Provinsi",
    nama_kabkota as "Kab/Kota",
    aset_id as "ID aset",
    jenis as "Jenis",
    kategori as "Kategori",
    round(nilai_rp / 1e9, 1) as "Nilai (Rp miliar)",
    kondisi_terburuk as "Kondisi terburuk",
    total_hujan_mm as "Total hujan (mm)",
    angin_maks_kmj as "Angin maks (km/jam)"
from gold.mart_peringatan_cuaca_aset
order by tanggal_lokal, nilai_rp desc
