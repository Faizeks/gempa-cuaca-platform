-- Lokasi aset yang berada dalam radius dampak gempa M>=5 dalam 12 bulan terakhir
select
    a.lat,
    a.lon,
    a.aset_id,
    a.jenis,
    a.nama_kabkota,
    round(a.nilai_rp / 1e9, 1) as nilai_miliar_rp,
    count(distinct p.sumber || ':' || p.sumber_event_id) as jumlah_gempa,
    max(p.magnitudo) as magnitudo_maks,
    min(p.jarak_km) as jarak_terdekat_km
from gold.fact_paparan_gempa p
join gold.dim_aset a using (aset_id)
where p.tanggal_wib >= current_date - interval '12 months'
group by a.lat, a.lon, a.aset_id, a.jenis, a.nama_kabkota, a.nilai_rp
