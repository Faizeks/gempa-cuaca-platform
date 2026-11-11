-- Ringkasan risiko per provinsi untuk 12 bulan terakhir
select
    nama_provinsi as "Provinsi",
    sum(gempa_m5_usgs) as "Gempa M5+",
    max(magnitudo_maks_usgs) as "Magnitudo maks",
    sum(gempa_dirasakan_bmkg) as "Gempa dirasakan (BMKG)",
    sum(hari_cuaca_ekstrem) as "Hari cuaca ekstrem",
    round(sum(gempa_m5_usgs) * 1000000.0 / max(penduduk), 2) as "Gempa M5+ per 1 juta penduduk"
from gold.mart_risiko_provinsi
where bulan >= date_trunc('month', current_date) - interval '11 months'
group by nama_provinsi
order by 2 desc, 4 desc
