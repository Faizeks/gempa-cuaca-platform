-- Episentrum gempa M>=5 dalam 12 bulan terakhir (USGS), untuk peta titik
select
    lat,
    lon,
    magnitudo,
    kedalaman_km,
    kategori_magnitudo,
    tanggal_wib,
    keterangan_lokasi
from gold.fact_gempa
where sumber = 'usgs'
  and magnitudo >= 5
  and tanggal_wib >= current_date - interval '12 months'
order by magnitudo desc
