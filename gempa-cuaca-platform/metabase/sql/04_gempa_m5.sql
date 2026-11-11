-- Jumlah gempa M>=5 di wilayah Indonesia (USGS) dalam 12 bulan terakhir
select count(*) as gempa_m5
from gold.fact_gempa
where sumber = 'usgs'
  and magnitudo >= 5
  and tanggal_wib >= current_date - interval '12 months'
