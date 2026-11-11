-- Jumlah gempa per bulan di Indonesia (USGS), dipisah menurut kekuatan; hanya bulan yang sudah lengkap
select
    date_trunc('month', tanggal_wib)::date as "Bulan",
    count(*) filter (where magnitudo < 5) as "M 4-4.9",
    count(*) filter (where magnitudo >= 5) as "M 5+"
from gold.fact_gempa
where sumber = 'usgs'
  and tanggal_wib < date_trunc('month', current_date)
group by 1
order by 1
