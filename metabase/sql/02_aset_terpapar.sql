-- Jumlah aset yang minimal sekali berada dalam radius dampak gempa M>=5 dalam 12 bulan terakhir
select count(distinct aset_id) as aset_terpapar
from gold.fact_paparan_gempa
where tanggal_wib >= current_date - interval '12 months'
