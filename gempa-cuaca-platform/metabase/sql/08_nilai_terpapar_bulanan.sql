-- Nilai aset terpapar gempa per bulan (miliar rupiah), 24 bulan lengkap terakhir, per jenis aset
select
    bulan as "Bulan",
    jenis as "Jenis aset",
    round(sum(nilai_terpapar_rp) / 1e9, 1) as "Nilai terpapar (Rp miliar)"
from gold.mart_paparan_aset
where bulan >= date_trunc('month', current_date) - interval '24 months'
  and bulan < date_trunc('month', current_date)
group by 1, 2
order by 1, 2
