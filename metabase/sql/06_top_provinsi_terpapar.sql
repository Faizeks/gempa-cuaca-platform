-- 10 provinsi dengan persentase nilai aset terpapar gempa tertinggi (12 bulan terakhir)
with terpapar as (
    select distinct aset_id
    from gold.fact_paparan_gempa
    where tanggal_wib >= current_date - interval '12 months'
)
select
    a.nama_provinsi as "Provinsi",
    round(sum(a.nilai_rp) filter (where t.aset_id is not null) * 100.0 / sum(a.nilai_rp), 1)
        as "% nilai aset terpapar"
from gold.dim_aset a
left join terpapar t using (aset_id)
group by a.nama_provinsi
having count(t.aset_id) > 0
order by 2 desc
limit 10
