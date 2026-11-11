-- Persentase nilai portofolio yang terpapar gempa dalam 12 bulan terakhir
-- (setiap aset dihitung sekali walau terpapar beberapa gempa)
with terpapar as (
    select distinct aset_id, nilai_rp
    from gold.fact_paparan_gempa
    where tanggal_wib >= current_date - interval '12 months'
)
select round(
    coalesce((select sum(nilai_rp) from terpapar), 0) * 100.0
    / (select sum(nilai_rp) from gold.dim_aset), 1
) as persen_nilai_terpapar
