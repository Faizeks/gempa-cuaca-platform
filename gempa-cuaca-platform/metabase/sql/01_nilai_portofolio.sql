-- Total nilai seluruh aset (gudang + polis), dalam triliun rupiah
select round(sum(nilai_rp) / 1e12, 2) as nilai_portofolio_triliun_rp
from gold.dim_aset
