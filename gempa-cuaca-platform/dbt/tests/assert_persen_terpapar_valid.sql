-- Persentase nilai terpapar harus 0-100, dan aset terpapar tidak boleh melebihi jumlah aset.
select *
from {{ ref('mart_paparan_aset') }}
where persen_nilai_terpapar < 0
   or persen_nilai_terpapar > 100
   or aset_terpapar > jumlah_aset
