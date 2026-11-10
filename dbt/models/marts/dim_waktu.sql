{{ config(indexes=[{'columns': ['tanggal'], 'unique': true}]) }}

-- Kalender harian dari awal histori sampai 10 hari ke depan (menampung prakiraan cuaca)
with tanggal as (
    select generate_series(
        '{{ var("tanggal_awal") }}'::date,
        (current_date + interval '10 days')::date,
        interval '1 day'
    )::date as tanggal
)

select
    tanggal,
    extract(year from tanggal)::int as tahun,
    extract(quarter from tanggal)::int as kuartal,
    extract(month from tanggal)::int as bulan,
    (array['Januari', 'Februari', 'Maret', 'April', 'Mei', 'Juni', 'Juli',
           'Agustus', 'September', 'Oktober', 'November', 'Desember'])[extract(month from tanggal)::int] as nama_bulan,
    date_trunc('month', tanggal)::date as awal_bulan,
    extract(isodow from tanggal)::int as hari_ke,
    (array['Senin', 'Selasa', 'Rabu', 'Kamis', 'Jumat', 'Sabtu', 'Minggu'])[extract(isodow from tanggal)::int] as nama_hari,
    extract(isodow from tanggal) in (6, 7) as is_akhir_pekan,
    -- Pembagian musim yang umum dipakai di Indonesia (perkiraan kasar, tiap daerah bisa berbeda)
    case when extract(month from tanggal) in (10, 11, 12, 1, 2, 3) then 'hujan' else 'kemarau' end as musim
from tanggal
