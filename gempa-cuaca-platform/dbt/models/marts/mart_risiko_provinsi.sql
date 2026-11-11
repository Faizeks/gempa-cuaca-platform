-- Grain: satu provinsi x satu bulan (semua kombinasi, termasuk bulan tanpa kejadian = 0).
-- Aturan sumber: USGS untuk hitungan gempa (histori lengkap sejak 2015, konsisten antar
-- bulan), BMKG untuk gempa yang dirasakan masyarakat.
with provinsi as (
    select kode_provinsi, max(nama_provinsi) as nama_provinsi, sum(penduduk) as penduduk
    from {{ ref('dim_wilayah') }}
    group by kode_provinsi
),

bulan as (
    select distinct awal_bulan as bulan
    from {{ ref('dim_waktu') }}
    where tanggal <= current_date
),

gempa as (
    select
        w.kode_provinsi,
        date_trunc('month', g.tanggal_wib)::date as bulan,
        count(*) filter (where g.sumber = 'usgs') as gempa_usgs,
        count(*) filter (where g.sumber = 'usgs' and g.magnitudo >= 5) as gempa_m5_usgs,
        max(g.magnitudo) filter (where g.sumber = 'usgs') as magnitudo_maks_usgs,
        count(*) filter (where g.sumber = 'bmkg' and g.is_dirasakan) as gempa_dirasakan_bmkg
    from {{ ref('fact_gempa') }} g
    join {{ ref('dim_wilayah') }} w using (kode_kabkota)
    group by 1, 2
),

cuaca as (
    select
        w.kode_provinsi,
        date_trunc('month', p.tanggal_lokal)::date as bulan,
        count(distinct p.tanggal_lokal) filter (where p.is_cuaca_ekstrem) as hari_cuaca_ekstrem
    from {{ ref('fact_prakiraan_cuaca') }} p
    join {{ ref('dim_wilayah') }} w using (kode_kabkota)
    group by 1, 2
)

select
    p.kode_provinsi,
    p.nama_provinsi,
    b.bulan,
    coalesce(g.gempa_usgs, 0) as gempa_usgs,
    coalesce(g.gempa_m5_usgs, 0) as gempa_m5_usgs,
    g.magnitudo_maks_usgs,
    coalesce(g.gempa_dirasakan_bmkg, 0) as gempa_dirasakan_bmkg,
    coalesce(c.hari_cuaca_ekstrem, 0) as hari_cuaca_ekstrem,
    p.penduduk,
    round(coalesce(g.gempa_m5_usgs, 0) * 1000000.0 / nullif(p.penduduk, 0), 3) as gempa_m5_per_1jt_penduduk
from provinsi p
cross join bulan b
left join gempa g on g.kode_provinsi = p.kode_provinsi and g.bulan = b.bulan
left join cuaca c on c.kode_provinsi = p.kode_provinsi and c.bulan = b.bulan
