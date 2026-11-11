{{
    config(
        materialized='incremental',
        unique_key=['adm4', 'analisis_utc', 'waktu_utc'],
        incremental_strategy='delete+insert',
        on_schema_change='append_new_columns',
        indexes=[
            {'columns': ['adm4', 'analisis_utc', 'waktu_utc'], 'unique': true},
            {'columns': ['waktu_utc'], 'type': 'brin'},
            {'columns': ['kode_kabkota', 'tanggal_lokal']},
        ],
    )
}}

-- Grain: satu lokasi x satu slot 3 jam x satu waktu analisis BMKG.
-- Prakiraan untuk slot yang sama disimpan per analisis, jadi perubahan prakiraan dari
-- waktu ke waktu tetap bisa dianalisis.
select
    p.adm4,
    p.kode_kabkota,
    p.desa,
    p.analisis_utc,
    p.waktu_utc,
    p.tanggal_lokal,
    p.jam_lokal,
    p.suhu_c,
    p.kelembapan_pct,
    p.curah_hujan_mm,
    p.tutupan_awan_pct,
    p.kec_angin_kmj,
    p.arah_angin_deg,
    p.jarak_pandang_m,
    p.kode_cuaca,
    p.cuaca,
    coalesce(p.curah_hujan_mm >= {{ var('ambang_hujan_lebat_mm') }}, false) as is_hujan_lebat,
    (coalesce(c.is_cuaca_ekstrem, false)
        or coalesce(p.curah_hujan_mm >= {{ var('ambang_hujan_lebat_mm') }}, false)) as is_cuaca_ekstrem,
    p.diekstrak_utc
from {{ ref('stg_prakiraan_cuaca') }} p
left join {{ ref('dim_cuaca') }} c using (kode_cuaca)
{% if is_incremental() %}
where p.diekstrak_utc > (select coalesce(max(diekstrak_utc), '1900-01-01'::timestamptz) from {{ this }})
{% endif %}
