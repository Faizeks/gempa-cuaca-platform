select
    sumber,
    sumber_event_id,
    waktu_utc,
    -- Tanggal dalam WIB, dipakai untuk analisis harian/bulanan
    (waktu_utc at time zone 'Asia/Jakarta')::date as tanggal_wib,
    lat,
    lon,
    kedalaman_km,
    magnitudo,
    tipe_magnitudo,
    keterangan_lokasi,
    potensi,
    dirasakan,
    status_review,
    diperbarui_utc,
    feed_bmkg,
    kode_kabkota,
    jarak_kabkota_km
from {{ source('silver', 'gempa') }}
