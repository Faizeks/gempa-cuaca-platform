-- Setiap baris paparan harus berada dalam radius dampak gempanya dan di atas magnitudo minimum.
-- Test gagal jika query ini mengembalikan baris.
select *
from {{ ref('fact_paparan_gempa') }}
where jarak_km > radius_dampak_km
   or magnitudo < {{ var('paparan_min_magnitudo') }}
