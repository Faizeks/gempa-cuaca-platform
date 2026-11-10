{#
  Jarak lingkaran besar (km) antara dua titik lat/lon, rumus Haversine.
  Sama dengan perhitungan kab/kota terdekat di Spark (pipeline/silver/gempa.py).
#}
{% macro haversine_km(lat1, lon1, lat2, lon2) -%}
    (2 * 6371.0 * asin(sqrt(
        power(sin(radians({{ lat2 }} - {{ lat1 }}) / 2), 2)
        + cos(radians({{ lat1 }})) * cos(radians({{ lat2 }}))
        * power(sin(radians({{ lon2 }} - {{ lon1 }}) / 2), 2)
    )))
{%- endmacro %}
