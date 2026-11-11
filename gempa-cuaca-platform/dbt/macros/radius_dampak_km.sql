{#
  Radius dampak gempa (km) sebagai fungsi magnitudo. Parameter ada di vars dbt_project.yml.
  M5 -> 25 km, M6 -> 63 km, M7 -> 158 km (dengan nilai default).
#}
{% macro radius_dampak_km(magnitudo) -%}
    ({{ var('radius_dampak_m5_km') }} * power(10, {{ var('faktor_radius_per_magnitudo') }} * ({{ magnitudo }} - 5)))
{%- endmacro %}
