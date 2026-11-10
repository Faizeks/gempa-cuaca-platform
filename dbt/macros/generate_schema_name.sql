{#
  Pakai nama schema apa adanya (staging, gold, referensi) alih-alih gaya bawaan dbt
  "gold_staging", agar nama schema di warehouse rapi dan mudah dibaca di Metabase.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
