{#
  Test generik: kombinasi beberapa kolom harus unik (kunci komposit).
  Ditulis sendiri agar project tidak bergantung pada paket dbt_utils.
#}
{% test unique_combination_of_columns(model, combination_of_columns) %}
select {{ combination_of_columns | join(', ') }}, count(*) as jumlah
from {{ model }}
group by {{ combination_of_columns | join(', ') }}
having count(*) > 1
{% endtest %}
