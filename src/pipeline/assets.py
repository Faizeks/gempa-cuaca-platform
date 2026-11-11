"""Asset Airflow yang menghubungkan DAG satu dengan lainnya.

`silver` menandai asset ini setelah quality check lolos; `gold` dijadwalkan pada
asset ini, jadi gold hanya berjalan di atas data silver yang sudah tervalidasi.

Catatan: URI skema postgres:// divalidasi provider PostgreSQL dan wajib menunjuk satu
tabel (database/schema/tabel). Asset ini mewakili seluruh layer silver, jadi dipakai
URI file:// ke folder Parquet silver.
"""

from airflow.sdk import Asset

WAREHOUSE_SILVER = Asset(name="warehouse_silver", uri="file:///opt/airflow/data/silver")
