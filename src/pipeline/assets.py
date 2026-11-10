"""Asset Airflow yang menghubungkan DAG satu dengan lainnya.

`silver` menandai asset ini setelah quality check lolos; `gold` dijadwalkan pada
asset ini, jadi gold hanya berjalan di atas data silver yang sudah tervalidasi.
"""

from airflow.sdk import Asset

WAREHOUSE_SILVER = Asset(name="warehouse_silver", uri="file:///opt/airflow/data/silver")
