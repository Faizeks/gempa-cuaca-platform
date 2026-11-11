#!/bin/bash
# Dijalankan otomatis oleh image postgres saat volume data masih kosong.
# Membuat database "warehouse" (untuk data project) dan "metabase" (metadata Metabase).
set -euo pipefail

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER ${WAREHOUSE_USER} WITH PASSWORD '${WAREHOUSE_PASSWORD}';
    CREATE DATABASE ${WAREHOUSE_DB} OWNER ${WAREHOUSE_USER};
    CREATE DATABASE metabase OWNER ${POSTGRES_USER};
EOSQL

echo "Database '${WAREHOUSE_DB}' dan 'metabase' sudah dibuat."
