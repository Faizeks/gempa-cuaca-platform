# Image untuk Airflow DAN Spark (master/worker) di project ini.
#
# Spark dijalankan dari paket PySpark yang terpasang di image ini, sehingga driver
# (task Airflow) dan worker Spark dijamin memakai versi Python, Spark, dan Java
# yang sama persis. Image Bitnami Spark yang dipakai repo contoh tidak lagi tersedia bebas.
FROM apache/airflow:3.4.0

USER root
# Java untuk Spark. Image Airflow 3.4.0 berbasis Debian 13, Java default-nya OpenJDK 21.
# procps dibutuhkan script Spark (perintah ps).
RUN apt-get update \
    && apt-get install -y --no-install-recommends default-jre-headless procps \
    && apt-get autoremove -yqq --purge \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow
COPY requirements.txt /requirements.txt
# Menyertakan apache-airflow dengan versi yang sama mencegah pip mengubah versi Airflow
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt

# dbt di virtualenv terpisah: dependensinya tidak bentrok dengan dependensi Airflow
COPY requirements-dbt.txt /requirements-dbt.txt
RUN python -m venv /home/airflow/dbt-venv \
    && /home/airflow/dbt-venv/bin/pip install --no-cache-dir -r /requirements-dbt.txt
