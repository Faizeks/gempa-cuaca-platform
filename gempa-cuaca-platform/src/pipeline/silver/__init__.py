"""Transformasi bronze -> silver dengan Spark.

Silver = data yang sudah bersih, bertipe benar, tanpa duplikat, dan diberi kode wilayah.
Setiap fungsi menerima/mengembalikan DataFrame supaya bisa diuji dengan Spark lokal.
Hanya fungsi bawaan Spark yang dipakai (tanpa Python UDF), sehingga eksekusi berjalan
penuh di JVM executor.
"""
