"""Silver: oczyszczone dane i obliczenia fizyczne.

Zakazane w silver/ i gold/ (plan 3.1): pandas, numpy, ``.collect()`` na dużych
danych, ``.toPandas()``, pętle po siatce/scenariuszach, Python UDF, ``.coalesce(1)``.
Powód: każda z tych rzeczy działa na laptopie, a na klastrze albo przenosi
wszystkie dane na jeden węzeł (driver), albo blokuje optymalizator Sparka.
Wszystko liczymy wyrażeniami kolumnowymi Sparka.
"""
