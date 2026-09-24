"""Bronze: surowe dane z landing jako tabele Delta + metadane pochodzenia.

Zasada: bronze NIE poprawia danych. Dodaje tylko ``source_file`` i
``ingest_ts`` — żeby każdy wiersz w silver/gold dało się prześledzić do pliku,
z którego przyszedł. Silver i gold da się zawsze odtworzyć od zera z bronze.
"""
