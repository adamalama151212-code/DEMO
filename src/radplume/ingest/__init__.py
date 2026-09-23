"""Ingest: pobranie danych źródłowych do strefy landing jako pliki surowe.

Warstwa ingestu świadomie NIE używa Sparka do pobierania — to wywołania HTTP
i małe pliki. Spark wchodzi dopiero w bronze, gdzie czyta pliki z landing.
Dzięki temu źródło (API, plik, symulator) można podmienić bez dotykania
transformacji (plan 3.1: „ingest odseparowany od transformacji”).
"""
