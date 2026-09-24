"""Orkiestracja: kolejność kroków potoku (bez logiki transformacji).

- ``batch``  — ścieżka A: meteo → dyspersja → odpowiedź „czy miasto Y zostanie skażone”
- ``stream`` — ścieżki B i C: czujniki (Structured Streaming) i rejestr urządzeń (CDC)

Każdy krok to funkcja ``(Context) -> None`` = jeden task Lakeflow Joba na Databricks.
Logika obliczeń mieszka w warstwach ``bronze/``, ``silver/``, ``gold/`` — tutaj tylko
czytamy tabele wejściowe, wołamy transformację i zapisujemy wynik.
"""
