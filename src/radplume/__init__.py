"""radplume — platforma do modelowania dyspersji radionuklidów.

Pytanie, na które odpowiada platforma (plan, Część 0):
    „Gdyby doszło do uwolnienia radionuklidów do atmosfery w elektrowni X —
    czy miasto Y zostanie skażone? Jak bardzo, jak szybko i przy jakiej pogodzie?”

Struktura pakietu odpowiada warstwom medalionu:
    ingest/  — pobieranie danych do strefy landing (pliki surowe)
    bronze/  — surowe dane jako tabele Delta, z metadanymi pochodzenia
    silver/  — oczyszczone dane i obliczenia fizyczne
    gold/    — agregaty odpowiadające na pytania biznesowe
    app/     — warstwa serwująca (pytania o miasta, guardrails SQL)
"""

__version__ = "0.1.0"

# Wersja modelu zapisywana w tabelach gold — po zmianie fizyki wiadomo,
# którym modelem policzono dany wynik.
MODEL_VERSION = "gauss-briggs-0.1"
