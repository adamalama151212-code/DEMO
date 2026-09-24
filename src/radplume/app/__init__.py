"""Warstwa serwująca: odpowiedzi na pytania o miasta i guardrails SQL (plan 4.8).

Zasada nadrzędna (P13): każda liczba w odpowiedzi pochodzi z wyniku zapytania
do tabel gold. LLM (docelowo Foundation Model API na Databricks) tylko
tłumaczy pytanie na SQL i wynik na język naturalny — nigdy nie szacuje sam.
Lokalnie ścieżkę liczbową realizuje deterministyczny szablon zapytania
(``city_query``), przepuszczony przez te same guardrails, których użyje LLM.
"""
