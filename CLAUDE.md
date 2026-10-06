# Instrukcje dla agentów AI pracujących w tym repo

## Stan projektu

Trwa **zmiana tematu**: z radplume (dyspersja radionuklidów) na prognozę przekroczeń
PM10/PM2.5 („czy jutro w mieście Y zostanie przekroczona norma i na ile to pewne?”).

**Zacznij od [`HANDOFF.md`](HANDOFF.md)** (stan na koniec poprzedniej sesji i pierwsze kroki). Potem **przeczytaj [`PLAN_SMOG.md`](PLAN_SMOG.md)** — etapy, decyzje,
zweryfikowane źródła danych i dziennik postępu — oraz [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)
(diagramy przepływu, wheele, tabele ze statusem). Po skończeniu etapu: zaktualizuj checkboxy
i dopisz wpis do sekcji 10 planu **oraz** statusy (✅/🔜) i nowe tabele w `docs/ARCHITECTURE.md`.
Kontynuuj od pierwszego niezamkniętego etapu.

Projekt jest zaliczeniowy (kurs Databricks): wymagania w `final-project-spec.md`.

## Zasady pracy

- **Wszystko uruchamiamy w Dockerze** (`docker compose run --rm <usługa> …`). Natywny Spark
  na Windowsie nie działa (brak Javy 17 / winutils).
- **Długie komendy** (`docker compose build`, pełne przebiegi potoku, pełny `pytest`) uruchamiaj
  **w tle** albo podaj autorowi do uruchomienia ręcznie i poczekaj na jego sygnał. Nie odpytuj w pętli.
- Przed oddaniem zmian: `ruff check packages` i testy dotknięte zmianą (w Dockerze).
- **Nie commitujemy lokalnie** (to repo PoC; autor odtworzy historię w docelowym repo). Zamiast tego
  przy każdym etapie w `PLAN_SMOG.md` jest lista „Commity do odtworzenia” — aktualizuj ją, jeśli zakres się zmienił.
- Język: **kod, komentarze, docstringi i logi po angielsku**; dokumentacja robocza (`README.md`, `docs/`,
  `PLAN_SMOG.md`) po polsku. **Aplikacja jest po angielsku** (decyzja autora 2026-10-01): interfejs, odpowiedzi
  asystenta (także `smogcast ask`) i baza wiedzy RAG (`docs/rag/*.md`; polskie źródła jako nieoficjalne tłumaczenia,
  oryginały tylko do wglądu). Komentarze wyjaśniają *dlaczego*.
- Struktura: monorepo z osobnym wheelem na każdy krok (`packages/<paczka>`), wspólna przestrzeń nazw
  `smogcast.*`, wspólny kod tylko w `smogcast-core` — szczegóły w `PLAN_SMOG.md`, sekcja 5a.
- Transformacje to czyste funkcje DataFrame → DataFrame; kroki to funkcje `(Context) -> None` w rejestrze `STEPS` paczki; orkiestracja lokalnie w `packages/cli`, na Databricks w Jobie;
  zależność od środowiska tylko w `smogcast-core` i plikach `conf/*.yaml`.
- **Walidacja modelu:** zbioru testowego (rok z `conf/model.yaml`) nie używaj do strojenia.
  Słaby wynik raportuj uczciwie — rubryka nagradza „honest discussion of trade-offs”.
- Dane z `data/` nie trafiają do gita. Oryginały pobranych plików trzymaj w podfolderze `_zrodlo/`.
