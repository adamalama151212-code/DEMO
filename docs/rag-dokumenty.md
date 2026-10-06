# Baza wiedzy asystenta (RAG) — dokumenty i źródła

> **Dla kogo:** autor projektu i każdy, kto rozbudowuje bazę wiedzy asystenta `smogcast`.
> Stan na 2026-10-01. Lista źródeł do pobrania jest w konfiguracji: `packages/core/src/smogcast/core/conf/app.yaml`
> (`app.rag.sources`) — ten plik ją opisuje i uzasadnia.

## Jak to działa

1. `smogcast run ingest rag-documents` pobiera oficjalne dokumenty do `data/landing/rag/_zrodlo/` (oryginały, bez
   zmian). Każde pobranie sprawdza format: część portali odpowiada automatom stroną HTML (captcha, pętla przekierowań)
   pod adresem `.pdf` — taki plik jest odrzucany, a nie zapisywany jako „rozporządzenie”.
2. `smogcast-app build-index` dzieli dokumenty na fragmenty (~1200 znaków, zakładka 200), zapamiętując miejsce w
   źródle (strona PDF, sekcja dokumentu), liczy embeddingi modelem wielojęzycznym
   `paraphrase-multilingual-MiniLM-L12-v2` (polskie pytania znajdują fragmenty angielskie) i zapisuje indeks w
   `data/models/rag_index/`.
3. Asystent odpowiada **po angielsku** (cała aplikacja jest po angielsku — decyzja autora 2026-10-01), **tylko** z
   odnalezionych fragmentów i zawsze pokazuje źródła; gdy fragmentów brak — „I could not find this information in the
   documents”. Pytania po polsku też działają (model embeddingów jest wielojęzyczny).

Liczby o prognozie i jakości modelu **nie** pochodzą z dokumentów, tylko z tabel gold (SQL przez guardrails).

## Dokumenty w repozytorium (po angielsku, `docs/rag/`)

**Cały indeks jest po angielsku.** Polskie źródła weszły jako **nieoficjalne tłumaczenia** (tylko fragmenty o pyle,
z odnośnikiem do oryginału i adnotacją, że wiąże tekst polski); oryginały są pobierane do `_zrodlo/` tylko do wglądu
(`index: false` w `app.yaml`). GIOŚ nie ma angielskiej wersji tej strony (tłumaczy tylko menu), rozporządzenie nie ma
oficjalnego tłumaczenia.

| Plik | Zawartość |
|---|---|
| `docs/rag/pl-air-quality-levels.md` | nieoficjalne tłumaczenie fragmentów rozporządzenia (Dz.U. 2021 poz. 845) o pyle: wartości dopuszczalne PM10/PM2,5 (zał. 1), docelowe (zał. 2), **alarmowy PM10 = 150 µg/m³** (zał. 4), **informowania PM10 = 100 µg/m³** (zał. 5), pułap ekspozycji (zał. 6) — przetłumaczone z tekstu PDF, nie z pamięci |
| `docs/rag/gios-high-pollution-advice.md` | nieoficjalne tłumaczenie strony GIOŚ: zalecenia przy poziomie informowania i alarmowym, gdzie publikowane są przekroczenia, podstawa prawna |
| `docs/rag/smogcast-project.md` | pytanie biznesowe, definicje, źródła danych, czas, metodyka (czyszczenie, średnie, cechy bez wycieku, modele, metryki, kryteria K1–K4), wyniki 2025 i dlaczego PM10 gorzej, działanie na żywo, demo usterek, ograniczenia, platforma |
| `docs/rag/smogcast-architecture.md` | przepływ danych (ścieżki A/B/C), pakiety i Joby, tabele, czas, idempotencja, środowiska |

Napisane bezosobowo, bez etapów pracy, dziennika i historii zmiany tematu (te zostają w `PLAN_SMOG.md` i
`docs/ARCHITECTURE.md`, które są dokumentami roboczymi po polsku). **Po zmianie metodyki lub wyników trzeba
zaktualizować oba pliki i przebudować indeks.**

## Dokumenty zewnętrzne (pobierane)

| id | Dokument | Wydawca | Co z niego bierzemy | Sprawdzone 2026-10-01 |
|---|---|---|---|---|
| `pl_regulation_air_levels` (tylko do wglądu) | Rozporządzenie Ministra Środowiska w sprawie poziomów niektórych substancji w powietrzu, tekst jednolity Dz.U. 2021 poz. 845 | ISAP / ELI | poziomy dopuszczalne, **poziom informowania PM10 = 100 µg/m³ i alarmowy = 150 µg/m³ (24 h)** | ✅ wartości odczytane z PDF (zał. 4 i 5; zmiana z 2019 r., Dz.U. 2019 poz. 1931) |
| `eu_directive_2024_2881` | Dyrektywa (UE) 2024/2881 w sprawie jakości powietrza (wersja przekształcona) | Urząd Publikacji UE (Cellar; EUR-Lex blokuje automaty) | nowe wartości od 2030 r.: PM10 dobowa 45 µg/m³ (18 dni/rok), roczna 20; PM2,5 dobowa 25 µg/m³ (18 dni/rok), roczna 10; progi informowania i alarmowe (PM2,5: 50 µg/m³ / 1 doba) | ✅ PDF; wartości potwierdzone też w raporcie EEA |
| `eu_directive_2008_50` | Dyrektywa 2008/50/WE | Urząd Publikacji UE (Cellar) | obecne wartości: PM10 dobowa 50 µg/m³ (35 dni/rok), roczna 40; PM2,5 roczna 25; kryterium kompletności danych (75%) | ✅ PDF |
| `gios_alarm_levels` (tylko do wglądu) | GIOŚ: informacje o wysokich stężeniach zanieczyszczeń w powietrzu | Główny Inspektorat Ochrony Środowiska | **zalecenia dla ludności** przy poziomie informowania i alarmowym (dzieci, kobiety w ciąży, seniorzy, choroby serca i układu oddechowego; ograniczenie aktywności na zewnątrz) | ✅ strona HTML |
| `who_ambient_air_quality` | WHO fact sheet: Ambient (outdoor) air pollution and health (aktualizacja 24.10.2024) | Światowa Organizacja Zdrowia | skutki zdrowotne pyłu (choroby serca, udar, POChP, infekcje dolnych dróg oddechowych, rak płuc) | ✅ strona HTML |

### Czego brakuje i dlaczego

- **Pełne wytyczne WHO 2021 (Global Air Quality Guidelines, PDF)** — repozytorium WHO (iris.who.int) i NCBI Bookshelf
  blokują pobieranie automatyczne (captcha), więc PDF nie jest w korpusie. Liczbowe wytyczne WHO dla PM nie są też
  podane w fact sheecie. Jeśli mają być w bazie: pobrać PDF ręcznie z przeglądarki
  (https://www.who.int/publications/i/item/9789240034228, podsumowanie: ISBN 9789240034433), zapisać w
  `data/landing/rag/_zrodlo/` i dopisać do `app.rag.sources` z poprawną nazwą pliku (pobieranie pominie plik, który
  już istnieje).
- **Komunikaty zdrowotne Polskiego Indeksu Jakości Powietrza** (GIOŚ) — do rozważenia jako kolejne źródło.

## Na Databricks (E9)

Pliki z `_zrodlo/` i `docs/rag/` trafiają do Volume (`/Volumes/smogcast_<env>/raw/landing/rag/`), indeks zastępuje
Vector Search (tabela fragmentów w gold/ops + indeks z embeddingami z endpointu modelu), a asystent korzysta z tego
samego interfejsu `Retriever` — patrz `docs/migracja-databricks.md`.
