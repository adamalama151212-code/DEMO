# Plan rozszerzeń modelu: harmonogram uwolnienia, tryb zdarzenia, model obłoków

Plan roboczy trzech zmian uzgodnionych po przeglądzie modelu. Status aktualizowany
w trakcie prac: ⬜ do zrobienia · 🟡 w toku · ✅ zrobione.

## Dlaczego

Obecny model ma trzy słabości, które wyszły w dyskusji:

1. **Emisja jest równomierna.** Każda godzina uwalnia 1/N całości. W Fukushimie emisja szła
   w kilku krótkich zrzutach i to, na jaki wiatr trafił każdy zrzut, decyduje o mapie skażenia.
   Tego samego potrzebuje pytanie „wyciek o 13:00 i drugi o 16:00”.
2. **Nie da się zapytać o konkretne zdarzenie.** Klimatologia odpowiada na pytanie „nie wiadomo,
   kiedy nastąpi awaria”. Brakuje pytania „znam dzień, godziny i ilości: gdzie pójdzie chmura?”,
   w którym Monte Carlo zaburza pogodę tego dnia o małe wartości (niepewność pogody).
3. **Smuga leci prosto.** Każda godzina uwolnienia to prosta smuga z wiatrem tej godziny.
   Przy 5 m/s chmura potrzebuje ok. 5 h na 100 km, a wiatr w tym czasie się zmienia.

## Kluczowe decyzje projektowe

| Decyzja | Uzasadnienie |
|---|---|
| Harmonogram jako **ułamki** całkowitej ilości na godzinę (`release_fraction`, suma = 1) | model zostaje liniowy względem Q: smugę liczymy dla 1 Bq, a ilość (i jej niepewność) mnożymy w gold jak dotąd |
| Całkowita ilość Q per **zestaw scenariuszy** (`silver.source_terms`) | klimatologia: z `sites.yaml`; walidacja: suma z pliku Katata; zdarzenie: suma podana przez użytkownika |
| Zdarzenie = nowy `scenario_set` (`event_…`) w tych samych tabelach | gold, `ask`, walidacja i dashboard działają bez zmian; zdarzeń może być wiele obok siebie |
| Zaburzenie pogody **stałe dla członka zespołu** (offset kierunku, mnożnik prędkości) | odpowiada systematycznemu błędowi reanalizy/prognozy dla danego dnia; proste i deterministyczne |
| Model obłoków: **obłok na godzinę uwolnienia, przesuwany co 15 min wiatrem z danej godziny**, stężenie całkowane analitycznie wzdłuż odcinka (funkcja Φ) | brak „dziur” między pozycjami obłoku; przy stałym wietrze wynik **równy** staremu modelowi (test); wykorzystuje już przetestowane wzory |
| Wybór modelu w konfiguracji (`physics.transport.model: puff \| straight`) | porównanie obu modeli w write-upie; stary model zostaje jako punkt odniesienia |
| Komórki dla odcinka wybierane z **prostokąta wokół odcinka** (indeksy siatki), a nie joinem z całą siatką | skalowalność: tylko kilka–kilkadziesiąt komórek na odcinek |

## Etapy

### Etap 1: Harmonogram uwolnienia ⬜
- ⬜ `silver.source_terms` (scenario_set, site, nuclide, median_bq, gsd, source)
- ⬜ `silver.release_schedule` (scenario_set, site, nuclide, h, release_fraction)
- ⬜ `silver.q_samples` z kolumną `scenario_set`, losowane z `source_terms`
- ⬜ klimatologia: harmonogram równomierny; walidacja: plik `landing/source_term/*.csv` (format z `przygotowanie-danych.md`, pkt E) albo równomierny z ostrzeżeniem
- ⬜ `bronze.source_term` (plik źródłowy), rozkład przedziałów na godziny
- ⬜ model (`straight`) używa `release_fraction` zamiast 1/N
- ⬜ gold łączy próbki Q po `scenario_set`
- ⬜ testy: rozkład przedziałów na godziny, suma ułamków = 1, brak pliku → równomierny

### Etap 2: Tryb zdarzenia ⬜
- ⬜ `radplume event --site … --date … --release 13:00=1e15 --release 16:00-18:00=5e15 [--timezone Europe/Warsaw] [--name …]`
- ⬜ zespół członków: offset kierunku ~ N(0, 15°), mnożnik prędkości ~ lognormal(0, 0,2), przesunięcie stabilności, v_d, Λ, wysokość; członek 0 = niezaburzony
- ⬜ kolumny `wind_dir_offset_deg`, `wind_speed_mult` w `silver.scenarios` (dla klimatologii 0 / 1)
- ⬜ automatyczne pobranie meteo dla dnia zdarzenia, jeśli go brak
- ⬜ zapis tylko wycinka `scenario_set = event_…` (nie niszczy klimatologii)
- ⬜ podsumowanie w terminalu: miasta z największym prawdopodobieństwem, czas dotarcia; `ask --scenario-set event_…`
- ⬜ testy: parsowanie wycieków i strefy czasowej, determinizm członków, przebieg end-to-end

### Etap 3: Model obłoków (trajektorie) ⬜
- ⬜ trajektoria obłoku: kroki 15 min, wiatr z danej godziny (z zaburzeniem członka), suma narastająca położenia
- ⬜ σy, σz z przebytej drogi, niemalejące wzdłuż trajektorii
- ⬜ depozycja z odcinka: istniejące wzory × ΔΦ wzdłuż odcinka (Φ przez przybliżenie erf)
- ⬜ zatrzymanie śledzenia po wyjściu z domeny, po horyzoncie albo przy braku meteo
- ⬜ `physics.transport` w konfiguracji, domyślnie `puff`
- ⬜ testy: dokładność Φ, zgodność ze starym modelem przy stałym wietrze, skręcająca chmura dociera tam, gdzie prosta smuga nie
- ⬜ pomiar czasu przebiegu lokalnie

### Etap 4: Dokumentacja i weryfikacja ⬜
- ⬜ README (komenda `event`, model obłoków)
- ⬜ ARCHITECTURE.md (model, wymiary MC, tabele, decyzje)
- ⬜ HOWTOREAD.md (nowe pliki i tabele, liczby wierszy)
- ⬜ przygotowanie-danych.md (pkt E: obsługiwane)
- ⬜ plan_finalny_radplume.md (rejestr poprawek P22–P24)
- ⬜ pełne testy, ruff, przebieg offline `run-all` + `event`, push
