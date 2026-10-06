# Jak poprawić prognozę PM10 — materiał do demo

> **Dla kogo:** autor projektu, do przygotowania wypowiedzi na Demo Day (część „ograniczenia i dalsze kroki”).
> **Stan na:** 2026-10-06. Liczby z backtestu na roku testowym 2025 w wersji powtarzalnej (identyczne lokalnie i na
> Databricks — `PLAN_SMOG.md`, E5, ramka „Aktualizacja”). Nic z poniższych ulepszeń **nie jest zaimplementowane** —
> decyzja autora: zostają obecne modele; to materiał do demo, nie wynik.

## 1. Punkt wyjścia: co dokładnie jest nie tak z PM10

| Rok testowy 2025 (3613 dni × miasto) | PM10 | PM2.5 (dla porównania) |
|---|---|---|
| dni z przekroczeniem | 292 (8,1%) | 697 (19,2%) |
| Brier: model / klimatologia / persystencja | **0,0434** / 0,0615 / 0,0933 | 0,0751 / 0,117 / 0,172 |
| AUC (porządkowanie dni od najbezpieczniejszych) | **0,95** | 0,94 |
| wykryte dni z przekroczeniem (POD, ostrzeżenie przy P ≥ 50%) | **44%** (129 z 292) | 70% |
| fałszywe alarmy (FAR) | 32% (60 z 189 ostrzeżeń) | 27% |
| kalibracja: największa rozbieżność „prognoza vs rzeczywistość” | 8,5 p.p. (wymagane ≤ 15) — **na granicy, kruche** | 7,2 p.p. |
| werdykt kryteriów zamrożonych przed treningiem | ❌ K4a (wykrywalność) | ✅ |

**Diagnoza w jednym zdaniu:** model PM10 bardzo dobrze **rozpoznaje, które dni są groźniejsze od innych**
(AUC 0,95), ale **przy ponad połowie prawdziwych epizodów daje za niskie prawdopodobieństwo** i ostrzeżenie nie pada.

**Kalibracja jest na granicy.** W wersjach modelu różniących się tylko szumem obliczeń największa rozbieżność
wynosiła od 8,5 do 21 p.p. — werdykt K3 zmieniał się w obie strony. Przedziały wysokich prognoz PM10 mają po ok.
30 dni (0,7–0,8: 29 dni, czyli poniżej minimum do oceny), więc kilka dni decyduje o wyniku. Na demo: „kalibracja PM10
mieści się w kryterium, ale z małym zapasem i na małej liczbie dni — nie traktujemy tego jako mocnego wyniku”. To nie jest problem algorytmu, tylko **brakującej
informacji**: modelowi brakuje danych o przyczynach, które odróżniają PM10 od PM2.5.

## 2. Dlaczego PM10 jest trudniejszy (przyczyna, nie objaw)

- **PM2.5** to głównie pył ze **spalania** (piece domowe, ruch). Zależy prawie wyłącznie od tego, co model zna z
  prognozy pogody: mróz → więcej palenia, cisza wiatrowa → dym zostaje nad miastem.
- **PM10** = PM2.5 **plus pył grubszy**: podrywany z suchych ulic przez ruch, z budów, ziemi, piasku po zimowym
  posypywaniu. **Tych źródeł nie ma w żadnej cesze modelu** — widzi tylko połowę przyczyn.
- Przekroczenia PM10 są **rzadsze** (8% dni vs 19%) i wymagają **silnego** epizodu (średnia dobowa > 50 µg/m³) —
  mniej przykładów do nauki i łatwiej o fałszywą pewność.
- PM10 mierzy więcej stacji przy ruchliwych ulicach (Kraków: 9 stanowisk PM10 vs 2 PM2.5); jedna stacja
  komunikacyjna wystarcza do przekroczenia w mieście, a lokalnego skoku nie widać w prognozie pogody.

## 3. Nowe cechy — co dodać i dlaczego to powinno pomóc

Uporządkowane od największego spodziewanego efektu. Każda pozycja: **mechanizm** (dlaczego wpływa na PM10), **skąd
dane**, **pułapka** (na co uważać, żeby nie zawyżyć wyniku).

### 3.1 Prognoza jakości powietrza z modelu fizycznego (CAMS)

- **Mechanizm:** Copernicus Atmosphere Monitoring Service (UE) liczy codziennie prognozę stężeń PM10 i PM2.5 nad
  Europą modelem chemii i transportu atmosfery — uwzględnia emisje, wiatr na wielu wysokościach, napływ zanieczyszczeń
  z innych regionów, **pył saharyjski**, pożary. Nasz model uczyłby się **poprawiać lokalnie** tę prognozę (zna
  konkretne stacje i ich historię), zamiast zgadywać wszystko z pogody. To typowe połączenie „model fizyczny +
  uczenie maszynowe”.
- **Dlaczego zwłaszcza PM10:** epizody napływowe (pył saharyjski, transport z innych regionów) i wiosenne pylenia
  są dla naszego modelu dziś niewidoczne, a CAMS je modeluje.
- **Skąd:** Open-Meteo Air Quality API (prognoza CAMS European, PM10 i PM2.5, godzinowo) — to samo źródło co nasza
  pogoda, ten sam kod pobierania.
- **Pułapka:** do treningu i testu wolno użyć tylko **prognozy wydanej dzień wcześniej**, nie późniejszej analizy
  (ta sama zasada co z pogodą, decyzja D7). Do sprawdzenia: od kiedy archiwum takich prognoz jest dostępne.

### 3.2 Pył z ulic: dni bez deszczu i sezon po zimowym posypywaniu

- **Mechanizm:** pył grubszy osiada na jezdniach i jest podrywany przez ruch, gdy nawierzchnia jest **sucha**. Deszcz
  go zmywa. Po zimie na ulicach zostaje piasek i sól z posypywania — w suche, wietrzne dni marca–kwietnia PM10 rośnie,
  choć PM2.5 nie.
- **Cechy:** liczba dni od ostatniego opadu > 1 mm, suma opadu z ostatnich 3–7 dni, wskaźnik „przedwiośnie” (luty–
  kwiecień) w połączeniu z suchością.
- **Skąd:** z danych, które **już mamy** (archiwum pogody Open-Meteo) — zero nowych źródeł, najtańsza zmiana.
- **Pułapka:** brak — opady z przeszłości są znane w chwili prognozy.

### 3.3 Inwersja temperatury i warstwa mieszania

- **Mechanizm:** w bezwietrzne, mroźne noce przy ziemi tworzy się warstwa zimnego powietrza przykryta cieplejszą
  („pokrywka”) — zanieczyszczenia nie mają dokąd ulecieć. Im niższa warstwa mieszania, tym wyższe stężenia przy tej samej
  emisji. Dziś model widzi to tylko pośrednio (temperatura, godziny ciszy).
- **Cechy:** różnica temperatury przy ziemi i na wysokości ~1,5 km (poziom 850 hPa) — dodatnia = inwersja;
  wysokość warstwy mieszania, jeśli dostępna.
- **Skąd:** Open-Meteo (`temperature_850hPa`). Wysokość warstwy mieszania (`boundary_layer_height`) jest **pusta** w
  archiwum prognoz, którego używamy (sprawdzone w E3) — dlatego inwersja z temperatur.

### 3.4 Kalendarz zdarzeń

- **Mechanizm:** dni o nietypowej emisji: **Nowy Rok** (fajerwerki — skok PM10 i PM2.5 w nocy 31.12/1.01),
  **Wszystkich Świętych** (ruch i znicze), święta i długie weekendy (inny ruch), wiosenne wypalanie traw.
- **Cechy:** flagi świąt i dni szczególnych.
- **Skąd:** stała lista dat — bez zewnętrznego API.

### 3.5 Charakter stacji

- **Mechanizm:** przekroczenie w mieście = najgorsza stacja (decyzja D4). Stacje komunikacyjne (przy ulicach) mają
  inną dynamikę PM10 niż tła miejskiego.
- **Cechy:** wczorajsze maksimum osobno dla stacji komunikacyjnych i tła; liczba stacji, które wczoraj przekroczyły
  próg.
- **Skąd:** typ stacji jest w metadanych GIOŚ, które już mamy (`bronze.gios_stations`).

## 4. Tańsze poprawki — bez nowych cech

| Poprawka | Co daje | Koszt |
|---|---|---|
| **Kalibracja po fakcie** (np. izotoniczna, dopasowana na roku walidacyjnym 2024) | „prostuje” prawdopodobieństwa, żeby „80%” znaczyło ~80% — bezpośrednio na K3 | mały: jeden krok po modelu |
| **Próg ostrzeżenia dla PM10 dobrany na walidacji** (np. najniższy próg, przy którym fałszywe alarmy ≤ 50%) | więcej wykrytych dni kosztem części fałszywych alarmów — dla mieszkańca lepiej ostrzec przy 35% niż przegapić epizod | bardzo mały: parametr w konfiguracji |

Uwaga do drugiej pozycji: kryterium K4 zostało zamrożone z progiem 0,5 — **werdykt K4 liczymy dalej przy 0,5**.
Inny próg może być ustawieniem dla użytkowników panelu, raportowanym obok, nie zamiast werdyktu.

## 5. Jak uczciwie sprawdzić, czy ulepszenie działa

Rok testowy 2025 został **już użyty** do oceny. Gdybyśmy teraz zmieniali model i sprawdzali go znowu na 2025, wybieralibyśmy
wersję „pod znane odpowiedzi” — jak poprawianie pracy po zobaczeniu klucza. Wynik wyglądałby lepiej, ale nie mówiłby nic
o tym, jak model zadziała jutro. Dlatego:

1. ulepszenia (nowe cechy, kalibrację, próg) wybieramy **tylko na roku walidacyjnym 2024**,
2. oceniamy je **raz**, na nowym, niewidzianym okresie: **styczeń–wrzesień 2026** — pomiary GIOŚ z API
   (`archivalData`) i prawdziwe prognozy pogody „na dzień wcześniej” są dostępne,
3. raportujemy oba wyniki obok siebie: 2025 (stary model) i 2026 (stary vs nowy) — bez przepisywania historii.

To nie „nowe dane” w sensie nowych cech, tylko **świeższe pomiary tego samego rodzaju** — jedyny sposób, żeby ocena
była niezależna od decyzji, które już podjęliśmy, patrząc na 2025.

## 6. Punkty do wypowiedzi na demo

- Model PM10 **pokonuje obie proste reguły** (kalendarz i „jutro jak dziś”) i bardzo dobrze porządkuje dni
  (AUC 0,95) — ale **nie spełnia kryterium wykrywalności, które sami ustaliliśmy przed treningiem** (wykrywa 44% dni
  z przekroczeniem, wymagaliśmy 60%), a kalibrację spełnia na styk. Raportujemy to wprost, zamiast zmieniać kryteria
  po fakcie.
- Wyniki są **powtarzalne**: te same liczby na laptopie i na Databricks — po drodze znaleźliśmy i naprawiliśmy
  czułość drzew decyzyjnych na szum obliczeń, która wcześniej zmieniała wynik między środowiskami.
- Przyczyna jest **fizyczna, nie algorytmiczna**: PM10 to w dużej części pył z ulic, budów i napływu z daleka, a model
  zna tylko pogodę i wczorajsze stężenia. Zmiana algorytmu (GBT, regresja logistyczna — sprawdziliśmy oba) tego nie
  naprawi; trzeba dać modelowi informację o brakujących przyczynach.
- Najbardziej obiecujące: **prognoza CAMS** jako cecha (model fizyczny + korekta ML), **suchość nawierzchni**
  (dni bez deszczu — dane już mamy) i **inwersja temperatury**.
- Najtańsze od zaraz: **kalibracja** i **próg ostrzeżenia dobrany na walidacji**.
- Każde ulepszenie sprawdzimy na **nowym okresie 2026**, nie na roku, który już widzieliśmy — inaczej poprawa byłaby
  złudzeniem.
