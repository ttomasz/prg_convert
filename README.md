# prg_convert

_English: tool to convert XML/GML files with addresses provided by Polish government._

Narzędzie do konwersji plików XML/GML z adresami z Państwowego Rejestru Granic ([paczka zbiorcza](https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml)).

## Status implementacji
- ✅ Parsowanie XML modelu 2012
- ✅ Parsowanie XML modelu 2021
- ✅ Zapis do CSV
- ✅ Zapis do GeoParquet
- Odczyt bezpośrednio z pliku ZIP
- Pasek stanu postępu
- Optymalizacja ustawień formatu parquet (encodings, bloom filters, etc)
- Wielowątkowość

## Obsługa
Pobierz plik zip (Windows) lub tar.gz (Linuks) z zakładki [Releases](https://github.com/ttomasz/prg_convert/releases) i rozpakuj go.

Uruchom plik w terminalu/wierszu poleceń wraz z odpowiednimi flagami wskazującymi lokalizację plików wejściowych i wyjściowych. Możesz użyć flagi `--help` żeby zobaczyć dostępne opcje.

Przykład:
```ps
%HOMEPATH%\Downloads\prg_convert.exe --help
```

```ps
./prg_convert.exe --schema-version 2012 --input-paths ./*.xml --output-format csv --output-path ./adresy.csv
```

Flaga `--schema-version` określa czy plik jest w poprzednim formacie (wtedy wartość: `2012`) czy [w nowym](https://www.geoportal.gov.pl/aktualnosci/dane-adresowe-dostepne-do-pobrania-w-nowej-strukturze/) (wtedy wartość: `2021`). Paczka zbiorcza zip zawiera pliki w obu formatach. Obecnie (listopad 2025) stare pliki mają rozszerzenie: `.xml`, a nowe: `.gml` i prefix w nazwie: `NOWE_`.

**Uwaga:** Dla `--schema-version 2021` trzeba podać także parametr `--teryt-path` ze ścieżką do pliku xml pobranego ze strony [eTERYT GUSu](https://eteryt.stat.gov.pl/eTeryt/rejestr_teryt/udostepnianie_danych/baza_teryt/uzytkownicy_indywidualni/pobieranie/pliki_pelne.aspx?contrast=default) (TERC, podstawowa). W nowym modelu PRG nie ma informacji o nazwach jednostek administracyjnych dlatego potrzebny jest ten dodatkowy plik żeby je dodać.
