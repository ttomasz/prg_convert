# prg_convert

_English: tool to convert XML/GML files with addresses provided by Polish government._

Narzędzie do konwersji plików XML/GML z adresami z Państwowego Rejestru Granic ([paczka zbiorcza](https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml)).

## Status implementacji
- ✅ Parsowanie XML modelu 2012
- ✅ Parsowanie XML modelu 2021
  - do wersji 0.5.0 koordynaty są czytane w odwróconej kolejności, od wersji 0.6.0 jest poprawnie
  - do wersji 0.6.0 nazwy miejscowości mogą być niepoprawnie czytane, od wersji 0.6.1 jest poprawnie
- ✅ Zapis do CSV
- ✅ Zapis do GeoParquet
- ✅ Odczyt bezpośrednio z pliku ZIP
- Pasek stanu postępu
- Optymalizacja ustawień formatu parquet (encodings, bloom filters, etc)
- Wielowątkowość
- Python bindings
- ✅ Opcja zapisu GeoParquet w EPSG:4326
- Zapis do FlatGeoBuf
- Zapis do GeoJSON
- Pobieranie pliku ZIP z adresami z Geoportalu
- Pobieranie pliku ZIP ze słownikami TERYT z API GUS

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

Jeżeli jako plik wejściowy podasz ścieżkę do paczki ZIP to flag `--schema-version` będzie determinować, które pliki będą czytane (2012: te z rozszerzeniem .xml, 2021: te z rozszerzeniem .gml).

**Uwaga:** W nowym modelu PRG ( kiedy używamy `--schema-version 2021`) nie ma informacji o nazwach jednostek administracyjnych dlatego potrzebny jest dodatkowy plik żeby je dodać. Można albo pobrać go ze strony [eTERYT GUSu](https://eteryt.stat.gov.pl/eTeryt/rejestr_teryt/udostepnianie_danych/baza_teryt/uzytkownicy_indywidualni/pobieranie/pliki_pelne.aspx?contrast=default) (TERC, podstawowa), wtedy trzeba podać parametr `--teryt-path` ze ścieżką do pliku xml (od wersji 0.6.2 można podac ścieżkę po prostu do pobranego pliku zip, nie trzeba go rozpakowywać) pobranego. Jeżeli używamy wersji 0.6.3 lub nowszej to można też ustawić parametr `--download-teryt` i plik ten zostanie pobrany dynamicznie z oficjalnego API GUS. Trzeba wtedy jednak dostać od GUS dane do logowania (patrz [strona eTERYT API](https://api.stat.gov.pl/Home/TerytApi)) i albo ustawić je jako zmienne środowiskowe (TERYT_API_USERNAME, TERYT_API_PASSWORD), albo podać je w parametrach (`--teryt-api-username`, `--teryt-api-password`).
