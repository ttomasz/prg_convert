#!/usr/bin/env bash
set -e

set -o allexport && source .env && set +o allexport

# cargo run --release -- --help

cargo run --release -- --schema-version 2012 --batch-size 300000 --output-format csv --crs-epsg 4326 --input-paths test_data/lubuskie_stare_2026-01-10.xml --output-path test_data/lubuskie_stare_2026-01-10_4326.csv
cargo run --release -- --schema-version 2012 --batch-size 300000 --output-format geoparquet --crs-epsg 4326 --input-paths test_data/lubuskie_stare_2026-01-10.xml --output-path test_data/lubuskie_stare_2026-01-10_4326.parquet
cargo run --release -- --schema-version 2012 --batch-size 300000 --output-format csv --crs-epsg 2180 --input-paths test_data/lubuskie_stare_2026-01-10.xml --output-path test_data/lubuskie_stare_2026-01-10_2180.csv
cargo run --release -- --schema-version 2012 --batch-size 300000 --output-format geoparquet --crs-epsg 2180 --input-paths test_data/lubuskie_stare_2026-01-10.xml --output-path test_data/lubuskie_stare_2026-01-10_2180.parquet
cargo run --release -- --schema-version 2012 --batch-size 300000 --output-format geoparquet --crs-epsg 4326 --input-paths test_data/PRG-punkty_adresowe_2026-01-10.zip --output-path test_data/prg_stare_2026-01-10_4326.parquet
cargo run --release -- --schema-version 2012 --batch-size 300000 --output-format geoparquet --crs-epsg 2180 --input-paths test_data/PRG-punkty_adresowe_2026-01-10.zip --output-path test_data/prg_stare_2026-01-10_2180.parquet

cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format csv --crs-epsg 4326 --input-paths test_data/lubuskie_nowe_2026-01-10.gml --output-path test_data/lubuskie_nowe_2026-01-10_4326.csv --teryt-path test_data/TERC_Urzedowy_2026-01-10.zip
cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format geoparquet --crs-epsg 4326 --input-paths test_data/lubuskie_nowe_2026-01-10.gml --output-path test_data/lubuskie_nowe_2026-01-10_4326.parquet --teryt-path test_data/TERC_Urzedowy_2026-01-10.zip
cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format csv --crs-epsg 2180 --input-paths test_data/lubuskie_nowe_2026-01-10.gml --output-path test_data/lubuskie_nowe_2026-01-10_2180.csv --teryt-path test_data/TERC_Urzedowy_2026-01-10.zip
cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format geoparquet --crs-epsg 2180 --input-paths test_data/lubuskie_nowe_2026-01-10.gml --output-path test_data/lubuskie_nowe_2026-01-10_2180.parquet --teryt-path test_data/TERC_Urzedowy_2026-01-10.zip
cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format geoparquet --crs-epsg 4326 --input-paths test_data/PRG-punkty_adresowe_2026-01-10.zip --output-path test_data/prg_stare_2026-01-10_4326.parquet --teryt-path test_data/TERC_Urzedowy_2026-01-10.zip
cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format geoparquet --crs-epsg 2180 --input-paths test_data/PRG-punkty_adresowe_2026-01-10.zip --output-path test_data/prg_stare_2026-01-10_2180.parquet --teryt-path test_data/TERC_Urzedowy_2026-01-10.zip

cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format csv --crs-epsg 4326 --input-paths test_data/lubuskie_nowe_2026-01-10.gml --output-path test_data/lubuskie_nowe_2026-01-10_4326.csv --download-teryt
cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format geoparquet --crs-epsg 4326 --input-paths test_data/lubuskie_nowe_2026-01-10.gml --output-path test_data/lubuskie_nowe_2026-01-10_4326.parquet --download-teryt
cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format csv --crs-epsg 2180 --input-paths test_data/lubuskie_nowe_2026-01-10.gml --output-path test_data/lubuskie_nowe_2026-01-10_2180.csv --download-teryt
cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format geoparquet --crs-epsg 2180 --input-paths test_data/lubuskie_nowe_2026-01-10.gml --output-path test_data/lubuskie_nowe_2026-01-10_2180.parquet --download-teryt
cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format geoparquet --crs-epsg 4326 --input-paths test_data/PRG-punkty_adresowe_2026-01-10.zip --output-path test_data/prg_stare_2026-01-10_4326.parquet --download-teryt
cargo run --release -- --schema-version 2021 --batch-size 300000 --output-format geoparquet --crs-epsg 2180 --input-paths test_data/PRG-punkty_adresowe_2026-01-10.zip --output-path test_data/prg_stare_2026-01-10_2180.parquet --download-teryt
