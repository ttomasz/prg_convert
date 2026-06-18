# Decouple Output Format from the Parser Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the XML parsers emit a single canonical Arrow `RecordBatch` (the 24-column `SCHEMA_CSV`) regardless of output format. Move geometry construction, CRS selection, and the GeoParquet column projection out of the parser and into the GeoParquet writer. The parser stops depending on `OutputFormat`, `CRS`, `geoarrow`, and `geo-types`.

**Architecture:** Today `AddressParser2012`/`AddressParser2021` branch on `OutputFormat` to decide whether to fill `x/y_epsg_2180` columns (CSV) or a geoarrow geometry column (GeoParquet). After this change the parser **always** fills the four float columns (`x_epsg_2180`, `y_epsg_2180`, `dlugosc_geograficzna`, `szerokosc_geograficzna`) and never builds geometry. The GeoParquet `OutputWriter` variant transforms each canonical batch into the GeoParquet schema (dropping `x/y_epsg_2180`, building `geometry` from the chosen CRS's coordinate columns). This gives library consumers one stable schema and removes the geo dependencies from the parser — which plan #7 then gates behind features.

**Tech Stack:** Rust 2024, `arrow` builders, `geoarrow`/`geo-types` (now only in the binary), `geoparquet` encoder.

## Global Constraints

- Rust edition 2024; `cargo fmt --all --check` must pass.
- **Depends on plan #3** (`OutputWriter` enum exists in `main.rs`). Do plan #3 first.
- **Output must be byte-identical** to before. The GeoParquet geometry is built from the *same* `f64` coordinate values the parser already computed, so values do not change. `tests/e2e.rs` (reads the produced CSV and Parquet) is the regression guard.
- The canonical schema produced by both parsers is exactly `crate::common::SCHEMA_CSV` (24 columns, in the order defined there).
- 2012 parses coordinates as `CoordOrder::YX`; 2021 parses as `CoordOrder::XY`. Preserve each.

### Canonical column order (for reference)

`SCHEMA_CSV` order, index → name: 0 `przestrzen_nazw`, 1 `lokalny_id`, 2 `wersja_id`, 3 `poczatek_wersji_obiektu`, 4 `wazny_od_lub_data_nadania`, 5 `wazny_do`, 6 `teryt_wojewodztwo`, 7 `wojewodztwo`, 8 `teryt_powiat`, 9 `powiat`, 10 `teryt_gmina`, 11 `gmina`, 12 `teryt_miejscowosc`, 13 `miejscowosc`, 14 `czesc_miejscowosci`, 15 `teryt_ulica`, 16 `ulica`, 17 `numer_porzadkowy`, 18 `kod_pocztowy`, 19 `status`, 20 `x_epsg_2180`, 21 `y_epsg_2180`, 22 `dlugosc_geograficzna`, 23 `szerokosc_geograficzna`.

The GeoParquet schema (`common::get_geoparquet_schema`) is columns 0–19 + 22 + 23 + a trailing `geometry` field (23 columns total).

---

### Task 1: Simplify `AddressParser2012` to always emit the canonical batch

**Files:**
- Modify: `src/model2012.rs` (imports, struct, `new`, `build_record_batch`, the `gml:pos` arm, the null-padding tail)

**Interfaces:**
- Produces (changed): `AddressParser2012::new(reader, batch_size: usize, additional_info: HashMap<String, AdditionalInfo>) -> Self`. Its `Iterator::Item` stays `arrow::array::RecordBatch`, now always shaped like `SCHEMA_CSV`.

- [ ] **Step 1: Remove the now-unused imports**

In `src/model2012.rs`, delete these `use` lines:

```rust
use arrow::datatypes::Schema;
use geo_types::Point;
use geoarrow::array::GeoArrowArray;
use geoarrow::array::PointBuilder;
use geoarrow::datatypes::PointType;

use crate::CRS;
use crate::OutputFormat;
```

Keep `use crate::CoordOrder;` and the `crate::common::*` imports — they are still used.

- [ ] **Step 2: Remove output/geometry fields from the struct**

In `pub struct AddressParser2012<R: BufRead>`, delete these five fields:

```rust
    output_format: OutputFormat,
    crs: crate::CRS,
    geoarrow_geom_type: PointType,
    arrow_schema: Arc<Schema>,
    ...
    geometry: Vec<Option<Point>>,
```

(`geometry` is the last field; the others are near the top. Keep every builder field.)

- [ ] **Step 3: Simplify `new`**

Replace the `new` signature and body. New signature:

```rust
    pub fn new(
        reader: Reader<R>,
        batch_size: usize,
        additional_info: HashMap<String, AdditionalInfo>,
    ) -> Self {
        Self {
            reader,
            batch_size,
            additional_info,
            id_namespace: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            uuid: StringBuilder::with_capacity(batch_size, 36 * batch_size),
            version: TimestampMillisecondBuilder::with_capacity(batch_size)
                .with_timezone(Arc::from("UTC")),
            lifecycle_start_date: TimestampMillisecondBuilder::with_capacity(batch_size)
                .with_timezone(Arc::from("UTC")),
            valid_since_date: Date32Builder::with_capacity(batch_size),
            valid_to_date: Date32Builder::with_capacity(batch_size),
            voivodeship: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            county: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            municipality: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            city: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            city_part: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            street: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            house_number: StringBuilder::with_capacity(batch_size, 6 * batch_size),
            postcode: StringBuilder::with_capacity(batch_size, 6 * batch_size),
            status: StringBuilder::with_capacity(batch_size, 10 * batch_size),
            x_epsg_2180: Float64Builder::with_capacity(batch_size),
            y_epsg_2180: Float64Builder::with_capacity(batch_size),
            longitude: Float64Builder::with_capacity(batch_size),
            latitude: Float64Builder::with_capacity(batch_size),
            voivodeship_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            county_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            municipality_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            city_teryt_id: StringBuilder::with_capacity(batch_size, 62 * batch_size),
            street_teryt_id: StringBuilder::with_capacity(batch_size, 91 * batch_size),
        }
    }
```

(The previous body wrote `batch_size: batch_size,` etc.; the shorthand above is equivalent. The `geometry: Vec::with_capacity(...)` line and the four removed params/assignments are gone.)

- [ ] **Step 4: Replace `build_record_batch` with a single canonical builder**

Replace the entire `fn build_record_batch(&mut self) -> RecordBatch { … }` with:

```rust
    fn build_record_batch(&mut self) -> RecordBatch {
        RecordBatch::try_new(
            crate::common::SCHEMA_CSV.clone(),
            vec![
                Arc::new(self.id_namespace.finish()),
                Arc::new(self.uuid.finish()),
                Arc::new(self.version.finish()),
                Arc::new(self.lifecycle_start_date.finish()),
                Arc::new(self.valid_since_date.finish()),
                Arc::new(self.valid_to_date.finish()),
                Arc::new(self.voivodeship_teryt_id.finish()),
                Arc::new(self.voivodeship.finish()),
                Arc::new(self.county_teryt_id.finish()),
                Arc::new(self.county.finish()),
                Arc::new(self.municipality_teryt_id.finish()),
                Arc::new(self.municipality.finish()),
                Arc::new(self.city_teryt_id.finish()),
                Arc::new(self.city.finish()),
                Arc::new(self.city_part.finish()),
                Arc::new(self.street_teryt_id.finish()),
                Arc::new(self.street.finish()),
                Arc::new(self.house_number.finish()),
                Arc::new(self.postcode.finish()),
                Arc::new(self.status.finish()),
                Arc::new(self.x_epsg_2180.finish()),
                Arc::new(self.y_epsg_2180.finish()),
                Arc::new(self.longitude.finish()),
                Arc::new(self.latitude.finish()),
            ],
        )
        .expect("Failed to create RecordBatch")
    }
```

- [ ] **Step 5: Replace the `gml:pos` text arm (always fill all four float columns)**

In `parse_address`, replace the `b"gml:pos" => { … }` arm with:

```rust
                        b"gml:pos" => {
                            let coords = parse_gml_pos(text_trimmed, CoordOrder::YX)
                                .expect("Could not parse coordinates.");
                            match coords {
                                None => {
                                    self.longitude.append_null();
                                    self.latitude.append_null();
                                    self.x_epsg_2180.append_null();
                                    self.y_epsg_2180.append_null();
                                }
                                Some(coords) => {
                                    self.longitude.append_value(coords.x4326);
                                    self.latitude.append_value(coords.y4326);
                                    self.x_epsg_2180.append_value(coords.x2180);
                                    self.y_epsg_2180.append_value(coords.y2180);
                                }
                            }
                        }
```

(Note `CoordOrder::YX` — this is the 2012 parser.)

- [ ] **Step 6: Replace the null-padding tail for coordinates**

In the `Ok(Event::End(ref e)) if e.name().as_ref() == ADDRESS_TAG` block, find the trailing coordinate padding, currently:

```rust
                    match self.output_format {
                        OutputFormat::CSV => {
                            if self.x_epsg_2180.len() < buffer_length {
                                self.x_epsg_2180.append_null();
                            }
                            if self.y_epsg_2180.len() < buffer_length {
                                self.y_epsg_2180.append_null();
                            }
                        }
                        OutputFormat::GeoParquet => {
                            if self.geometry.len() < buffer_length {
                                self.geometry.push(None);
                            }
                        }
                    }
```

Replace it with:

```rust
                    if self.x_epsg_2180.len() < buffer_length {
                        self.x_epsg_2180.append_null();
                    }
                    if self.y_epsg_2180.len() < buffer_length {
                        self.y_epsg_2180.append_null();
                    }
```

(The `longitude`/`latitude` padding lines just above this block are unconditional already — leave them.)

- [ ] **Step 7: Build the library only (callers fixed in later tasks)**

Run: `cargo build --lib`
Expected: FAIL — `src/lib.rs` still calls `AddressParser2012::new(...)` with the old arguments. That is fixed in Task 3. (`model2012.rs` itself should now be free of `OutputFormat`/`geo` references; if the compiler still flags an unused import in this file, remove it.)

---

### Task 2: Apply the identical simplification to `AddressParser2021`

**Files:**
- Modify: `src/model2021.rs` (imports, struct, `new`, `build_record_batch`, `gml:pos` arm, null-padding tail)

`model2021.rs` is a near-duplicate of `model2012.rs`. Apply the **same** edits as Task 1 with these differences:
- The struct **keeps** its `mappings: Mappings` and `teryt_names: HashMap<String, Terc>` fields.
- `new` **keeps** the `additional_info: Mappings` and `teryt_names: HashMap<String, Terc>` parameters.
- The `gml:pos` arm uses `CoordOrder::XY` (not `YX`).

- [ ] **Step 1: Remove the now-unused imports**

Delete from `src/model2021.rs`:

```rust
use arrow::datatypes::Schema;
use geo_types::Point;
use geoarrow::array::GeoArrowArray;
use geoarrow::array::PointBuilder;
use geoarrow::datatypes::PointType;

use crate::CRS;
use crate::OutputFormat;
```

Keep `use crate::CoordOrder;`, the `crate::common::*` imports, and `use crate::terc::Terc;`.

- [ ] **Step 2: Remove output/geometry fields from the struct**

Delete these fields from `pub struct AddressParser2021<R: BufRead>`:

```rust
    output_format: OutputFormat,
    crs: crate::CRS,
    geoarrow_geom_type: PointType,
    arrow_schema: Arc<Schema>,
    ...
    geometry: Vec<Option<Point>>,
```

Keep `mappings: Mappings,` and `teryt_names: HashMap<String, Terc>,`.

- [ ] **Step 3: Simplify `new`**

New signature and body (note it keeps `additional_info` and `teryt_names`):

```rust
    pub fn new(
        reader: Reader<R>,
        batch_size: usize,
        additional_info: Mappings,
        teryt_names: HashMap<String, Terc>,
    ) -> Self {
        Self {
            reader,
            batch_size,
            mappings: additional_info,
            teryt_names,
            id_namespace: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            uuid: StringBuilder::with_capacity(batch_size, 36 * batch_size),
            version: TimestampMillisecondBuilder::with_capacity(batch_size)
                .with_timezone(Arc::from("UTC")),
            lifecycle_start_date: TimestampMillisecondBuilder::with_capacity(batch_size)
                .with_timezone(Arc::from("UTC")),
            valid_since_date: Date32Builder::with_capacity(batch_size),
            valid_to_date: Date32Builder::with_capacity(batch_size),
            voivodeship: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            county: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            municipality: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            city: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            city_part: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            street: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            house_number: StringBuilder::with_capacity(batch_size, 6 * batch_size),
            postcode: StringBuilder::with_capacity(batch_size, 6 * batch_size),
            status: StringBuilder::with_capacity(batch_size, 10 * batch_size),
            x_epsg_2180: Float64Builder::with_capacity(batch_size),
            y_epsg_2180: Float64Builder::with_capacity(batch_size),
            longitude: Float64Builder::with_capacity(batch_size),
            latitude: Float64Builder::with_capacity(batch_size),
            voivodeship_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            county_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            municipality_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            city_teryt_id: StringBuilder::with_capacity(batch_size, 62 * batch_size),
            street_teryt_id: StringBuilder::with_capacity(batch_size, 91 * batch_size),
        }
    }
```

- [ ] **Step 4: Replace `build_record_batch`**

Use the **exact same** single-arm `build_record_batch` body shown in Task 1 Step 4 (the 24-column canonical builder over `crate::common::SCHEMA_CSV` — the builder field names are identical in both files).

- [ ] **Step 5: Replace the `gml:pos` arm — with `CoordOrder::XY`**

```rust
                        b"gml:pos" => {
                            let coords = parse_gml_pos(text_trimmed, CoordOrder::XY)
                                .expect("Could not parse coordinates.");
                            match coords {
                                None => {
                                    self.longitude.append_null();
                                    self.latitude.append_null();
                                    self.x_epsg_2180.append_null();
                                    self.y_epsg_2180.append_null();
                                }
                                Some(coords) => {
                                    self.longitude.append_value(coords.x4326);
                                    self.latitude.append_value(coords.y4326);
                                    self.x_epsg_2180.append_value(coords.x2180);
                                    self.y_epsg_2180.append_value(coords.y2180);
                                }
                            }
                        }
```

- [ ] **Step 6: Replace the null-padding tail**

Use the **exact same** replacement shown in Task 1 Step 6 (drop the `match self.output_format { … }`, keep two `if … append_null()` checks for `x_epsg_2180` and `y_epsg_2180`).

- [ ] **Step 7: Build the library only**

Run: `cargo build --lib`
Expected: still FAIL on `lib.rs` callers (fixed next). `model2021.rs` itself should be free of `OutputFormat`/`geo` references.

---

### Task 3: Slim the library entry points in `lib.rs`

**Files:**
- Modify: `src/lib.rs` (the four `get_address_parser_*` functions; imports)

**Interfaces:**
- Produces (changed signatures):
  - `get_address_parser_2012_uncompressed(file_path: &PathBuf, batch_size: &usize) -> anyhow::Result<AddressParser2012<BufReader<File>>>`
  - `get_address_parser_2012_zip(archive: &mut ZipArchive<File>, batch_size: &usize, zip_file_index: usize) -> anyhow::Result<AddressParser2012<BufReader<ZipFile<File>>>>`
  - `get_address_parser_2021_uncompressed(file_path: &PathBuf, batch_size: &usize, teryt_mapping: &HashMap<String, Terc>) -> anyhow::Result<AddressParser2021<BufReader<File>>>`
  - `get_address_parser_2021_zip(archive: &mut ZipArchive<File>, batch_size: &usize, teryt_mapping: &HashMap<String, Terc>, zip_file_index: usize) -> anyhow::Result<AddressParser2021<BufReader<ZipFile<File>>>>`

- [ ] **Step 1: Remove now-unused imports**

In `src/lib.rs`, delete:

```rust
use arrow::datatypes::Schema;
use geoarrow::datatypes::PointType;
```

(Keep `use std::sync::Arc;` — still used elsewhere. If after editing `Arc` is unused, remove it too; the compiler will tell you.)

- [ ] **Step 2: Rewrite `get_address_parser_2012_uncompressed`**

```rust
pub fn get_address_parser_2012_uncompressed(
    file_path: &PathBuf,
    batch_size: &usize,
) -> anyhow::Result<AddressParser2012<std::io::BufReader<File>>> {
    let reader = get_xml_reader_from_uncompressed_file(file_path)?;
    println!("Building dictionaries...");
    let dict = model2012::build_dictionaries(reader);
    let reader = get_xml_reader_from_uncompressed_file(file_path)?;
    Ok(AddressParser2012::new(reader, *batch_size, dict))
}
```

- [ ] **Step 3: Rewrite `get_address_parser_2012_zip`**

```rust
pub fn get_address_parser_2012_zip<'a>(
    archive: &'a mut ZipArchive<File>,
    batch_size: &usize,
    zip_file_index: usize,
) -> anyhow::Result<AddressParser2012<std::io::BufReader<ZipFile<'a, File>>>> {
    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true;
    println!("Building dictionaries...");
    let dict = model2012::build_dictionaries(reader);

    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true;

    Ok(AddressParser2012::new(reader, *batch_size, dict))
}
```

- [ ] **Step 4: Rewrite `get_address_parser_2021_uncompressed`**

```rust
pub fn get_address_parser_2021_uncompressed(
    file_path: &PathBuf,
    batch_size: &usize,
    teryt_mapping: &HashMap<String, Terc>,
) -> anyhow::Result<AddressParser2021<std::io::BufReader<File>>> {
    let reader = get_xml_reader_from_uncompressed_file(file_path)?;
    println!("Building dictionaries...");
    let dict = model2021::build_dictionaries(reader);
    let reader = get_xml_reader_from_uncompressed_file(file_path)?;
    Ok(AddressParser2021::new(
        reader,
        *batch_size,
        dict,
        teryt_mapping.clone(),
    ))
}
```

- [ ] **Step 5: Rewrite `get_address_parser_2021_zip`**

```rust
pub fn get_address_parser_2021_zip<'a>(
    archive: &'a mut ZipArchive<File>,
    batch_size: &usize,
    teryt_mapping: &HashMap<String, Terc>,
    zip_file_index: usize,
) -> anyhow::Result<AddressParser2021<std::io::BufReader<ZipFile<'a, File>>>> {
    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true;
    println!("Building dictionaries...");
    let dict = model2021::build_dictionaries(reader);

    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true;

    Ok(AddressParser2021::new(
        reader,
        *batch_size,
        dict,
        teryt_mapping.clone(),
    ))
}
```

- [ ] **Step 6: Build the library**

Run: `cargo build --lib`
Expected: the library compiles (the `#[cfg(test)] mod tests` in `lib.rs` is fixed in Task 4; if you run `cargo build --lib` it does not build tests, so this should pass now).

---

### Task 4: Update the `lib.rs` tests to the canonical-only API

**Files:**
- Modify: `src/lib.rs` `#[cfg(test)] mod tests`

- [ ] **Step 1: Remove geo-only test imports and helper**

In the test module, delete:
```rust
use geoarrow::datatypes::CoordType;
```
and delete the entire `fn make_geoparquet_geom_type() -> PointType { … }` helper. (This removes the last `geoarrow` usage from the library so plan #7 can gate `geoarrow` behind the `cli` feature.) Keep `use geoarrow::datatypes::PointType;`? No — also remove any remaining `geoarrow`/`PointType` references per the steps below.

- [ ] **Step 2: Fix the CSV / XML test calls (drop removed args)**

Apply these exact replacements:

In `test_address_parser_2012_zip_csv`, replace the `get_address_parser_2012_zip(...)` call's argument list:
```rust
        let parser = get_address_parser_2012_zip(
            &mut archive,
            &1,
            &OutputFormat::CSV,
            0,
            &CRS::Epsg4326,
            crate::common::SCHEMA_CSV.clone(),
            &PointType::new(
                geoarrow::datatypes::Dimension::XY,
                Arc::new(geoarrow::datatypes::Metadata::new(
                    geoarrow::datatypes::Crs::from_srid("4326".to_string()),
                    None,
                )),
            ),
        );
```
with:
```rust
        let parser = get_address_parser_2012_zip(&mut archive, &1, 0);
```

In `test_address_parser_2021_zip_csv`, replace the `get_address_parser_2021_zip(...)` call with:
```rust
        let parser = get_address_parser_2021_zip(&mut archive, &1, &teryt_mapping, 1);
```

In `test_address_parser_2012_xml_csv`, replace the call with:
```rust
        let parser = get_address_parser_2012_uncompressed(&file_path, &100_000);
```

In `test_address_parser_2021_xml_csv`, replace the call with:
```rust
        let parser =
            get_address_parser_2021_uncompressed(&file_path, &100_000, &teryt_mapping);
```

In `test_address_parser_2012_zip_csv_multi_batch`, replace the call with:
```rust
        let parser = get_address_parser_2012_zip(&mut archive, &1, 0);
```

(These tests already `concat_batches(&crate::common::SCHEMA_CSV.clone(), &batches)` and assert 24 columns — those assertions stay correct.)

- [ ] **Step 3: Convert the two GeoParquet parser tests to canonical-output tests**

The parser no longer produces a geometry column, so the two GeoParquet tests now assert the canonical batch. Replace `test_address_parser_2012_zip_geoparquet` entirely with:

```rust
    #[test]
    fn test_address_parser_2012_zip_canonical() {
        let sample_file_path = "fixtures/PRG-punkty_adresowe.zip";
        let f = std::fs::File::open(&sample_file_path)
            .expect(format!("Failed to open file: `{}`.", &sample_file_path).as_str());
        let mut archive = ZipArchive::new(f)
            .expect(format!("Failed to decompress ZIP file: `{}`.", &sample_file_path).as_str());
        let parser = get_address_parser_2012_zip(&mut archive, &100_000, 0);
        let batches: Vec<arrow::array::RecordBatch> = parser
            .expect("Something wrong while creating parser object.")
            .into_iter()
            .collect();
        let arrow_batch = concat_batches(&crate::common::SCHEMA_CSV.clone(), &batches)
            .expect("Error in concatenating batches");
        assert_eq!(arrow_batch.num_rows(), 2);
        assert_eq!(arrow_batch.num_columns(), 24);
        let x = arrow_batch
            .column_by_name("x_epsg_2180")
            .expect("Expected x_epsg_2180 column");
        assert_eq!(x.null_count(), 0);
    }
```

Replace `test_address_parser_2021_zip_geoparquet` entirely with:

```rust
    #[test]
    fn test_address_parser_2021_zip_canonical() {
        let sample_file_path = "fixtures/PRG-punkty_adresowe.zip";
        let teryt_file_path = "fixtures/TERC_Urzedowy_2025-11-18.zip";
        let teryt_mapping =
            get_teryt_mapping(false, &None, &None, &Some(PathBuf::from(teryt_file_path))).unwrap();
        let f = std::fs::File::open(&sample_file_path)
            .expect(format!("Failed to open file: `{}`.", &sample_file_path).as_str());
        let mut archive = ZipArchive::new(f)
            .expect(format!("Failed to decompress ZIP file: `{}`.", &sample_file_path).as_str());
        let parser = get_address_parser_2021_zip(&mut archive, &100_000, &teryt_mapping, 1);
        let batches: Vec<arrow::array::RecordBatch> = parser
            .expect("Something wrong while creating parser object.")
            .into_iter()
            .collect();
        let arrow_batch = concat_batches(&crate::common::SCHEMA_CSV.clone(), &batches)
            .expect("Error in concatenating batches");
        assert_eq!(arrow_batch.num_rows(), 3);
        assert_eq!(arrow_batch.num_columns(), 24);
        let x = arrow_batch
            .column_by_name("x_epsg_2180")
            .expect("Expected x_epsg_2180 column");
        assert_eq!(x.null_count(), 0);
    }
```

- [ ] **Step 4: Remove leftover unused imports**

At the top of the test module, ensure these are gone if now unused: `use geoarrow::datatypes::PointType;`, `use geoarrow::datatypes::CoordType;`. The remaining imports (`Date32Array`, `Float64Array`, `StringArray`, `TimestampMillisecondArray`, `concat_batches`) are still used.

- [ ] **Step 5: Run the library tests**

Run: `cargo test --lib`
Expected: PASS — all parser tests now exercise the canonical 24-column output.

- [ ] **Step 6: Commit the parser + library changes**

```bash
cargo fmt --all
git add src/model2012.rs src/model2021.rs src/lib.rs
git commit -m "refactor: parser emits canonical batch, output format decoupled"
```

---

### Task 5: Move geometry construction into the GeoParquet `OutputWriter`

**Files:**
- Modify: `src/main.rs` (extend the `OutputWriter::GeoParquet` variant from plan #3; add `canonical_to_geoparquet_batch`; update call sites and imports)

**Interfaces:**
- Consumes: `OutputWriter` from plan #3; `prg_convert::{CRS, common::get_geoparquet_schema}`; `parsed_args.crs`, `parsed_args.geoarrow_geom_type`, `parsed_args.arrow_schema`.

- [ ] **Step 1: Add imports to `main.rs`**

Add to the top of `src/main.rs`:

```rust
use std::sync::Arc;
use arrow::array::{ArrayRef, Float64Array};
use arrow::datatypes::Schema;
use geoarrow::array::{GeoArrowArray, PointBuilder};
use geoarrow::datatypes::PointType;
use prg_convert::CRS;
```

- [ ] **Step 2: Extend the `OutputWriter::GeoParquet` variant**

Change the variant definition (from plan #3) to carry the data needed to build geometry:

```rust
enum OutputWriter {
    Csv(arrow::csv::writer::Writer<std::fs::File>),
    GeoParquet {
        writer: ArrowWriter<std::fs::File>,
        encoder: GeoParquetRecordBatchEncoder,
        crs: CRS,
        geom_type: PointType,
        geoparquet_schema: Arc<Schema>,
    },
}
```

- [ ] **Step 3: Add the canonical→GeoParquet transform function**

Add this free function in `main.rs`:

```rust
/// Convert a canonical (SCHEMA_CSV-shaped) batch into a GeoParquet batch:
/// build a `geometry` point column from the coordinate columns selected by `crs`,
/// drop `x_epsg_2180`/`y_epsg_2180`, and reorder to match `geoparquet_schema`.
fn canonical_to_geoparquet_batch(
    batch: &RecordBatch,
    crs: &CRS,
    geom_type: &PointType,
    geoparquet_schema: &Arc<Schema>,
) -> anyhow::Result<RecordBatch> {
    let (x_name, y_name) = match crs {
        CRS::Epsg2180 => ("x_epsg_2180", "y_epsg_2180"),
        CRS::Epsg4326 => ("dlugosc_geograficzna", "szerokosc_geograficzna"),
    };
    let xs = batch
        .column_by_name(x_name)
        .context("canonical batch missing x column")?
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("x column is not Float64")?;
    let ys = batch
        .column_by_name(y_name)
        .context("canonical batch missing y column")?
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("y column is not Float64")?;
    let points: Vec<Option<geo_types::Point>> = (0..batch.num_rows())
        .map(|i| {
            if xs.is_null(i) || ys.is_null(i) {
                None
            } else {
                Some(geo_types::point!(x: xs.value(i), y: ys.value(i)))
            }
        })
        .collect();
    let geometry =
        PointBuilder::from_nullable_points(points.iter().map(Option::as_ref), geom_type.clone())
            .finish();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(geoparquet_schema.fields().len());
    for field in geoparquet_schema.fields() {
        if field.name() == "geometry" {
            columns.push(geometry.to_array_ref());
        } else {
            let col = batch
                .column_by_name(field.name())
                .with_context(|| format!("canonical batch missing column `{}`", field.name()))?;
            columns.push(col.clone());
        }
    }
    Ok(RecordBatch::try_new(geoparquet_schema.clone(), columns)?)
}
```

(Requires `geo_types` in `Cargo.toml` — it is already a dependency.)

- [ ] **Step 4: Use the transform in `OutputWriter::write_batch`**

Replace the `OutputWriter::GeoParquet { writer, encoder }` arm of `write_batch` with:

```rust
            OutputWriter::GeoParquet {
                writer,
                encoder,
                crs,
                geom_type,
                geoparquet_schema,
            } => {
                let geo_batch =
                    canonical_to_geoparquet_batch(batch, crs, geom_type, geoparquet_schema)?;
                let encoded = encoder
                    .encode_record_batch(&geo_batch)
                    .context("Failed to encode GeoParquet batch.")?;
                writer
                    .write(&encoded)
                    .context("Failed to write GeoParquet batch.")?;
            }
```

In `finish`, update the destructuring pattern to ignore the new fields:

```rust
            OutputWriter::GeoParquet {
                mut writer,
                encoder,
                ..
            } => {
```

- [ ] **Step 5: Populate the new fields when constructing the writer in `main`**

In `main`, the GeoParquet construction arm builds `OutputWriter::GeoParquet { writer, encoder }`. Change it to:

```rust
            OutputWriter::GeoParquet {
                writer,
                encoder,
                crs: parsed_args.crs.clone(),
                geom_type: parsed_args.geoarrow_geom_type.clone(),
                geoparquet_schema: parsed_args.arrow_schema.clone(),
            }
```

(`parsed_args.arrow_schema` is the GeoParquet schema for the GeoParquet output format — see `cli.rs` `try_from`, which sets it via `get_geoparquet_schema`.)

- [ ] **Step 6: Build**

Run: `cargo build`
Expected: success.

- [ ] **Step 7: Run the end-to-end regression suite**

Run: `cargo test --test e2e`
Expected: PASS — the GeoParquet file has the same geometry and columns as before (geometry built from identical coordinate values).

Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
cargo fmt --all
git add src/main.rs
git commit -m "feat: build GeoParquet geometry in the writer from canonical batches"
```

---

## Self-review checklist

- [ ] Both parser structs lost `output_format`, `crs`, `geoarrow_geom_type`, `arrow_schema`, `geometry`; `new` signatures slimmed; `geo_types`/`geoarrow`/`OutputFormat`/`CRS` imports removed from `model2012.rs` and `model2021.rs`.
- [ ] `build_record_batch` is a single arm producing `SCHEMA_CSV` in both files; `gml:pos` fills all four float columns (YX for 2012, XY for 2021); null-padding always pads `x/y_epsg_2180`.
- [ ] `lib.rs` `get_address_parser_*` signatures match the Interfaces block; library has no remaining `geoarrow` usage (including tests).
- [ ] `main.rs` builds geometry via `canonical_to_geoparquet_batch`; `OutputWriter::GeoParquet` carries `crs`/`geom_type`/`geoparquet_schema`.
- [ ] `cargo test --lib`, `cargo test --test e2e`, `cargo fmt --all --check` all pass; GeoParquet output unchanged.
