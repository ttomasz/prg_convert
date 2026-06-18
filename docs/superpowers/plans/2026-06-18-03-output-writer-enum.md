# `OutputWriter` Enum Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the awkward `Writer { csv: Option<…>, geoparquet: Option<…> }` struct (in `lib.rs`) plus the parallel `Option<GeoParquetRecordBatchEncoder>` with a single `enum OutputWriter` defined in `main.rs`, removing every `.as_mut().unwrap()` and the free `write_batch` function.

**Architecture:** The CSV and GeoParquet writers are mutually exclusive, so model them as one enum with two variants and two methods: `write_batch(&RecordBatch)` and `finish(self)`. The enum is a binary/CLI concern, so it lives in `main.rs`, and the `Writer` struct is deleted from the library. The four `parser.for_each(...)` blocks in `parse_file` become `for batch in parser { ... }` loops so errors propagate with `?`.

**Tech Stack:** Rust 2024, `arrow` CSV writer, `parquet` ArrowWriter, `geoparquet` encoder, `anyhow`.

## Global Constraints

- Rust edition 2024; `cargo fmt --all --check` must pass.
- Output bytes must be byte-for-byte equivalent to before (same CSV header, same GeoParquet metadata). The `tests/e2e.rs` suite (which reads the produced CSV and Parquet) is the regression guard — it must keep passing.
- This plan does not change any feature flags or dependencies; it only relocates code. (Removing `Writer` from `lib.rs` makes the later feature-gating plan #7 cleaner, but no Cargo.toml change happens here.)

---

### Task 1: Define `OutputWriter` and delete the `Writer` struct

**Files:**
- Modify: `src/main.rs` (add the enum + impl; update imports)
- Modify: `src/lib.rs:89-92` (delete `pub struct Writer { … }`)

**Interfaces:**
- Produces: `enum OutputWriter` in `main.rs` with:
  - `fn write_batch(&mut self, batch: &arrow::array::RecordBatch) -> anyhow::Result<()>`
  - `fn finish(self) -> anyhow::Result<()>`

- [ ] **Step 1: Delete the `Writer` struct from the library**

In `src/lib.rs`, remove these lines (around line 89):

```rust
pub struct Writer {
    pub csv: Option<arrow::csv::writer::Writer<std::fs::File>>,
    pub geoparquet: Option<parquet::arrow::arrow_writer::ArrowWriter<std::fs::File>>,
}
```

- [ ] **Step 2: Add the `OutputWriter` enum to `main.rs`**

In `src/main.rs`, add near the top (after the `use` block, before `fn parse_file`):

```rust
enum OutputWriter {
    Csv(arrow::csv::writer::Writer<std::fs::File>),
    GeoParquet {
        writer: ArrowWriter<std::fs::File>,
        encoder: GeoParquetRecordBatchEncoder,
    },
}

impl OutputWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> anyhow::Result<()> {
        match self {
            OutputWriter::Csv(w) => {
                w.write(batch).context("Failed to write CSV batch.")?;
            }
            OutputWriter::GeoParquet { writer, encoder } => {
                let encoded = encoder
                    .encode_record_batch(batch)
                    .context("Failed to encode GeoParquet batch.")?;
                writer
                    .write(&encoded)
                    .context("Failed to write GeoParquet batch.")?;
            }
        }
        Ok(())
    }

    fn finish(self) -> anyhow::Result<()> {
        match self {
            OutputWriter::Csv(_) => Ok(()),
            OutputWriter::GeoParquet {
                mut writer,
                encoder,
            } => {
                let kv_metadata = encoder
                    .into_keyvalue()
                    .context("Could not create GeoParquet K/V metadata.")?;
                writer.append_key_value_metadata(kv_metadata);
                writer.finish().context("Failed to write GeoParquet metadata.")?;
                Ok(())
            }
        }
    }
}
```

- [ ] **Step 3: Fix imports in `main.rs`**

In the `use prg_convert::{...}` block (around line 10), **remove** `Writer,` from the import list (it no longer exists).

Confirm these are imported at the top of `main.rs` (they already are, from the existing code): `arrow::array::RecordBatch`, `geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions}`, `parquet::arrow::arrow_writer::ArrowWriter`, and `anyhow::{Context, Result}`. `Context` is needed for the `.context(...)` calls above — it is already imported (`use anyhow::{Context, Result};`).

- [ ] **Step 4: Delete the free `write_batch` function**

Remove the entire `fn write_batch(...)` function (lines ~19-48 in `main.rs`). It is replaced by `OutputWriter::write_batch`.

- [ ] **Step 5: Build (expect errors in `parse_file`/`main` — fixed in Task 2)**

Run: `cargo build`
Expected: FAIL with errors about `Writer`, `write_batch`, and `geoparquet_encoder` still being referenced in `parse_file` and `main`. That is fine; Task 2 fixes them. (You may also do Tasks 1 and 2 as a single edit session; they are split for review clarity.)

---

### Task 2: Rewire `parse_file` and `main` to use `OutputWriter`

**Files:**
- Modify: `src/main.rs` (`parse_file` signature + bodies; `main` construction + finalisation)

**Interfaces:**
- Consumes: `OutputWriter` from Task 1.

- [ ] **Step 1: Change `parse_file`'s signature**

Replace the `parse_file` signature:

```rust
fn parse_file(
    file_type: &FileType,
    parsed_args: &cli::ParsedArgs,
    file_path: &PathBuf,
    mut writer: &mut Writer,
    mut geoparquet_encoder: &mut Option<GeoParquetRecordBatchEncoder>,
    zip_file_index: &Option<usize>,
    teryt_mapping: &Option<HashMap<String, Terc>>,
) -> anyhow::Result<usize> {
```

with:

```rust
fn parse_file(
    file_type: &FileType,
    parsed_args: &cli::ParsedArgs,
    file_path: &PathBuf,
    output_writer: &mut OutputWriter,
    zip_file_index: &Option<usize>,
    teryt_mapping: &Option<HashMap<String, Terc>>,
) -> anyhow::Result<usize> {
```

- [ ] **Step 2: Convert each of the four `for_each` blocks to a `for` loop**

There are four match arms in `parse_file`, one per `(FileType, SchemaVersion)`. Each currently looks like:

```rust
            get_address_parser_2012_uncompressed(/* args */)?
            .for_each(|batch| {
                processed_rows += batch.num_rows();
                println!("Read batch of {} addresses.", batch.num_rows());
                write_batch(
                    &parsed_args.output_format,
                    &mut writer,
                    &mut geoparquet_encoder,
                    batch,
                );
            });
```

Rewrite each arm's body to a `for` loop (keeping that arm's specific `get_address_parser_*` call and its arguments exactly as-is):

```rust
            for batch in get_address_parser_2012_uncompressed(/* same args */)? {
                processed_rows += batch.num_rows();
                println!("Read batch of {} addresses.", batch.num_rows());
                output_writer.write_batch(&batch)?;
            }
```

Apply the same transformation to all four arms:
- `(FileType::XML, SchemaVersion::Model2012)` → `get_address_parser_2012_uncompressed(...)`
- `(FileType::ZIP, SchemaVersion::Model2012)` → `get_address_parser_2012_zip(&mut archive, ...)`
- `(FileType::XML, SchemaVersion::Model2021)` → `get_address_parser_2021_uncompressed(...)`
- `(FileType::ZIP, SchemaVersion::Model2021)` → `get_address_parser_2021_zip(&mut archive, ...)`

Leave the ZIP arms' `let f = ...; let mut archive = ZipArchive::new(f)...?;` setup lines unchanged; only the `for_each` becomes a `for` loop.

- [ ] **Step 3: Replace writer construction in `main`**

In `fn main`, replace this block (around lines 195-225):

```rust
    let (mut writer, mut geoparquet_encoder) = match &parsed_args.output_format {
        OutputFormat::CSV => (
            Writer {
                csv: Some(WriterBuilder::new().with_header(true).build(output_file)),
                geoparquet: None,
            },
            None,
        ),
        OutputFormat::GeoParquet => {
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(parsed_args.parquet_row_group_size))
                .set_writer_version(parsed_args.parquet_version)
                .set_compression(parsed_args.parquet_compression)
                .build();
            let gpq_encoder = GeoParquetRecordBatchEncoder::try_new(
                &parsed_args.arrow_schema,
                &GeoParquetWriterOptions::default(),
            )
            .expect("Could not create GeoParquet encoder.");
            (
                Writer {
                    csv: None,
                    geoparquet: Some(
                        ArrowWriter::try_new(output_file, gpq_encoder.target_schema(), Some(props))
                            .expect("Could not create GeoParquet writer."),
                    ),
                },
                Some(gpq_encoder),
            )
        }
    };
```

with:

```rust
    let mut output_writer = match &parsed_args.output_format {
        OutputFormat::CSV => {
            OutputWriter::Csv(WriterBuilder::new().with_header(true).build(output_file))
        }
        OutputFormat::GeoParquet => {
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(parsed_args.parquet_row_group_size))
                .set_writer_version(parsed_args.parquet_version)
                .set_compression(parsed_args.parquet_compression)
                .build();
            let encoder = GeoParquetRecordBatchEncoder::try_new(
                &parsed_args.arrow_schema,
                &GeoParquetWriterOptions::default(),
            )
            .expect("Could not create GeoParquet encoder.");
            let writer =
                ArrowWriter::try_new(output_file, encoder.target_schema(), Some(props))
                    .expect("Could not create GeoParquet writer.");
            OutputWriter::GeoParquet { writer, encoder }
        }
    };
```

- [ ] **Step 4: Update the two `parse_file` call sites in `main`**

In the `FileType::XML` branch, change:

```rust
                let processed_rows = parse_file(
                    &file.file_type,
                    &parsed_args,
                    &file.path,
                    &mut writer,
                    &mut geoparquet_encoder,
                    &None,
                    &teryt_mapping,
                )?;
```
to:
```rust
                let processed_rows = parse_file(
                    &file.file_type,
                    &parsed_args,
                    &file.path,
                    &mut output_writer,
                    &None,
                    &teryt_mapping,
                )?;
```

In the `FileType::ZIP` branch, change the analogous call from `&mut writer, &mut geoparquet_encoder,` to `&mut output_writer,` (keeping `&Some(compressed_file.index)` and `&teryt_mapping`).

- [ ] **Step 5: Replace the GeoParquet finalisation block in `main`**

Replace this block near the end of `main` (around lines 288-298):

```rust
    if matches!(parsed_args.output_format, OutputFormat::GeoParquet) {
        let kv_metadata = geoparquet_encoder
            .unwrap()
            .into_keyvalue()
            .expect("Could not create GeoParquet K/V metadata.");
        let parquet_writer = writer.geoparquet.as_mut().unwrap();
        parquet_writer.append_key_value_metadata(kv_metadata);
        parquet_writer
            .finish()
            .expect("Failed to write geoparquet metadata.");
    }
```

with:

```rust
    output_writer.finish()?;
```

- [ ] **Step 6: Build**

Run: `cargo build`
Expected: success, no warnings about unused `Writer`/`geoparquet_encoder`.

- [ ] **Step 7: Run the end-to-end suite (regression guard)**

Run: `cargo test --test e2e`
Expected: PASS — both the CSV and GeoParquet outputs are produced and validated exactly as before.

Also run the library tests to be safe:
Run: `cargo test --lib`
Expected: PASS.

- [ ] **Step 8: Format and commit**

```bash
cargo fmt --all
git add src/main.rs src/lib.rs
git commit -m "refactor: replace Writer struct with OutputWriter enum"
```

---

## Self-review checklist

- [ ] `pub struct Writer` is gone from `lib.rs`; `Writer` removed from `main.rs`'s import list.
- [ ] `OutputWriter` has exactly two variants and the two methods; no `.as_mut().unwrap()` remains in `main.rs`.
- [ ] The free `write_batch` function is deleted; all four parse arms use `for batch in … { output_writer.write_batch(&batch)?; }`.
- [ ] `parse_file` takes `&mut OutputWriter` (no `geoparquet_encoder` param).
- [ ] `main` calls `output_writer.finish()?` once after the file loop.
- [ ] `cargo test --test e2e`, `cargo test --lib`, `cargo fmt --all --check` all pass.
