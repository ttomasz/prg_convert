# clap `ValueEnum` Arguments Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace hand-rolled `String`/`to_lowercase()` validation for `--output-format`, `--schema-version`, `--parquet-compression`, and `--parquet-version` with clap `#[derive(ValueEnum)]` types. clap then validates these at parse time, auto-generates the "possible values" in `--help`, and the manual `match … { _ => anyhow::bail!(…) }` arms in `try_from` disappear.

**Architecture:** Define four small CLI-local enums in `cli.rs` that implement `clap::ValueEnum`, and change the corresponding `RawArgs` fields to use them. Map each to the existing library enums (`OutputFormat`, `SchemaVersion`) / parquet types inside `try_from`. The library enums stay clap-free (important for plan #7's feature gating — the library must not depend on clap).

**Tech Stack:** Rust 2024, `clap` 4 derive (`ValueEnum`), `parquet` basic types, `anyhow`.

## Global Constraints

- Rust edition 2024; `cargo fmt --all --check` must pass.
- **Depends on plan #5** (the `impl` is `TryFrom<RawArgs> for ParsedArgs` with receiver `value`). If plan #5 has not landed, do it first.
- Keep accepted values backwards-compatible and case-insensitive (the old code did `.to_lowercase()`), via `ignore_case = true`.
- `--crs-epsg` stays an `Option<i32>` (out of scope here).
- The library enums (`OutputFormat`, `SchemaVersion` in `lib.rs`) must **not** gain a `clap` dependency.

---

### Task 1: Define the arg enums, change `RawArgs`, and simplify `try_from`

**Files:**
- Modify: `src/cli.rs` (add enums; change 4 `RawArgs` fields; rewrite matching in `try_from`)

**Interfaces:**
- Produces (CLI-local enums): `OutputFormatArg { Csv, Geoparquet }`, `SchemaVersionArg { V2012, V2021 }`, `ParquetCompressionArg { Zstd, Snappy, Brotli }`, `ParquetVersionArg { V1, V2 }`.

- [ ] **Step 1: Define the four arg enums**

In `src/cli.rs`, just above `pub struct RawArgs` (after the `use` block), add:

```rust
#[derive(Clone, Copy, clap::ValueEnum)]
pub enum OutputFormatArg {
    Csv,
    Geoparquet,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum SchemaVersionArg {
    #[value(name = "2012")]
    V2012,
    #[value(name = "2021")]
    V2021,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum ParquetCompressionArg {
    Zstd,
    Snappy,
    Brotli,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum ParquetVersionArg {
    #[value(name = "v1")]
    V1,
    #[value(name = "v2")]
    V2,
}
```

(clap derives value names by lowercasing variant names: `Csv`→`csv`, `Geoparquet`→`geoparquet`, `Zstd`→`zstd`, etc. The numeric/version names need the explicit `#[value(name = …)]` because identifiers cannot start with a digit and we want `v1`/`v2`.)

- [ ] **Step 2: Change the four `RawArgs` fields**

In `RawArgs`, replace the four field declarations.

`output_format`:
```rust
    #[arg(long = "output-format", help = "Output file format (one of: csv, geoparquet).")]
    output_format: String,
```
→
```rust
    #[arg(
        long = "output-format",
        ignore_case = true,
        help = "Output file format."
    )]
    output_format: OutputFormatArg,
```

`schema_version`:
```rust
    #[arg(long = "schema-version", help = "Schema version (one of: 2012, 2021).")]
    schema_version: String,
```
→
```rust
    #[arg(long = "schema-version", help = "Schema version.")]
    schema_version: SchemaVersionArg,
```

`parquet_compression`:
```rust
    #[arg(
        long = "parquet-compression",
        help = "(Optional) What type of compression to use when writing parquet file (one of: zstd, snappy,) (default: zstd)."
    )]
    parquet_compression: Option<String>,
```
→
```rust
    #[arg(
        long = "parquet-compression",
        ignore_case = true,
        help = "(Optional) Compression to use when writing parquet file (default: zstd)."
    )]
    parquet_compression: Option<ParquetCompressionArg>,
```

`parquet_version`:
```rust
    #[arg(
        long = "parquet-version",
        help = "(Optional) Version of parquet standard to use (one of: v1, v2,) (default: v2)."
    )]
    parquet_version: Option<String>,
```
→
```rust
    #[arg(
        long = "parquet-version",
        ignore_case = true,
        help = "(Optional) Version of parquet standard to use (default: v2)."
    )]
    parquet_version: Option<ParquetVersionArg>,
```

- [ ] **Step 3: Rewrite the schema-version checks and conversion in `try_from`**

In `try_from`, the `download_teryt_flag` block currently has:
```rust
            if value.schema_version.to_lowercase() == "2012" && flag {
```
→
```rust
            if matches!(value.schema_version, SchemaVersionArg::V2012) && flag {
```

The missing-teryt guard currently:
```rust
        if value.schema_version.to_lowercase() == "2021"
            && value.teryt_path.is_none()
            && !download_teryt_flag
        {
```
→
```rust
        if matches!(value.schema_version, SchemaVersionArg::V2021)
            && value.teryt_path.is_none()
            && !download_teryt_flag
        {
```

The final schema match currently:
```rust
        let schema_version = match value.schema_version.to_lowercase().as_str() {
            "2012" => SchemaVersion::Model2012,
            "2021" => SchemaVersion::Model2021,
            _ => {
                anyhow::bail!(
                    "unsupported schema version `{}`, expected one of: 2012, 2021",
                    &value.schema_version
                );
            }
        };
```
→
```rust
        let schema_version = match value.schema_version {
            SchemaVersionArg::V2012 => SchemaVersion::Model2012,
            SchemaVersionArg::V2021 => SchemaVersion::Model2021,
        };
```

- [ ] **Step 4: Rewrite the output-format conversion in `try_from`**

Currently:
```rust
        let output_format = match value.output_format.to_lowercase().as_str() {
            "csv" => OutputFormat::CSV,
            "geoparquet" => OutputFormat::GeoParquet,
            _ => {
                anyhow::bail!(
                    "unsupported format `{}`, expected one of: csv, geoparquet",
                    &value.output_format
                );
            }
        };
```
→
```rust
        let output_format = match value.output_format {
            OutputFormatArg::Csv => OutputFormat::CSV,
            OutputFormatArg::Geoparquet => OutputFormat::GeoParquet,
        };
```

- [ ] **Step 5: Rewrite the parquet compression + level + version conversions**

Currently:
```rust
        let compression_level = match &value.parquet_compression.as_deref() {
            None | Some("zstd") => Some(value.compression_level.unwrap_or(11)),
            Some("brotli") => Some(value.compression_level.unwrap_or(6)),
            _ => None,
        };
        let parquet_compression = match &value.parquet_compression.as_deref() {
            None | Some("zstd") => {
                Compression::ZSTD(ZstdLevel::try_new(compression_level.unwrap())?)
            }
            Some("snappy") => Compression::SNAPPY,
            Some("brotli") => Compression::BROTLI(BrotliLevel::try_new(
                compression_level.unwrap().cast_unsigned(),
            )?),
            _ => {
                anyhow::bail!(
                    "Unexpected compression type for parquet writer: `{:?}`",
                    &value.parquet_compression
                )
            }
        };
        let parquet_row_group_size = value.parquet_row_group_size.unwrap_or(batch_size);
        let parquet_version = match &value.parquet_version.as_deref() {
            None | Some("v2") => WriterVersion::PARQUET_2_0,
            Some("v1") => WriterVersion::PARQUET_1_0,
            _ => {
                anyhow::bail!(
                    "Unexpected version for parquet writer: `{:?}`",
                    &value.parquet_version
                )
            }
        };
```
→
```rust
        let compression_level = match value.parquet_compression {
            None | Some(ParquetCompressionArg::Zstd) => Some(value.compression_level.unwrap_or(11)),
            Some(ParquetCompressionArg::Brotli) => Some(value.compression_level.unwrap_or(6)),
            Some(ParquetCompressionArg::Snappy) => None,
        };
        let parquet_compression = match value.parquet_compression {
            None | Some(ParquetCompressionArg::Zstd) => {
                Compression::ZSTD(ZstdLevel::try_new(compression_level.unwrap())?)
            }
            Some(ParquetCompressionArg::Snappy) => Compression::SNAPPY,
            Some(ParquetCompressionArg::Brotli) => Compression::BROTLI(BrotliLevel::try_new(
                compression_level.unwrap().cast_unsigned(),
            )?),
        };
        let parquet_row_group_size = value.parquet_row_group_size.unwrap_or(batch_size);
        let parquet_version = match value.parquet_version {
            None | Some(ParquetVersionArg::V2) => WriterVersion::PARQUET_2_0,
            Some(ParquetVersionArg::V1) => WriterVersion::PARQUET_1_0,
        };
```

(`Option<T>` where `T: Copy` is itself `Copy`, so matching `value.parquet_compression` twice does not move it.)

- [ ] **Step 6: Build (tests will not compile yet — fixed in Task 2)**

Run: `cargo build`
Expected: the library + binary compile. `cargo build --tests` (or `cargo test`) will fail because the `cli.rs` test module still builds `RawArgs` with `String` values — Task 2 fixes that. To confirm non-test code is good:

Run: `cargo build`
Expected: PASS.

---

### Task 2: Update the `cli.rs` tests for the new field types

**Files:**
- Modify: `src/cli.rs` `#[cfg(test)] mod tests` (the `make_base_raw_args` helper, several override tests, and the four invalid-value tests)

- [ ] **Step 1: Make the test module able to call clap's parser**

At the top of the `mod tests` block (it currently has `use super::*;` and `use std::convert::TryInto;`), add:

```rust
    use clap::Parser;
```

- [ ] **Step 2: Update `make_base_raw_args` to the new field types**

In `make_base_raw_args`, change these four lines:
```rust
            output_format: "csv".to_string(),
            schema_version: "2012".to_string(),
            ...
            parquet_compression: None,
            parquet_version: None,
```
to:
```rust
            output_format: OutputFormatArg::Csv,
            schema_version: SchemaVersionArg::V2012,
            ...
            parquet_compression: None,
            parquet_version: None,
```
(The `parquet_compression`/`parquet_version` `None` lines stay textually the same — only their inferred type changes.)

- [ ] **Step 3: Update the override tests that set `schema_version` as a string**

In each of these tests, replace `schema_version: "2021".to_string()` with `schema_version: SchemaVersionArg::V2021` and `schema_version: "2012".to_string()` with `schema_version: SchemaVersionArg::V2012`:

- `test_try_into_valid_model2021_with_teryt_path` → `SchemaVersionArg::V2021`
- `test_try_into_schema_2021_missing_teryt` → `SchemaVersionArg::V2021`
- `test_try_into_download_teryt_missing_credentials` → `SchemaVersionArg::V2021`
- `test_try_into_download_teryt_with_schema_2012_is_downgraded` → `SchemaVersionArg::V2012`

(`test_try_into_invalid_crs_epsg` sets only `crs_epsg`, which is unchanged — leave it.)

- [ ] **Step 4: Delete the four now-obsolete invalid-value tests**

These tested validation that now happens in clap at parse time, not in `try_from`, so they can no longer construct an invalid `RawArgs`. Delete them entirely:
- `test_try_into_invalid_schema_version`
- `test_try_into_invalid_output_format`
- `test_try_into_invalid_parquet_compression`
- `test_try_into_invalid_parquet_version`

- [ ] **Step 5: Add clap parse-level rejection tests**

Add these four tests to `mod tests` (they assert clap rejects bad values before `try_from` ever runs):

```rust
    #[test]
    fn test_parse_rejects_invalid_output_format() {
        let result = RawArgs::try_parse_from([
            "prg_convert",
            "--output-path", "/tmp/o.csv",
            "--schema-version", "2012",
            "--output-format", "excel",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rejects_invalid_schema_version() {
        let result = RawArgs::try_parse_from([
            "prg_convert",
            "--output-path", "/tmp/o.csv",
            "--schema-version", "9999",
            "--output-format", "csv",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rejects_invalid_parquet_compression() {
        let result = RawArgs::try_parse_from([
            "prg_convert",
            "--output-path", "/tmp/o.parquet",
            "--schema-version", "2012",
            "--output-format", "geoparquet",
            "--parquet-compression", "lz4",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rejects_invalid_parquet_version() {
        let result = RawArgs::try_parse_from([
            "prg_convert",
            "--output-path", "/tmp/o.parquet",
            "--schema-version", "2012",
            "--output-format", "geoparquet",
            "--parquet-version", "v3",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_accepts_uppercase_output_format() {
        // ignore_case = true keeps the old case-insensitive behaviour
        let result = RawArgs::try_parse_from([
            "prg_convert",
            "--output-path", "/tmp/o.csv",
            "--schema-version", "2012",
            "--output-format", "CSV",
        ]);
        assert!(result.is_ok());
    }
```

- [ ] **Step 6: Build and test**

Run: `cargo test --lib`
Expected: PASS — the valid-path `try_into` tests still pass (they exercise `TryFrom` via the blanket impl), and the new parse-rejection tests pass.

Run: `cargo test --test e2e`
Expected: PASS — the binary still accepts `--output-format csv|geoparquet`, `--schema-version 2012|2021`, etc.

- [ ] **Step 7: Manually confirm `--help` now lists possible values**

Run: `cargo run -- --help`
Expected: `--output-format` shows `[possible values: csv, geoparquet]` and `--schema-version` shows `[possible values: 2012, 2021]` (auto-generated by clap).

- [ ] **Step 8: Format and commit**

```bash
cargo fmt --all
git add src/cli.rs
git commit -m "refactor: use clap ValueEnum for output-format/schema-version/parquet options"
```

---

## Self-review checklist

- [ ] Four `ValueEnum` enums defined in `cli.rs`; library enums in `lib.rs` untouched (no clap dependency leaks into the library).
- [ ] All four `RawArgs` fields use the new enum types with `ignore_case = true` where applicable.
- [ ] Every `anyhow::bail!` arm for these four options removed from `try_from`; all matches are exhaustive without a catch-all.
- [ ] `make_base_raw_args` and the four schema-version override tests use enum values; the four obsolete invalid tests deleted; five parse-level tests added.
- [ ] `cargo test --lib`, `cargo test --test e2e`, `cargo fmt --all --check` pass; `--help` lists possible values.
