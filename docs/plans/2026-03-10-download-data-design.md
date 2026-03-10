# Design: `--download-data` CLI flag

## Overview

Add a `--download-data` flag as an alternative to `--input-paths`. When set, the tool streams the PRG address dataset from the official GUGiK URL to a temporary file on disk and processes it as a ZIP file. Exactly one of `--input-paths` or `--download-data` must be provided.

**Download URL:** `https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml`

## CLI Changes (`src/cli.rs`)

### `RawArgs`

Add field:

```rust
#[arg(long = "download-data", action = ArgAction::SetTrue,
      help = "Download PRG address data from the official GUGiK URL instead of providing --input-paths.")]
download_data: Option<bool>,
```

### `ParsedArgs`

Add field:

```rust
pub download_data: bool,
```

`parsed_paths` remains `Vec<FileRecord>` but is left empty when `download_data` is true (populated later in `main()`).

### `TryInto<ParsedArgs>` validation

Add mutual exclusion check:

- If both `--input-paths` (non-empty) and `--download-data` are provided → bail
- If neither is provided → bail
- If `--download-data` is set → skip `parse_input_paths()`, set `parsed_paths: vec![]`

### `print_parsed_args`

When `download_data` is true, print the source URL instead of the file list.

## Download Function (`src/cli.rs`)

```rust
pub fn download_prg_data() -> anyhow::Result<tempfile::NamedTempFile>
```

- Creates a `NamedTempFile` with `.zip` suffix via `tempfile::Builder`
- Opens a `reqwest::blocking::Client` and sends a GET to the URL
- Checks the response status
- Streams the body to disk using `std::io::copy(&mut response, &mut file)`
- Returns the `NamedTempFile` (caller holds it to prevent deletion)

No new dependencies required (`reqwest` blocking and `tempfile` are already in `Cargo.toml`).

## `main()` Changes

After `print_parsed_args()`, insert:

```rust
let _temp_file; // keep alive for processing duration
let file_records: Vec<FileRecord>;

if parsed_args.download_data {
    println!("Downloading PRG data...");
    let temp = download_prg_data()?;
    let path_str = temp.path().to_string_lossy().to_string();
    file_records = parse_input_paths(&vec![path_str], &parsed_args.schema_version)?;
    _temp_file = Some(temp);
} else {
    file_records = parsed_args.parsed_paths;
    _temp_file = None;
}
```

Then iterate `file_records` instead of `parsed_args.parsed_paths`. The `NamedTempFile` is held in `_temp_file` until the end of `main()`, at which point it is dropped and the file deleted.

## Validation Table

| Condition | Result |
|---|---|
| Both `--input-paths` and `--download-data` | error |
| Neither `--input-paths` nor `--download-data` | error |
| `--download-data` without `--schema-version` | error (existing, unchanged) |
| `--download-data` + schema 2021 + no teryt source | error (existing, unchanged) |

## Testing

- Unit test in `cli.rs`: both flags provided → error
- Unit test in `cli.rs`: neither flag provided → error
- Existing tests remain unchanged
