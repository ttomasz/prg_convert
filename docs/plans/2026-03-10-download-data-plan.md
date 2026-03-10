# download-data Feature Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add a `--download-data` CLI flag that downloads the PRG address ZIP from GUGiK and processes it, as a mutually exclusive alternative to `--input-paths`.

**Architecture:** `RawArgs` gets the new flag; `try_into()` validates mutual exclusion and sets `download_data: bool` in `ParsedArgs` but does no I/O; `main()` triggers the streaming download to a `NamedTempFile` after printing args, then calls the existing `parse_input_paths()` on the temp path and processes it exactly like a user-provided ZIP.

**Tech Stack:** Rust, `clap` (ArgAction::SetTrue), `reqwest::blocking` (already in Cargo.toml), `tempfile::NamedTempFile` (already in Cargo.toml), `std::io::copy` for streaming.

---

### Task 1: Add `download_data` field to `RawArgs`, `ParsedArgs`, and test helper

**Files:**
- Modify: `src/cli.rs`

**Step 1: Add field to `RawArgs`**

In `src/cli.rs`, add to the `RawArgs` struct after the `input_paths` field:

```rust
#[arg(long = "download-data", action = ArgAction::SetTrue, help = "Download PRG address data from the official GUGiK URL instead of providing --input-paths. URL: https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml")]
download_data: Option<bool>,
```

**Step 2: Add field to `ParsedArgs`**

In the `ParsedArgs` struct, add after `input_paths`:

```rust
pub download_data: bool,
```

**Step 3: Update `make_base_raw_args()` test helper**

In the `#[cfg(test)]` block, add `download_data: None` to the `RawArgs` struct literal in `make_base_raw_args()`:

```rust
fn make_base_raw_args() -> RawArgs {
    RawArgs {
        input_paths: vec!["fixtures/sample_model2012.xml".to_string()],
        download_data: None,
        // ... rest unchanged
    }
}
```

**Step 4: Update `TryInto<ParsedArgs>` to populate the new field**

In the `try_into()` function, after `let batch_size = ...`, add:

```rust
let download_data = self.download_data.unwrap_or(false);
```

And in the `Ok(ParsedArgs { ... })` return, add:

```rust
download_data: download_data,
```

Keep the existing `parse_input_paths` call for now (mutual exclusion logic comes in Task 2).

**Step 5: Run tests to verify nothing is broken**

```bash
cargo test
```

Expected: all existing tests pass.

**Step 6: Commit**

```bash
git add src/cli.rs
git commit -m "feat: add download_data field to RawArgs and ParsedArgs structs"
```

---

### Task 2: Add mutual exclusion validation in `try_into()` (TDD)

**Files:**
- Modify: `src/cli.rs`

**Step 1: Write failing test — both flags provided**

In the `#[cfg(test)]` block, add:

```rust
#[test]
fn test_try_into_both_input_paths_and_download_data() {
    let args = RawArgs {
        download_data: Some(true),
        ..make_base_raw_args() // make_base_raw_args has non-empty input_paths
    };
    let result: anyhow::Result<ParsedArgs> = args.try_into();
    assert!(result.is_err());
    let err_str = format!("{}", result.err().unwrap());
    assert!(
        err_str.contains("input-paths") || err_str.contains("download-data"),
        "Error message was: {}",
        err_str
    );
}
```

**Step 2: Write failing test — neither flag provided**

```rust
#[test]
fn test_try_into_neither_input_paths_nor_download_data() {
    let args = RawArgs {
        input_paths: vec![],
        download_data: None,
        ..make_base_raw_args()
    };
    let result: anyhow::Result<ParsedArgs> = args.try_into();
    assert!(result.is_err());
    let err_str = format!("{}", result.err().unwrap());
    assert!(
        err_str.contains("input-paths") || err_str.contains("download-data"),
        "Error message was: {}",
        err_str
    );
}
```

**Step 3: Run tests to confirm they fail**

```bash
cargo test test_try_into_both_input_paths_and_download_data test_try_into_neither_input_paths_nor_download_data
```

Expected: both tests FAIL (currently no mutual exclusion logic exists).

**Step 4: Implement mutual exclusion validation in `try_into()`**

In `try_into()`, after `let download_data = self.download_data.unwrap_or(false);`, add:

```rust
let has_input_paths = !self.input_paths.is_empty();
if has_input_paths && download_data {
    anyhow::bail!(
        "Provide either --input-paths or --download-data, but not both."
    );
}
if !has_input_paths && !download_data {
    anyhow::bail!(
        "Either --input-paths or --download-data must be provided."
    );
}
```

**Step 5: Skip `parse_input_paths` when `download_data` is true**

Replace the existing line:

```rust
let parsed_paths = parse_input_paths(&self.input_paths, &schema_version)?;
```

With:

```rust
let parsed_paths = if download_data {
    vec![]
} else {
    parse_input_paths(&self.input_paths, &schema_version)?
};
```

**Step 6: Run tests to verify they pass**

```bash
cargo test
```

Expected: all tests pass including the two new ones.

**Step 7: Commit**

```bash
git add src/cli.rs
git commit -m "feat: add mutual exclusion validation for --input-paths and --download-data"
```

---

### Task 3: Implement `download_prg_data()` streaming download function

**Files:**
- Modify: `src/cli.rs`

**Step 1: Add required imports at top of `src/cli.rs`**

Ensure these are present (add if missing):

```rust
use std::io::Write;
use tempfile::NamedTempFile;
```

**Step 2: Implement the download function**

Add this function (outside `impl`, before or after `parse_input_paths`):

```rust
const PRG_DOWNLOAD_URL: &str =
    "https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml";

pub fn download_prg_data() -> anyhow::Result<NamedTempFile> {
    let mut temp_file = tempfile::Builder::new()
        .suffix(".zip")
        .tempfile()
        .with_context(|| "Failed to create temporary file for download.")?;
    let client = reqwest::blocking::Client::new();
    println!("Sending download request to: {}", PRG_DOWNLOAD_URL);
    let mut response = client
        .get(PRG_DOWNLOAD_URL)
        .send()
        .with_context(|| format!("Failed to send download request to: {}", PRG_DOWNLOAD_URL))?;
    if !response.status().is_success() {
        anyhow::bail!(
            "Download request failed with status: {}",
            response.status()
        );
    }
    println!("Download started, saving to temporary file...");
    std::io::copy(&mut response, &mut temp_file)
        .with_context(|| "Failed to stream download to temporary file.")?;
    println!("Download complete.");
    Ok(temp_file)
}
```

Note: `reqwest::blocking::Response` implements `std::io::Read`, so `std::io::copy` streams the body in chunks without loading it into memory.

**Step 3: Run tests to verify nothing broken**

```bash
cargo test
```

Expected: all tests pass (the new function is not yet called by anything testable in unit tests).

**Step 4: Commit**

```bash
git add src/cli.rs
git commit -m "feat: implement streaming download_prg_data() function"
```

---

### Task 4: Update `print_parsed_args` to handle `download_data`

**Files:**
- Modify: `src/cli.rs`

**Step 1: Update `print_parsed_args`**

In `print_parsed_args`, replace the "Input paths/patterns" and "Input:" sections with a conditional block:

```rust
pub fn print_parsed_args(parsed_args: &ParsedArgs) {
    println!("⚙️  Parameters:");
    if parsed_args.download_data {
        println!("  Input: download from URL: {}", PRG_DOWNLOAD_URL);
    } else {
        println!("  Input paths/patterns:");
        for path in &parsed_args.input_paths {
            println!("    - {}", path);
        }
        println!("  Input:");
        for file in &parsed_args.parsed_paths {
            // ... existing file printing logic unchanged
        }
    }
    // ... rest of function unchanged
```

**Step 2: Run tests**

```bash
cargo test
```

Expected: all tests pass.

**Step 3: Commit**

```bash
git add src/cli.rs
git commit -m "feat: update print_parsed_args to display download URL when --download-data is used"
```

---

### Task 5: Wire download into `main()`

**Files:**
- Modify: `src/main.rs`

**Step 1: Import the new function and type**

Ensure `main.rs` can access `download_prg_data` — it's in the `cli` module so `cli::download_prg_data()` works already.

**Step 2: Replace the `parsed_paths` loop with a download-aware version**

In `main()`, after `cli::print_parsed_args(&parsed_args);`, replace the block that uses `parsed_args.parsed_paths` with:

```rust
// Download data if requested, keeping the temp file alive for the duration of processing
let _temp_file;
let files_to_process: Vec<cli::FileRecord>;
if parsed_args.download_data {
    println!("⬇️  Downloading PRG data...");
    let temp = cli::download_prg_data()?;
    let path_str = temp.path().to_string_lossy().to_string();
    files_to_process = cli::parse_input_paths(&vec![path_str], &parsed_args.schema_version)?;
    _temp_file = Some(temp);
} else {
    files_to_process = parsed_args.parsed_paths;
    _temp_file = None;
}
```

Then replace all uses of `&parsed_args.parsed_paths` in the loop below with `&files_to_process`:

```rust
let num_files_to_process = &files_to_process.len();
// ...
for file in &files_to_process {
    total_file_size += &file.size_in_bytes;
    // ... rest unchanged
}
```

**Step 3: Run tests**

```bash
cargo test
```

Expected: all tests pass.

**Step 4: Build in release mode to verify it compiles cleanly**

```bash
cargo build --release 2>&1 | head -50
```

Expected: no errors, only possible warnings.

**Step 5: Commit**

```bash
git add src/main.rs
git commit -m "feat: wire download_prg_data into main() processing flow"
```

---

### Task 6: Final verification

**Step 1: Run the full test suite**

```bash
cargo test
```

Expected: all tests pass.

**Step 2: Check for compiler warnings**

```bash
cargo clippy 2>&1 | head -30
```

Expected: no new warnings introduced by the feature (pre-existing warnings are acceptable).

**Step 3: Verify CLI help shows the new flag**

```bash
cargo run -- --help 2>&1 | grep -A2 "download-data"
```

Expected: `--download-data` appears with its help text.
