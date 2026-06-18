# Feature Gating for Library Embedding Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Let a downstream crate depend on `prg_convert` as a library that "just reads Arrow batches" without compiling the heavy/CLI-only dependency tree (`reqwest`→`tokio`/`hyper`/TLS, `parquet`, `geoparquet`, `geoarrow`, `geo-types`, `clap`, `glob`). Introduce a `download` feature and make the existing `cli` feature pull everything CLI/output-related.

**Architecture:** Mark the optional crates `optional = true`, define two features (`download`, `cli`), and `#[cfg(...)]`-gate the code that uses each. The binary target already requires `cli`, so `main.rs`/`cli.rs` only compile with `cli` on. The XML→Arrow parsing path stays in the default-buildable core.

**Tech Stack:** Cargo features, `#[cfg(feature = …)]`, GitHub Actions.

## Global Constraints

- Rust edition 2024; `cargo fmt --all --check` must pass.
- **Depends on plans #3 and #4.** After those, the library core (everything except `cli.rs`/`main.rs`) no longer uses `parquet`, `geoparquet`, `geoarrow`, or `geo-types` — that is what makes this gating possible. Do #3 and #4 first.
- Default features stay `["cli"]`, so `cargo build`, `cargo test`, and the release binary behave exactly as today.
- `cargo build --no-default-features` must compile the library alone.
- Do **not** run the *test targets* with `--no-default-features` (the `e2e` integration test and some lib tests need CLI/output deps). The no-default-features CI step is a **build**, not a test.

### Final feature/dependency layout (target state)

```toml
[features]
default = ["cli"]
cli = ["download", "dep:clap", "dep:glob", "dep:geoparquet", "dep:parquet", "dep:geoarrow", "dep:geo-types", "arrow/csv"]
download = ["dep:reqwest", "dep:base64", "dep:uuid"]
```

| Crate | Gated under | Used by |
|-------|-------------|---------|
| `clap`, `glob` | `cli` | `cli.rs` (binary) |
| `parquet`, `geoparquet` | `cli` | `main.rs` GeoParquet writer, `e2e` test |
| `geoarrow`, `geo-types` | `cli` | `main.rs` geometry build, `common::get_geoparquet_schema`/`CRS_*` |
| `arrow/csv` feature | `cli` | `main.rs` CSV writer |
| `reqwest`, `base64`, `uuid` | `download` | `terc.rs` download, `cli.rs` PRG download |
| everything else (`arrow` core, `quick-xml`, `zip`, `proj4rs`, `chrono`, `chrono-tz`, `serde`, `serde_json`, `tempfile`, `anyhow`) | always | core parsing |

---

### Task 1: Rewrite `Cargo.toml` features and optional deps

**Files:**
- Modify: `Cargo.toml`

- [ ] **Step 1: Replace the `[features]` section**

```toml
[features]
default = ["cli"]
cli = ["download", "dep:clap", "dep:glob", "dep:geoparquet", "dep:parquet", "dep:geoarrow", "dep:geo-types", "arrow/csv"]
download = ["dep:reqwest", "dep:base64", "dep:uuid"]
```

- [ ] **Step 2: Mark dependencies optional and drop `csv` from the always-on `arrow` features**

Edit `[dependencies]` so these lines read exactly as below (changes: `arrow` loses `"csv"`; `base64`, `geo-types`, `geoarrow`, `geoparquet`, `parquet`, `reqwest` gain `optional = true`):

```toml
arrow = { version = "58.1.0", default-features = false, features = ["chrono-tz"] }
base64 = { version = "0.22.1", optional = true }
geo-types = { version = "0.7.18", optional = true }
geoarrow = { version = "0.8.0", optional = true }
geoparquet = { version = "0.8.0", optional = true }
parquet = { version = "58.1.0", features = ["arrow", "zstd", "simdutf8", "snap", "brotli"], optional = true }
reqwest = { version = "0.13.1", features = ["blocking"], optional = true }
```

`clap` and `glob` already have `optional = true` — leave them. Leave `anyhow`, `chrono`, `chrono-tz`, `proj4rs`, `quick-xml`, `serde`, `serde_json`, `tempfile`, `zip` unchanged (always-on). `tempfile` stays non-optional (it is used by tests and the download code).

- [ ] **Step 3: Mark the `uuid` dependency optional**

In the `[dependencies.uuid]` table, add `optional = true`:

```toml
[dependencies.uuid]
version = "1.19.0"
features = [
    "v4",
]
optional = true
```

- [ ] **Step 4: Verify default build still works**

Run: `cargo build`
Expected: success (default features = `cli`, so everything is available, same as before).

- [ ] **Step 5: Commit the manifest (code gating follows; build of `--no-default-features` not expected to pass yet)**

```bash
git add Cargo.toml Cargo.lock
git commit -m "chore: make CLI/download dependencies optional"
```

---

### Task 2: Gate the geo/geoparquet helpers in `common.rs`

**Files:**
- Modify: `src/common.rs`

**Background:** After plan #4, `CRS_2180`, `CRS_4326`, and `get_geoparquet_schema` (all using `geoarrow`) are only used by `cli.rs`/`main.rs`. Gate them behind `cli`.

- [ ] **Step 1: Gate the geoarrow imports**

In `src/common.rs`, put `#[cfg(feature = "cli")]` on the geoarrow imports:

```rust
#[cfg(feature = "cli")]
use geoarrow::datatypes::Crs;
#[cfg(feature = "cli")]
use geoarrow::datatypes::PointType;
```

- [ ] **Step 2: Gate `CRS_2180`, `CRS_4326`, and `get_geoparquet_schema`**

Add `#[cfg(feature = "cli")]` directly above each of these items:

```rust
#[cfg(feature = "cli")]
pub static CRS_2180: LazyLock<Crs> = LazyLock::new(|| {
    Crs::from_projjson(serde_json::from_str(include_str!("crs/epsg2180.json")).unwrap())
});
#[cfg(feature = "cli")]
pub static CRS_4326: LazyLock<Crs> = LazyLock::new(|| {
    Crs::from_projjson(serde_json::from_str(include_str!("crs/epsg4326.json")).unwrap())
});
```

and:

```rust
#[cfg(feature = "cli")]
pub fn get_geoparquet_schema(geoarrow_geom_type: PointType) -> Arc<Schema> {
    // ... unchanged body ...
}
```

Leave `EPSG_2180`/`EPSG_4326` (proj4rs) and `SCHEMA_CSV` ungated — the core parser uses them.

- [ ] **Step 3: Verify the library compiles without default features**

Run: `cargo build --no-default-features`
Expected: the gated items disappear and the library compiles **if** plans #3/#4 are in place. If you see errors about `geoarrow`, `geo-types`, `parquet`, or `Writer` still being referenced in core code, that code was missed by plans #3/#4 — fix there. (Proceed to Task 3 for the `terc.rs` download gating, then re-run.)

---

### Task 3: Gate the download path in `terc.rs`

**Files:**
- Modify: `src/terc.rs`

- [ ] **Step 1: Split the `std::io` import and gate download-only imports**

At the top of `src/terc.rs`, change:

```rust
use std::{
    collections::HashMap,
    io::{BufReader, Seek, Write},
    path::PathBuf,
};

use anyhow::Context;
use base64::{Engine as _, engine::general_purpose};
use chrono::Local;
use quick_xml::de::Deserializer;
use serde::Deserialize;
use tempfile::tempfile;
use uuid::Uuid;
use zip::ZipArchive;
```

to:

```rust
use std::{
    collections::HashMap,
    io::BufReader,
    path::PathBuf,
};
#[cfg(feature = "download")]
use std::io::{Seek, Write};

use anyhow::Context;
#[cfg(feature = "download")]
use base64::{Engine as _, engine::general_purpose};
#[cfg(feature = "download")]
use chrono::Local;
use quick_xml::de::Deserializer;
use serde::Deserialize;
#[cfg(feature = "download")]
use tempfile::tempfile;
#[cfg(feature = "download")]
use uuid::Uuid;
use zip::ZipArchive;
```

- [ ] **Step 2: Gate the download-only response structs**

Add `#[cfg(feature = "download")]` above each of `PobierzKatalogTERCResult`, `Envelope`, `Body`, `PobierzKatalogTERCResponse` (the four structs used only to parse the SOAP download response). Leave `Teryt`, `Catalog`, `Row`, `Terc` ungated.

Example:
```rust
#[cfg(feature = "download")]
#[derive(Deserialize)]
struct PobierzKatalogTERCResult {
    /// Contains Base64 encoded zip file
    pub plik_zawartosc: String,
}
```
(Apply the same one-line attribute to the other three.)

- [ ] **Step 3: Gate the download functions and their tests**

Add `#[cfg(feature = "download")]` above:
- `pub fn download_terc_mapping(...)`
- `fn get_file_content_from_response(...)`
- `fn build_terc_soap_payload(...)` (from plan #2)
- `#[test] fn test_parse_api_response()` (uses `get_file_content_from_response`)
- `#[test] fn test_build_terc_soap_payload_escapes_credentials()` (from plan #2)

(`parse_terc_zip_file`, `get_terc_mapping`, `prepare_mapping_from_teryt`, and their tests stay ungated.)

- [ ] **Step 4: Build with and without download**

Run: `cargo build --no-default-features`
Expected: success (no `download`, no `cli`).

Run: `cargo build --no-default-features --features download`
Expected: success (download path compiles, still no CLI/output deps).

---

### Task 4: Make `lib.rs::get_teryt_mapping` degrade gracefully without `download`

**Files:**
- Modify: `src/lib.rs` (`get_teryt_mapping`)

- [ ] **Step 1: Gate the download branch**

Replace the body of `get_teryt_mapping`:

```rust
pub fn get_teryt_mapping(
    download_teryt: bool,
    teryt_api_username: &Option<String>,
    teryt_api_password: &Option<String>,
    teryt_file_path: &Option<PathBuf>,
) -> anyhow::Result<HashMap<String, Terc>> {
    if download_teryt {
        #[cfg(feature = "download")]
        {
            download_terc_mapping(
                teryt_api_username.clone().unwrap().as_str(),
                teryt_api_password.clone().unwrap().as_str(),
            )
        }
        #[cfg(not(feature = "download"))]
        {
            let _ = (teryt_api_username, teryt_api_password);
            anyhow::bail!(
                "This build was compiled without the `download` feature; downloading TERYT is unavailable. Provide a TERYT file via --teryt-path."
            )
        }
    } else {
        get_terc_mapping(teryt_file_path.as_ref().unwrap())
    }
}
```

- [ ] **Step 2: Confirm `download_terc_mapping` import is only referenced under cfg**

In `lib.rs`, the line `use terc::download_terc_mapping;` is now only used inside the `#[cfg(feature = "download")]` block. To avoid an unused-import error when `download` is off, gate it:

```rust
#[cfg(feature = "download")]
use terc::download_terc_mapping;
```

(`use terc::get_terc_mapping;` stays ungated.)

- [ ] **Step 3: Full build matrix**

Run each and expect success:
```bash
cargo build --no-default-features
cargo build --no-default-features --features download
cargo build            # default = cli
```

- [ ] **Step 4: Run the default test suite (unchanged behaviour)**

Run: `cargo test --lib && cargo test --test e2e`
Expected: PASS (default features include `cli`).

- [ ] **Step 5: Commit the gating**

```bash
cargo fmt --all
git add src/common.rs src/terc.rs src/lib.rs
git commit -m "feat: gate CLI/download code behind features for library embedding"
```

---

### Task 5: Add a no-default-features build check to CI

**Files:**
- Modify: `.github/workflows/checks.yaml`
- Modify: `.github/workflows/release.yaml`

- [ ] **Step 1: Add build steps to `checks.yaml`**

In `.github/workflows/checks.yaml`, in the `run-checks` job `steps:`, after the `cargo build --all-features` line add:

```yaml
    - run: cargo build --no-default-features
    - run: cargo build --no-default-features --features download
```

- [ ] **Step 2: Mirror the same two steps in `release.yaml`**

Add the identical two `- run:` lines after `cargo build --all-features` in the `run-checks` job of `.github/workflows/release.yaml`.

- [ ] **Step 3: Validate locally (the commands CI will run)**

Run:
```bash
cargo build --all-features
cargo build --no-default-features
cargo build --no-default-features --features download
cargo test --lib --all-features
cargo test --test e2e
cargo fmt --all --check
```
Expected: all succeed.

- [ ] **Step 4: Commit CI changes**

```bash
git add .github/workflows/checks.yaml .github/workflows/release.yaml
git commit -m "ci: build the library with no default features"
```

---

## Self-review checklist

- [ ] `Cargo.toml`: `download` + `cli` features defined; `clap`/`glob`/`geoparquet`/`parquet`/`geoarrow`/`geo-types`/`reqwest`/`base64`/`uuid` optional; `arrow` no longer pulls `csv` by default but `cli` adds `arrow/csv`.
- [ ] `common.rs` geoarrow items (`CRS_2180`, `CRS_4326`, `get_geoparquet_schema`) gated under `cli`; `SCHEMA_CSV`/`EPSG_*` ungated.
- [ ] `terc.rs` download functions, response structs, their imports, and their tests gated under `download`; file-based parsing stays core.
- [ ] `lib.rs::get_teryt_mapping` compiles and gives a clear error when built without `download`.
- [ ] `cargo build --no-default-features` and `… --features download` both compile; default `cargo build`/`cargo test`/`e2e` unchanged.
- [ ] CI builds the no-default-features and download-only configurations.

> A consumer can now depend on the parser only:
> ```toml
> prg_convert = { version = "0.7", default-features = false }
> ```
> getting `get_address_parser_*` → `Iterator<Item = RecordBatch>` without `tokio`/`hyper`/`parquet`/`geoparquet`/`clap`.
