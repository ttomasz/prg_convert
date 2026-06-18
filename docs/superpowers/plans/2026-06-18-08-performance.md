# Parser Performance Improvements Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove the hottest avoidable allocations in the parsing path: (1) the per-tag `Vec<u8>` allocation on every XML start element, (2) a per-record `String` key allocation plus full-struct clone in the 2012 component lookup, (3) the full deep-clone of the TERYT map for every parsed file, and (4) needless `.clone()` on tiny field-less enums.

**Architecture:** Four independent, behaviour-preserving micro-optimisations. None changes output bytes; the existing `cargo test --lib` and `cargo test --test e2e` suites are the regression guard. The biggest win (Task 2) reuses a single byte buffer for the "current tag" instead of allocating a fresh `Vec` per element.

**Tech Stack:** Rust 2024, `arrow` builders, `std::sync::Arc`.

## Global Constraints

- Rust edition 2024; `cargo fmt --all --check` must pass.
- **Depends on plan #4** (parser signatures and `model2021` field types are the post-#4 versions). Do plan #4 first.
- No behaviour change: every existing test must keep passing without edits, except the test-call updates explicitly listed in Task 4 (which only adjust how the TERYT map is passed).

---

### Task 1: Derive `Copy` on the field-less enums

**Files:**
- Modify: `src/lib.rs` (enum derives)
- Modify: `src/main.rs` (drop a redundant `.clone()`)

**Background:** `OutputFormat`, `CRS`, `CoordOrder`, and `FileType` are field-less enums currently `#[derive(Clone)]`. Making them `Copy` lets them pass by value without heap-free-but-noisy `.clone()` calls and documents that they are trivially copyable.

- [ ] **Step 1: Add `Copy` to the four enum derives**

In `src/lib.rs`, change each of these four derive attributes from `#[derive(Clone)]` to `#[derive(Clone, Copy)]`:
- `pub enum CoordOrder`
- `pub enum OutputFormat`
- `pub enum FileType`
- `pub enum CRS`

(`SchemaVersion` has no derive and is not `Copy`-relevant here — leave it.)

- [ ] **Step 2: Drop the redundant clone in `main.rs`**

In `src/main.rs`, in the `OutputWriter::GeoParquet { … }` construction (added in plan #4), change:

```rust
                crs: parsed_args.crs.clone(),
```
to:
```rust
                crs: parsed_args.crs,
```

(`PointType` and `Arc<Schema>` are not `Copy`; leave `geom_type: parsed_args.geoarrow_geom_type.clone()` and `geoparquet_schema: parsed_args.arrow_schema.clone()` as-is.)

- [ ] **Step 3: Build and test**

Run: `cargo build && cargo test --lib`
Expected: PASS. (If the compiler flags any other now-redundant `.clone()` on these enums as a warning, you may delete those `.clone()` calls too, but it is optional.)

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add src/lib.rs src/main.rs
git commit -m "perf: make field-less enums Copy"
```

---

### Task 2: Reuse the `last_tag` buffer instead of allocating per element

**Files:**
- Modify: `src/model2012.rs` (`parse_additional_info`, `parse_address`)
- Modify: `src/model2021.rs` (`parse_city`, `parse_street`, `parse_address`)

**Background:** Every `Ok(Event::Start(ref e))` branch does `last_tag = e.name().as_ref().to_vec();`, allocating a brand-new `Vec<u8>` for each start tag of each field of each record (tens of millions of allocations over a full dataset). Reusing the existing `Vec`'s capacity with `clear()` + `extend_from_slice()` removes essentially all of these allocations while keeping the exact same byte-slice matching logic.

- [ ] **Step 1: Find every occurrence**

Run: `grep -rn 'last_tag = e.name().as_ref().to_vec();' src/`
Expected: 5 matches — `model2012.rs` (in `parse_additional_info` and `parse_address`) and `model2021.rs` (in `parse_city`, `parse_street`, and `parse_address`).

- [ ] **Step 2: Replace each occurrence**

Replace every line:

```rust
                last_tag = e.name().as_ref().to_vec();
```

with:

```rust
                last_tag.clear();
                last_tag.extend_from_slice(e.name().as_ref());
```

Keep the surrounding `Ok(Event::Start(ref e)) => { … }` structure unchanged. In `parse_address` (both files) the `Start` arm also matches `e.name().as_ref()` right after — leave that match exactly as it is; only the assignment line changes.

Note: the existing `let mut last_tag = Vec::new();` declarations stay (the first `extend_from_slice` allocates once, then the capacity is reused). The existing `last_tag.clear();` calls after processing a `Text` event also stay — they keep the "not currently inside a tag" guard (`if last_tag.is_empty()`) working.

- [ ] **Step 3: Build and test**

Run: `cargo build && cargo test --lib && cargo test --test e2e`
Expected: PASS — identical parse output, fewer allocations.

- [ ] **Step 4: Commit**

```bash
cargo fmt --all
git add src/model2012.rs src/model2021.rs
git commit -m "perf: reuse last_tag buffer instead of allocating per XML element"
```

---

### Task 3: Avoid the key allocation and struct clone in the 2012 component lookup

**Files:**
- Modify: `src/model2012.rs` (`parse_address`, the `b"prg-ad:komponent"` arm)

**Background:** The arm does `self.additional_info.get(&attr.to_string()).cloned().unwrap_or_default()`, which (a) allocates a `String` just to look up a `HashMap<String, _>` and (b) clones the whole `AdditionalInfo` (including its `name: String`) on every component of every address. We can look up with `&str` (no allocation) and clone out only the small `(KomponentType, Option<String>)` we actually use.

- [ ] **Step 1: Replace the component arm body**

Find:

```rust
                        b"prg-ad:komponent" => {
                            let attr = get_attribute(e, b"xlink:href");
                            let info = self
                                .additional_info
                                .get(&attr.to_string())
                                .cloned()
                                .unwrap_or_default();
                            match info.typ {
                                KomponentType::Country => {}
                                KomponentType::Voivodeship => {
                                    option_append_value_or_null(
                                        &mut self.voivodeship_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::County => {
                                    option_append_value_or_null(
                                        &mut self.county_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::Municipality => {
                                    option_append_value_or_null(
                                        &mut self.municipality_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::City => {
                                    option_append_value_or_null(
                                        &mut self.city_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::Street => {
                                    option_append_value_or_null(
                                        &mut self.street_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::Unknown => {}
                            }
                            nested_tag = false;
                            tag_ignore_text = true;
                        }
```

Replace it with:

```rust
                        b"prg-ad:komponent" => {
                            let attr = get_attribute(e, b"xlink:href");
                            // Look up by &str (no key allocation) and copy out only what we use.
                            let info = self
                                .additional_info
                                .get(attr.as_ref())
                                .map(|i| (i.typ.clone(), i.teryt_id.clone()));
                            if let Some((typ, teryt_id)) = info {
                                match typ {
                                    KomponentType::Voivodeship => option_append_value_or_null(
                                        &mut self.voivodeship_teryt_id,
                                        teryt_id,
                                    ),
                                    KomponentType::County => option_append_value_or_null(
                                        &mut self.county_teryt_id,
                                        teryt_id,
                                    ),
                                    KomponentType::Municipality => option_append_value_or_null(
                                        &mut self.municipality_teryt_id,
                                        teryt_id,
                                    ),
                                    KomponentType::City => option_append_value_or_null(
                                        &mut self.city_teryt_id,
                                        teryt_id,
                                    ),
                                    KomponentType::Street => option_append_value_or_null(
                                        &mut self.street_teryt_id,
                                        teryt_id,
                                    ),
                                    KomponentType::Country | KomponentType::Unknown => {}
                                }
                            }
                            nested_tag = false;
                            tag_ignore_text = true;
                        }
```

(Behaviour is identical: a missing key previously produced `AdditionalInfo::default()` whose `typ` is `Unknown`, which did nothing — now a missing key simply skips the `if let`. `KomponentType` already derives `Clone`.)

- [ ] **Step 2: Build and test**

Run: `cargo build && cargo test --lib && cargo test --test e2e`
Expected: PASS — the 2012 TERYT-id columns are unchanged (covered by `test_address_parser_2012_zip_csv` and the e2e 2012 cases).

- [ ] **Step 3: Commit**

```bash
cargo fmt --all
git add src/model2012.rs
git commit -m "perf: avoid key allocation and struct clone in 2012 component lookup"
```

---

### Task 4: Share the TERYT map via `Arc` instead of deep-cloning per file

**Files:**
- Modify: `src/model2021.rs` (`teryt_names` field type + `new` param)
- Modify: `src/lib.rs` (the two 2021 `get_address_parser_*` functions + their test calls)
- Modify: `src/main.rs` (build the map once as `Arc`, thread `&Arc` through)

**Background:** `get_address_parser_2021_*` takes `&HashMap<String, Terc>` and calls `teryt_mapping.clone()` — a full deep clone of the entire thousands-entry map (each `Terc` holds five `String`s) for **every** parsed file/zip entry. Storing an `Arc<HashMap<String, Terc>>` makes each "clone" a cheap refcount bump.

**Interfaces:**
- Produces (changed): `AddressParser2021::new(reader, batch_size, additional_info: Mappings, teryt_names: Arc<HashMap<String, Terc>>)`; `get_address_parser_2021_uncompressed(file_path, batch_size, teryt_mapping: &Arc<HashMap<String, Terc>>)`; `get_address_parser_2021_zip(archive, batch_size, teryt_mapping: &Arc<HashMap<String, Terc>>, zip_file_index)`.

- [ ] **Step 1: Change the `teryt_names` field and `new` parameter type in `model2021.rs`**

In `pub struct AddressParser2021<R: BufRead>`, change:
```rust
    teryt_names: HashMap<String, Terc>,
```
to:
```rust
    teryt_names: Arc<HashMap<String, Terc>>,
```

In `new`, change the parameter:
```rust
        teryt_names: HashMap<String, Terc>,
```
to:
```rust
        teryt_names: Arc<HashMap<String, Terc>>,
```
The field initialiser `teryt_names,` (shorthand) stays the same. (`use std::sync::Arc;` is already imported in `model2021.rs`.) The lookups `self.teryt_names.get(&c.municipality_teryt_id)` work unchanged because `Arc<HashMap<…>>` derefs to `HashMap`.

- [ ] **Step 2: Change the two `get_address_parser_2021_*` signatures in `lib.rs`**

In `get_address_parser_2021_uncompressed`, change the parameter `teryt_mapping: &HashMap<String, Terc>` to `teryt_mapping: &Arc<HashMap<String, Terc>>`, and the call `AddressParser2021::new(reader, *batch_size, dict, teryt_mapping.clone())` keeps `teryt_mapping.clone()` — now a cheap `Arc` clone.

In `get_address_parser_2021_zip`, make the same parameter-type change; `teryt_mapping.clone()` likewise becomes an `Arc` clone.

(`use std::sync::Arc;` is already imported at the top of `lib.rs`.)

- [ ] **Step 3: Update the `lib.rs` test calls to wrap the map in `Arc`**

The 2021 tests build `teryt_mapping` via `get_teryt_mapping(...)` (returns a plain `HashMap`). Wrap it once and pass `&` to the parser. In `test_address_parser_2021_zip_csv`, `test_address_parser_2021_xml_csv`, and `test_address_parser_2021_zip_canonical` (the canonical test added in plan #4), change the line:

```rust
        let teryt_mapping =
            get_teryt_mapping(false, &None, &None, &Some(PathBuf::from(teryt_file_path))).unwrap();
```
to:
```rust
        let teryt_mapping = Arc::new(
            get_teryt_mapping(false, &None, &None, &Some(PathBuf::from(teryt_file_path))).unwrap(),
        );
```

and update the corresponding parser call to pass `&teryt_mapping`:
- `get_address_parser_2021_zip(&mut archive, &1, &teryt_mapping, 1)` (already `&teryt_mapping`; the variable is now an `Arc`, so this still type-checks as `&Arc<…>`)
- `get_address_parser_2021_uncompressed(&file_path, &100_000, &teryt_mapping)`
- `get_address_parser_2021_zip(&mut archive, &100_000, &teryt_mapping, 1)`

(No call-site text changes are needed beyond wrapping the variable in `Arc::new(...)`, because they already pass `&teryt_mapping`.)

- [ ] **Step 4: Update `main.rs` to build the map once as `Arc`**

In `main`, the `teryt_mapping` value is built once before the file loop:

```rust
    let teryt_mapping = match &parsed_args.schema_version {
        SchemaVersion::Model2012 => None,
        SchemaVersion::Model2021 => Some(get_teryt_mapping(
            parsed_args.download_teryt,
            &parsed_args.teryt_api_username,
            &parsed_args.teryt_api_password,
            &parsed_args.teryt_path,
        )?),
    };
```

Wrap the map in `Arc` so its type is `Option<Arc<HashMap<String, Terc>>>`:

```rust
    let teryt_mapping: Option<std::sync::Arc<HashMap<String, Terc>>> =
        match &parsed_args.schema_version {
            SchemaVersion::Model2012 => None,
            SchemaVersion::Model2021 => Some(std::sync::Arc::new(get_teryt_mapping(
                parsed_args.download_teryt,
                &parsed_args.teryt_api_username,
                &parsed_args.teryt_api_password,
                &parsed_args.teryt_path,
            )?)),
        };
```

`parse_file` already receives `teryt_mapping: &Option<HashMap<String, Terc>>`; change that parameter to `&Option<std::sync::Arc<HashMap<String, Terc>>>`, and inside the 2021 arms the existing `teryt_mapping.as_ref().unwrap()` now yields `&Arc<HashMap<String, Terc>>`, which matches the new `get_address_parser_2021_*` signature. No other body changes needed.

- [ ] **Step 5: Build and test**

Run: `cargo build && cargo test --lib && cargo test --test e2e`
Expected: PASS — same TERYT enrichment, no functional change.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add src/model2021.rs src/lib.rs src/main.rs
git commit -m "perf: share TERYT map via Arc instead of deep-cloning per file"
```

---

## Self-review checklist

- [ ] `CoordOrder`, `OutputFormat`, `FileType`, `CRS` derive `Copy`; the `main.rs` `crs` clone removed.
- [ ] All 5 `last_tag = … .to_vec()` sites replaced with `clear()` + `extend_from_slice()`.
- [ ] 2012 component lookup uses `get(attr.as_ref())` and clones only `(typ, teryt_id)`; behaviour identical.
- [ ] `AddressParser2021.teryt_names` is `Arc<…>`; the two 2021 entry points take `&Arc<…>`; `main` builds the `Arc` once; tests wrap in `Arc::new`.
- [ ] `cargo build`, `cargo test --lib`, `cargo test --test e2e`, `cargo fmt --all --check` all pass.

## Out of scope (noted, not planned here)

A larger optimisation — replacing the `last_tag: Vec<u8>` byte-matching with a small `Copy` enum discriminant set at the `Start` event — would remove the per-element `match last_tag.as_slice()` byte comparisons entirely, but it is a deeper rewrite of all five parse functions and is intentionally left out to keep these changes low-risk. Revisit if profiling shows tag-matching is still hot after Task 2.
