# `TryFrom<RawArgs> for ParsedArgs` Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the backwards `impl TryInto<ParsedArgs> for RawArgs` with the idiomatic `impl TryFrom<RawArgs> for ParsedArgs`. Callers that use `.try_into()` keep working unchanged (the standard library provides a blanket `TryInto` for any `TryFrom`).

**Architecture:** Pure mechanical refactor of one `impl` block in `cli.rs`. The function body is unchanged except the receiver renames from `self` to `value`.

**Tech Stack:** Rust 2024, `anyhow`.

## Global Constraints

- Rust edition 2024; `cargo fmt --all --check` must pass.
- No behaviour change. Every existing test must keep passing without modification.
- Do this **before** plan #6 (clap ValueEnum), which edits the same `impl` body.

---

### Task 1: Convert the impl direction

**Files:**
- Modify: `src/cli.rs:367-521` (the `impl TryInto<ParsedArgs> for RawArgs` block)

- [ ] **Step 1: Change the impl header and function signature**

Find:

```rust
impl TryInto<ParsedArgs> for RawArgs {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<ParsedArgs> {
```

Replace with:

```rust
impl TryFrom<RawArgs> for ParsedArgs {
    type Error = anyhow::Error;

    fn try_from(value: RawArgs) -> anyhow::Result<ParsedArgs> {
```

- [ ] **Step 2: Rename every `self.` to `value.` inside this function only**

Within the body of `try_from` (everything between the `{` after the signature and its matching closing `}` — roughly lines 370-520), replace each occurrence of `self.` with `value.`. In this function `self` is only ever the `RawArgs` being consumed, so every `self.field` becomes `value.field`.

Concretely, these field accesses must all change (this is the complete list in the current body): `self.batch_size`, `self.download_data` (×3), `self.input_paths` (×2 — the `!self.input_paths.is_empty()` check and the `input_paths: self.input_paths` in the returned struct), `self.teryt_download`, `self.schema_version` (several `.to_lowercase()` comparisons and the final match), `self.teryt_path` (×2), `self.teryt_api_username`, `self.teryt_api_password`, `self.output_format`, `self.parquet_compression` (several), `self.compression_level`, `self.parquet_row_group_size`, `self.parquet_version`, `self.crs_epsg`, `self.output_path`.

After editing, verify none remain:

Run: `awk 'NR>=367 && NR<=525' src/cli.rs | grep -n 'self\.'`
Expected: no output. (Line numbers shift as you edit; re-check the whole `impl` block.)

- [ ] **Step 3: Build**

Run: `cargo build`
Expected: success. `main.rs` uses `let mut parsed_args: cli::ParsedArgs = args.try_into().expect(...)`, and the `cli.rs` tests use `let result: anyhow::Result<ParsedArgs> = args.try_into();` — both continue to compile because `TryInto` is auto-derived from `TryFrom`.

- [ ] **Step 4: Run the full test suite (no test changes needed)**

Run: `cargo test --lib && cargo test --test e2e`
Expected: PASS — all existing `cli.rs` unit tests (`test_try_into_*`) and the e2e tests still pass.

- [ ] **Step 5: Format and commit**

```bash
cargo fmt --all
git add src/cli.rs
git commit -m "refactor: implement TryFrom<RawArgs> for ParsedArgs instead of TryInto"
```

---

## Self-review checklist

- [ ] The `impl` reads `impl TryFrom<RawArgs> for ParsedArgs` with `fn try_from(value: RawArgs)`.
- [ ] No `self.` remains inside the converted function.
- [ ] `main.rs` and the `cli.rs` tests are unchanged and still compile/pass.
- [ ] `cargo test --lib`, `cargo test --test e2e`, `cargo fmt --all --check` all pass.

> Optional follow-up (clippy): once `cargo clippy` is enabled in CI (see plan #7), this change clears the `clippy::wrong_self_convention` / manual-`TryInto` lint.
