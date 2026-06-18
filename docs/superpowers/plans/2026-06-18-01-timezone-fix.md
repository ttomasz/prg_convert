# Timezone Fix (model 2021 `poczatekWersjiObiektu`) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Convert the naive `poczatekWersjiObiektu` datetime (PRG schema 2021) to UTC using the real `Europe/Warsaw` timezone instead of a hardcoded `+02:00` offset, so winter (CET / `+01:00`) timestamps are correct.

**Architecture:** Add one small, unit-tested helper in `common.rs` that interprets a `NaiveDateTime` as `Europe/Warsaw` wall-clock time and returns UTC epoch milliseconds. Call it from `model2021.rs`. Pull the IANA timezone database via the `chrono-tz` crate.

**Tech Stack:** Rust 2024, `chrono` 0.4, new dep `chrono-tz`, `arrow` builders.

## Global Constraints

- Rust edition 2024.
- Do **not** change `wersjaId` parsing. `wersjaId` carries an explicit offset in the source file (GUGiK always writes `+02:00`, even in winter); we honour the stated offset. Only the **naive** `poczatekWersjiObiektu` field changes.
- The 2012 parser (`model2012.rs`) already uses `parse_from_rfc3339` (offset-aware) for `poczatekWersjiObiektu` — leave it untouched.
- `cargo fmt --all --check` must pass.

### Background facts the implementer must know

- `fixtures/sample_model2021.xml` contains records dated `2025-10-14` (summer, CEST `+02:00`), `2025-11-06` (**winter**, CET `+01:00`) and `2017-04-13` (summer). Poland's DST runs last Sunday of March → last Sunday of October.
- Currently the code interprets `poczatekWersjiObiektu` as `+02:00` always. After this fix, **only the `2025-11-06` (winter) records change**: their UTC value increases by exactly `3_600_000` ms (one hour), because CET is `+01:00` not `+02:00`.
- This means after the fix, `poczatek_wersji_obiektu` will no longer equal `wersja_id` for those winter rows. That is expected and correct.

---

### Task 1: Add `chrono-tz` dependency and the `warsaw_naive_to_utc_millis` helper

**Files:**
- Modify: `Cargo.toml` (add dependency)
- Modify: `src/common.rs` (add helper + unit tests, near the other helpers ~line 100–140)

**Interfaces:**
- Produces: `pub fn warsaw_naive_to_utc_millis(naive: chrono::NaiveDateTime) -> anyhow::Result<i64>` in module `crate::common`. Returns UTC epoch **milliseconds**.

- [ ] **Step 1: Add the dependency**

In `Cargo.toml`, under `[dependencies]`, add:

```toml
chrono-tz = "0.10"
```

Note: if `cargo tree -i chrono-tz` later shows two different `chrono-tz` versions (one pulled by `arrow`), align this version to match arrow's to avoid a duplicate build. Not required for correctness.

- [ ] **Step 2: Write the failing tests**

Add to the bottom of `src/common.rs` (these are inline `#[test]` functions like the others already in the file):

```rust
#[test]
fn test_warsaw_naive_to_utc_millis_summer() {
    // 2025-10-14 14:04:04 Warsaw is CEST (+02:00) -> 12:04:04 UTC
    let naive = chrono::NaiveDate::from_ymd_opt(2025, 10, 14)
        .unwrap()
        .and_hms_opt(14, 4, 4)
        .unwrap();
    let expected = chrono::DateTime::parse_from_rfc3339("2025-10-14T12:04:04Z")
        .unwrap()
        .timestamp()
        * 1000;
    assert_eq!(warsaw_naive_to_utc_millis(naive).unwrap(), expected);
}

#[test]
fn test_warsaw_naive_to_utc_millis_winter() {
    // 2025-11-06 15:01:26 Warsaw is CET (+01:00) -> 14:01:26 UTC
    let naive = chrono::NaiveDate::from_ymd_opt(2025, 11, 6)
        .unwrap()
        .and_hms_opt(15, 1, 26)
        .unwrap();
    let expected = chrono::DateTime::parse_from_rfc3339("2025-11-06T14:01:26Z")
        .unwrap()
        .timestamp()
        * 1000;
    assert_eq!(warsaw_naive_to_utc_millis(naive).unwrap(), expected);
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --lib warsaw_naive_to_utc_millis`
Expected: FAIL — compile error `cannot find function warsaw_naive_to_utc_millis`.

- [ ] **Step 4: Implement the helper**

Add these imports near the top of `src/common.rs` (the file already imports several `chrono` items):

```rust
use chrono::NaiveDateTime;
use chrono::TimeZone;
use chrono_tz::Europe::Warsaw;
```

Add the function (place it just after `parse_gml_pos`, before the `#[test]` block):

```rust
/// Interpret a naive datetime as `Europe/Warsaw` wall-clock time and return the
/// corresponding UTC instant as epoch milliseconds.
///
/// Uses the IANA tz database so winter (CET, +01:00) and summer (CEST, +02:00)
/// are handled correctly. On an ambiguous local time (the autumn DST overlap)
/// the earlier of the two instants is chosen; a non-existent local time (the
/// spring-forward gap) returns an error.
pub fn warsaw_naive_to_utc_millis(naive: NaiveDateTime) -> anyhow::Result<i64> {
    let dt = Warsaw
        .from_local_datetime(&naive)
        .earliest()
        .with_context(|| {
            format!(
                "Local time `{}` does not exist in Europe/Warsaw (DST spring-forward gap).",
                naive
            )
        })?;
    Ok(dt.timestamp() * 1000)
}
```

(`with_context` comes from `anyhow::Context`, already imported at the top of `common.rs`.)

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib warsaw_naive_to_utc_millis`
Expected: PASS (2 tests).

- [ ] **Step 6: Format and commit**

```bash
cargo fmt --all
git add Cargo.toml Cargo.lock src/common.rs
git commit -m "feat: add Europe/Warsaw naive->UTC helper"
```

---

### Task 2: Use the helper in the 2021 parser

**Files:**
- Modify: `src/model2021.rs:579-596` (the `b"prgad:poczatekWersjiObiektu"` match arm inside `parse_address`)

**Interfaces:**
- Consumes: `crate::common::warsaw_naive_to_utc_millis`.

- [ ] **Step 1: Replace the hardcoded-offset block**

Find this block in `src/model2021.rs` (inside `parse_address`, the `Ok(Event::Text(e))` handler):

```rust
                        b"prgad:poczatekWersjiObiektu" => {
                            if text_trimmed.is_empty() {
                                self.lifecycle_start_date.append_null();
                            } else {
                                let dt = NaiveDateTime::parse_from_str(
                                    &text_trimmed,
                                    "%Y-%m-%dT%H:%M:%S",
                                )
                                .expect("Failed to parse datetime")
                                .and_local_timezone(
                                    chrono::FixedOffset::east_opt(2 * 60 * 60).unwrap(),
                                ) // assume +02:00 tz
                                .unwrap()
                                .to_utc();
                                self.lifecycle_start_date
                                    .append_value(dt.timestamp() * 1000);
                            }
                        }
```

Replace it with:

```rust
                        b"prgad:poczatekWersjiObiektu" => {
                            if text_trimmed.is_empty() {
                                self.lifecycle_start_date.append_null();
                            } else {
                                let naive = NaiveDateTime::parse_from_str(
                                    text_trimmed,
                                    "%Y-%m-%dT%H:%M:%S",
                                )
                                .expect("Failed to parse datetime");
                                let millis =
                                    crate::common::warsaw_naive_to_utc_millis(naive)
                                        .expect("Failed to convert Warsaw local time to UTC");
                                self.lifecycle_start_date.append_value(millis);
                            }
                        }
```

- [ ] **Step 2: Build**

Run: `cargo build`
Expected: success. If the compiler warns that `chrono::FixedOffset` or `chrono::DateTime` is now unused in `model2021.rs`, remove only the now-unused import lines (do not remove imports still used elsewhere in the file — `DateTime` is still used for `wersjaId`).

- [ ] **Step 3: Run the 2021 parser tests (expect a known failure to fix next)**

Run: `cargo test --lib test_address_parser_2021_zip_csv`
Expected: FAIL on `expected_poczatek_wersji_obiektu`. The failure prints `left` (actual) and `right` (expected) arrays differing only at the index of the `2025-11-06` row.

- [ ] **Step 4: Update the test expectation**

In `src/lib.rs`, inside `test_address_parser_2021_zip_csv`, find:

```rust
        let expected_poczatek_wersji_obiektu =
            &TimestampMillisecondArray::from(vec![1760443546000, 1762434168000, 1492090215000])
                .with_timezone(Arc::from("UTC"));
```

Change **only the middle value** `1762434168000` to `1762437768000` (that is `+3_600_000`):

```rust
        let expected_poczatek_wersji_obiektu =
            &TimestampMillisecondArray::from(vec![1760443546000, 1762437768000, 1492090215000])
                .with_timezone(Arc::from("UTC"));
```

**Verification rule (do not skip):** the only changed value must differ from the old by exactly `3_600_000` ms, and the corresponding source record's `poczatekWersjiObiektu` must fall in the winter window (last Sun Oct → last Sun Mar). Leave `expected_wersja_id` **unchanged** — `wersjaId` is offset-aware and is not affected. Any other delta means a real bug; stop and investigate.

- [ ] **Step 5: Re-run all parser tests**

Run: `cargo test --lib`
Expected: PASS. (`test_address_parser_2012_*` are unaffected; the 2012 path was not changed.)

- [ ] **Step 6: Confirm e2e is unaffected**

Run: `grep -n "poczatek\|wersja_id\|Timestamp" tests/e2e.rs`
Expected: no matches (the e2e test asserts ids/names/coordinates, not timestamps). Then:

Run: `cargo test --test e2e`
Expected: PASS. If e2e *does* assert a 2021 winter timestamp, apply the same `+3_600_000` correction there.

- [ ] **Step 7: Format and commit**

```bash
cargo fmt --all
git add src/model2021.rs src/lib.rs
git commit -m "fix: use Europe/Warsaw timezone for 2021 poczatekWersjiObiektu"
```

---

## Self-review checklist

- [ ] `chrono-tz` added to `Cargo.toml`; `Cargo.lock` updated.
- [ ] Helper has summer + winter unit tests, both passing.
- [ ] `model2021.rs` calls the helper; unused `chrono` imports removed.
- [ ] Only `expected_poczatek_wersji_obiektu`'s winter element changed (+3,600,000); `expected_wersja_id` untouched.
- [ ] `cargo test --lib` and `cargo test --test e2e` both pass.
- [ ] `cargo fmt --all --check` passes.
