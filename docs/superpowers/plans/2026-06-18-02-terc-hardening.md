# TERYT Module Hardening Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `src/terc.rs` robust and clean: a real error message for unsupported file extensions, XML-escaped SOAP credentials, the 112 KB inline base64 test fixture moved out of source, and `prepare_mapping_from_teryt` no longer panicking when TERYT rows arrive out of order.

**Architecture:** Four small, independent changes to one module plus one new fixture file. `prepare_mapping_from_teryt` changes from returning `HashMap` to returning `anyhow::Result<HashMap>`, so its two callers propagate the error with `?`.

**Tech Stack:** Rust 2024, `anyhow`, `quick-xml` (XML escaping + deserialization), `zip`, `base64`, `reqwest` (blocking).

## Global Constraints

- Rust edition 2024; `cargo fmt --all --check` must pass.
- Public function names that other modules call must keep working: `get_terc_mapping(&PathBuf) -> anyhow::Result<HashMap<String, Terc>>` and `download_terc_mapping(&str, &str) -> anyhow::Result<HashMap<String, Terc>>` keep their signatures. `prepare_mapping_from_teryt` is private to the module, so its signature may change.
- Do not change the SOAP request structure other than escaping the two credential values.

---

### Task 1: Better error for unsupported TERYT file extension

**Files:**
- Modify: `src/terc.rs:188-216` (`get_terc_mapping`, the `_ => { anyhow::bail!("") }` arm)

- [ ] **Step 1: Write a failing test asserting the message mentions the extension**

The existing `test_get_terc_mapping_unsupported_extension` only checks `is_err()`. Strengthen it. Replace it (currently near the bottom of `src/terc.rs`) with:

```rust
#[test]
fn test_get_terc_mapping_unsupported_extension() {
    let temp_file = tempfile::Builder::new()
        .suffix(".csv")
        .tempfile()
        .expect("Failed to create temp file");
    let result = get_terc_mapping(&temp_file.path().to_path_buf());
    assert!(result.is_err());
    let err = format!("{}", result.err().unwrap());
    assert!(
        err.contains("extension") && err.contains("csv"),
        "error message was: {}",
        err
    );
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --lib test_get_terc_mapping_unsupported_extension`
Expected: FAIL — the current empty `bail!("")` message contains neither "extension" nor "csv".

- [ ] **Step 3: Improve the error arm**

In `get_terc_mapping`, find:

```rust
        "zip" => parse_terc_zip_file(teryt_file),
        _ => {
            anyhow::bail!("")
        }
    }
```

Replace the `_` arm so it binds the extension and produces a useful message:

```rust
        "zip" => parse_terc_zip_file(teryt_file),
        other => {
            anyhow::bail!(
                "Unsupported TERYT file extension `{}` for `{}`. Expected `.xml` or `.zip`.",
                other,
                file_path.display()
            )
        }
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cargo test --lib test_get_terc_mapping_unsupported_extension`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add src/terc.rs
git commit -m "fix: descriptive error for unsupported TERYT file extension"
```

---

### Task 2: XML-escape SOAP credentials (injection fix)

**Files:**
- Modify: `src/terc.rs:123-152` (`download_terc_mapping`, the `payload` `format!`)

**Interfaces:**
- Consumes: `quick_xml::escape::escape` (returns `Cow<str>`).

- [ ] **Step 1: Write a unit test for the escaping**

The full `download_terc_mapping` hits the network, so test only the payload construction by extracting it into a pure helper. Add this helper next to `download_terc_mapping` in `src/terc.rs`:

```rust
fn build_terc_soap_payload(
    message_uuid: &uuid::Uuid,
    url: &str,
    api_username: &str,
    api_password: &str,
    todays_date: &str,
) -> String {
    use quick_xml::escape::escape;
    format!(
        r#"
<soap-env:Envelope xmlns:soap-env="http://schemas.xmlsoap.org/soap/envelope/">
  <soap-env:Header xmlns:wsa="http://www.w3.org/2005/08/addressing">
    <wsa:Action>http://tempuri.org/ITerytWs1/PobierzKatalogTERC</wsa:Action>
    <wsa:MessageID>urn:uuid:{}</wsa:MessageID>
    <wsa:To>{}</wsa:To>
    <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
      <wsse:UsernameToken>
        <wsse:Username>{}</wsse:Username>
        <wsse:Password Type="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-username-token-profile-1.0#PasswordText">{}</wsse:Password>
      </wsse:UsernameToken>
    </wsse:Security>
  </soap-env:Header>
  <soap-env:Body>
    <ns0:PobierzKatalogTERC xmlns:ns0="http://tempuri.org/">
      <ns0:DataStanu>{}</ns0:DataStanu>
    </ns0:PobierzKatalogTERC>
  </soap-env:Body>
</soap-env:Envelope>
    "#,
        message_uuid,
        url,
        escape(api_username),
        escape(api_password),
        todays_date
    )
}

#[test]
fn test_build_terc_soap_payload_escapes_credentials() {
    let uuid = uuid::Uuid::nil();
    let payload = build_terc_soap_payload(
        &uuid,
        "https://example.test",
        "user&<>\"'",
        "p@ss<word>",
        "2026-01-01",
    );
    // raw special characters must not appear inside the credential elements
    assert!(payload.contains("<wsse:Username>user&amp;&lt;&gt;"));
    assert!(payload.contains("p@ss&lt;word&gt;</wsse:Password>"));
    assert!(!payload.contains("user&<>"));
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --lib test_build_terc_soap_payload_escapes_credentials`
Expected: FAIL — `build_terc_soap_payload` does not exist yet (compile error).

- [ ] **Step 3: Make `download_terc_mapping` use the helper**

In `download_terc_mapping`, replace the inline `let payload = format!(r#"..."#, uuid, url, api_username, api_password, todays_date);` block (lines ~130-152) with:

```rust
    let payload = build_terc_soap_payload(&uuid, url, api_username, api_password, &todays_date);
```

Keep the surrounding lines (`let uuid = Uuid::new_v4();`, `let todays_date = ...`, and everything after `let client = ...`) unchanged.

- [ ] **Step 4: Run the test**

Run: `cargo test --lib test_build_terc_soap_payload_escapes_credentials`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add src/terc.rs
git commit -m "fix: XML-escape TERYT SOAP credentials to prevent injection"
```

---

### Task 3: Move the 112 KB inline base64 test fixture out of source

**Files:**
- Create: `fixtures/terc_api_response_sample.xml`
- Modify: `src/terc.rs:282-318` (`test_parse_api_response`)

**Background:** `src/terc.rs` line 300 is a single ~112 KB line — a base64-encoded ZIP inside a `<b:plik_zawartosc>` element, embedded in the `response_text` raw string literal of `test_parse_api_response`. It bloats the file and makes it unreadable. Move the whole SOAP response into a fixture file and `include_str!` it.

- [ ] **Step 1: Extract the raw response string into a fixture file**

Run this to write the literal contents of the `r#"..."#` string (lines 284-304, i.e. between `let response_text = r#"` and `"#.to_string();`) to a new fixture, preserving the giant base64 line exactly:

```bash
sed -n '284,304p' src/terc.rs > fixtures/terc_api_response_sample.xml
```

Verify the fixture starts with `<s:Envelope` and ends with `</s:Envelope>`, and that it is large (~112 KB):

```bash
head -c 60 fixtures/terc_api_response_sample.xml; echo; ls -l fixtures/terc_api_response_sample.xml
```

Expected: first line begins `\n<s:Envelope ...`; size on the order of 100+ KB.

- [ ] **Step 2: Replace the inline string in the test with `include_str!`**

In `src/terc.rs`, replace the start of `test_parse_api_response`:

```rust
fn test_parse_api_response() {
    let response_text = r#"
<s:Envelope ...
    ...giant base64...
</s:Envelope>
    "#.to_string();
    let bytes = get_file_content_from_response(&response_text).unwrap();
```

with:

```rust
fn test_parse_api_response() {
    let response_text = include_str!("../fixtures/terc_api_response_sample.xml").to_string();
    let bytes = get_file_content_from_response(&response_text).unwrap();
```

Leave the rest of the test (from `let mut file = tempfile()...` through the assertions) unchanged.

- [ ] **Step 3: Verify the giant line is gone from source**

Run: `awk '{ print length }' src/terc.rs | sort -n | tail -1`
Expected: a small number (a few hundred at most), not ~112000.

- [ ] **Step 4: Run the test**

Run: `cargo test --lib test_parse_api_response`
Expected: PASS — same assertions, now reading the fixture.

- [ ] **Step 5: Commit**

```bash
cargo fmt --all
git add fixtures/terc_api_response_sample.xml src/terc.rs
git commit -m "chore: move 112KB TERYT API response fixture out of source"
```

Note: confirm `fixtures/` is **not** gitignored (it is committed and used by CI — see `.github/workflows/checks.yaml` `paths: fixtures/**`). It is tracked, so `git add` works.

---

### Task 4: Make `prepare_mapping_from_teryt` order-independent and non-panicking

**Files:**
- Modify: `src/terc.rs:218-255` (`prepare_mapping_from_teryt`)
- Modify: `src/terc.rs:168` and `src/terc.rs:210` (the two call sites that consume its result)

**Background:** The function builds three maps in one pass and indexes `woj[&row.woj]` and `pow[&teryt_id[..4]]`. If a 7-digit municipality row appears before its 2-digit voivodeship or 4-digit county row, the `HashMap` index panics. The official file is ordered so it works today, but it is fragile. Fix by doing two passes (first collect voivodeship + county names, then build municipalities) and returning a `Result` with context instead of panicking.

**Interfaces:**
- Produces (changed): `fn prepare_mapping_from_teryt(teryt: Teryt) -> anyhow::Result<HashMap<String, Terc>>`.

- [ ] **Step 1: Write a failing test with out-of-order rows**

Add this test to the `#[test]` section of `src/terc.rs`. It builds a `Teryt` whose municipality row comes *before* its voivodeship/county rows:

```rust
#[test]
fn test_prepare_mapping_handles_out_of_order_rows() {
    fn row(woj: &str, pow: Option<&str>, gmi: Option<&str>, rodz: Option<&str>, nazwa: &str) -> Row {
        Row {
            woj: woj.to_string(),
            pow: pow.map(str::to_string),
            gmi: gmi.map(str::to_string),
            rodz: rodz.map(str::to_string),
            nazwa: nazwa.to_string(),
            nazwa_dod: String::new(),
            stan_na: "2026-01-01".to_string(),
        }
    }
    let teryt = Teryt {
        catalog: Catalog {
            name: "TERC".to_string(),
            catalog_type: "TERC".to_string(),
            date: "2026-01-01".to_string(),
            row: vec![
                // municipality first (7 digits worth of components), then county, then voivodeship
                row("02", Some("01"), Some("011"), Some("1"), "Bolesławiec"),
                row("02", Some("01"), None, None, "bolesławiecki"),
                row("02", None, None, None, "DOLNOŚLĄSKIE"),
            ],
        },
    };
    let mapping = prepare_mapping_from_teryt(teryt).expect("should not panic on out-of-order rows");
    let m = &mapping["0201011"];
    assert_eq!(m.municipality_name, "Bolesławiec");
    assert_eq!(m.county_teryt_id, "0201");
    assert_eq!(m.county_name, "bolesławiecki");
    assert_eq!(m.voivodeship_teryt_id, "02");
    assert_eq!(m.voivodeship_name, "dolnośląskie"); // lowercased
}
```

(This test needs `Teryt`, `Catalog`, `Row` to be visible to the test module — they are defined in the same file, so `use super::*;` or being in the same module already gives access. The `#[test]` functions in `terc.rs` are at file scope in the same module, so the structs are directly in scope.)

- [ ] **Step 2: Run it to verify it fails**

Run: `cargo test --lib test_prepare_mapping_handles_out_of_order_rows`
Expected: FAIL — currently panics with "no entry found for key" (the `woj[..]` / `pow[..]` index), or fails to compile because `prepare_mapping_from_teryt` returns `HashMap` not `Result` (the test calls `.expect(...)`). Either way it does not pass.

- [ ] **Step 3: Rewrite the function as two passes returning `Result`**

Replace the whole `prepare_mapping_from_teryt` function:

```rust
fn prepare_mapping_from_teryt(teryt: Teryt) -> anyhow::Result<HashMap<String, Terc>> {
    let mut woj = HashMap::new();
    let mut pow = HashMap::new();
    // First pass: collect voivodeship (2-digit) and county (4-digit) names.
    for row in &teryt.catalog.row {
        let teryt_id = [
            row.woj.clone(),
            row.pow.clone().unwrap_or_default(),
            row.gmi.clone().unwrap_or_default(),
            row.rodz.clone().unwrap_or_default(),
        ]
        .join("");
        match teryt_id.len() {
            2 => {
                // teryt dictionary stores these uppercase; previous PRG schema used lowercase
                woj.insert(teryt_id, row.nazwa.to_lowercase());
            }
            4 => {
                pow.insert(teryt_id, row.nazwa.clone());
            }
            7 => {} // handled in the second pass
            other => anyhow::bail!("Unrecognized teryt code length {} for code `{}`.", other, teryt_id),
        }
    }
    // Second pass: build municipality entries, now that woj/pow are fully populated.
    let mut mapping = HashMap::new();
    for row in &teryt.catalog.row {
        let teryt_id = [
            row.woj.clone(),
            row.pow.clone().unwrap_or_default(),
            row.gmi.clone().unwrap_or_default(),
            row.rodz.clone().unwrap_or_default(),
        ]
        .join("");
        if teryt_id.len() != 7 {
            continue;
        }
        let voivodeship_name = woj
            .get(&row.woj)
            .with_context(|| format!("No voivodeship name found for code `{}`.", row.woj))?
            .clone();
        let county_id = teryt_id[..4].to_string();
        let county_name = pow
            .get(&county_id)
            .with_context(|| format!("No county name found for code `{}`.", county_id))?
            .clone();
        mapping.insert(
            teryt_id.clone(),
            Terc {
                voivodeship_teryt_id: row.woj.clone(),
                voivodeship_name,
                county_teryt_id: county_id,
                county_name,
                municipality_name: row.nazwa.clone(),
            },
        );
    }
    Ok(mapping)
}
```

(`with_context` is from `anyhow::Context` — already imported at the top of `terc.rs`.)

- [ ] **Step 4: Update the two call sites to propagate the `Result`**

In `get_terc_mapping` (~line 210), change:

```rust
    let mapping = prepare_mapping_from_teryt(teryt);
```
to:
```rust
    let mapping = prepare_mapping_from_teryt(teryt)?;
```

In `download_terc_mapping` (~line 168), change:

```rust
    let mapping = prepare_mapping_from_teryt(teryt);
```
to:
```rust
    let mapping = prepare_mapping_from_teryt(teryt)?;
```

Also update `test_parse_api_response`, which calls it directly (~line 311):

```rust
    let teryt_mapping = prepare_mapping_from_teryt(teryt).unwrap();
```

- [ ] **Step 5: Build and run all terc tests**

Run: `cargo build && cargo test --lib --test e2e -- terc`
Then run the full module test set to be safe:
Run: `cargo test --lib`
Expected: PASS, including `get_terc_mapping_xml`, `get_terc_mapping_zip`, `test_parse_api_response`, and the new out-of-order test.

- [ ] **Step 6: Commit**

```bash
cargo fmt --all
git add src/terc.rs
git commit -m "fix: make prepare_mapping_from_teryt order-independent and fallible"
```

---

## Self-review checklist

- [ ] Unsupported-extension error names the extension and the path; its test asserts that.
- [ ] SOAP credentials pass through `quick_xml::escape::escape`; payload test passes.
- [ ] `src/terc.rs` longest line is now small; `fixtures/terc_api_response_sample.xml` exists and the API test reads it.
- [ ] `prepare_mapping_from_teryt` returns `Result`, does two passes, and all three callers use `?`/`.unwrap()` appropriately.
- [ ] `cargo test --lib`, `cargo test --test e2e`, `cargo fmt --all --check` all pass.
