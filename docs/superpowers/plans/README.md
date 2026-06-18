# prg_convert — Implementation Plans (2026-06-18)

Eight independent plans derived from the codebase analysis. Each plan is self-contained
and produces working, tested software on its own.

## Recommended implementation order

Some plans touch the same files; this order minimises rebase pain.

| # | Plan | Depends on | Touches |
|---|------|-----------|---------|
| 1 | [Timezone fix](2026-06-18-01-timezone-fix.md) | — | `common.rs`, `model2021.rs`, `Cargo.toml` |
| 2 | [TERYT hardening](2026-06-18-02-terc-hardening.md) | — | `terc.rs`, `fixtures/` |
| 5 | [TryFrom for ParsedArgs](2026-06-18-05-tryfrom-parsedargs.md) | — | `cli.rs` |
| 6 | [clap ValueEnum args](2026-06-18-06-clap-valueenum.md) | 5 | `cli.rs` |
| 3 | [OutputWriter enum](2026-06-18-03-output-writer-enum.md) | — | `main.rs`, `lib.rs` |
| 4 | [Decouple output format from parser](2026-06-18-04-decouple-output-format-from-parser.md) | 3 | `lib.rs`, `common.rs`, `model2012.rs`, `model2021.rs`, `main.rs` |
| 7 | [Feature gating](2026-06-18-07-feature-gating.md) | 3, 4 | `Cargo.toml`, `lib.rs`, `terc.rs`, `main.rs`, CI |
| 8 | [Performance](2026-06-18-08-performance.md) | 4 | `lib.rs`, `model2012.rs`, `model2021.rs` |

Plans 1, 2, 5, 3 are fully independent and can be done in any order / in parallel.
Plan 6 assumes 5 has landed. Plan 4 assumes 3. Plans 7 and 8 assume 4.

## Conventions used in every plan

- **Build:** `cargo build` (add `--all-features` after plan 7 lands).
- **Unit tests:** `cargo test --lib` (optionally a specific name, e.g. `cargo test --lib test_name`).
- **End-to-end tests:** `cargo test --test e2e` (these spawn the compiled binary).
- **Format:** `cargo fmt --all` before each commit; CI runs `cargo fmt --all --check`.
- **Lint (not yet enforced in CI):** `cargo clippy --all-targets --all-features`.
- Conventional-commit style messages (`feat:`, `fix:`, `refactor:`, `test:`, `chore:`).
- Co-author trailer is optional; follow the repo's existing commit style.

The project is Rust 2024 edition. The binary target `prg_convert` requires the `cli` feature
(already the default). Tests live both inline (`#[cfg(test)] mod tests`) and in `tests/e2e.rs`.
