use std::path::PathBuf;
use std::process::Command;

const MODEL_2012_XML: &str = "fixtures/sample_model2012.xml";
const MODEL_2021_XML: &str = "fixtures/sample_model2021.xml";
const PRG_ZIP: &str = "fixtures/PRG-punkty_adresowe.zip";
const TERYT_XML: &str = "fixtures/TERC_Urzedowy_2025-11-18.xml";
const TERYT_ZIP: &str = "fixtures/TERC_Urzedowy_2025-11-18.zip";

fn bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_prg_convert"))
}

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn parse_row_count(stdout: &str) -> usize {
    stdout
        .lines()
        .find(|l| l.contains("Total addresses read"))
        .and_then(|l| l.split("Total addresses read ").nth(1))
        .and_then(|s| s.split('.').next())
        .and_then(|s| s.trim().parse::<usize>().ok())
        .expect("Could not parse row count from stdout")
}

fn run(
    schema_version: &str,
    crs_epsg: &str,
    output_format: &str,
    input_path: &str,
    expected_rows: usize,
    teryt_path: Option<&str>,
) {
    let ext = if output_format == "csv" { "csv" } else { "parquet" };
    let output_file = tempfile::Builder::new()
        .suffix(&format!(".{ext}"))
        .tempfile()
        .expect("Failed to create temp output file");

    let mut cmd = Command::new(bin());
    cmd.current_dir(manifest_dir())
        .arg("--schema-version")
        .arg(schema_version)
        .arg("--crs-epsg")
        .arg(crs_epsg)
        .arg("--output-format")
        .arg(output_format)
        .arg("--input-paths")
        .arg(input_path)
        .arg("--output-path")
        .arg(output_file.path());

    if let Some(teryt) = teryt_path {
        cmd.arg("--teryt-path").arg(teryt);
    }

    let result = cmd.output().expect("Failed to execute binary");

    if !result.status.success() {
        panic!(
            "Command failed:\nstdout: {}\nstderr: {}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr),
        );
    }

    let stdout = String::from_utf8_lossy(&result.stdout).to_string();
    let actual_rows = parse_row_count(&stdout);
    assert_eq!(
        actual_rows, expected_rows,
        "Row count mismatch. stdout:\n{stdout}"
    );

    let file_size = std::fs::metadata(output_file.path())
        .expect("Output file should exist")
        .len();
    assert!(file_size > 0, "Output file should be non-empty");

    if output_format == "csv" {
        let content =
            std::fs::read_to_string(output_file.path()).expect("Failed to read CSV file");
        let data_rows = content
            .lines()
            .filter(|l| !l.is_empty())
            .count()
            .saturating_sub(1); // subtract header row
        assert_eq!(
            data_rows, expected_rows,
            "CSV data row count mismatch. File content:\n{content}"
        );
    }
}

// --- Schema 2012, uncompressed XML (2 rows) ---

#[test]
fn test_e2e_schema2012_xml_csv_4326() {
    run("2012", "4326", "csv", MODEL_2012_XML, 2, None);
}

#[test]
fn test_e2e_schema2012_xml_csv_2180() {
    run("2012", "2180", "csv", MODEL_2012_XML, 2, None);
}

#[test]
fn test_e2e_schema2012_xml_geoparquet_4326() {
    run("2012", "4326", "geoparquet", MODEL_2012_XML, 2, None);
}

#[test]
fn test_e2e_schema2012_xml_geoparquet_2180() {
    run("2012", "2180", "geoparquet", MODEL_2012_XML, 2, None);
}

// --- Schema 2012, compressed ZIP (2 rows) ---

#[test]
fn test_e2e_schema2012_zip_csv_4326() {
    run("2012", "4326", "csv", PRG_ZIP, 2, None);
}

#[test]
fn test_e2e_schema2012_zip_csv_2180() {
    run("2012", "2180", "csv", PRG_ZIP, 2, None);
}

#[test]
fn test_e2e_schema2012_zip_geoparquet_4326() {
    run("2012", "4326", "geoparquet", PRG_ZIP, 2, None);
}

#[test]
fn test_e2e_schema2012_zip_geoparquet_2180() {
    run("2012", "2180", "geoparquet", PRG_ZIP, 2, None);
}

// --- Schema 2021, uncompressed XML (3 rows) ---

#[test]
fn test_e2e_schema2021_xml_csv_4326() {
    run("2021", "4326", "csv", MODEL_2021_XML, 3, Some(TERYT_XML));
}

#[test]
fn test_e2e_schema2021_xml_csv_2180() {
    run("2021", "2180", "csv", MODEL_2021_XML, 3, Some(TERYT_XML));
}

#[test]
fn test_e2e_schema2021_xml_geoparquet_4326() {
    run("2021", "4326", "geoparquet", MODEL_2021_XML, 3, Some(TERYT_XML));
}

#[test]
fn test_e2e_schema2021_xml_geoparquet_2180() {
    run("2021", "2180", "geoparquet", MODEL_2021_XML, 3, Some(TERYT_XML));
}

// --- Schema 2021, compressed ZIP (3 rows) ---

#[test]
fn test_e2e_schema2021_zip_csv_4326() {
    run("2021", "4326", "csv", PRG_ZIP, 3, Some(TERYT_XML));
}

#[test]
fn test_e2e_schema2021_zip_csv_2180() {
    run("2021", "2180", "csv", PRG_ZIP, 3, Some(TERYT_XML));
}

#[test]
fn test_e2e_schema2021_zip_geoparquet_4326() {
    run("2021", "4326", "geoparquet", PRG_ZIP, 3, Some(TERYT_XML));
}

#[test]
fn test_e2e_schema2021_zip_geoparquet_2180() {
    run("2021", "2180", "geoparquet", PRG_ZIP, 3, Some(TERYT_XML));
}

// --- Schema 2021, uncompressed XML + TERYT ZIP (3 rows) ---

#[test]
fn test_e2e_schema2021_xml_teryt_zip_csv_4326() {
    run("2021", "4326", "csv", MODEL_2021_XML, 3, Some(TERYT_ZIP));
}

#[test]
fn test_e2e_schema2021_xml_teryt_zip_csv_2180() {
    run("2021", "2180", "csv", MODEL_2021_XML, 3, Some(TERYT_ZIP));
}

#[test]
fn test_e2e_schema2021_xml_teryt_zip_geoparquet_4326() {
    run("2021", "4326", "geoparquet", MODEL_2021_XML, 3, Some(TERYT_ZIP));
}

#[test]
fn test_e2e_schema2021_xml_teryt_zip_geoparquet_2180() {
    run("2021", "2180", "geoparquet", MODEL_2021_XML, 3, Some(TERYT_ZIP));
}

// --- Schema 2021, compressed ZIP + TERYT ZIP (3 rows) ---

#[test]
fn test_e2e_schema2021_zip_teryt_zip_csv_4326() {
    run("2021", "4326", "csv", PRG_ZIP, 3, Some(TERYT_ZIP));
}

#[test]
fn test_e2e_schema2021_zip_teryt_zip_csv_2180() {
    run("2021", "2180", "csv", PRG_ZIP, 3, Some(TERYT_ZIP));
}

#[test]
fn test_e2e_schema2021_zip_teryt_zip_geoparquet_4326() {
    run("2021", "4326", "geoparquet", PRG_ZIP, 3, Some(TERYT_ZIP));
}

#[test]
fn test_e2e_schema2021_zip_teryt_zip_geoparquet_2180() {
    run("2021", "2180", "geoparquet", PRG_ZIP, 3, Some(TERYT_ZIP));
}
