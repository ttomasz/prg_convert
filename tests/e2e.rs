use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::Command;

use arrow::array::{Float64Array, StringArray};
use arrow::compute::concat_batches;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

const MODEL_2012_XML: &str = "fixtures/sample_model2012.xml";
const MODEL_2021_XML: &str = "fixtures/sample_model2021.xml";
const PRG_ZIP: &str = "fixtures/PRG-punkty_adresowe.zip";
const TERYT_XML: &str = "fixtures/TERC_Urzedowy_2025-11-18.xml";
const TERYT_ZIP: &str = "fixtures/TERC_Urzedowy_2025-11-18.zip";

// Tolerances for floating-point comparison
const COORD_TOLERANCE_2180: f64 = 0.01; // 1cm, in metres
const COORD_TOLERANCE_4326: f64 = 1e-6; // ~0.1m

struct ExpectedRow {
    lokalny_id: &'static str,
    numer_porzadkowy: &'static str,
    miejscowosc: &'static str,
    ulica: Option<&'static str>,
    kod_pocztowy: &'static str,
    gmina: &'static str,
    /// x_epsg_2180 column in CSV
    x_epsg_2180: f64,
    /// y_epsg_2180 column in CSV
    y_epsg_2180: f64,
    /// dlugosc_geograficzna column (longitude, EPSG:4326)
    lon: f64,
    /// szerokosc_geograficzna column (latitude, EPSG:4326)
    lat: f64,
}

// 2 rows from sample_model2012.xml and from PRG-punkty_adresowe.zip (schema 2012 XML inside)
const EXPECTED_2012: &[ExpectedRow] = &[
    ExpectedRow {
        lokalny_id: "fd9c9319-0a6a-44b4-972a-1e6c4ec0d4ca",
        numer_porzadkowy: "2",
        miejscowosc: "Konotop",
        ulica: Some("Podgórna"),
        kod_pocztowy: "67-416",
        gmina: "Kolsko",
        x_epsg_2180: 287772.37,
        y_epsg_2180: 456005.140000001,
        lon: 15.9121240698886,
        lat: 51.92977532639213,
    },
    ExpectedRow {
        lokalny_id: "5baa8bef-75ef-4241-a2fe-9d4137845693",
        numer_porzadkowy: "1",
        miejscowosc: "Konotop",
        ulica: Some("Podgórna"),
        kod_pocztowy: "67-416",
        gmina: "Kolsko",
        x_epsg_2180: 287751.0102,
        y_epsg_2180: 456027.7794,
        lon: 15.911799807186908,
        lat: 51.92997049675426,
    },
];

// 3 rows from sample_model2021.xml and from PRG-punkty_adresowe.zip (schema 2021 GML inside)
const EXPECTED_2021: &[ExpectedRow] = &[
    ExpectedRow {
        lokalny_id: "7343b2d2-c2ac-4951-ae9a-fe1932ffecfb",
        numer_porzadkowy: "21A",
        miejscowosc: "Żubrów",
        ulica: None,
        kod_pocztowy: "69-200",
        gmina: "Sulęcin",
        x_epsg_2180: 238651.83,
        y_epsg_2180: 519741.27,
        lon: 15.149797186509767,
        lat: 52.48080576032958,
    },
    ExpectedRow {
        lokalny_id: "07bcb481-4975-4c77-ab58-c8e4b9e05362",
        numer_porzadkowy: "1A",
        miejscowosc: "Rzepin",
        ulica: Some("Inwalidów Wojennych"),
        kod_pocztowy: "69-110",
        gmina: "Rzepin",
        x_epsg_2180: 216691.39,
        y_epsg_2180: 505645.69,
        lon: 14.839103470789498,
        lat: 52.3434219342925,
    },
    ExpectedRow {
        lokalny_id: "e4ed4971-15f6-473d-b9a4-e9e12e602f6e",
        numer_porzadkowy: "2A",
        miejscowosc: "Lubniewice",
        ulica: Some("Plac Kasztanowy"),
        kod_pocztowy: "69-210",
        gmina: "Lubniewice",
        x_epsg_2180: 245250.11,
        y_epsg_2180: 522957.46,
        lon: 15.24431221852159,
        lat: 52.51278706040695,
    },
];

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

/// Parse a CSV file and check key fields against expected rows.
/// Null values are written by the arrow CSV writer as empty strings.
fn validate_csv(path: &Path, expected: &[ExpectedRow]) {
    let content = std::fs::read_to_string(path).expect("Failed to read CSV file");
    let mut lines = content.lines().filter(|l| !l.is_empty());

    let header = lines.next().expect("CSV must have a header row");
    let col_idx: HashMap<&str, usize> = header
        .split(',')
        .enumerate()
        .map(|(i, name)| (name, i))
        .collect();

    let data_rows: Vec<Vec<&str>> = lines.map(|l| l.split(',').collect()).collect();
    assert_eq!(data_rows.len(), expected.len(), "CSV row count mismatch");

    for (i, (row, exp)) in data_rows.iter().zip(expected).enumerate() {
        let get = |col: &str| row[col_idx[col]];
        let parse_f64 = |col: &str| -> f64 {
            get(col)
                .parse()
                .unwrap_or_else(|_| panic!("row {i}: failed to parse {col} as f64: '{}'", get(col)))
        };

        assert_eq!(get("lokalny_id"), exp.lokalny_id, "row {i} lokalny_id");
        assert_eq!(
            get("numer_porzadkowy"),
            exp.numer_porzadkowy,
            "row {i} numer_porzadkowy"
        );
        assert_eq!(get("miejscowosc"), exp.miejscowosc, "row {i} miejscowosc");
        let ulica_val = get("ulica");
        assert_eq!(
            if ulica_val.is_empty() {
                None
            } else {
                Some(ulica_val)
            },
            exp.ulica,
            "row {i} ulica"
        );
        assert_eq!(
            get("kod_pocztowy"),
            exp.kod_pocztowy,
            "row {i} kod_pocztowy"
        );
        assert_eq!(get("gmina"), exp.gmina, "row {i} gmina");

        let x = parse_f64("x_epsg_2180");
        let y = parse_f64("y_epsg_2180");
        let lon = parse_f64("dlugosc_geograficzna");
        let lat = parse_f64("szerokosc_geograficzna");

        assert!(
            (x - exp.x_epsg_2180).abs() < COORD_TOLERANCE_2180,
            "row {i} x_epsg_2180: {x} vs {}",
            exp.x_epsg_2180
        );
        assert!(
            (y - exp.y_epsg_2180).abs() < COORD_TOLERANCE_2180,
            "row {i} y_epsg_2180: {y} vs {}",
            exp.y_epsg_2180
        );
        assert!(
            (lon - exp.lon).abs() < COORD_TOLERANCE_4326,
            "row {i} dlugosc_geograficzna: {lon} vs {}",
            exp.lon
        );
        assert!(
            (lat - exp.lat).abs() < COORD_TOLERANCE_4326,
            "row {i} szerokosc_geograficzna: {lat} vs {}",
            exp.lat
        );
    }
}

/// Read a GeoParquet file and check key fields against expected rows.
/// Geometry is validated as non-null; EPSG:4326 float columns are checked
/// for value; the geometry column's CRS is not decoded here.
fn validate_geoparquet(path: &Path, expected: &[ExpectedRow]) {
    let file = std::fs::File::open(path).expect("Failed to open GeoParquet file");
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)
        .expect("Failed to create parquet reader builder")
        .build()
        .expect("Failed to build parquet reader");
    let batches: Vec<_> = reader
        .collect::<Result<_, _>>()
        .expect("Failed to read parquet record batches");

    assert!(!batches.is_empty(), "GeoParquet file contains no batches");
    let schema = batches[0].schema();
    let batch = concat_batches(&schema, &batches).expect("Failed to concat batches");

    assert_eq!(
        batch.num_rows(),
        expected.len(),
        "GeoParquet row count mismatch"
    );

    let geometry = batch
        .column_by_name("geometry")
        .expect("Expected geometry column");
    assert_eq!(
        geometry.null_count(),
        0,
        "Geometry column must have no nulls"
    );

    let lokalny_id: &StringArray = batch
        .column_by_name("lokalny_id")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    let numer_porzadkowy: &StringArray = batch
        .column_by_name("numer_porzadkowy")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    let miejscowosc: &StringArray = batch
        .column_by_name("miejscowosc")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    let gmina: &StringArray = batch
        .column_by_name("gmina")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    let dlugosc: &Float64Array = batch
        .column_by_name("dlugosc_geograficzna")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();
    let szerokosc: &Float64Array = batch
        .column_by_name("szerokosc_geograficzna")
        .unwrap()
        .as_any()
        .downcast_ref()
        .unwrap();

    for (i, exp) in expected.iter().enumerate() {
        assert_eq!(lokalny_id.value(i), exp.lokalny_id, "row {i} lokalny_id");
        assert_eq!(
            numer_porzadkowy.value(i),
            exp.numer_porzadkowy,
            "row {i} numer_porzadkowy"
        );
        assert_eq!(miejscowosc.value(i), exp.miejscowosc, "row {i} miejscowosc");
        assert_eq!(gmina.value(i), exp.gmina, "row {i} gmina");
        assert!(
            (dlugosc.value(i) - exp.lon).abs() < COORD_TOLERANCE_4326,
            "row {i} dlugosc_geograficzna: {} vs {}",
            dlugosc.value(i),
            exp.lon
        );
        assert!(
            (szerokosc.value(i) - exp.lat).abs() < COORD_TOLERANCE_4326,
            "row {i} szerokosc_geograficzna: {} vs {}",
            szerokosc.value(i),
            exp.lat
        );
    }
}

fn run(
    schema_version: &str,
    crs_epsg: &str,
    output_format: &str,
    input_path: &str,
    expected: &[ExpectedRow],
    teryt_path: Option<&str>,
) {
    let ext = if output_format == "csv" {
        "csv"
    } else {
        "parquet"
    };
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
        actual_rows,
        expected.len(),
        "Row count in stdout mismatch. stdout:\n{stdout}"
    );

    if output_format == "csv" {
        validate_csv(output_file.path(), expected);
    } else {
        validate_geoparquet(output_file.path(), expected);
    }
}

// --- Schema 2012, uncompressed XML ---

#[test]
fn test_e2e_schema2012_xml_csv_4326() {
    run("2012", "4326", "csv", MODEL_2012_XML, EXPECTED_2012, None);
}

#[test]
fn test_e2e_schema2012_xml_csv_2180() {
    run("2012", "2180", "csv", MODEL_2012_XML, EXPECTED_2012, None);
}

#[test]
fn test_e2e_schema2012_xml_geoparquet_4326() {
    run(
        "2012",
        "4326",
        "geoparquet",
        MODEL_2012_XML,
        EXPECTED_2012,
        None,
    );
}

#[test]
fn test_e2e_schema2012_xml_geoparquet_2180() {
    run(
        "2012",
        "2180",
        "geoparquet",
        MODEL_2012_XML,
        EXPECTED_2012,
        None,
    );
}

// --- Schema 2012, compressed ZIP ---

#[test]
fn test_e2e_schema2012_zip_csv_4326() {
    run("2012", "4326", "csv", PRG_ZIP, EXPECTED_2012, None);
}

#[test]
fn test_e2e_schema2012_zip_csv_2180() {
    run("2012", "2180", "csv", PRG_ZIP, EXPECTED_2012, None);
}

#[test]
fn test_e2e_schema2012_zip_geoparquet_4326() {
    run("2012", "4326", "geoparquet", PRG_ZIP, EXPECTED_2012, None);
}

#[test]
fn test_e2e_schema2012_zip_geoparquet_2180() {
    run("2012", "2180", "geoparquet", PRG_ZIP, EXPECTED_2012, None);
}

// --- Schema 2021, uncompressed XML + TERYT XML ---

#[test]
fn test_e2e_schema2021_xml_csv_4326() {
    run(
        "2021",
        "4326",
        "csv",
        MODEL_2021_XML,
        EXPECTED_2021,
        Some(TERYT_XML),
    );
}

#[test]
fn test_e2e_schema2021_xml_csv_2180() {
    run(
        "2021",
        "2180",
        "csv",
        MODEL_2021_XML,
        EXPECTED_2021,
        Some(TERYT_XML),
    );
}

#[test]
fn test_e2e_schema2021_xml_geoparquet_4326() {
    run(
        "2021",
        "4326",
        "geoparquet",
        MODEL_2021_XML,
        EXPECTED_2021,
        Some(TERYT_XML),
    );
}

#[test]
fn test_e2e_schema2021_xml_geoparquet_2180() {
    run(
        "2021",
        "2180",
        "geoparquet",
        MODEL_2021_XML,
        EXPECTED_2021,
        Some(TERYT_XML),
    );
}

// --- Schema 2021, compressed ZIP + TERYT XML ---

#[test]
fn test_e2e_schema2021_zip_csv_4326() {
    run(
        "2021",
        "4326",
        "csv",
        PRG_ZIP,
        EXPECTED_2021,
        Some(TERYT_XML),
    );
}

#[test]
fn test_e2e_schema2021_zip_csv_2180() {
    run(
        "2021",
        "2180",
        "csv",
        PRG_ZIP,
        EXPECTED_2021,
        Some(TERYT_XML),
    );
}

#[test]
fn test_e2e_schema2021_zip_geoparquet_4326() {
    run(
        "2021",
        "4326",
        "geoparquet",
        PRG_ZIP,
        EXPECTED_2021,
        Some(TERYT_XML),
    );
}

#[test]
fn test_e2e_schema2021_zip_geoparquet_2180() {
    run(
        "2021",
        "2180",
        "geoparquet",
        PRG_ZIP,
        EXPECTED_2021,
        Some(TERYT_XML),
    );
}

// --- Schema 2021, uncompressed XML + TERYT ZIP ---

#[test]
fn test_e2e_schema2021_xml_teryt_zip_csv_4326() {
    run(
        "2021",
        "4326",
        "csv",
        MODEL_2021_XML,
        EXPECTED_2021,
        Some(TERYT_ZIP),
    );
}

#[test]
fn test_e2e_schema2021_xml_teryt_zip_csv_2180() {
    run(
        "2021",
        "2180",
        "csv",
        MODEL_2021_XML,
        EXPECTED_2021,
        Some(TERYT_ZIP),
    );
}

#[test]
fn test_e2e_schema2021_xml_teryt_zip_geoparquet_4326() {
    run(
        "2021",
        "4326",
        "geoparquet",
        MODEL_2021_XML,
        EXPECTED_2021,
        Some(TERYT_ZIP),
    );
}

#[test]
fn test_e2e_schema2021_xml_teryt_zip_geoparquet_2180() {
    run(
        "2021",
        "2180",
        "geoparquet",
        MODEL_2021_XML,
        EXPECTED_2021,
        Some(TERYT_ZIP),
    );
}

// --- Schema 2021, compressed ZIP + TERYT ZIP ---

#[test]
fn test_e2e_schema2021_zip_teryt_zip_csv_4326() {
    run(
        "2021",
        "4326",
        "csv",
        PRG_ZIP,
        EXPECTED_2021,
        Some(TERYT_ZIP),
    );
}

#[test]
fn test_e2e_schema2021_zip_teryt_zip_csv_2180() {
    run(
        "2021",
        "2180",
        "csv",
        PRG_ZIP,
        EXPECTED_2021,
        Some(TERYT_ZIP),
    );
}

#[test]
fn test_e2e_schema2021_zip_teryt_zip_geoparquet_4326() {
    run(
        "2021",
        "4326",
        "geoparquet",
        PRG_ZIP,
        EXPECTED_2021,
        Some(TERYT_ZIP),
    );
}

#[test]
fn test_e2e_schema2021_zip_teryt_zip_geoparquet_2180() {
    run(
        "2021",
        "2180",
        "geoparquet",
        PRG_ZIP,
        EXPECTED_2021,
        Some(TERYT_ZIP),
    );
}
