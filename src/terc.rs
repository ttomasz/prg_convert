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

#[derive(Deserialize)]
struct Teryt {
    pub catalog: Catalog,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct Catalog {
    #[serde(rename = "@name")]
    pub name: String,
    #[serde(rename = "@type")]
    pub catalog_type: String,
    #[serde(rename = "@date")]
    pub date: String,
    pub row: Vec<Row>,
}

#[allow(dead_code)]
#[derive(Deserialize)]
struct Row {
    #[serde(rename = "WOJ")]
    pub woj: String,
    #[serde(rename = "POW")]
    pub pow: Option<String>,
    #[serde(rename = "GMI")]
    pub gmi: Option<String>,
    #[serde(rename = "RODZ")]
    pub rodz: Option<String>,
    #[serde(rename = "NAZWA")]
    pub nazwa: String,
    #[serde(rename = "NAZWA_DOD")]
    pub nazwa_dod: String,
    #[serde(rename = "STAN_NA")]
    pub stan_na: String,
}

#[derive(Deserialize)]
struct PobierzKatalogTERCResult {
    /// Contains Base64 encoded zip file
    pub plik_zawartosc: String,
}

#[derive(Deserialize)]
struct Envelope {
    #[serde(rename = "Body")]
    body: Body,
}

#[derive(Deserialize)]
struct Body {
    #[serde(rename = "PobierzKatalogTERCResponse")]
    response: PobierzKatalogTERCResponse,
}

#[derive(Deserialize)]
struct PobierzKatalogTERCResponse {
    #[serde(rename = "PobierzKatalogTERCResult")]
    result: PobierzKatalogTERCResult,
}

#[derive(Clone)]
pub struct Terc {
    pub voivodeship_teryt_id: String,
    pub voivodeship_name: String,
    pub county_teryt_id: String,
    pub county_name: String,
    pub municipality_name: String,
}

fn parse_terc_zip_file(teryt_file: std::fs::File) -> anyhow::Result<Teryt> {
    let mut archive =
        ZipArchive::new(teryt_file).with_context(|| "Failed to decompress TERC ZIP file.")?;
    let mut idx_to_read: isize = -1;
    for idx in 0..archive.len() {
        let entry = archive
            .by_index(idx)
            .with_context(|| "Could not access file inside ZIP archive")?;
        let name = entry
            .enclosed_name()
            .with_context(|| "Could not read file name inside ZIP archive.")?;
        // for now we'll determine if the file inside zip should be processed based on extension
        let file_extension = &name
            .extension()
            .expect("Could not read file extension.")
            .to_string_lossy()
            .to_lowercase();
        if file_extension == "xml" {
            idx_to_read = idx as isize;
        }
    }
    if idx_to_read == -1 {
        anyhow::bail!("Did not find XML file in ZIP archive with TERYT data.")
    }
    let f = archive
        .by_index(idx_to_read as usize)
        .with_context(|| "Could not access file inside ZIP archive")?;
    let reader = BufReader::new(f);
    let mut deserializer = Deserializer::from_reader(reader);
    let teryt = Teryt::deserialize(&mut deserializer)
        .with_context(|| "Could not deserialize TERC data from file.")?;
    Ok(teryt)
}

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
    // username "user&<>\"'" fully escaped
    assert!(payload.contains("<wsse:Username>user&amp;&lt;&gt;&quot;&apos;</wsse:Username>"));
    // password "p@ss<word>" escaped, raw form absent
    assert!(payload.contains("p@ss&lt;word&gt;</wsse:Password>"));
    assert!(!payload.contains("user&<>"));
    assert!(!payload.contains("p@ss<word>"));
}

/// Get TERC mapping from official SOAP API. Uses today's date to get newest possible file.
pub fn download_terc_mapping(
    api_username: &str,
    api_password: &str,
) -> anyhow::Result<HashMap<String, Terc>> {
    let url = "https://uslugaterytws1.stat.gov.pl/TerytWs1.svc";
    let uuid = Uuid::new_v4();
    let todays_date = Local::now().format("%Y-%m-%d").to_string();
    let payload = build_terc_soap_payload(&uuid, url, api_username, api_password, &todays_date);
    let client = reqwest::blocking::Client::new();
    println!("Sending request to TERYT API...");
    let res = client
        .post(url)
        .header("Content-Type", "text/xml;charset=UTF-8")
        .body(payload)
        .send()?;
    println!("Response received.");
    let xml_string = &res.text()?;
    let bytes = get_file_content_from_response(xml_string)?;
    let mut file = tempfile()?;
    file.write_all(&bytes)
        .with_context(|| "Could not write downloaded TERC file to temp storage.")?;
    file.seek(std::io::SeekFrom::Start(0))?;
    let teryt = parse_terc_zip_file(file)?;
    let mapping = prepare_mapping_from_teryt(teryt)?;
    if mapping.is_empty() {
        anyhow::bail!("After parsing TERYT file mapping dict is empty.")
    } else {
        Ok(mapping)
    }
}

fn get_file_content_from_response(xml_string: &String) -> anyhow::Result<Vec<u8>> {
    let mut deserializer = Deserializer::from_str(xml_string);
    let response = Envelope::deserialize(&mut deserializer)?
        .body
        .response
        .result;
    let bytes = general_purpose::STANDARD
        .decode(response.plik_zawartosc)
        .expect("Could not decode file from API response.");
    Ok(bytes)
}

pub fn get_terc_mapping(file_path: &PathBuf) -> anyhow::Result<HashMap<String, Terc>> {
    let teryt_file = std::fs::File::open(&file_path)
        .with_context(|| format!("could not open file `{}`", &file_path.to_string_lossy()))?;
    let teryt = match file_path
        .extension()
        .expect("Could not read file extension from teryt file.")
        .to_string_lossy()
        .to_lowercase()
        .as_str()
    {
        "xml" => {
            let reader = BufReader::new(teryt_file);
            let mut deserializer = Deserializer::from_reader(reader);
            Teryt::deserialize(&mut deserializer)
                .with_context(|| "Could not deserialize TERC XML file.")
        }
        "zip" => parse_terc_zip_file(teryt_file),
        other => {
            anyhow::bail!(
                "Unsupported TERYT file extension `{}` for `{}`. Expected `.xml` or `.zip`.",
                other,
                file_path.display()
            )
        }
    }
    .with_context(|| "Could not deserialize teryt dictionary from XML file.")?;
    let mapping = prepare_mapping_from_teryt(teryt)?;
    if mapping.is_empty() {
        anyhow::bail!("After parsing TERYT file mapping dict is empty.")
    } else {
        Ok(mapping)
    }
}

/// Concatenate a row's WOJ/POW/GMI/RODZ components into its TERYT code
/// (2 chars for a voivodeship, 4 for a county, 7 for a municipality).
fn teryt_code(row: &Row) -> String {
    [
        row.woj.clone(),
        row.pow.clone().unwrap_or_default(),
        row.gmi.clone().unwrap_or_default(),
        row.rodz.clone().unwrap_or_default(),
    ]
    .join("")
}

fn prepare_mapping_from_teryt(teryt: Teryt) -> anyhow::Result<HashMap<String, Terc>> {
    let mut woj = HashMap::new();
    let mut pow = HashMap::new();
    // First pass: collect voivodeship (2-digit) and county (4-digit) names.
    for row in &teryt.catalog.row {
        let teryt_id = teryt_code(row);
        match teryt_id.len() {
            2 => {
                // teryt dictionary stores these uppercase; previous PRG schema used lowercase
                woj.insert(teryt_id, row.nazwa.to_lowercase());
            }
            4 => {
                pow.insert(teryt_id, row.nazwa.clone());
            }
            7 => {} // handled in the second pass
            other => anyhow::bail!(
                "Unrecognized teryt code length {} for code `{}`.",
                other,
                teryt_id
            ),
        }
    }
    // Second pass: build municipality entries, now that woj/pow are fully populated.
    let mut mapping = HashMap::new();
    for row in &teryt.catalog.row {
        let teryt_id = teryt_code(row);
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
            teryt_id,
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

#[test]
fn get_terc_mapping_xml() {
    let teryt_file_path = PathBuf::from("fixtures/TERC_Urzedowy_2025-11-18.xml");
    let teryt_mapping = crate::terc::get_terc_mapping(&teryt_file_path).unwrap();
    let k0201011 = &teryt_mapping["0201011"];
    assert_eq!(k0201011.municipality_name, "Bolesławiec");
    assert_eq!(k0201011.county_teryt_id, "0201");
    assert_eq!(k0201011.county_name, "bolesławiecki");
    assert_eq!(k0201011.voivodeship_teryt_id, "02");
    assert_eq!(k0201011.voivodeship_name, "dolnośląskie");
}

#[test]
fn get_terc_mapping_zip() {
    let teryt_file_path = PathBuf::from("fixtures/TERC_Urzedowy_2025-11-18.zip");
    let teryt_mapping = crate::terc::get_terc_mapping(&teryt_file_path).unwrap();
    let k0201011 = &teryt_mapping["0201011"];
    assert_eq!(k0201011.municipality_name, "Bolesławiec");
    assert_eq!(k0201011.county_teryt_id, "0201");
    assert_eq!(k0201011.county_name, "bolesławiecki");
    assert_eq!(k0201011.voivodeship_teryt_id, "02");
    assert_eq!(k0201011.voivodeship_name, "dolnośląskie");
}

#[test]
fn test_parse_api_response() {
    let response_text = include_str!("../fixtures/terc_api_response_sample.xml").to_string();
    let bytes = get_file_content_from_response(&response_text).unwrap();
    let mut file = tempfile().unwrap();
    file.write_all(&bytes).unwrap();
    file.seek(std::io::SeekFrom::Start(0)).unwrap();
    let teryt = parse_terc_zip_file(file).unwrap();
    let teryt_mapping = prepare_mapping_from_teryt(teryt).unwrap();
    let k0201011 = &teryt_mapping["0201011"];
    assert_eq!(k0201011.municipality_name, "Bolesławiec");
    assert_eq!(k0201011.county_teryt_id, "0201");
    assert_eq!(k0201011.county_name, "bolesławiecki");
    assert_eq!(k0201011.voivodeship_teryt_id, "02");
    assert_eq!(k0201011.voivodeship_name, "dolnośląskie");
}

#[test]
fn test_get_terc_mapping_file_not_found() {
    let result = get_terc_mapping(&PathBuf::from("fixtures/definitely_nonexistent.xml"));
    assert!(result.is_err());
}

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

#[test]
fn test_prepare_mapping_handles_out_of_order_rows() {
    fn row(
        woj: &str,
        pow: Option<&str>,
        gmi: Option<&str>,
        rodz: Option<&str>,
        nazwa: &str,
    ) -> Row {
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
                row("02", Some("01"), Some("01"), Some("1"), "Bolesławiec"),
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
