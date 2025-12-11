use std::{collections::HashMap, io::BufReader, path::PathBuf};

use anyhow::Context;
use quick_xml::de::Deserializer;
use serde::Deserialize;
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

pub struct Terc {
    pub voivodeship_teryt_id: String,
    pub voivodeship_name: String,
    pub county_teryt_id: String,
    pub county_name: String,
    pub municipality_name: String,
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
        }
        "zip" => {
            let mut archive = ZipArchive::new(teryt_file).with_context(|| {
                format!("Failed to decompress ZIP file: `{}`.", &file_path.display())
            })?;
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
            Teryt::deserialize(&mut deserializer)
        }
        _ => {
            anyhow::bail!("")
        }
    }
    .with_context(|| "Could not deserialize teryt dictionary from XML file.")?;

    let mut woj = HashMap::new();
    let mut pow = HashMap::new();
    let mut mapping = HashMap::new();
    for row in teryt.catalog.row {
        let teryt_id = [
            row.woj.clone(),
            row.pow.unwrap_or_default(),
            row.gmi.unwrap_or_default(),
            row.rodz.unwrap_or_default(),
        ]
        .join("");
        match teryt_id.len() {
            2 => {
                woj.insert(teryt_id, row.nazwa.to_lowercase()); // teryt dictionary has them all uppercase, while previous prg schema had them all lowercase
            }
            4 => {
                pow.insert(teryt_id, row.nazwa);
            }
            7 => {
                mapping.insert(
                    teryt_id.clone(),
                    Terc {
                        voivodeship_teryt_id: row.woj.clone(),
                        voivodeship_name: woj[&row.woj].to_string(),
                        county_teryt_id: teryt_id[..4].to_string(),
                        county_name: pow[&teryt_id[..4]].to_string(),
                        municipality_name: row.nazwa.to_string(),
                    },
                );
            }
            _ => {
                panic!("Unrecognized teryt code type: {}.", teryt_id)
            }
        }
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
