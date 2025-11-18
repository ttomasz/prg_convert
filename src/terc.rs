use std::collections::HashMap;

use quick_xml::de::Deserializer;
use serde::Deserialize;

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

pub struct TERC {
    pub voivodeship_teryt_id: String,
    pub voivodeship_name: String,
    pub county_teryt_id: String,
    pub county_name: String,
    pub municipality_name: String,
}

pub fn get_terc_mapping(reader: std::io::BufReader<std::fs::File>) -> HashMap<String, TERC> {
    let mut deserializer = Deserializer::from_reader(reader);
    let teryt = Teryt::deserialize(&mut deserializer)
        .expect("Could not deserialize teryt dictionary from xml file.");
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
                woj.insert(teryt_id, row.nazwa);
            }
            4 => {
                pow.insert(teryt_id, row.nazwa);
            }
            7 => {
                mapping.insert(
                    teryt_id.clone(),
                    TERC {
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
    mapping
}
