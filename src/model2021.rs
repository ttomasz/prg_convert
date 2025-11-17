use std::collections::HashMap;

use once_cell::sync::Lazy;
use quick_xml::{Reader, events::Event};

use crate::get_attribute;

const CITY_TAG: &[u8] = b"prgad:AD_Miejscowosc";
const STREET_TAG: &[u8] = b"prgad:AD_UlicaPlac";

struct City {
    name: String,
    kind: String,
    city_teryt_id: String,
    municipality_teryt_id: String,
}

struct Street {
    name: String,
    kind: String,
    teryt_id: String,
}

pub struct Mappings {
    city: HashMap<String, City>,
    street: HashMap<String, Street>,
}

static CITY_TYPE: Lazy<HashMap<&str, &str>> = Lazy::new(|| {
    let mut mapping = HashMap::new();
    mapping.insert("02", "kolonia");
    mapping.insert("03", "przysiółek");
    mapping.insert("06", "osiedle");
    mapping.insert("05", "osada leśna");
    mapping.insert("04", "osada");
    mapping.insert("96", "miasto");
    mapping.insert("01", "wieś");
    mapping.insert("00", "część miejscowości");
    mapping.insert("07", "schronisko turystyczne");
    mapping.insert("95", "dzielnica Warszawy");
    mapping.insert("98", "delegatura");
    mapping.insert("99", "część miasta");
    mapping
});

static STREET_TYPE: Lazy<HashMap<&str, &str>> = Lazy::new(|| {
    let mut mapping = HashMap::new();
    mapping.insert("1", ""); // originally: ulica, which is default type and doesn't require to be provided
    mapping.insert("3", "plac");
    mapping.insert("11", "osiedle");
    mapping.insert("6", "rondo");
    mapping.insert("2", "aleja");
    mapping.insert("4", "skwer");
    mapping.insert("5", "bulwar");
    mapping.insert("7", "park");
    mapping.insert("8", "rynek");
    mapping.insert("9", "szosa");
    mapping.insert("10", "droga");
    mapping.insert("12", "ogród");
    mapping.insert("13", "wyspa");
    mapping.insert("14", "wybrzeże");
    mapping.insert("15", ""); //originally: innyLiniowy, which is catch-all term for any linear type
    mapping.insert("16", ""); // originally: innyPowierzchniowy, which is catch-all term for any area type
    mapping
});

/// Concatenates parts of the name and the street type.
/// If name contains shorthand type then we don't replace it with the full type name.
pub fn construct_full_name_from_parts(part1: String, part2: Option<String>, typ: &str) -> String {
    let str_typ = STREET_TYPE.get(typ).cloned().unwrap_or_default();
    let prefix = match typ {
        "3" => {
            if part1.to_lowercase().starts_with(str_typ) || part1.to_lowercase().starts_with("pl.")
            {
                ""
            } else {
                str_typ
            }
        }
        "11" => {
            if part1.to_lowercase().starts_with(str_typ) || part1.to_lowercase().starts_with("os.")
            {
                ""
            } else {
                str_typ
            }
        }
        "6" => {
            if part1.to_lowercase().starts_with(str_typ)
                || part1.to_lowercase().starts_with("rondo")
            {
                ""
            } else {
                str_typ
            }
        }
        "2" => {
            if part1.to_lowercase().starts_with(str_typ) || part1.to_lowercase().starts_with("al.")
            {
                ""
            } else {
                str_typ
            }
        }
        _ => {
            if part1.to_lowercase().starts_with(str_typ) {
                ""
            } else {
                str_typ
            }
        }
    };
    let name_parts = [prefix.to_string(), part2.unwrap_or_default(), part1];
    let non_empty_parts: Vec<String> = name_parts.into_iter().filter(|s| !s.is_empty()).collect();
    non_empty_parts.join(" ")
}

fn parse_city(reader: &mut Reader<std::io::BufReader<std::fs::File>>) -> City {
    let mut buffer = Vec::new();
    let mut last_tag = Vec::new();
    let mut kind = String::new();
    let mut name = String::new();
    let mut city_teryt_id = String::new();
    let mut municipality_teryt_id = String::new();
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(ref e)) => {
                last_tag = e.name().as_ref().to_vec();
            }
            Ok(Event::Text(e)) => {
                if last_tag.is_empty() {
                    // if last_tag is empty, we are not inside a tag and we don't want that text
                    continue;
                }
                let text_decoded = e.decode().unwrap();
                let text_trimmed = text_decoded.trim();
                match last_tag.as_slice() {
                    b"prg-ad:nazwa" => {
                        name = text_trimmed.to_string();
                    }
                    b"prgad:rodzaj" => {
                        kind = CITY_TYPE
                            .get(text_trimmed)
                            .cloned()
                            .unwrap_or("")
                            .to_string();
                    }
                    b"prgad:identyfikatorSIMC" => {
                        city_teryt_id = text_trimmed.to_string();
                    }
                    b"prgad:TERYTGminy" => {
                        municipality_teryt_id = text_trimmed.to_string();
                    }
                    _ => (),
                }
                last_tag.clear();
            }
            Ok(Event::End(ref e)) if e.name().as_ref() == CITY_TAG => {
                break;
            }
            Ok(Event::Eof) => {
                panic!("Error: reached end of file before end of address entry");
            }
            Err(e) => {
                panic!("Error at position {}: {:?}", reader.error_position(), e);
            }
            _ => (), // we do not care about other events here
        }
        buffer.clear();
    }
    City {
        kind: kind,
        name: name,
        city_teryt_id: city_teryt_id,
        municipality_teryt_id: municipality_teryt_id,
    }
}

fn parse_street(reader: &mut Reader<std::io::BufReader<std::fs::File>>) -> Street {
    let mut buffer = Vec::new();
    let mut last_tag = Vec::new();
    let mut kind = "";
    let mut name = String::new();
    let mut teryt_id = String::new();
    let mut part1 = String::new();
    let mut part2 = None;
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(ref e)) => {
                last_tag = e.name().as_ref().to_vec();
            }
            Ok(Event::Text(e)) => {
                if last_tag.is_empty() {
                    // if last_tag is empty, we are not inside a tag and we don't want that text
                    continue;
                }
                let text_decoded = e.decode().unwrap();
                let text_trimmed = text_decoded.trim();
                match last_tag.as_slice() {
                    b"prg-ad:nazwa" => {
                        name = text_trimmed.to_string();
                    }
                    b"prgad:rodzaj" => {
                        kind = STREET_TYPE.get(text_trimmed).cloned().unwrap_or("");
                    }
                    b"prgad:identyfikatorULIC" => {
                        teryt_id = text_trimmed.to_string();
                    }
                    b"prgad:TERYTNazwa1" => {
                        part1 = text_trimmed.to_string();
                    }
                    b"prgad:TERYTNazwa2" => {
                        part2 = Some(text_trimmed.to_string());
                    }
                    _ => (),
                }
                last_tag.clear();
            }
            Ok(Event::End(ref e)) if e.name().as_ref() == STREET_TAG => {
                name = construct_full_name_from_parts(part1, part2, kind);
                break;
            }
            Ok(Event::Eof) => {
                panic!("Error: reached end of file before end of address entry");
            }
            Err(e) => {
                panic!("Error at position {}: {:?}", reader.error_position(), e);
            }
            _ => (), // we do not care about other events here
        }
        buffer.clear();
    }
    Street {
        kind: kind.to_string(),
        name: name,
        teryt_id: teryt_id,
    }
}

pub fn build_dictionaries(mut reader: Reader<std::io::BufReader<std::fs::File>>) -> Mappings {
    let mut city_dict = HashMap::<String, City>::new();
    let mut street_dict = HashMap::<String, Street>::new();
    let mut buffer = Vec::new();
    // main loop that catches events when new object starts
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(ref e)) => match e.name().as_ref() {
                CITY_TAG => {
                    let id = get_attribute(e, b"gml:id").to_string();
                    let info = parse_city(&mut reader);
                    city_dict.insert(id, info);
                },
                STREET_TAG => {
                    let id = get_attribute(e, b"gml:id").to_string();
                    let info = parse_street(&mut reader);
                    street_dict.insert(id, info);
                },
                _ => (),
            },
            Ok(Event::Eof) => break, // exits the loop when reaching end of file
            Err(e) => panic!("Error at position {}: {:?}", reader.error_position(), e),
            _ => (), // we do not care about other events here
        }
        buffer.clear();
    }
    Mappings {
        city: city_dict,
        street: street_dict,
    }
}

#[test]
fn name_from_part1() {
    let typ = "1";
    let part1 = "Test".to_string();
    let part2 = None;
    let expected_name = "Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_part2() {
    let typ = "1";
    let part1 = "Test".to_string();
    let part2 = Some("Test2".to_string());
    let expected_name = "Test2 Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_typ_3() {
    let typ = "3";
    let part1 = "Test".to_string();
    let part2 = None;
    let expected_name = "plac Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_part2_typ_3() {
    let typ = "3";
    let part1 = "Test".to_string();
    let part2 = Some("Test2".to_string());
    let expected_name = "plac Test2 Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_typ_3_prefix() {
    let typ = "3";
    let part1 = "plac Test".to_string();
    let part2 = None;
    let expected_name = "plac Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_typ_3_prefix_short() {
    let typ = "3";
    let part1 = "pl. Test".to_string();
    let part2 = None;
    let expected_name = "pl. Test";
    let name = construct_full_name_from_parts(part1, part2, typ);
    assert_eq!(name, expected_name);
}
