use std::collections::HashMap;
use std::io::BufRead;

use chrono::DateTime;
use chrono::NaiveDate;
use quick_xml::Reader;
use quick_xml::events::Event;

use crate::CoordOrder;
use crate::common::CanonicalBuilders;
use crate::common::EPOCH_DATE;
use crate::common::get_attribute;
use crate::common::option_append_value_or_null;
use crate::common::parse_gml_pos;
use crate::common::str_append_value_or_null;

const ADDRESS_TAG: &[u8] = b"prg-ad:PRG_PunktAdresowy";
const ADMINISTRATIVE_UNIT_TAG: &[u8] = b"prg-ad:PRG_JednostkaAdministracyjnaNazwa";
const CITY_TAG: &[u8] = b"prg-ad:PRG_MiejscowoscNazwa";
const STREET_TAG: &[u8] = b"prg-ad:PRG_UlicaNazwa";

#[derive(Clone, PartialEq, Debug)]
pub enum KomponentType {
    Country,
    Voivodeship,
    County,
    Municipality,
    City,
    Street,
    Unknown,
}

#[allow(dead_code)]
#[derive(Clone)]
pub struct AdditionalInfo {
    typ: KomponentType,
    name: String,
    teryt_id: Option<String>,
}

impl Default for AdditionalInfo {
    fn default() -> Self {
        Self {
            typ: KomponentType::Unknown,
            name: String::new(),
            teryt_id: None,
        }
    }
}

/// Concatenates parts of the street name.
pub fn construct_full_name_from_parts(
    name_part_1: String,
    name_part_2: String,
    name_part_3: String,
    name_part_4: String,
) -> String {
    let name_parts = [name_part_1, name_part_2, name_part_3, name_part_4];
    let non_empty_parts: Vec<String> = name_parts.into_iter().filter(|s| !s.is_empty()).collect();
    non_empty_parts.join(" ")
}

fn parse_additional_info<R: BufRead>(reader: &mut Reader<R>, tag: &[u8]) -> AdditionalInfo {
    let mut buffer = Vec::new();
    let mut last_tag = Vec::new();
    let mut typ: Option<KomponentType> = None;
    let mut name: Option<String> = None;
    let mut name_part_1 = String::new();
    let mut name_part_2 = String::new();
    let mut name_part_3 = String::new();
    let mut name_part_4 = String::new();
    let mut teryt_id: Option<String> = None;
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(ref e)) => {
                last_tag.clear();
                last_tag.extend_from_slice(e.name().as_ref());
            }
            Ok(Event::Text(e)) => {
                if last_tag.is_empty() {
                    // if last_tag is empty, we are not inside a tag and we don't want that text
                    continue;
                }
                let text_decoded = e.decode().expect("Failed to decode text.");
                let text_trimmed = text_decoded.trim();
                match last_tag.as_slice() {
                    b"prg-ad:nazwa" => {
                        if tag != STREET_TAG {
                            name = Some(text_trimmed.to_string());
                        }
                    }
                    b"mua:przedrostek1Czesc" => {
                        name_part_1 = text_trimmed.to_string();
                    }
                    b"mua:przedrostek2Czesc" => {
                        name_part_2 = text_trimmed.to_string();
                    }
                    b"mua:nazwaCzesc" => {
                        name_part_3 = text_trimmed.to_string();
                    }
                    b"mua:nazwaGlownaCzesc" => {
                        name_part_4 = text_trimmed.to_string();
                    }
                    b"prg-ad:idTERYT" => {
                        if !text_trimmed.is_empty() {
                            teryt_id = Some(text_trimmed.to_string());
                        }
                    }
                    b"mua:idTERYT" => {
                        if !text_trimmed.is_empty() {
                            teryt_id = Some(text_trimmed.to_string());
                        }
                    }
                    b"prg-ad:poziom" => match text_trimmed {
                        "1poziom" => {
                            typ = Some(KomponentType::Country);
                        }
                        "2poziom" => {
                            typ = Some(KomponentType::Voivodeship);
                        }
                        "3poziom" => {
                            typ = Some(KomponentType::County);
                        }
                        "4poziom" => {
                            typ = Some(KomponentType::Municipality);
                        }
                        _ => {
                            panic!("Unexpected value of `prg-ad:poziom`: `{}`.", text_trimmed)
                        }
                    },
                    _ => (),
                }
                last_tag.clear();
            }
            Ok(Event::End(ref e)) if e.name().as_ref() == tag => {
                match tag {
                    CITY_TAG => {
                        typ = Some(KomponentType::City);
                    }
                    STREET_TAG => {
                        typ = Some(KomponentType::Street);
                        name = Some(construct_full_name_from_parts(
                            name_part_1,
                            name_part_2,
                            name_part_3,
                            name_part_4,
                        ));
                    }
                    _ => (),
                }
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
    AdditionalInfo {
        typ: typ.unwrap(),
        name: name.unwrap(),
        teryt_id: teryt_id,
    }
}

pub fn build_dictionaries<R: BufRead>(mut reader: Reader<R>) -> HashMap<String, AdditionalInfo> {
    let mut dict = HashMap::<String, AdditionalInfo>::new();
    let mut buffer = Vec::new();
    // main loop that catches events when new object starts
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(ref e)) => match e.name().as_ref() {
                ADMINISTRATIVE_UNIT_TAG => {
                    let id = "http://geoportal.gov.pl/PZGIK/dane/".to_string()
                        + &get_attribute(e, b"gml:id");
                    let info = parse_additional_info(&mut reader, ADMINISTRATIVE_UNIT_TAG);
                    dict.insert(id, info);
                }
                CITY_TAG => {
                    let id = "http://geoportal.gov.pl/PZGIK/dane/".to_string()
                        + &get_attribute(e, b"gml:id");
                    let info = parse_additional_info(&mut reader, CITY_TAG);
                    dict.insert(id, info);
                }
                STREET_TAG => {
                    let id = "http://geoportal.gov.pl/PZGIK/dane/".to_string()
                        + &get_attribute(e, b"gml:id");
                    let info = parse_additional_info(&mut reader, STREET_TAG);
                    dict.insert(id, info);
                }
                _ => (),
            },
            Ok(Event::Eof) => break, // exits the loop when reaching end of file
            Err(e) => panic!("Error at position {}: {:?}", reader.error_position(), e),
            _ => (), // we do not care about other events here
        }
        buffer.clear();
    }
    dict
}

pub struct AddressParser2012<R: BufRead> {
    reader: Reader<R>,
    batch_size: usize,
    additional_info: HashMap<String, AdditionalInfo>,
    builders: CanonicalBuilders,
}

impl<R: BufRead> AddressParser2012<R> {
    pub fn new(
        reader: Reader<R>,
        batch_size: usize,
        additional_info: HashMap<String, AdditionalInfo>,
    ) -> Self {
        Self {
            reader,
            batch_size,
            additional_info,
            builders: CanonicalBuilders::with_capacity(batch_size),
        }
    }

    fn parse_address(&mut self) {
        let mut buffer = Vec::new();
        let mut last_tag = Vec::new();
        let mut nested_tag = false; // informs if we're processing a nested tag
        let mut tag_ignore_text = false; // informs if we're processing a tag that won't have any text content
        let mut admin_unit_counter: u8 = 0;
        // inside loop to process the content of the current address
        loop {
            match self.reader.read_event_into(&mut buffer) {
                Ok(Event::Start(ref e)) => {
                    last_tag.clear();
                    last_tag.extend_from_slice(e.name().as_ref());
                    match e.name().as_ref() {
                        b"prg-ad:idIIP"
                        | b"bt:BT_Identyfikator"
                        | b"prg-ad:cyklZycia"
                        | b"bt:BT_CyklZyciaInfo"
                        | b"prg-ad:pozycja"
                        | b"gml:Point" => {
                            nested_tag = true;
                            tag_ignore_text = false;
                        }
                        b"prg-ad:komponent" => {
                            let attr = get_attribute(e, b"xlink:href");
                            // Look up by &str (no key allocation) and copy out only what we use.
                            let info = self
                                .additional_info
                                .get(attr.as_ref())
                                .map(|i| (i.typ.clone(), i.teryt_id.clone()));
                            if let Some((typ, teryt_id)) = info {
                                match typ {
                                    KomponentType::Voivodeship => option_append_value_or_null(
                                        &mut self.builders.voivodeship_teryt_id,
                                        teryt_id,
                                    ),
                                    KomponentType::County => option_append_value_or_null(
                                        &mut self.builders.county_teryt_id,
                                        teryt_id,
                                    ),
                                    KomponentType::Municipality => option_append_value_or_null(
                                        &mut self.builders.municipality_teryt_id,
                                        teryt_id,
                                    ),
                                    KomponentType::City => option_append_value_or_null(
                                        &mut self.builders.city_teryt_id,
                                        teryt_id,
                                    ),
                                    KomponentType::Street => option_append_value_or_null(
                                        &mut self.builders.street_teryt_id,
                                        teryt_id,
                                    ),
                                    KomponentType::Country | KomponentType::Unknown => {}
                                }
                            }
                            nested_tag = false;
                            tag_ignore_text = true;
                        }
                        b"prg-ad:obiektEMUiA" => {
                            nested_tag = false;
                            tag_ignore_text = true;
                        }
                        _ => {
                            nested_tag = false;
                            tag_ignore_text = false;
                        }
                    }
                }
                Ok(Event::Text(e)) => {
                    if last_tag.is_empty() || nested_tag || tag_ignore_text {
                        // if last_tag is empty, we are not inside a tag and we don't want that text
                        // if nested_tag is true, we are inside a nested tag that we want to skip (only read innermost text not the whole tree branch)
                        continue;
                    }
                    let text_decoded = e.decode().expect("Failed to decode text.");
                    let text_trimmed = text_decoded.trim();
                    match last_tag.as_slice() {
                        b"gml:identifier" => {}
                        b"bt:lokalnyId" => {
                            self.builders.uuid.append_value(text_trimmed);
                        }
                        b"bt:przestrzenNazw" => {
                            self.builders.id_namespace.append_value(text_trimmed);
                        }
                        b"bt:wersjaId" => {
                            let dt = DateTime::parse_from_rfc3339(text_trimmed)
                                .expect("Failed to parse datetime")
                                .to_utc();
                            self.builders.version.append_value(dt.timestamp() * 1000);
                        }
                        b"bt:poczatekWersjiObiektu" => {
                            if text_trimmed.is_empty() {
                                self.builders.lifecycle_start_date.append_null();
                            } else {
                                let dt = DateTime::parse_from_rfc3339(text_trimmed)
                                    .expect("Failed to parse datetime")
                                    .to_utc();
                                self.builders
                                    .lifecycle_start_date
                                    .append_value(dt.timestamp() * 1000);
                            }
                        }
                        b"prg-ad:waznyOd" => {
                            if text_trimmed.is_empty() {
                                self.builders.valid_since_date.append_null();
                            } else {
                                let date = NaiveDate::parse_from_str(text_trimmed, "%Y-%m-%d")
                                    .expect("Failed to parse date");
                                self.builders.valid_since_date.append_value(
                                    date.signed_duration_since(EPOCH_DATE).num_days() as i32,
                                );
                            }
                        }
                        b"prg-ad:waznyDo" => {
                            if text_trimmed.is_empty() {
                                self.builders.valid_to_date.append_null();
                            } else {
                                let date = NaiveDate::parse_from_str(text_trimmed, "%Y-%m-%d")
                                    .expect("Failed to parse date");
                                self.builders.valid_to_date.append_value(
                                    date.signed_duration_since(EPOCH_DATE).num_days() as i32,
                                );
                            }
                        }
                        b"prg-ad:jednostkaAdmnistracyjna" => {
                            // sic!
                            match admin_unit_counter {
                                0 => {}
                                1 => {
                                    self.builders.voivodeship.append_value(text_trimmed);
                                }
                                2 => {
                                    self.builders.county.append_value(text_trimmed);
                                }
                                3 => {
                                    self.builders.municipality.append_value(text_trimmed);
                                }
                                _ => {
                                    panic!(
                                        "More than 4 administrative units found! Probably bug in the code."
                                    );
                                }
                            }
                            admin_unit_counter += 1;
                        }
                        b"prg-ad:miejscowosc" => {
                            self.builders.city.append_value(text_trimmed);
                        }
                        b"prg-ad:czescMiejscowosci" => {
                            str_append_value_or_null(&mut self.builders.city_part, text_trimmed);
                        }
                        b"prg-ad:ulica" => {
                            str_append_value_or_null(&mut self.builders.street, text_trimmed);
                        }
                        b"prg-ad:numerPorzadkowy" => {
                            self.builders.house_number.append_value(text_trimmed);
                        }
                        b"prg-ad:kodPocztowy" => {
                            str_append_value_or_null(&mut self.builders.postcode, text_trimmed);
                        }
                        b"prg-ad:status" => {
                            self.builders.status.append_value(text_trimmed);
                        }
                        b"gml:pos" => {
                            let coords = parse_gml_pos(text_trimmed, CoordOrder::YX)
                                .expect("Could not parse coordinates.");
                            match coords {
                                None => {
                                    self.builders.longitude.append_null();
                                    self.builders.latitude.append_null();
                                    self.builders.x_epsg_2180.append_null();
                                    self.builders.y_epsg_2180.append_null();
                                }
                                Some(coords) => {
                                    self.builders.longitude.append_value(coords.x4326);
                                    self.builders.latitude.append_value(coords.y4326);
                                    self.builders.x_epsg_2180.append_value(coords.x2180);
                                    self.builders.y_epsg_2180.append_value(coords.y2180);
                                }
                            }
                        }
                        _ => {
                            println!(
                                "Unknown tag `{:?}` for parse_address function.",
                                std::str::from_utf8(&last_tag)
                            );
                        }
                    }
                    last_tag.clear();
                }
                Ok(Event::End(ref e)) if e.name().as_ref() == ADDRESS_TAG => {
                    // ensure all builders have the same length
                    self.builders.pad_short_columns();
                    // end of the current address entry
                    break;
                }
                Ok(Event::Eof) => {
                    panic!("Error: reached end of file before end of address entry");
                }
                Err(e) => {
                    panic!(
                        "Error at position {}: {:?}",
                        self.reader.error_position(),
                        e
                    );
                }
                _ => (), // we do not care about other events here
            }
            buffer.clear();
        }
    }
}

impl<R: BufRead> Iterator for AddressParser2012<R> {
    type Item = arrow::array::RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = Vec::new();
        let mut row_count: usize = 0;
        // main loop that catches events when new object starts
        loop {
            match self.reader.read_event_into(&mut buffer) {
                Ok(Event::Start(ref e)) => {
                    if e.name().as_ref() == ADDRESS_TAG {
                        row_count += 1;
                        self.parse_address();
                        if row_count == self.batch_size {
                            let record_batch = self.builders.build_record_batch();
                            return Some(record_batch);
                        }
                    }
                }
                Ok(Event::Eof) => break, // exits the loop when reaching end of file
                Err(e) => panic!(
                    "Error at position {}: {:?}",
                    self.reader.error_position(),
                    e
                ),
                _ => (), // we do not care about other events here
            }
            buffer.clear();
        }
        let record_batch = self.builders.build_record_batch();
        if record_batch.num_rows() > 0 {
            Some(record_batch)
        } else {
            None
        }
    }
}

#[test]
fn name_from_part1() {
    let name_part_1 = "Test".to_string();
    let name_part_2 = String::new();
    let name_part_3 = String::new();
    let name_part_4 = String::new();
    let expected_name = "Test".to_string();
    let name = construct_full_name_from_parts(name_part_1, name_part_2, name_part_3, name_part_4);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_part2() {
    let name_part_1 = "Test".to_string();
    let name_part_2 = "Test2".to_string();
    let name_part_3 = String::new();
    let name_part_4 = String::new();
    let expected_name = "Test Test2".to_string();
    let name = construct_full_name_from_parts(name_part_1, name_part_2, name_part_3, name_part_4);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_part2_part3() {
    let name_part_1 = "Test".to_string();
    let name_part_2 = "Test2".to_string();
    let name_part_3 = "Test3".to_string();
    let name_part_4 = String::new();
    let expected_name = "Test Test2 Test3".to_string();
    let name = construct_full_name_from_parts(name_part_1, name_part_2, name_part_3, name_part_4);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_part2_part3_part4() {
    let name_part_1 = "Test".to_string();
    let name_part_2 = "Test2".to_string();
    let name_part_3 = "Test3".to_string();
    let name_part_4 = "Test4".to_string();
    let expected_name = "Test Test2 Test3 Test4".to_string();
    let name = construct_full_name_from_parts(name_part_1, name_part_2, name_part_3, name_part_4);
    assert_eq!(name, expected_name);
}

#[test]
fn test_build_dictionaries() {
    let sample_file_path = "fixtures/sample_model2012.xml";
    let mut reader = Reader::from_file(sample_file_path).unwrap();
    reader.config_mut().expand_empty_elements = true;
    let dict = build_dictionaries(reader);
    let country = &dict["http://geoportal.gov.pl/PZGIK/dane/PL.PZGIK.200_366263"];
    assert_eq!(country.typ, KomponentType::Country);
    assert_eq!(country.name, "POLSKA");
    let voivodeship = &dict["http://geoportal.gov.pl/PZGIK/dane/PL.PZGIK.200_366267"];
    assert_eq!(voivodeship.typ, KomponentType::Voivodeship);
    assert_eq!(voivodeship.name, "lubuskie");
    assert_eq!(voivodeship.teryt_id, Some("08".to_string()));
    let county = &dict["http://geoportal.gov.pl/PZGIK/dane/PL.PZGIK.200_366439"];
    assert_eq!(county.typ, KomponentType::County);
    assert_eq!(county.name, "powiat nowosolski");
    assert_eq!(county.teryt_id, Some("0804".to_string()));
    let municipality = &dict["http://geoportal.gov.pl/PZGIK/dane/PL.PZGIK.200_370095"];
    assert_eq!(municipality.typ, KomponentType::Municipality);
    assert_eq!(municipality.name, "Kolsko");
    assert_eq!(municipality.teryt_id, Some("0804032".to_string()));
    let city = &dict["http://geoportal.gov.pl/PZGIK/dane/PL.ZIPIN.4404.EMUiA_0910140"];
    assert_eq!(city.typ, KomponentType::City);
    assert_eq!(city.name, "Konotop");
    assert_eq!(city.teryt_id, Some("0910140".to_string()));
    let street = &dict["http://geoportal.gov.pl/PZGIK/dane/PL.ZIPIN.4404.EMUiA_95d1f98c-7a1e-4726-a17d-a3c7bdaec79e"];
    assert_eq!(street.typ, KomponentType::Street);
    assert_eq!(street.name, "Podgórna");
    assert_eq!(street.teryt_id, Some("16742".to_string()));
}
