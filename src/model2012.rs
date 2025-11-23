use std::collections::HashMap;
use std::io::BufRead;
use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow::array::Date32Builder;
use arrow::array::Float64Builder;
use arrow::array::RecordBatch;
use arrow::array::StringBuilder;
use arrow::array::TimestampMillisecondBuilder;
use chrono::DateTime;
use chrono::NaiveDate;
use geo_types::Point;
use geoarrow::array::GeoArrowArray;
use geoarrow::array::PointBuilder;
use quick_xml::Reader;
use quick_xml::events::Event;

use crate::OutputFormat;
use crate::constants::EPOCH_DATE;
use crate::constants::GEOM_TYPE;
use crate::constants::SCHEMA_CSV;
use crate::constants::SCHEMA_GEOPARQUET;
use crate::get_attribute;
use crate::option_append_value_or_null;
use crate::parse_gml_pos;
use crate::str_append_value_or_null;

const ADDRESS_TAG: &[u8] = b"prg-ad:PRG_PunktAdresowy";
const ADMINISTRATIVE_UNIT_TAG: &[u8] = b"prg-ad:PRG_JednostkaAdministracyjnaNazwa";
const CITY_TAG: &[u8] = b"prg-ad:PRG_MiejscowoscNazwa";
const STREET_TAG: &[u8] = b"prg-ad:PRG_UlicaNazwa";

#[derive(Clone)]
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
                last_tag = e.name().as_ref().to_vec();
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
    output_format: OutputFormat,
    additional_info: HashMap<String, AdditionalInfo>,
    uuid: StringBuilder,
    id_namespace: StringBuilder,
    version: TimestampMillisecondBuilder,
    lifecycle_start_date: TimestampMillisecondBuilder,
    valid_since_date: Date32Builder,
    valid_to_date: Date32Builder,
    voivodeship: StringBuilder,
    county: StringBuilder,
    municipality: StringBuilder,
    city: StringBuilder,
    city_part: StringBuilder,
    street: StringBuilder,
    house_number: StringBuilder,
    postcode: StringBuilder,
    status: StringBuilder,
    x_epsg_2180: Float64Builder,
    y_epsg_2180: Float64Builder,
    longitude: Float64Builder,
    latitude: Float64Builder,
    voivodeship_teryt_id: StringBuilder,
    county_teryt_id: StringBuilder,
    municipality_teryt_id: StringBuilder,
    city_teryt_id: StringBuilder,
    street_teryt_id: StringBuilder,
    geometry: Vec<Option<Point>>,
}

impl<R: BufRead> AddressParser2012<R> {
    pub fn new(
        reader: Reader<R>,
        batch_size: usize,
        output_format: OutputFormat,
        additional_info: HashMap<String, AdditionalInfo>,
    ) -> Self {
        Self {
            reader,
            batch_size: batch_size,
            output_format: output_format,
            additional_info: additional_info,
            id_namespace: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            uuid: StringBuilder::with_capacity(batch_size, 36 * batch_size),
            version: TimestampMillisecondBuilder::with_capacity(batch_size)
                .with_timezone(Arc::from("UTC")),
            lifecycle_start_date: TimestampMillisecondBuilder::with_capacity(batch_size)
                .with_timezone(Arc::from("UTC")),
            valid_since_date: Date32Builder::with_capacity(batch_size),
            valid_to_date: Date32Builder::with_capacity(batch_size),
            voivodeship: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            county: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            municipality: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            city: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            city_part: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            street: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            house_number: StringBuilder::with_capacity(batch_size, 6 * batch_size),
            postcode: StringBuilder::with_capacity(batch_size, 6 * batch_size),
            status: StringBuilder::with_capacity(batch_size, 10 * batch_size),
            x_epsg_2180: Float64Builder::with_capacity(batch_size),
            y_epsg_2180: Float64Builder::with_capacity(batch_size),
            longitude: Float64Builder::with_capacity(batch_size),
            latitude: Float64Builder::with_capacity(batch_size),
            voivodeship_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            county_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            municipality_teryt_id: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            city_teryt_id: StringBuilder::with_capacity(batch_size, 62 * batch_size),
            street_teryt_id: StringBuilder::with_capacity(batch_size, 91 * batch_size),
            geometry: Vec::with_capacity(batch_size),
        }
    }

    fn build_record_batch(&mut self) -> RecordBatch {
        match self.output_format {
            OutputFormat::CSV => RecordBatch::try_new(
                SCHEMA_CSV.clone(),
                vec![
                    Arc::new(self.id_namespace.finish()),
                    Arc::new(self.uuid.finish()),
                    Arc::new(self.version.finish()),
                    Arc::new(self.lifecycle_start_date.finish()),
                    Arc::new(self.valid_since_date.finish()),
                    Arc::new(self.valid_to_date.finish()),
                    Arc::new(self.voivodeship_teryt_id.finish()),
                    Arc::new(self.voivodeship.finish()),
                    Arc::new(self.county_teryt_id.finish()),
                    Arc::new(self.county.finish()),
                    Arc::new(self.municipality_teryt_id.finish()),
                    Arc::new(self.municipality.finish()),
                    Arc::new(self.city_teryt_id.finish()),
                    Arc::new(self.city.finish()),
                    Arc::new(self.city_part.finish()),
                    Arc::new(self.street_teryt_id.finish()),
                    Arc::new(self.street.finish()),
                    Arc::new(self.house_number.finish()),
                    Arc::new(self.postcode.finish()),
                    Arc::new(self.status.finish()),
                    Arc::new(self.x_epsg_2180.finish()),
                    Arc::new(self.y_epsg_2180.finish()),
                    Arc::new(self.longitude.finish()),
                    Arc::new(self.latitude.finish()),
                ],
            )
            .expect("Failed to create RecordBatch"),
            OutputFormat::GeoParquet => {
                let iter = self.geometry.iter().map(Option::as_ref);
                let geometry_array =
                    PointBuilder::from_nullable_points(iter, GEOM_TYPE.clone()).finish();
                // reset geometry buffer before the next iteration
                // arrow builders reset automatically on .finish() call
                self.geometry = Vec::with_capacity(self.batch_size);
                RecordBatch::try_new(
                    SCHEMA_GEOPARQUET.clone(),
                    vec![
                        Arc::new(self.id_namespace.finish()),
                        Arc::new(self.uuid.finish()),
                        Arc::new(self.version.finish()),
                        Arc::new(self.lifecycle_start_date.finish()),
                        Arc::new(self.valid_since_date.finish()),
                        Arc::new(self.valid_to_date.finish()),
                        Arc::new(self.voivodeship_teryt_id.finish()),
                        Arc::new(self.voivodeship.finish()),
                        Arc::new(self.county_teryt_id.finish()),
                        Arc::new(self.county.finish()),
                        Arc::new(self.municipality_teryt_id.finish()),
                        Arc::new(self.municipality.finish()),
                        Arc::new(self.city_teryt_id.finish()),
                        Arc::new(self.city.finish()),
                        Arc::new(self.city_part.finish()),
                        Arc::new(self.street_teryt_id.finish()),
                        Arc::new(self.street.finish()),
                        Arc::new(self.house_number.finish()),
                        Arc::new(self.postcode.finish()),
                        Arc::new(self.status.finish()),
                        Arc::new(self.longitude.finish()),
                        Arc::new(self.latitude.finish()),
                        Arc::new(geometry_array.to_array_ref()),
                    ],
                )
                .expect("Failed to create RecordBatch")
            }
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
                    last_tag = e.name().as_ref().to_vec();
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
                            let info = self
                                .additional_info
                                .get(&attr.to_string())
                                .cloned()
                                .unwrap_or_default();
                            match info.typ {
                                KomponentType::Country => {}
                                KomponentType::Voivodeship => {
                                    option_append_value_or_null(
                                        &mut self.voivodeship_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::County => {
                                    option_append_value_or_null(
                                        &mut self.county_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::Municipality => {
                                    option_append_value_or_null(
                                        &mut self.municipality_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::City => {
                                    option_append_value_or_null(
                                        &mut self.city_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::Street => {
                                    option_append_value_or_null(
                                        &mut self.street_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::Unknown => {}
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
                            self.uuid.append_value(text_trimmed);
                        }
                        b"bt:przestrzenNazw" => {
                            self.id_namespace.append_value(text_trimmed);
                        }
                        b"bt:wersjaId" => {
                            let dt = DateTime::parse_from_rfc3339(text_trimmed)
                                .expect("Failed to parse datetime")
                                .to_utc();
                            self.version.append_value(dt.timestamp() * 1000);
                        }
                        b"bt:poczatekWersjiObiektu" => {
                            if text_trimmed.is_empty() {
                                self.lifecycle_start_date.append_null();
                            } else {
                                let dt = DateTime::parse_from_rfc3339(text_trimmed)
                                    .expect("Failed to parse datetime")
                                    .to_utc();
                                self.lifecycle_start_date
                                    .append_value(dt.timestamp() * 1000);
                            }
                        }
                        b"prg-ad:waznyOd" => {
                            if text_trimmed.is_empty() {
                                self.valid_since_date.append_null();
                            } else {
                                let date = NaiveDate::parse_from_str(text_trimmed, "%Y-%m-%d")
                                    .expect("Failed to parse date");
                                self.valid_since_date.append_value(
                                    date.signed_duration_since(EPOCH_DATE).num_days() as i32,
                                );
                            }
                        }
                        b"prg-ad:waznyDo" => {
                            if text_trimmed.is_empty() {
                                self.valid_to_date.append_null();
                            } else {
                                let date = NaiveDate::parse_from_str(text_trimmed, "%Y-%m-%d")
                                    .expect("Failed to parse date");
                                self.valid_to_date.append_value(
                                    date.signed_duration_since(EPOCH_DATE).num_days() as i32,
                                );
                            }
                        }
                        b"prg-ad:jednostkaAdmnistracyjna" => {
                            // sic!
                            match admin_unit_counter {
                                0 => {}
                                1 => {
                                    self.voivodeship.append_value(text_trimmed);
                                }
                                2 => {
                                    self.county.append_value(text_trimmed);
                                }
                                3 => {
                                    self.municipality.append_value(text_trimmed);
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
                            self.city.append_value(text_trimmed);
                        }
                        b"prg-ad:czescMiejscowosci" => {
                            str_append_value_or_null(&mut self.city_part, text_trimmed);
                        }
                        b"prg-ad:ulica" => {
                            str_append_value_or_null(&mut self.street, text_trimmed);
                        }
                        b"prg-ad:numerPorzadkowy" => {
                            self.house_number.append_value(text_trimmed);
                        }
                        b"prg-ad:kodPocztowy" => {
                            str_append_value_or_null(&mut self.postcode, text_trimmed);
                        }
                        b"prg-ad:status" => {
                            self.status.append_value(text_trimmed);
                        }
                        b"gml:pos" => {
                            parse_gml_pos(
                                text_trimmed,
                                &mut self.longitude,
                                &mut self.latitude,
                                &mut self.x_epsg_2180,
                                &mut self.y_epsg_2180,
                                &mut self.geometry,
                                &self.output_format,
                            );
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
                    let buffer_length = self.uuid.len();
                    // ensure all builders have the same length
                    if self.id_namespace.len() < buffer_length {
                        self.id_namespace.append_null();
                    }
                    if self.version.len() < buffer_length {
                        self.version.append_null();
                    }
                    if self.lifecycle_start_date.len() < buffer_length {
                        self.lifecycle_start_date.append_null();
                    }
                    if self.valid_since_date.len() < buffer_length {
                        self.valid_since_date.append_null();
                    }
                    if self.valid_to_date.len() < buffer_length {
                        self.valid_to_date.append_null();
                    }
                    if self.voivodeship.len() < buffer_length {
                        self.voivodeship.append_null();
                    }
                    if self.county.len() < buffer_length {
                        self.county.append_null();
                    }
                    if self.municipality.len() < buffer_length {
                        self.municipality.append_null();
                    }
                    if self.city.len() < buffer_length {
                        self.city.append_null();
                    }
                    if self.city_part.len() < buffer_length {
                        self.city_part.append_null();
                    }
                    if self.street.len() < buffer_length {
                        self.street.append_null();
                    }
                    if self.house_number.len() < buffer_length {
                        self.house_number.append_null();
                    }
                    if self.postcode.len() < buffer_length {
                        self.postcode.append_null();
                    }
                    if self.status.len() < buffer_length {
                        self.status.append_null();
                    }
                    if self.longitude.len() < buffer_length {
                        self.longitude.append_null();
                    }
                    if self.latitude.len() < buffer_length {
                        self.latitude.append_null();
                    }
                    if self.voivodeship_teryt_id.len() < buffer_length {
                        self.voivodeship_teryt_id.append_null();
                    }
                    if self.county_teryt_id.len() < buffer_length {
                        self.county_teryt_id.append_null();
                    }
                    if self.municipality_teryt_id.len() < buffer_length {
                        self.municipality_teryt_id.append_null();
                    }
                    if self.city_teryt_id.len() < buffer_length {
                        self.city_teryt_id.append_null();
                    }
                    if self.street_teryt_id.len() < buffer_length {
                        self.street_teryt_id.append_null();
                    }
                    match self.output_format {
                        OutputFormat::CSV => {
                            if self.x_epsg_2180.len() < buffer_length {
                                self.x_epsg_2180.append_null();
                            }
                            if self.y_epsg_2180.len() < buffer_length {
                                self.y_epsg_2180.append_null();
                            }
                        }
                        OutputFormat::GeoParquet => {
                            if self.geometry.len() < buffer_length {
                                self.geometry.push(None);
                            }
                        }
                    }
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
                            let record_batch = self.build_record_batch();
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
        let record_batch = self.build_record_batch();
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
