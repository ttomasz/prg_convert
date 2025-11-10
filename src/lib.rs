use core::f64;
use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow::array::Date32Builder;
use arrow::array::RecordBatch;
use arrow::array::TimestampSecondBuilder;
use arrow::array::Float64Builder;
use arrow::array::StringBuilder;
use arrow::datatypes::DataType;
use arrow::datatypes::Field;
use arrow::datatypes::Schema;
use arrow::datatypes::TimeUnit;
use chrono::NaiveDate;
use chrono::DateTime;
use proj4rs::Proj;
// use geoarrow::array::GeoArrowArray;
// use geoarrow::datatypes::Crs;
// use geoarrow::array::PointBuilder;
// use geoarrow::datatypes::Metadata;
// use geoarrow::datatypes::{CoordType, Dimension, PointType};
use quick_xml::Reader;
use quick_xml::events::Event;

const ADDRESS_TAG: &[u8] = b"prg-ad:PRG_PunktAdresowy";
const ADMINISTRATIVE_UNIT_TAG: &[u8] = b"prg-ad:PRG_JednostkaAdministracyjnaNazwa";
const CITY_TAG: &[u8] = b"prg-ad:PRG_MiejscowoscNazwa";
const STREET_TAG: &[u8] = b"prg-ad:PRG_UlicaNazwa";
const EPOCH_DATE: NaiveDate = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();

fn get_attribute<'a>(event_start: &'a quick_xml::events::BytesStart<'_>, attribute: &'a [u8]) -> Cow<'a, str> {
    event_start
    .attributes()
    .find(|a| {
        a.as_ref().unwrap().key.as_ref() == attribute
    })
    .unwrap()
    .unwrap()
    .decode_and_unescape_value(event_start.decoder())
    .unwrap()
    // .to_string()
}

fn str_append_value_or_null(builder: &mut StringBuilder, value: &str) {
    if value.is_empty() {
        builder.append_null();
    } else {
        builder.append_value(value);
    }
}

fn option_append_value_or_null(builder: &mut StringBuilder, value: Option<String>) {
    if value.is_none() {
        builder.append_null();
    } else {
        builder.append_value(value.unwrap());
    }
}

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

fn parse_additional_info(reader: &mut Reader<std::io::BufReader<std::fs::File>>, tag: &[u8]) -> AdditionalInfo {
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
            },
            Ok(Event::Text(e)) => {
                if last_tag.is_empty() {
                    // if last_tag is empty, we are not inside a tag and we don't want that text
                    continue;
                }
                let text_decoded = e.decode().unwrap();
                let text_trimmed = text_decoded.trim();
                match last_tag.as_slice() {
                    b"prg-ad:nazwa" => { 
                        if tag != STREET_TAG { name = Some(text_trimmed.to_string()); }
                    },
                    b"mua:przedrostek1Czesc" => { name_part_1 = text_trimmed.to_string(); },
                    b"mua:przedrostek2Czesc" => { name_part_2 = text_trimmed.to_string(); },
                    b"mua:nazwaCzesc" => { name_part_3 = text_trimmed.to_string(); },
                    b"mua:nazwaGlownaCzesc" => { name_part_4 = text_trimmed.to_string(); },
                    b"prg-ad:idTERYT" => { 
                        if !text_trimmed.is_empty() { teryt_id = Some(text_trimmed.to_string()); }
                    },
                    b"prg-ad:poziom" => {
                        match text_trimmed {
                            "1poziom" => { typ = Some(KomponentType::Country); },
                            "2poziom" => { typ = Some(KomponentType::Voivodeship); },
                            "3poziom" => { typ = Some(KomponentType::County); },
                            "4poziom" => { typ = Some(KomponentType::Municipality); },
                            _ => { panic!("Unexpected value of `prg-ad:poziom`: `{}`.", text_trimmed) },
                        }
                    },
                    _ => (),
                }
                last_tag.clear();
            },
            Ok(Event::End(ref e)) if e.name().as_ref() == tag => {
                match tag {
                    CITY_TAG => { typ = Some(KomponentType::City); },
                    STREET_TAG => {
                        typ = Some(KomponentType::Street);
                        ["a", "b"].join(" ");
                        let name_parts = [name_part_1, name_part_2, name_part_3, name_part_4];
                        let non_empty_parts: Vec<String> =
                            name_parts
                            .into_iter()
                            .filter(|s| {!s.is_empty()})
                            .collect();
                        name = Some(non_empty_parts.join(" "));
                    },
                    _ => (),
                }
                break;
            },
            Ok(Event::Eof) => { panic!("Error: reached end of file before end of address entry"); },
            Err(e) => { panic!("Error at position {}: {:?}", reader.error_position(), e); },
            _ => (), // we do not care about other events here
        }
        buffer.clear();
    }
    return AdditionalInfo { typ: typ.unwrap(), name: name.unwrap(), teryt_id: teryt_id };
}

pub fn build_dictionaries(mut reader: Reader<std::io::BufReader<std::fs::File>>) -> HashMap::<String, AdditionalInfo> {
    let mut dict = HashMap::<String, AdditionalInfo>::new();
    let mut buffer = Vec::new();
    // main loop that catches events when new object starts
    loop {
        match reader.read_event_into(&mut buffer) {
            Ok(Event::Start(ref e)) => match e.name().as_ref() {
                ADMINISTRATIVE_UNIT_TAG => {
                    let id = "http://geoportal.gov.pl/PZGIK/dane/".to_string() + &get_attribute(e, b"gml:id").to_string();
                    let info = parse_additional_info(&mut reader, ADMINISTRATIVE_UNIT_TAG);
                    dict.insert(id, info);
                },
                CITY_TAG => {
                    let id = "http://geoportal.gov.pl/PZGIK/dane/".to_string() + &get_attribute(e, b"gml:id").to_string();
                    let info = parse_additional_info(&mut reader, CITY_TAG);
                    dict.insert(id, info);
                },
                STREET_TAG => {
                    let id = "http://geoportal.gov.pl/PZGIK/dane/".to_string() + &get_attribute(e, b"gml:id").to_string();
                    let info = parse_additional_info(&mut reader, STREET_TAG);
                    dict.insert(id, info);
                },
                _ => (),
            },
            Ok(Event::Eof) => break, // exits the loop when reaching end of file
            Err(e) => panic!("Error at position {}: {:?}", reader.error_position(), e),
            _ => (), // we do not care about other events here
        }
        buffer.clear();
    }
    return dict;
}

pub struct AddressParser {
    reader: Reader<std::io::BufReader<std::fs::File>>,
    batch_size: usize,
    additional_info: HashMap<String, AdditionalInfo>,
    count: usize,
    uuid: StringBuilder,
    id_namespace: StringBuilder,
    version: TimestampSecondBuilder,
    lifecycle_start_date: TimestampSecondBuilder,
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
    // geometry: PointBuilder,
    schema: Arc<Schema>,
    // geom_type: PointType,
    epsg_2180: Proj,
    epsg_4326: Proj,
}

impl AddressParser {
    pub fn new(reader: Reader<std::io::BufReader<std::fs::File>>, batch_size: usize, additional_info: HashMap<String, AdditionalInfo>) -> Self {
        // let crs = Crs::from_authority_code("EPSG:2180".to_string());
        // let metadata = Arc::new(Metadata::new(crs, None));
        // let geom_type = PointType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
        Self {
            reader: reader,
            batch_size: batch_size,
            additional_info: additional_info,
            count: 0,
            id_namespace: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            uuid: StringBuilder::with_capacity(batch_size, 36 * batch_size),
            version: TimestampSecondBuilder::with_capacity(batch_size).with_timezone(Arc::from("UTC")),
            lifecycle_start_date: TimestampSecondBuilder::with_capacity(batch_size).with_timezone(Arc::from("UTC")),
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
            // geometry: PointBuilder::with_capacity(geom_type.clone(), batch_size),
            schema: Arc::new(Schema::new(vec![
                Field::new("przestrzen_nazw", DataType::Utf8, false),
                Field::new("lokalny_id", DataType::Utf8, false),
                Field::new("wersja_id", DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))), false),
                Field::new("poczatek_wersji_obiektu", DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))), true),
                Field::new("wazny_od", DataType::Date32, true),
                Field::new("wazny_do", DataType::Date32, true),
                Field::new("wojewodztwo", DataType::Utf8, false),
                Field::new("powiat", DataType::Utf8, false),
                Field::new("gmina", DataType::Utf8, false),
                Field::new("miejscowosc", DataType::Utf8, false),
                Field::new("czesc_miejscowosci", DataType::Utf8, true),
                Field::new("ulica", DataType::Utf8, true),
                Field::new("numer_porzadkowy", DataType::Utf8, false),
                Field::new("kod_pocztowy", DataType::Utf8, true),
                Field::new("status", DataType::Utf8, false),
                Field::new("x_epsg_2180", DataType::Float64, true),
                Field::new("y_epsg_2180", DataType::Float64, true),
                Field::new("dlugosc_geograficzna", DataType::Float64, true),
                Field::new("szerokosc_geograficzna", DataType::Float64, true),
                Field::new("teryt_wojewodztwo", DataType::Utf8, true),
                Field::new("teryt_powiat", DataType::Utf8, true),
                Field::new("teryt_gmina", DataType::Utf8, true),
                Field::new("teryt_miejscowosc", DataType::Utf8, true),
                Field::new("teryt_ulica", DataType::Utf8, true),
                // geom_type.to_field("geometry", true),
            ])),
            // geom_type: geom_type,
            epsg_2180: Proj::from_epsg_code(2180).unwrap(),
            epsg_4326: Proj::from_epsg_code(4326).unwrap(),
        }
    }

    fn build_record_batch(&mut self) -> RecordBatch {
        // let geometry_array = self.geometry.finish();
        RecordBatch::try_new(self.schema.clone(), vec![
            Arc::new(self.id_namespace.finish()),
            Arc::new(self.uuid.finish()),
            Arc::new(self.version.finish()),
            Arc::new(self.lifecycle_start_date.finish()),
            Arc::new(self.valid_since_date.finish()),
            Arc::new(self.valid_to_date.finish()),
            Arc::new(self.voivodeship.finish()),
            Arc::new(self.county.finish()),
            Arc::new(self.municipality.finish()),
            Arc::new(self.city.finish()),
            Arc::new(self.city_part.finish()),
            Arc::new(self.street.finish()),
            Arc::new(self.house_number.finish()),
            Arc::new(self.postcode.finish()),
            Arc::new(self.status.finish()),
            Arc::new(self.x_epsg_2180.finish()),
            Arc::new(self.y_epsg_2180.finish()),
            Arc::new(self.longitude.finish()),
            Arc::new(self.latitude.finish()),
            Arc::new(self.voivodeship_teryt_id.finish()),
            Arc::new(self.county_teryt_id.finish()),
            Arc::new(self.municipality_teryt_id.finish()),
            Arc::new(self.city_teryt_id.finish()),
            Arc::new(self.street_teryt_id.finish()),
            // Arc::new(geometry_array.to_array_ref()),
        ]).expect("Failed to create RecordBatch")
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
                    match e.name().as_ref(){
                        b"prg-ad:idIIP" | b"bt:BT_Identyfikator" | b"prg-ad:cyklZycia" | b"bt:BT_CyklZyciaInfo" | b"prg-ad:pozycja" | b"gml:Point" => {
                            nested_tag = true;
                            tag_ignore_text = false;
                        },
                        b"prg-ad:komponent" => {
                            let attr = get_attribute(e, b"xlink:href");
                            let info = self.additional_info.get(&attr.to_string()).cloned().unwrap_or_default();
                            match info.typ {
                                KomponentType::Country => {},
                                KomponentType::Voivodeship => { option_append_value_or_null(&mut self.voivodeship_teryt_id, info.teryt_id.clone()); },
                                KomponentType::County => { option_append_value_or_null(&mut self.county_teryt_id, info.teryt_id.clone()); },
                                KomponentType::Municipality => { option_append_value_or_null(&mut self.municipality_teryt_id, info.teryt_id.clone()); },
                                KomponentType::City => { option_append_value_or_null(&mut self.city_teryt_id, info.teryt_id.clone()); },
                                KomponentType::Street => { option_append_value_or_null(&mut self.street_teryt_id, info.teryt_id.clone()); },
                                KomponentType::Unknown => {},
                            }
                            nested_tag = false;
                            tag_ignore_text = true;
                        },
                        b"prg-ad:obiektEMUiA" => {
                            nested_tag = false;
                            tag_ignore_text = true;
                        },
                        _ => { nested_tag = false; tag_ignore_text = false;}
                    }
                },
                Ok(Event::Text(e)) => {
                    if last_tag.is_empty() || nested_tag || tag_ignore_text{
                        // if last_tag is empty, we are not inside a tag and we don't want that text
                        // if nested_tag is true, we are inside a nested tag that we want to skip (only read innermost text not the whole tree branch)
                        continue;
                    }
                    let text_decoded = e.decode().unwrap();
                    let text_trimmed = text_decoded.trim();
                    match last_tag.as_slice() {
                        b"gml:identifier" => {},
                        b"bt:lokalnyId" => { self.uuid.append_value(text_trimmed); },
                        b"bt:przestrzenNazw" => { self.id_namespace.append_value(text_trimmed); },
                        b"bt:wersjaId" => {
                            let dt = DateTime::parse_from_rfc3339(&text_trimmed)
                                .expect("Failed to parse datetime").to_utc();
                            self.version.append_value(dt.timestamp());
                        },
                        b"bt:poczatekWersjiObiektu" => {
                            if text_trimmed.is_empty() { self.lifecycle_start_date.append_null(); }
                            else {
                                let dt = DateTime::parse_from_rfc3339(&text_trimmed)
                                    .expect("Failed to parse datetime").to_utc();
                                self.lifecycle_start_date.append_value(dt.timestamp());
                            }
                        },
                        b"prg-ad:waznyOd" => {
                            if text_trimmed.is_empty() { self.valid_since_date.append_null(); }
                            else {
                                let date = NaiveDate::parse_from_str(&text_trimmed, "%Y-%m-%d")
                                    .expect("Failed to parse date");
                                self.valid_since_date.append_value(date.signed_duration_since(EPOCH_DATE).num_days() as i32);
                            }
                        },
                        b"prg-ad:waznyDo" => {
                            if text_trimmed.is_empty() { self.valid_to_date.append_null(); }
                            else {
                                let date = NaiveDate::parse_from_str(&text_trimmed, "%Y-%m-%d")
                                    .expect("Failed to parse date");
                                self.valid_to_date.append_value(date.signed_duration_since(EPOCH_DATE).num_days() as i32);
                            }
                        },
                        b"prg-ad:jednostkaAdmnistracyjna" => { // sic!
                            match admin_unit_counter {
                                0 => {},
                                1 => { self.voivodeship.append_value(text_trimmed); },
                                2 => { self.county.append_value(text_trimmed); },
                                3 => { self.municipality.append_value(text_trimmed); },
                                _ => { panic!("More than 4 administrative units found! Probably bug in the code."); }
                            }
                            admin_unit_counter += 1;
                        },
                        b"prg-ad:miejscowosc" => { self.city.append_value(text_trimmed); },
                        b"prg-ad:czescMiejscowosci" => { str_append_value_or_null(&mut self.city_part, &text_trimmed); },
                        b"prg-ad:ulica" => { str_append_value_or_null(&mut self.street, &text_trimmed); },
                        b"prg-ad:numerPorzadkowy" => { self.house_number.append_value(text_trimmed); },
                        b"prg-ad:kodPocztowy" => { str_append_value_or_null(&mut self.postcode, &text_trimmed); },
                        b"prg-ad:status" => { self.status.append_value(text_trimmed); },
                        b"gml:pos" => {
                            let coords: Vec<&str> = text_trimmed.split_whitespace().collect();
                            if coords.len() == 2 {
                                let x2180 = coords[0].parse::<f64>().unwrap_or(f64::NAN);
                                let y2180 = coords[1].parse::<f64>().unwrap_or(f64::NAN);
                                let mut p = (x2180.clone(), y2180.clone());
                                proj4rs::transform::transform(&self.epsg_2180, &self.epsg_4326, &mut p).expect("Failed to transform coordinates from EPSG:2180 to EPSG:4326");
                                self.x_epsg_2180.append_value(x2180);
                                self.y_epsg_2180.append_value(y2180);
                                self.longitude.append_value(p.0.to_degrees());
                                self.latitude.append_value(p.1.to_degrees());
                                // self.geometry.push_coord(Some(Coord::from((x2180, y2180)).as_ref()));
                            } else {
                                self.x_epsg_2180.append_null();
                                self.y_epsg_2180.append_null();
                                self.longitude.append_null();
                                self.latitude.append_null();
                                // self.geometry.push_null();
                                println!("Warning: could not parse coordinates in gml:pos: {}", text_trimmed);
                            }
                        },
                        _ => { println!("Unknown tag {:?}", std::str::from_utf8(&last_tag)); }
                    }
                    last_tag.clear();
                },
                Ok(Event::End(ref e)) if e.name().as_ref() == ADDRESS_TAG => {
                    let buffer_length = self.uuid.len();
                    // ensure all builders have the same length
                    if self.id_namespace.len() < buffer_length { self.id_namespace.append_null(); }
                    if self.version.len() < buffer_length { self.version.append_null(); }
                    if self.lifecycle_start_date.len() < buffer_length { self.lifecycle_start_date.append_null(); }
                    if self.valid_since_date.len() < buffer_length { self.valid_since_date.append_null(); }
                    if self.valid_to_date.len() < buffer_length { self.valid_to_date.append_null(); }
                    if self.voivodeship.len() < buffer_length { self.voivodeship.append_null(); }
                    if self.county.len() < buffer_length { self.county.append_null(); }
                    if self.municipality.len() < buffer_length { self.municipality.append_null(); }
                    if self.city.len() < buffer_length { self.city.append_null(); }
                    if self.city_part.len() < buffer_length { self.city_part.append_null(); }
                    if self.street.len() < buffer_length { self.street.append_null(); }
                    if self.house_number.len() < buffer_length { self.house_number.append_null(); }
                    if self.postcode.len() < buffer_length { self.postcode.append_null(); }
                    if self.status.len() < buffer_length { self.status.append_null(); }
                    if self.x_epsg_2180.len() < buffer_length { self.x_epsg_2180.append_null(); }
                    if self.y_epsg_2180.len() < buffer_length { self.y_epsg_2180.append_null(); }
                    if self.longitude.len() < buffer_length { self.longitude.append_null(); }
                    if self.latitude.len() < buffer_length { self.latitude.append_null(); }
                    if self.voivodeship_teryt_id.len() < buffer_length { self.voivodeship_teryt_id.append_null(); }
                    if self.county_teryt_id.len() < buffer_length { self.county_teryt_id.append_null(); }
                    if self.municipality_teryt_id.len() < buffer_length { self.municipality_teryt_id.append_null(); }
                    if self.city_teryt_id.len() < buffer_length { self.city_teryt_id.append_null(); }
                    if self.street_teryt_id.len() < buffer_length { self.street_teryt_id.append_null(); }
                    // while self.geometry.len() < buffer_length { self.geometry.push_null(); }
                    // end of the current address entry
                    break;
                },
                Ok(Event::Eof) => { panic!("Error: reached end of file before end of address entry"); },
                Err(e) => {
                    panic!(
                        "Error at position {}: {:?}",
                        self.reader.error_position(),
                        e
                    );
                },
                _ => (), // we do not care about other events here
            }
            buffer.clear();
        }
    }
}

impl Iterator for AddressParser {
    type Item = arrow::array::RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = Vec::new();
        // main loop that catches events when new object starts
        loop {
            match self.reader.read_event_into(&mut buffer) {
                Ok(Event::Start(ref e)) => match e.name().as_ref() {
                    ADDRESS_TAG => {
                        self.count += 1;
                        self.parse_address();
                        if self.count == self.batch_size {
                            let record_batch = self.build_record_batch();
                            // self.geometry = PointBuilder::with_capacity(self.geom_type.clone(), self.batch_size);
                            self.count = 0;
                            return Some(record_batch);
                        }
                    }
                    _ => (),
                },
                Ok(Event::Eof) => break, // exits the loop when reaching end of file
                Err(e) => panic!("Error at position {}: {:?}", self.reader.error_position(), e),
                _ => (), // we do not care about other events here
            }
            buffer.clear();
        }
        let record_batch = self.build_record_batch();
        if record_batch.num_rows() > 0 {
            return Some(record_batch);
        } else {
            return None;
        }
    }
}
