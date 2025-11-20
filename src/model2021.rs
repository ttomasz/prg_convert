use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow::array::Date32Builder;
use arrow::array::Float64Builder;
use arrow::array::RecordBatch;
use arrow::array::StringBuilder;
use arrow::array::TimestampMillisecondBuilder;
use chrono::DateTime;
use chrono::NaiveDate;
use chrono::NaiveDateTime;
use geo_types::Point;
use geoarrow::array::GeoArrowArray;
use geoarrow::array::PointBuilder;
use once_cell::sync::Lazy;
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
use crate::terc::Terc;

const CITY_TAG: &[u8] = b"prgad:AD_Miejscowosc";
const STREET_TAG: &[u8] = b"prgad:AD_UlicaPlac";
const ADDRESS_TAG: &[u8] = b"prgad:AD_PunktAdresowy";

#[allow(dead_code)]
struct City {
    name: String,
    kind: String,
    city_teryt_id: Option<String>,
    municipality_teryt_id: String,
}

#[allow(dead_code)]
struct Street {
    name: String,
    kind: String,
    teryt_id: Option<String>,
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
pub fn construct_full_name_from_parts(part1: &String, part2: &Option<String>, typ: &str) -> String {
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
    let name_parts = [
        prefix.to_string(),
        part2.clone().unwrap_or_default(),
        part1.to_string(),
    ];
    let non_empty_parts: Vec<String> = name_parts.into_iter().filter(|s| !s.is_empty()).collect();
    non_empty_parts.join(" ")
}

fn parse_city(reader: &mut Reader<std::io::BufReader<std::fs::File>>) -> City {
    let mut buffer = Vec::new();
    let mut last_tag = Vec::new();
    let mut kind = String::new();
    let mut name = String::new();
    let mut city_teryt_id = None;
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
                let text_decoded = e.decode().expect("Failed to decode text.");
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
                        city_teryt_id = Some(text_trimmed.to_string());
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
    let mut kind = String::new();
    let mut name = String::new();
    let mut teryt_id = None;
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
                let text_decoded = e.decode().expect("Failed to decode text.");
                let text_trimmed = text_decoded.trim();
                match last_tag.as_slice() {
                    b"prgad:rodzaj" => {
                        kind = text_trimmed.to_owned();
                    }
                    b"prgad:identyfikatorULIC" => {
                        teryt_id = Some(text_trimmed.to_string());
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
                if !kind.is_empty() && !part1.is_empty() {
                    name = construct_full_name_from_parts(&part1, &part2, &kind);
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
    if name.is_empty() {
        panic!(
            "Could not parse street name for: {} {} {}",
            kind,
            part1,
            part2.unwrap_or_default()
        );
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
                }
                STREET_TAG => {
                    let id = get_attribute(e, b"gml:id").to_string();
                    let info = parse_street(&mut reader);
                    street_dict.insert(id, info);
                }
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

pub struct AddressParser2021 {
    reader: Reader<std::io::BufReader<std::fs::File>>,
    batch_size: usize,
    output_format: OutputFormat,
    mappings: Mappings,
    teryt_names: HashMap<String, Terc>,
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

impl AddressParser2021 {
    pub fn new(
        reader: Reader<std::io::BufReader<std::fs::File>>,
        batch_size: usize,
        output_format: OutputFormat,
        additional_info: Mappings,
        teryt_names: HashMap<String, Terc>,
    ) -> Self {
        Self {
            reader: reader,
            batch_size: batch_size,
            output_format: output_format,
            mappings: additional_info,
            teryt_names: teryt_names,
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
            OutputFormat::CSV => {
                RecordBatch::try_new(
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
                .expect("Failed to create RecordBatch")
            }
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
        // inside loop to process the content of the current address
        loop {
            match self.reader.read_event_into(&mut buffer) {
                Ok(Event::Start(ref e)) => {
                    last_tag = e.name().as_ref().to_vec();
                    match e.name().as_ref() {
                        b"prgad:idIIP"
                        | b"prgad:AD_IdentyfikatorIIP"
                        | b"prgad:georeferencja"
                        | b"gml:Point" => {
                            nested_tag = true;
                            tag_ignore_text = false;
                        }
                        b"prgad:miejscowosc" => {
                            let id = &get_attribute(e, b"xlink:href")[1..];
                            let city = self.mappings.city.get(id);
                            match city {
                                None => {}
                                Some(c) => {
                                    self.city.append_value(&c.name);
                                    self.municipality_teryt_id
                                        .append_value(&c.municipality_teryt_id);
                                    option_append_value_or_null(
                                        &mut self.city_teryt_id,
                                        c.city_teryt_id.clone(),
                                    );
                                    let terc_info = self.teryt_names.get(&c.municipality_teryt_id);
                                    match terc_info {
                                        None => {
                                            println!(
                                                "Could not find info for municipality with teryt id: {}",
                                                &c.municipality_teryt_id
                                            );
                                        }
                                        Some(t) => {
                                            self.voivodeship_teryt_id
                                                .append_value(t.voivodeship_teryt_id.clone());
                                            self.voivodeship
                                                .append_value(t.voivodeship_name.clone());
                                            self.county_teryt_id
                                                .append_value(t.county_teryt_id.clone());
                                            self.county.append_value(t.county_name.clone());
                                            self.municipality
                                                .append_value(t.municipality_name.clone());
                                        }
                                    }
                                }
                            }
                            nested_tag = false;
                            tag_ignore_text = true;
                        }
                        b"prgad:ulica2" => {
                            let id = &get_attribute(e, b"xlink:href")[1..];
                            let street = self.mappings.street.get(id);
                            match street {
                                None => {}
                                Some(s) => {
                                    self.street.append_value(&s.name);
                                    option_append_value_or_null(
                                        &mut self.street_teryt_id,
                                        s.teryt_id.clone(),
                                    );
                                }
                            }
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
                        b"prgad:lokalnyId" => {
                            self.uuid.append_value(text_trimmed);
                        }
                        b"prgad:przestrzenNazw" => {
                            self.id_namespace.append_value(text_trimmed);
                        }
                        b"prgad:wersjaId" => {
                            let dt = DateTime::parse_from_rfc3339(text_trimmed)
                                .expect("Failed to parse datetime")
                                .to_utc();
                            self.version.append_value(dt.timestamp() * 1000);
                        }
                        b"prgad:poczatekWersjiObiektu" => {
                            if text_trimmed.is_empty() {
                                self.lifecycle_start_date.append_null();
                            } else {
                                let dt = NaiveDateTime::parse_from_str(
                                    &text_trimmed,
                                    "%Y-%m-%dT%H:%M:%S",
                                )
                                .expect("Failed to parse datetime")
                                .and_local_timezone(
                                    chrono::FixedOffset::east_opt(2 * 60 * 60).unwrap(),
                                ) // assume +02:00 tz
                                .unwrap()
                                .to_utc();
                                self.lifecycle_start_date
                                    .append_value(dt.timestamp() * 1000);
                            }
                        }
                        b"prgad:dataNadania" => {
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
                        b"prgad:numerPorzadkowy" => {
                            self.house_number.append_value(text_trimmed);
                        }
                        b"prgad:kodPocztowy" => {
                            str_append_value_or_null(&mut self.postcode, text_trimmed);
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

impl Iterator for AddressParser2021 {
    type Item = arrow::array::RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = Vec::new();
        let mut row_count: usize = 0;
        // main loop that catches events when new object starts
        loop {
            match self.reader.read_event_into(&mut buffer) {
                Ok(Event::Start(ref e)) => if e.name().as_ref() == ADDRESS_TAG {
                    row_count += 1;
                    self.parse_address();
                    if row_count == self.batch_size {
                        let record_batch = self.build_record_batch();
                        return Some(record_batch);
                    }
                },
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
    let typ = "1";
    let part1 = "Test".to_string();
    let part2 = None;
    let expected_name = "Test";
    let name = construct_full_name_from_parts(&part1, &part2, &typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_part2() {
    let typ = "1";
    let part1 = "Test".to_string();
    let part2 = Some("Test2".to_string());
    let expected_name = "Test2 Test";
    let name = construct_full_name_from_parts(&part1, &part2, &typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_typ_3() {
    let typ = "3";
    let part1 = "Test".to_string();
    let part2 = None;
    let expected_name = "plac Test";
    let name = construct_full_name_from_parts(&part1, &part2, &typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_part2_typ_3() {
    let typ = "3";
    let part1 = "Test".to_string();
    let part2 = Some("Test2".to_string());
    let expected_name = "plac Test2 Test";
    let name = construct_full_name_from_parts(&part1, &part2, &typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_typ_3_prefix() {
    let typ = "3";
    let part1 = "plac Test".to_string();
    let part2 = None;
    let expected_name = "plac Test";
    let name = construct_full_name_from_parts(&part1, &part2, &typ);
    assert_eq!(name, expected_name);
}

#[test]
fn name_from_part1_typ_3_prefix_short() {
    let typ = "3";
    let part1 = "pl. Test".to_string();
    let part2 = None;
    let expected_name = "pl. Test";
    let name = construct_full_name_from_parts(&part1, &part2, &typ);
    assert_eq!(name, expected_name);
}
