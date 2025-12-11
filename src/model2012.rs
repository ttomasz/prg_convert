use std::collections::HashMap;
use std::io::BufRead;
use std::sync::Arc;

use arrow::array::ArrayBuilder;
use arrow::array::Date32Builder;
use arrow::array::Float64Builder;
use arrow::array::RecordBatch;
use arrow::array::StringBuilder;
use arrow::array::TimestampMillisecondBuilder;
use arrow::datatypes::Schema;
use chrono::DateTime;
use chrono::NaiveDate;
use geo_types::Point;
use geoarrow::array::GeoArrowArray;
use geoarrow::array::PointBuilder;
use geoarrow::datatypes::PointType;
use quick_xml::Reader;
use quick_xml::events::Event;

use crate::CRS;
use crate::CoordOrder;
use crate::OutputFormat;
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
    crs: crate::CRS,
    geoarrow_geom_type: PointType,
    arrow_schema: Arc<Schema>,
}

impl<R: BufRead> AddressParser2012<R> {
    pub fn new(
        reader: Reader<R>,
        batch_size: usize,
        output_format: OutputFormat,
        additional_info: HashMap<String, AdditionalInfo>,
        crs: crate::CRS,
        arrow_schema: Arc<Schema>,
        geoarrow_geom_type: PointType,
    ) -> Self {
        Self {
            reader,
            batch_size: batch_size,
            output_format: output_format,
            additional_info: additional_info,
            crs: crs,
            geoarrow_geom_type: geoarrow_geom_type,
            arrow_schema: arrow_schema,
        }
    }
}

impl<R: BufRead> Iterator for AddressParser2012<R> {
    type Item = arrow::array::RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        let mut buffer = Vec::new();
        let mut row_count: usize = 0;

        let mut id_namespace = StringBuilder::with_capacity(self.batch_size, 12 * self.batch_size);
        let mut uuid = StringBuilder::with_capacity(self.batch_size, 36 * self.batch_size);
        let mut version = TimestampMillisecondBuilder::with_capacity(self.batch_size)
            .with_timezone(Arc::from("UTC"));
        let mut lifecycle_start_date = TimestampMillisecondBuilder::with_capacity(self.batch_size)
            .with_timezone(Arc::from("UTC"));
        let mut valid_since_date = Date32Builder::with_capacity(self.batch_size);
        let mut valid_to_date = Date32Builder::with_capacity(self.batch_size);
        let mut voivodeship = StringBuilder::with_capacity(self.batch_size, 12 * self.batch_size);
        let mut county = StringBuilder::with_capacity(self.batch_size, 12 * self.batch_size);
        let mut municipality = StringBuilder::with_capacity(self.batch_size, 12 * self.batch_size);
        let mut city = StringBuilder::with_capacity(self.batch_size, 12 * self.batch_size);
        let mut city_part = StringBuilder::with_capacity(self.batch_size, 12 * self.batch_size);
        let mut street = StringBuilder::with_capacity(self.batch_size, 12 * self.batch_size);
        let mut house_number = StringBuilder::with_capacity(self.batch_size, 6 * self.batch_size);
        let mut postcode = StringBuilder::with_capacity(self.batch_size, 6 * self.batch_size);
        let mut status = StringBuilder::with_capacity(self.batch_size, 10 * self.batch_size);
        let mut x_epsg_2180 = Float64Builder::with_capacity(self.batch_size);
        let mut y_epsg_2180 = Float64Builder::with_capacity(self.batch_size);
        let mut longitude = Float64Builder::with_capacity(self.batch_size);
        let mut latitude = Float64Builder::with_capacity(self.batch_size);
        let mut voivodeship_teryt_id =
            StringBuilder::with_capacity(self.batch_size, 54 * self.batch_size);
        let mut county_teryt_id =
            StringBuilder::with_capacity(self.batch_size, 54 * self.batch_size);
        let mut municipality_teryt_id =
            StringBuilder::with_capacity(self.batch_size, 54 * self.batch_size);
        let mut city_teryt_id = StringBuilder::with_capacity(self.batch_size, 62 * self.batch_size);
        let mut street_teryt_id =
            StringBuilder::with_capacity(self.batch_size, 91 * self.batch_size);
        let mut geometry: Vec<Option<Point>> = Vec::with_capacity(self.batch_size);

        // main loop that catches events when new object starts
        loop {
            match self.reader.read_event_into(&mut buffer) {
                Ok(Event::Start(ref e)) => {
                    if e.name().as_ref() == ADDRESS_TAG {
                        row_count += 1;
        let mut buffer2 = Vec::new();
        let mut last_tag = Vec::new();
        let mut nested_tag = false; // informs if we're processing a nested tag
        let mut tag_ignore_text = false; // informs if we're processing a tag that won't have any text content
        let mut admin_unit_counter: u8 = 0;
        // inside loop to process the content of the current address
        loop {
            match self.reader.read_event_into(&mut buffer2) {
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
                                        &mut voivodeship_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::County => {
                                    option_append_value_or_null(
                                        &mut county_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::Municipality => {
                                    option_append_value_or_null(
                                        &mut municipality_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::City => {
                                    option_append_value_or_null(
                                        &mut city_teryt_id,
                                        info.teryt_id.clone(),
                                    );
                                }
                                KomponentType::Street => {
                                    option_append_value_or_null(
                                        &mut street_teryt_id,
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
                            uuid.append_value(text_trimmed);
                        }
                        b"bt:przestrzenNazw" => {
                            id_namespace.append_value(text_trimmed);
                        }
                        b"bt:wersjaId" => {
                            let dt = DateTime::parse_from_rfc3339(text_trimmed)
                                .expect("Failed to parse datetime")
                                .to_utc();
                            version.append_value(dt.timestamp() * 1000);
                        }
                        b"bt:poczatekWersjiObiektu" => {
                            if text_trimmed.is_empty() {
                                lifecycle_start_date.append_null();
                            } else {
                                let dt = DateTime::parse_from_rfc3339(text_trimmed)
                                    .expect("Failed to parse datetime")
                                    .to_utc();
                                lifecycle_start_date.append_value(dt.timestamp() * 1000);
                            }
                        }
                        b"prg-ad:waznyOd" => {
                            if text_trimmed.is_empty() {
                                valid_since_date.append_null();
                            } else {
                                let date = NaiveDate::parse_from_str(text_trimmed, "%Y-%m-%d")
                                    .expect("Failed to parse date");
                                valid_since_date.append_value(
                                    date.signed_duration_since(EPOCH_DATE).num_days() as i32,
                                );
                            }
                        }
                        b"prg-ad:waznyDo" => {
                            if text_trimmed.is_empty() {
                                valid_to_date.append_null();
                            } else {
                                let date = NaiveDate::parse_from_str(text_trimmed, "%Y-%m-%d")
                                    .expect("Failed to parse date");
                                valid_to_date.append_value(
                                    date.signed_duration_since(EPOCH_DATE).num_days() as i32,
                                );
                            }
                        }
                        b"prg-ad:jednostkaAdmnistracyjna" => {
                            // sic!
                            match admin_unit_counter {
                                0 => {}
                                1 => {
                                    voivodeship.append_value(text_trimmed);
                                }
                                2 => {
                                    county.append_value(text_trimmed);
                                }
                                3 => {
                                    municipality.append_value(text_trimmed);
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
                            city.append_value(text_trimmed);
                        }
                        b"prg-ad:czescMiejscowosci" => {
                            str_append_value_or_null(&mut city_part, text_trimmed);
                        }
                        b"prg-ad:ulica" => {
                            str_append_value_or_null(&mut street, text_trimmed);
                        }
                        b"prg-ad:numerPorzadkowy" => {
                            house_number.append_value(text_trimmed);
                        }
                        b"prg-ad:kodPocztowy" => {
                            str_append_value_or_null(&mut postcode, text_trimmed);
                        }
                        b"prg-ad:status" => {
                            status.append_value(text_trimmed);
                        }
                        b"gml:pos" => {
                            let coords = parse_gml_pos(text_trimmed, CoordOrder::YX)
                                .expect("Could not parse coordinates.");
                            match coords {
                                None => {
                                    longitude.append_null();
                                    latitude.append_null();
                                    match self.output_format {
                                        OutputFormat::CSV => {
                                            x_epsg_2180.append_null();
                                            y_epsg_2180.append_null();
                                        }
                                        OutputFormat::GeoParquet => {
                                            geometry.push(None);
                                        }
                                    }
                                }
                                Some(coords) => {
                                    longitude.append_value(coords.x4326);
                                    latitude.append_value(coords.y4326);
                                    match self.output_format {
                                        OutputFormat::CSV => {
                                            x_epsg_2180.append_value(coords.x2180);
                                            y_epsg_2180.append_value(coords.y2180);
                                        }
                                        OutputFormat::GeoParquet => match self.crs {
                                            CRS::Epsg2180 => {
                                                geometry.push(Some(geo_types::point!(x: coords.x2180, y: coords.y2180)));
                                            }
                                            CRS::Epsg4326 => {
                                                geometry.push(Some(geo_types::point!(x: coords.x4326, y: coords.y4326)));
                                            }
                                        },
                                    }
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
                    let buffer_length = uuid.len();
                    // ensure all builders have the same length
                    if id_namespace.len() < buffer_length {
                        id_namespace.append_null();
                    }
                    if version.len() < buffer_length {
                        version.append_null();
                    }
                    if lifecycle_start_date.len() < buffer_length {
                        lifecycle_start_date.append_null();
                    }
                    if valid_since_date.len() < buffer_length {
                        valid_since_date.append_null();
                    }
                    if valid_to_date.len() < buffer_length {
                        valid_to_date.append_null();
                    }
                    if voivodeship.len() < buffer_length {
                        voivodeship.append_null();
                    }
                    if county.len() < buffer_length {
                        county.append_null();
                    }
                    if municipality.len() < buffer_length {
                        municipality.append_null();
                    }
                    if city.len() < buffer_length {
                        city.append_null();
                    }
                    if city_part.len() < buffer_length {
                        city_part.append_null();
                    }
                    if street.len() < buffer_length {
                        street.append_null();
                    }
                    if house_number.len() < buffer_length {
                        house_number.append_null();
                    }
                    if postcode.len() < buffer_length {
                        postcode.append_null();
                    }
                    if status.len() < buffer_length {
                        status.append_null();
                    }
                    if longitude.len() < buffer_length {
                        longitude.append_null();
                    }
                    if latitude.len() < buffer_length {
                        latitude.append_null();
                    }
                    if voivodeship_teryt_id.len() < buffer_length {
                        voivodeship_teryt_id.append_null();
                    }
                    if county_teryt_id.len() < buffer_length {
                        county_teryt_id.append_null();
                    }
                    if municipality_teryt_id.len() < buffer_length {
                        municipality_teryt_id.append_null();
                    }
                    if city_teryt_id.len() < buffer_length {
                        city_teryt_id.append_null();
                    }
                    if street_teryt_id.len() < buffer_length {
                        street_teryt_id.append_null();
                    }
                    match self.output_format {
                        OutputFormat::CSV => {
                            if x_epsg_2180.len() < buffer_length {
                                x_epsg_2180.append_null();
                            }
                            if y_epsg_2180.len() < buffer_length {
                                y_epsg_2180.append_null();
                            }
                        }
                        OutputFormat::GeoParquet => {
                            if geometry.len() < buffer_length {
                                geometry.push(None);
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
            buffer2.clear();
        }
                        if row_count == self.batch_size {
                            let record_batch = match self.output_format {
                                OutputFormat::CSV => RecordBatch::try_new(
                                    self.arrow_schema.clone(),
                                    vec![
                                        Arc::new(id_namespace.finish()),
                                        Arc::new(uuid.finish()),
                                        Arc::new(version.finish()),
                                        Arc::new(lifecycle_start_date.finish()),
                                        Arc::new(valid_since_date.finish()),
                                        Arc::new(valid_to_date.finish()),
                                        Arc::new(voivodeship_teryt_id.finish()),
                                        Arc::new(voivodeship.finish()),
                                        Arc::new(county_teryt_id.finish()),
                                        Arc::new(county.finish()),
                                        Arc::new(municipality_teryt_id.finish()),
                                        Arc::new(municipality.finish()),
                                        Arc::new(city_teryt_id.finish()),
                                        Arc::new(city.finish()),
                                        Arc::new(city_part.finish()),
                                        Arc::new(street_teryt_id.finish()),
                                        Arc::new(street.finish()),
                                        Arc::new(house_number.finish()),
                                        Arc::new(postcode.finish()),
                                        Arc::new(status.finish()),
                                        Arc::new(x_epsg_2180.finish()),
                                        Arc::new(y_epsg_2180.finish()),
                                        Arc::new(longitude.finish()),
                                        Arc::new(latitude.finish()),
                                    ],
                                )
                                .expect("Failed to create RecordBatch"),
                                OutputFormat::GeoParquet => {
                                    let iter = geometry.iter().map(Option::as_ref);
                                    let geometry_array = PointBuilder::from_nullable_points(
                                        iter,
                                        self.geoarrow_geom_type.clone(),
                                    )
                                    .finish();
                                    RecordBatch::try_new(
                                        self.arrow_schema.clone(),
                                        vec![
                                            Arc::new(id_namespace.finish()),
                                            Arc::new(uuid.finish()),
                                            Arc::new(version.finish()),
                                            Arc::new(lifecycle_start_date.finish()),
                                            Arc::new(valid_since_date.finish()),
                                            Arc::new(valid_to_date.finish()),
                                            Arc::new(voivodeship_teryt_id.finish()),
                                            Arc::new(voivodeship.finish()),
                                            Arc::new(county_teryt_id.finish()),
                                            Arc::new(county.finish()),
                                            Arc::new(municipality_teryt_id.finish()),
                                            Arc::new(municipality.finish()),
                                            Arc::new(city_teryt_id.finish()),
                                            Arc::new(city.finish()),
                                            Arc::new(city_part.finish()),
                                            Arc::new(street_teryt_id.finish()),
                                            Arc::new(street.finish()),
                                            Arc::new(house_number.finish()),
                                            Arc::new(postcode.finish()),
                                            Arc::new(status.finish()),
                                            Arc::new(longitude.finish()),
                                            Arc::new(latitude.finish()),
                                            Arc::new(geometry_array.to_array_ref()),
                                        ],
                                    )
                                    .expect("Failed to create RecordBatch")
                                }
                            };
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
        let record_batch = match self.output_format {
            OutputFormat::CSV => RecordBatch::try_new(
                self.arrow_schema.clone(),
                vec![
                    Arc::new(id_namespace.finish()),
                    Arc::new(uuid.finish()),
                    Arc::new(version.finish()),
                    Arc::new(lifecycle_start_date.finish()),
                    Arc::new(valid_since_date.finish()),
                    Arc::new(valid_to_date.finish()),
                    Arc::new(voivodeship_teryt_id.finish()),
                    Arc::new(voivodeship.finish()),
                    Arc::new(county_teryt_id.finish()),
                    Arc::new(county.finish()),
                    Arc::new(municipality_teryt_id.finish()),
                    Arc::new(municipality.finish()),
                    Arc::new(city_teryt_id.finish()),
                    Arc::new(city.finish()),
                    Arc::new(city_part.finish()),
                    Arc::new(street_teryt_id.finish()),
                    Arc::new(street.finish()),
                    Arc::new(house_number.finish()),
                    Arc::new(postcode.finish()),
                    Arc::new(status.finish()),
                    Arc::new(x_epsg_2180.finish()),
                    Arc::new(y_epsg_2180.finish()),
                    Arc::new(longitude.finish()),
                    Arc::new(latitude.finish()),
                ],
            )
            .expect("Failed to create RecordBatch"),
            OutputFormat::GeoParquet => {
                let iter = geometry.iter().map(Option::as_ref);
                let geometry_array =
                    PointBuilder::from_nullable_points(iter, self.geoarrow_geom_type.clone())
                        .finish();
                RecordBatch::try_new(
                    self.arrow_schema.clone(),
                    vec![
                        Arc::new(id_namespace.finish()),
                        Arc::new(uuid.finish()),
                        Arc::new(version.finish()),
                        Arc::new(lifecycle_start_date.finish()),
                        Arc::new(valid_since_date.finish()),
                        Arc::new(valid_to_date.finish()),
                        Arc::new(voivodeship_teryt_id.finish()),
                        Arc::new(voivodeship.finish()),
                        Arc::new(county_teryt_id.finish()),
                        Arc::new(county.finish()),
                        Arc::new(municipality_teryt_id.finish()),
                        Arc::new(municipality.finish()),
                        Arc::new(city_teryt_id.finish()),
                        Arc::new(city.finish()),
                        Arc::new(city_part.finish()),
                        Arc::new(street_teryt_id.finish()),
                        Arc::new(street.finish()),
                        Arc::new(house_number.finish()),
                        Arc::new(postcode.finish()),
                        Arc::new(status.finish()),
                        Arc::new(longitude.finish()),
                        Arc::new(latitude.finish()),
                        Arc::new(geometry_array.to_array_ref()),
                    ],
                )
                .expect("Failed to create RecordBatch")
            }
        };
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
