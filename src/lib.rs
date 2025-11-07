use core::f64;
use std::borrow::Cow;
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
// const ADMINISTRATIVE_UNIT_TAG: &[u8] = b"prg-ad:PRG_JednostkaAdministracyjnaNazwa";
// const LOCALITY_TAG: &[u8] = b"prg-ad:PRG_MiejscowoscNazwa";
// const STREET_TAG: &[u8] = b"prg-ad:PRG_UlicaNazwa";
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

fn append_value_or_null(builder: &mut StringBuilder, value: &str) {
    if value.is_empty() {
        builder.append_null();
    } else {
        builder.append_value(value);
    }
}

pub struct AddressParser {
    reader: Reader<std::io::BufReader<std::fs::File>>,
    batch_size: usize,
    buffer: Vec<u8>,
    count: usize,
    gml_identifier: StringBuilder,
    id: StringBuilder,
    id_namespace: StringBuilder,
    version: TimestampSecondBuilder,
    lifecycle_start_date: TimestampSecondBuilder,
    valid_since_date: Date32Builder,
    valid_to_date: Date32Builder,
    administrative_unit_0: StringBuilder,
    administrative_unit_1: StringBuilder,
    administrative_unit_2: StringBuilder,
    administrative_unit_3: StringBuilder,
    city: StringBuilder,
    city_part: StringBuilder,
    street: StringBuilder,
    house_number: StringBuilder,
    postcode: StringBuilder,
    status: StringBuilder,
    x: Float64Builder,
    y: Float64Builder,
    component_id_0: StringBuilder,
    component_id_1: StringBuilder,
    component_id_2: StringBuilder,
    component_id_3: StringBuilder,
    component_id_4: StringBuilder,
    component_id_5: StringBuilder,
    emuia_id: StringBuilder,
    // geometry: PointBuilder,
    raw_schema: Arc<Schema>,
    // geom_type: PointType,
    epsg_2180: Proj,
    epsg_4326: Proj,
}

impl AddressParser {
    pub fn new(reader: Reader<std::io::BufReader<std::fs::File>>, batch_size: usize) -> Self {
        // let crs = Crs::from_authority_code("EPSG:2180".to_string());
        // let metadata = Arc::new(Metadata::new(crs, None));
        // let geom_type = PointType::new(Dimension::XY, metadata).with_coord_type(CoordType::Separated);
        Self {
            reader: reader,
            batch_size: batch_size,
            buffer: Vec::new(),
            count: 0,
            gml_identifier: StringBuilder::with_capacity(batch_size, 91 * batch_size),
            id: StringBuilder::with_capacity(batch_size, 36 * batch_size),
            id_namespace: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            version: TimestampSecondBuilder::with_capacity(batch_size).with_timezone(Arc::from("UTC")),
            lifecycle_start_date: TimestampSecondBuilder::with_capacity(batch_size).with_timezone(Arc::from("UTC")),
            valid_since_date: Date32Builder::with_capacity(batch_size),
            valid_to_date: Date32Builder::with_capacity(batch_size),
            administrative_unit_0: StringBuilder::with_capacity(batch_size, 6 * batch_size),
            administrative_unit_1: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            administrative_unit_2: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            administrative_unit_3: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            city: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            city_part: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            street: StringBuilder::with_capacity(batch_size, 12 * batch_size),
            house_number: StringBuilder::with_capacity(batch_size, 6 * batch_size),
            postcode: StringBuilder::with_capacity(batch_size, 6 * batch_size),
            status: StringBuilder::with_capacity(batch_size, 10 * batch_size),
            x: Float64Builder::with_capacity(batch_size),
            y: Float64Builder::with_capacity(batch_size),
            component_id_0: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            component_id_1: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            component_id_2: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            component_id_3: StringBuilder::with_capacity(batch_size, 54 * batch_size),
            component_id_4: StringBuilder::with_capacity(batch_size, 62 * batch_size),
            component_id_5: StringBuilder::with_capacity(batch_size, 91 * batch_size),
            emuia_id: StringBuilder::with_capacity(batch_size, 91 * batch_size),
            // geometry: PointBuilder::with_capacity(geom_type.clone(), batch_size),
            raw_schema: Arc::new(Schema::new(vec![
                Field::new("gml_identifier", DataType::Utf8, false),
                Field::new("id", DataType::Utf8, false),
                Field::new("id_namespace", DataType::Utf8, false),
                Field::new("version", DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))), false),
                Field::new("lifecycle_start_date", DataType::Timestamp(TimeUnit::Second, Some(Arc::from("UTC"))), true),
                Field::new("valid_since_date", DataType::Date32, true),
                Field::new("valid_to_date", DataType::Date32, true),
                Field::new("administrative_unit_0", DataType::Utf8, false),
                Field::new("administrative_unit_1", DataType::Utf8, false),
                Field::new("administrative_unit_2", DataType::Utf8, false),
                Field::new("administrative_unit_3", DataType::Utf8, false),
                Field::new("city", DataType::Utf8, false),
                Field::new("city_part", DataType::Utf8, true),
                Field::new("street", DataType::Utf8, true),
                Field::new("house_number", DataType::Utf8, false),
                Field::new("postcode", DataType::Utf8, true),
                Field::new("status", DataType::Utf8, false),
                Field::new("x", DataType::Float64, true),
                Field::new("y", DataType::Float64, true),
                Field::new("component_id_0", DataType::Utf8, true),
                Field::new("component_id_1", DataType::Utf8, true),
                Field::new("component_id_2", DataType::Utf8, true),
                Field::new("component_id_3", DataType::Utf8, true),
                Field::new("component_id_4", DataType::Utf8, true),
                Field::new("component_id_5", DataType::Utf8, true),
                Field::new("emuia_id", DataType::Utf8, true),
                // geom_type.to_field("geometry", true),
            ])),
            // geom_type: geom_type,
            epsg_2180: Proj::from_epsg_code(2180).unwrap(),
            epsg_4326: Proj::from_epsg_code(4326).unwrap(),
        }
    }

    fn build_record_batch(&mut self) -> RecordBatch {
        // let geometry_array = self.geometry.finish();
        RecordBatch::try_new(self.raw_schema.clone(), vec![
            Arc::new(self.gml_identifier.finish()),
            Arc::new(self.id.finish()),
            Arc::new(self.id_namespace.finish()),
            Arc::new(self.version.finish()),
            Arc::new(self.lifecycle_start_date.finish()),
            Arc::new(self.valid_since_date.finish()),
            Arc::new(self.valid_to_date.finish()),
            Arc::new(self.administrative_unit_0.finish()),
            Arc::new(self.administrative_unit_1.finish()),
            Arc::new(self.administrative_unit_2.finish()),
            Arc::new(self.administrative_unit_3.finish()),
            Arc::new(self.city.finish()),
            Arc::new(self.city_part.finish()),
            Arc::new(self.street.finish()),
            Arc::new(self.house_number.finish()),
            Arc::new(self.postcode.finish()),
            Arc::new(self.status.finish()),
            Arc::new(self.x.finish()),
            Arc::new(self.y.finish()),
            Arc::new(self.component_id_0.finish()),
            Arc::new(self.component_id_1.finish()),
            Arc::new(self.component_id_2.finish()),
            Arc::new(self.component_id_3.finish()),
            Arc::new(self.component_id_4.finish()),
            Arc::new(self.component_id_5.finish()),
            Arc::new(self.emuia_id.finish()),
            // Arc::new(geometry_array.to_array_ref()),
        ]).expect("Failed to create RecordBatch")
    }

    fn parse_address(&mut self) {
        let mut buffer = Vec::new();
        let mut last_tag = Vec::new();
        let mut nested_tag = false; // informs if we're processing a nested tag
        let mut tag_ignore_text = false; // informs if we're processing a tag that won't have any text content
        let mut admin_unit_counter: u8 = 0;
        let mut component_counter: u8 = 0;
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
                            match component_counter {
                                0 => { append_value_or_null(&mut self.component_id_0, &attr); },
                                1 => { append_value_or_null(&mut self.component_id_1, &attr); },
                                2 => { append_value_or_null(&mut self.component_id_2, &attr); },
                                3 => { append_value_or_null(&mut self.component_id_3, &attr); },
                                4 => { append_value_or_null(&mut self.component_id_4, &attr); },
                                5 => { append_value_or_null(&mut self.component_id_5, &attr); },
                                _ => { panic!("More than 6 components found! Probably bug in the code."); }
                            }
                            component_counter += 1;
                            nested_tag = false;
                            tag_ignore_text = true;
                        },
                        b"prg-ad:obiektEMUiA" => {
                            let attr = get_attribute(e, b"xlink:href");
                            append_value_or_null(&mut self.emuia_id, &attr);
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
                        b"gml:identifier" => { self.gml_identifier.append_value(text_trimmed); },
                        b"bt:lokalnyId" => { self.id.append_value(text_trimmed); },
                        b"bt:przestrzenNazw" => { self.id_namespace.append_value(text_trimmed); },
                        b"bt:wersjaId" => {
                            let dt = DateTime::parse_from_rfc3339(&text_trimmed)
                                .expect("Failed to parse datetime").to_utc();
                            self.version.append_value(dt.timestamp());
                        },
                        b"bt:poczatekWersjiObiektu" => {
                            let dt = DateTime::parse_from_rfc3339(&text_trimmed)
                                .expect("Failed to parse datetime").to_utc();
                            self.lifecycle_start_date.append_value(dt.timestamp());
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
                                0 => { self.administrative_unit_0.append_value(text_trimmed); },
                                1 => { self.administrative_unit_1.append_value(text_trimmed); },
                                2 => { self.administrative_unit_2.append_value(text_trimmed); },
                                3 => { self.administrative_unit_3.append_value(text_trimmed); },
                                _ => { panic!("More than 4 administrative units found! Probably bug in the code."); }
                            }
                            admin_unit_counter += 1;
                        },
                        b"prg-ad:miejscowosc" => { self.city.append_value(text_trimmed); },
                        b"prg-ad:czescMiejscowosci" => { append_value_or_null(&mut self.city_part, &text_trimmed); },
                        b"prg-ad:ulica" => { append_value_or_null(&mut self.street, &text_trimmed); },
                        b"prg-ad:numerPorzadkowy" => { self.house_number.append_value(text_trimmed); },
                        b"prg-ad:kodPocztowy" => { append_value_or_null(&mut self.postcode, &text_trimmed); },
                        b"prg-ad:status" => { self.status.append_value(text_trimmed); },
                        b"gml:pos" => {
                            let coords: Vec<&str> = text_trimmed.split_whitespace().collect();
                            if coords.len() == 2 {
                                let x2180 = coords[0].parse::<f64>().unwrap_or(f64::NAN);
                                let y2180 = coords[1].parse::<f64>().unwrap_or(f64::NAN);
                                let mut p = (x2180.clone(), y2180.clone());
                                proj4rs::transform::transform(&self.epsg_2180, &self.epsg_4326, &mut p).expect("Failed to transform coordinates from EPSG:2180 to EPSG:4326");
                                self.x.append_value(p.0.to_degrees());
                                self.y.append_value(p.1.to_degrees());
                                // self.geometry.push_coord(Some(Coord::from((x2180, y2180)).as_ref()));
                            } else {
                                self.x.append_null();
                                self.y.append_null();
                                // self.geometry.push_null();
                                println!("Warning: could not parse coordinates in gml:pos: {}", text_trimmed);
                            }
                        },
                        _ => { println!("Unknown tag {:?}", std::str::from_utf8(&last_tag)); }
                    }
                    last_tag.clear();
                },
                Ok(Event::End(ref e)) if e.name().as_ref() == ADDRESS_TAG => {
                    let buffer_length = self.gml_identifier.len();
                    // ensure all builders have the same length
                    if self.id.len() < buffer_length { self.id.append_null(); }
                    if self.id_namespace.len() < buffer_length { self.id_namespace.append_null(); }
                    if self.version.len() < buffer_length { self.version.append_null(); }
                    if self.lifecycle_start_date.len() < buffer_length { self.lifecycle_start_date.append_null(); }
                    if self.valid_since_date.len() < buffer_length { self.valid_since_date.append_null(); }
                    if self.valid_to_date.len() < buffer_length { self.valid_to_date.append_null(); }
                    if self.administrative_unit_0.len() < buffer_length { self.administrative_unit_0.append_null(); }
                    if self.administrative_unit_1.len() < buffer_length { self.administrative_unit_1.append_null(); }
                    if self.administrative_unit_2.len() < buffer_length { self.administrative_unit_2.append_null(); }
                    if self.administrative_unit_3.len() < buffer_length { self.administrative_unit_3.append_null(); }
                    if self.city.len() < buffer_length { self.city.append_null(); }
                    if self.city_part.len() < buffer_length { self.city_part.append_null(); }
                    if self.street.len() < buffer_length { self.street.append_null(); }
                    if self.house_number.len() < buffer_length { self.house_number.append_null(); }
                    if self.postcode.len() < buffer_length { self.postcode.append_null(); }
                    if self.status.len() < buffer_length { self.status.append_null(); }
                    if self.x.len() < buffer_length { self.x.append_null(); }
                    if self.y.len() < buffer_length { self.y.append_null(); }
                    if self.component_id_0.len() < buffer_length { self.component_id_0.append_null(); }
                    if self.component_id_1.len() < buffer_length { self.component_id_1.append_null(); }
                    if self.component_id_2.len() < buffer_length { self.component_id_2.append_null(); }
                    if self.component_id_3.len() < buffer_length { self.component_id_3.append_null(); }
                    if self.component_id_4.len() < buffer_length { self.component_id_4.append_null(); }
                    if self.component_id_5.len() < buffer_length { self.component_id_5.append_null(); }
                    if self.emuia_id.len() < buffer_length { self.emuia_id.append_null(); }
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
    // main loop that catches events when new object starts
        loop {
            match self.reader.read_event_into(&mut self.buffer) {
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
            self.buffer.clear();
        }
        let record_batch = self.build_record_batch();
        if record_batch.num_rows() > 0 {
            return Some(record_batch);
        } else {
            return None;
        }
    }
}
