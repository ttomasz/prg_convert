use std::borrow::Cow;
use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::array::Float64Builder;
use arrow::array::StringBuilder;

mod constants;
use constants::EPSG_2180;
use constants::EPSG_4326;
pub use constants::SCHEMA_CSV;
pub use constants::SCHEMA_GEOPARQUET;
mod model2012;
use model2012::AddressParser;
use model2012::build_dictionaries;
use quick_xml::Reader;

mod model2021;

fn get_attribute<'a>(
    event_start: &'a quick_xml::events::BytesStart<'_>,
    attribute: &'a [u8],
) -> Cow<'a, str> {
    event_start
        .attributes()
        .find(|a| a.as_ref().unwrap().key.as_ref() == attribute)
        .unwrap()
        .unwrap()
        .decode_and_unescape_value(event_start.decoder())
        .unwrap()
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

fn parse_gml_pos(
    text_trimmed: &str,
    longitude: &mut Float64Builder,
    latitude: &mut Float64Builder,
    x_epsg_2180: &mut Float64Builder,
    y_epsg_2180: &mut Float64Builder,
    geometry: &mut Vec<Option<geo_types::Point>>,
    output_format: &OutputFormat,
) {
    let coords: Vec<&str> = text_trimmed.split_whitespace().collect();
    if coords.len() == 2 {
        let y2180 = coords[0].parse::<f64>().unwrap_or(f64::NAN);
        let x2180 = coords[1].parse::<f64>().unwrap_or(f64::NAN);
        if x2180.is_nan() || y2180.is_nan() {
            longitude.append_null();
            latitude.append_null();
            match output_format {
                OutputFormat::CSV => {
                    x_epsg_2180.append_null();
                    y_epsg_2180.append_null();
                }
                OutputFormat::GeoParquet => {
                    geometry.push(None);
                }
            }
        } else {
            let mut p = (x2180.clone(), y2180.clone());
            proj4rs::transform::transform(&EPSG_2180, &EPSG_4326, &mut p)
                .expect("Failed to transform coordinates from EPSG:2180 to EPSG:4326");
            longitude.append_value(p.0.to_degrees());
            latitude.append_value(p.1.to_degrees());
            match output_format {
                OutputFormat::CSV => {
                    x_epsg_2180.append_value(x2180);
                    y_epsg_2180.append_value(y2180);
                }
                OutputFormat::GeoParquet => {
                    geometry.push(Some(geo_types::point!(x: x2180, y: y2180)));
                }
            }
        }
    } else {
        panic!(
            "Warning: could not parse coordinates in gml:pos: `{}`.",
            text_trimmed
        );
    }
}

#[derive(Clone)]
pub enum OutputFormat {
    CSV,
    GeoParquet,
}

pub enum SchemaVersion {
    Model2012,
    Model2021,
}

fn get_xml_reader(path: &PathBuf) -> Result<Reader<std::io::BufReader<std::fs::File>>> {
    let mut reader = Reader::from_file(&path)
        .with_context(|| format!("could not read XML from file `{}`", &path.display()))?;
    reader.config_mut().expand_empty_elements = true; // makes it easier to process empty tags (</x>)
    return Ok(reader);
}

pub fn get_address_parser_2012(
    file_path: &PathBuf,
    batch_size: &usize,
    output_format: &OutputFormat,
    print_configuration: bool,
) -> AddressParser {
    let mut reader = get_xml_reader(&file_path).unwrap();
    if print_configuration {
        println!("⚙️  XML reader configuration: {:#?}", reader.config());
        println!("----------------------------------------");
    }
    println!("Building dictionaries...");
    let dict = build_dictionaries(reader);
    reader = get_xml_reader(&file_path).unwrap();
    AddressParser::new(reader, batch_size.clone(), dict, output_format.clone())
}

#[test]
fn test_get_attribute_returns_value() {
    let xml = r#"<root attr="hello" key="value"/>"#;
    let mut reader = Reader::from_str(xml);
    reader.config_mut().expand_empty_elements = true;
    let mut buf = Vec::new();
    loop {
        match reader.read_event_into(&mut buf).unwrap() {
            quick_xml::events::Event::Start(e) => {
                assert_eq!(get_attribute(&e, b"attr"), Cow::from("hello"));
                assert_eq!(get_attribute(&e, b"key"), Cow::from("value"));
                break;
            }
            _ => {}
        }
    }
}

#[test]
#[should_panic]
fn test_parse_gml_pos_empty() {
    let mut longitude = Float64Builder::new();
    let mut latitude = Float64Builder::new();
    let mut x_epsg_2180 = Float64Builder::new();
    let mut y_epsg_2180 = Float64Builder::new();
    let mut geometry= Vec::new();
    let gml_pos = "";
    parse_gml_pos(gml_pos, &mut longitude, &mut latitude, &mut x_epsg_2180, &mut y_epsg_2180, &mut geometry, &OutputFormat::CSV);
}

#[test]
#[should_panic]
fn test_parse_gml_pos_1() {
    let mut longitude = Float64Builder::new();
    let mut latitude = Float64Builder::new();
    let mut x_epsg_2180 = Float64Builder::new();
    let mut y_epsg_2180 = Float64Builder::new();
    let mut geometry= Vec::new();
    let gml_pos = "0.0";
    parse_gml_pos(gml_pos, &mut longitude, &mut latitude, &mut x_epsg_2180, &mut y_epsg_2180, &mut geometry, &OutputFormat::CSV);
}

#[test]
#[should_panic]
fn test_parse_gml_pos_3() {
    let mut longitude = Float64Builder::new();
    let mut latitude = Float64Builder::new();
    let mut x_epsg_2180 = Float64Builder::new();
    let mut y_epsg_2180 = Float64Builder::new();
    let mut geometry= Vec::new();
    let gml_pos = "0.0 1.1 2.2";
    parse_gml_pos(gml_pos, &mut longitude, &mut latitude, &mut x_epsg_2180, &mut y_epsg_2180, &mut geometry, &OutputFormat::CSV);
}

#[test]
fn test_parse_gml_pos_nan_csv() {
    use arrow::array::ArrayBuilder;
    let mut longitude = Float64Builder::new();
    let mut latitude = Float64Builder::new();
    let mut x_epsg_2180 = Float64Builder::new();
    let mut y_epsg_2180 = Float64Builder::new();
    let mut geometry= Vec::new();
    let gml_pos = "NaN NaN";
    parse_gml_pos(gml_pos, &mut longitude, &mut latitude, &mut x_epsg_2180, &mut y_epsg_2180, &mut geometry, &OutputFormat::CSV);
    assert_eq!(longitude.len(), 1);
    assert_eq!(latitude.len(), 1);
    assert_eq!(x_epsg_2180.len(), 1);
    assert_eq!(y_epsg_2180.len(), 1);
    assert!(geometry.is_empty());
    assert!(!longitude.values_slice().is_empty());
    assert!(!latitude.values_slice().is_empty());
    assert!(!x_epsg_2180.values_slice().is_empty());
    assert!(!y_epsg_2180.values_slice().is_empty());
    assert!(!longitude.validity_slice().unwrap().is_empty());
    assert!(!latitude.validity_slice().unwrap().is_empty());
    assert!(!x_epsg_2180.validity_slice().unwrap().is_empty());
    assert!(!y_epsg_2180.validity_slice().unwrap().is_empty());
}

#[test]
fn test_parse_gml_pos_nan_geoparquet() {
    use arrow::array::ArrayBuilder;
    let mut longitude = Float64Builder::new();
    let mut latitude = Float64Builder::new();
    let mut x_epsg_2180 = Float64Builder::new();
    let mut y_epsg_2180 = Float64Builder::new();
    let mut geometry= Vec::new();
    let gml_pos = "NaN NaN";
    parse_gml_pos(gml_pos, &mut longitude, &mut latitude, &mut x_epsg_2180, &mut y_epsg_2180, &mut geometry, &OutputFormat::GeoParquet);
    assert_eq!(longitude.len(), 1);
    assert_eq!(latitude.len(), 1);
    assert!(x_epsg_2180.is_empty());
    assert!(y_epsg_2180.is_empty());
    assert_eq!(geometry.len(), 1);
    assert!(geometry[0].is_none());
    assert!(!longitude.values_slice().is_empty());
    assert!(!latitude.values_slice().is_empty());
    assert!(x_epsg_2180.values_slice().is_empty());
    assert!(y_epsg_2180.values_slice().is_empty());
    assert!(!longitude.validity_slice().unwrap().is_empty());
    assert!(!latitude.validity_slice().unwrap().is_empty());
    assert!(x_epsg_2180.validity_slice().is_none());
    assert!(y_epsg_2180.validity_slice().is_none());
}
