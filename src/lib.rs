use std::borrow::Cow;
use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::array::StringBuilder;

mod constants;
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
