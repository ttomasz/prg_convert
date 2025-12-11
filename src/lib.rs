use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use arrow::datatypes::Schema;
use geoarrow::datatypes::PointType;
use quick_xml::Reader;
use zip::ZipArchive;
use zip::read::ZipFile;

mod terc;
use terc::get_terc_mapping;
pub mod common;
mod model2012;
use model2012::AddressParser2012;
mod model2021;
use model2021::AddressParser2021;

#[derive(Clone)]
pub enum CoordOrder {
    XY,
    YX,
}

#[derive(Clone)]
pub enum OutputFormat {
    CSV,
    GeoParquet,
}

impl std::fmt::Display for OutputFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            OutputFormat::CSV => write!(f, "csv"),
            OutputFormat::GeoParquet => write!(f, "geoparquet"),
        }
    }
}

#[derive(Clone)]
pub enum FileType {
    XML,
    ZIP,
}

impl std::fmt::Display for FileType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FileType::XML => write!(f, "XML"),
            FileType::ZIP => write!(f, "ZIP"),
        }
    }
}

pub enum SchemaVersion {
    Model2012,
    Model2021,
}

impl std::fmt::Display for SchemaVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SchemaVersion::Model2012 => write!(f, "2012"),
            SchemaVersion::Model2021 => write!(f, "2021"),
        }
    }
}

#[derive(Clone)]
pub enum CRS {
    Epsg2180,
    Epsg4326,
}

impl std::fmt::Display for CRS {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            CRS::Epsg2180 => write!(f, "EPSG:2180"),
            CRS::Epsg4326 => write!(f, "EPSG:4326"),
        }
    }
}

pub struct Writer {
    pub csv: Option<arrow::csv::writer::Writer<std::fs::File>>,
    pub geoparquet: Option<parquet::arrow::arrow_writer::ArrowWriter<std::fs::File>>,
}

fn get_xml_reader_from_uncompressed_file(
    path: &PathBuf,
) -> anyhow::Result<Reader<BufReader<File>>> {
    let mut reader = Reader::from_file(path)
        .with_context(|| format!("Failed to open file: `{}`.", &path.display()))?;
    reader.config_mut().expand_empty_elements = true; // makes it easier to process empty tags (<x/>)
    Ok(reader)
}

pub fn get_address_parser_2012_uncompressed(
    file_path: &PathBuf,
    batch_size: &usize,
    output_format: &OutputFormat,
    crs: &CRS,
    arrow_schema: Arc<Schema>,
    geoarrow_geom_type: &PointType,
) -> anyhow::Result<AddressParser2012<std::io::BufReader<File>>> {
    let mut reader = get_xml_reader_from_uncompressed_file(file_path)?;
    println!("Building dictionaries...");
    let dict = model2012::build_dictionaries(reader);
    reader = get_xml_reader_from_uncompressed_file(file_path)?;
    Ok(AddressParser2012::new(
        reader,
        batch_size.clone(),
        output_format.clone(),
        dict,
        crs.clone(),
        arrow_schema.clone(),
        geoarrow_geom_type.clone(),
    ))
}

pub fn get_address_parser_2012_zip<'a>(
    archive: &'a mut ZipArchive<File>,
    batch_size: &usize,
    output_format: &OutputFormat,
    zip_file_index: usize,
    crs: &CRS,
    arrow_schema: Arc<Schema>,
    geoarrow_geom_type: &PointType,
) -> anyhow::Result<AddressParser2012<std::io::BufReader<ZipFile<'a, File>>>> {
    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true; // makes it easier to process empty tags (<x/>)

    println!("Building dictionaries...");
    let dict = model2012::build_dictionaries(reader);

    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true; // makes it easier to process empty tags (<x/>)

    Ok(AddressParser2012::new(
        reader,
        batch_size.clone(),
        output_format.clone(),
        dict,
        crs.clone(),
        arrow_schema.clone(),
        geoarrow_geom_type.clone(),
    ))
}

pub fn get_address_parser_2021_uncompressed(
    file_path: &PathBuf,
    batch_size: &usize,
    output_format: &OutputFormat,
    teryt_file_path: &PathBuf,
    crs: &CRS,
    arrow_schema: Arc<Schema>,
    geoarrow_geom_type: &PointType,
) -> anyhow::Result<AddressParser2021<std::io::BufReader<File>>> {
    let teryt_mapping = get_terc_mapping(teryt_file_path)?;

    let mut reader = get_xml_reader_from_uncompressed_file(file_path)?;
    println!("Building dictionaries...");
    let dict = model2021::build_dictionaries(reader);

    reader = get_xml_reader_from_uncompressed_file(file_path)?;
    Ok(AddressParser2021::new(
        reader,
        batch_size.clone(),
        output_format.clone(),
        dict,
        teryt_mapping,
        crs.clone(),
        arrow_schema.clone(),
        geoarrow_geom_type.clone(),
    ))
}

pub fn get_address_parser_2021_zip<'a>(
    archive: &'a mut ZipArchive<File>,
    batch_size: &usize,
    output_format: &OutputFormat,
    teryt_file_path: &PathBuf,
    zip_file_index: usize,
    crs: &CRS,
    arrow_schema: Arc<Schema>,
    geoarrow_geom_type: &PointType,
) -> anyhow::Result<AddressParser2021<std::io::BufReader<ZipFile<'a, File>>>> {
    let teryt_mapping = get_terc_mapping(teryt_file_path)?;

    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true; // makes it easier to process empty tags (<x/>)
    println!("Building dictionaries...");
    let dict = model2021::build_dictionaries(reader);

    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true; // makes it easier to process empty tags (<x/>)

    Ok(AddressParser2021::new(
        reader,
        batch_size.clone(),
        output_format.clone(),
        dict,
        teryt_mapping,
        crs.clone(),
        arrow_schema.clone(),
        geoarrow_geom_type.clone(),
    ))
}
