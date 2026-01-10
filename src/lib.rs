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
use terc::download_terc_mapping;
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
    download_teryt: bool,
    teryt_api_username: &Option<String>,
    teryt_api_password: &Option<String>,
    teryt_file_path: &Option<PathBuf>,
    crs: &CRS,
    arrow_schema: Arc<Schema>,
    geoarrow_geom_type: &PointType,
) -> anyhow::Result<AddressParser2021<std::io::BufReader<File>>> {
    let teryt_mapping = {
        if download_teryt {
            download_terc_mapping(
                teryt_api_username.clone().unwrap().as_str(),
                teryt_api_password.clone().unwrap().as_str(),
            )?
        } else {
            get_terc_mapping(teryt_file_path.as_ref().unwrap())?
        }
    };

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
    download_teryt: bool,
    teryt_api_username: &Option<String>,
    teryt_api_password: &Option<String>,
    teryt_file_path: &Option<PathBuf>,
    zip_file_index: usize,
    crs: &CRS,
    arrow_schema: Arc<Schema>,
    geoarrow_geom_type: &PointType,
) -> anyhow::Result<AddressParser2021<std::io::BufReader<ZipFile<'a, File>>>> {
    let teryt_mapping = {
        if download_teryt {
            download_terc_mapping(
                teryt_api_username.clone().unwrap().as_str(),
                teryt_api_password.clone().unwrap().as_str(),
            )?
        } else {
            get_terc_mapping(teryt_file_path.as_ref().unwrap())?
        }
    };

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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, Float64Array, StringArray, TimestampMillisecondArray};
    use arrow::compute::concat_batches;

    #[test]
    fn test_address_parser_2012_zip_csv() {
        let sample_file_path = "fixtures/PRG-punkty_adresowe.zip";
        let f = std::fs::File::open(&sample_file_path)
            .expect(format!("Failed to open file: `{}`.", &sample_file_path).as_str());
        let mut archive = ZipArchive::new(f)
            .expect(format!("Failed to decompress ZIP file: `{}`.", &sample_file_path).as_str());
        let parser = get_address_parser_2012_zip(
            &mut archive,
            &1,
            &OutputFormat::CSV,
            0,
            &CRS::Epsg4326,
            crate::common::SCHEMA_CSV.clone(),
            &PointType::new(
                geoarrow::datatypes::Dimension::XY,
                Arc::new(geoarrow::datatypes::Metadata::new(
                    geoarrow::datatypes::Crs::from_srid("4326".to_string()),
                    None,
                )),
            ),
        );
        let batches: Vec<arrow::array::RecordBatch> = parser
            .expect("Something wrong while creating parser object.")
            .into_iter()
            .collect();
        let arrow_batch = concat_batches(&crate::common::SCHEMA_CSV.clone(), &batches)
            .expect("Error in concatenating batches");
        assert_eq!(arrow_batch.num_rows(), 2);
        assert_eq!(arrow_batch.num_columns(), 24);
        let expected_przestrzen_nazw = &StringArray::from(vec!["PL.PZGIK.200", "PL.PZGIK.200"]);
        let przestrzen_nazw: &StringArray = &arrow_batch
            .column_by_name("przestrzen_nazw")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&przestrzen_nazw, &expected_przestrzen_nazw);
        let expected_lokalny_id = &StringArray::from(vec![
            "fd9c9319-0a6a-44b4-972a-1e6c4ec0d4ca",
            "5baa8bef-75ef-4241-a2fe-9d4137845693",
        ]);
        let lokalny_id: &StringArray = &arrow_batch
            .column_by_name("lokalny_id")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&lokalny_id, &expected_lokalny_id);
        //
        let expected_wersja_id =
            &TimestampMillisecondArray::from(vec![1662740296000, 1492765775000])
                .with_timezone(Arc::from("UTC"));
        let wersja_id: &TimestampMillisecondArray = &arrow_batch
            .column_by_name("wersja_id")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&wersja_id, &expected_wersja_id);
        let expected_poczatek_wersji_obiektu =
            &TimestampMillisecondArray::from(vec![1662747496000, 1492772975000])
                .with_timezone(Arc::from("UTC"));
        let poczatek_wersji_obiektu: &TimestampMillisecondArray = &arrow_batch
            .column_by_name("poczatek_wersji_obiektu")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&poczatek_wersji_obiektu, &expected_poczatek_wersji_obiektu);
        //
        let expected_wazny_od_lub_data_nadania = &Date32Array::from(vec![19244, 16134]);
        let wazny_od_lub_data_nadania: &Date32Array = &arrow_batch
            .column_by_name("wazny_od_lub_data_nadania")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(
            &wazny_od_lub_data_nadania,
            &expected_wazny_od_lub_data_nadania
        );
        let expected_wazny_do = &Date32Array::from(vec![None, None]);
        let wazny_do: &Date32Array = &arrow_batch
            .column_by_name("wazny_do")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&wazny_do, &expected_wazny_do);
        //
        let expected_teryt_wojewodztwo = &StringArray::from(vec!["08", "08"]);
        let teryt_wojewodztwo: &StringArray = &arrow_batch
            .column_by_name("teryt_wojewodztwo")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_wojewodztwo, &expected_teryt_wojewodztwo);
        let expected_wojewodztwo = &StringArray::from(vec!["lubuskie", "lubuskie"]);
        let wojewodztwo: &StringArray = &arrow_batch
            .column_by_name("wojewodztwo")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&wojewodztwo, &expected_wojewodztwo);
        let expected_teryt_powiat = &StringArray::from(vec!["0804", "0804"]);
        let teryt_powiat: &StringArray = &arrow_batch
            .column_by_name("teryt_powiat")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_powiat, &expected_teryt_powiat);
        let expected_powiat = &StringArray::from(vec!["nowosolski", "nowosolski"]);
        let powiat: &StringArray = &arrow_batch
            .column_by_name("powiat")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&powiat, &expected_powiat);
        let expected_teryt_gmina = &StringArray::from(vec!["0804032", "0804032"]);
        let teryt_gmina: &StringArray = &arrow_batch
            .column_by_name("teryt_gmina")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_gmina, &expected_teryt_gmina);
        let expected_gmina = &StringArray::from(vec!["Kolsko", "Kolsko"]);
        let gmina: &StringArray = &arrow_batch
            .column_by_name("gmina")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&gmina, &expected_gmina);
        let expected_teryt_miejscowosc = &StringArray::from(vec!["0910140", "0910140"]);
        let teryt_miejscowosc: &StringArray = &arrow_batch
            .column_by_name("teryt_miejscowosc")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_miejscowosc, &expected_teryt_miejscowosc);
        let expected_miejscowosc = &StringArray::from(vec!["Konotop", "Konotop"]);
        let miejscowosc: &StringArray = &arrow_batch
            .column_by_name("miejscowosc")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&miejscowosc, &expected_miejscowosc);
        let expected_czesc_miejscowosci =
            &StringArray::from(vec![None, None] as Vec<Option<String>>);
        let czesc_miejscowosci: &StringArray = &arrow_batch
            .column_by_name("czesc_miejscowosci")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&czesc_miejscowosci, &expected_czesc_miejscowosci);
        let expected_teryt_ulica = &StringArray::from(vec!["16742", "16742"]);
        let teryt_ulica: &StringArray = &arrow_batch
            .column_by_name("teryt_ulica")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_ulica, &expected_teryt_ulica);
        let expected_ulica = &StringArray::from(vec!["Podgórna", "Podgórna"]);
        let ulica: &StringArray = &arrow_batch
            .column_by_name("ulica")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&ulica, &expected_ulica);
        let expected_numer_porzadkowy = &StringArray::from(vec!["2", "1"]);
        let numer_porzadkowy: &StringArray = &arrow_batch
            .column_by_name("numer_porzadkowy")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&numer_porzadkowy, &expected_numer_porzadkowy);
        let expected_kod_pocztowy = &StringArray::from(vec!["67-416", "67-416"]);
        let kod_pocztowy: &StringArray = &arrow_batch
            .column_by_name("kod_pocztowy")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&kod_pocztowy, &expected_kod_pocztowy);
        let expected_status = &StringArray::from(vec!["istniejacy", "istniejacy"]);
        let status: &StringArray = &arrow_batch
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&status, &expected_status);
        //
        let expected_x_epsg_2180 = &Float64Array::from(vec![287772.37, 287751.0102]);
        let x_epsg_2180: &Float64Array = &arrow_batch
            .column_by_name("x_epsg_2180")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&x_epsg_2180, &expected_x_epsg_2180);
        let expected_y_epsg_2180 = &Float64Array::from(vec![456005.140000001, 456027.7794]);
        let y_epsg_2180: &Float64Array = &arrow_batch
            .column_by_name("y_epsg_2180")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&y_epsg_2180, &expected_y_epsg_2180);
        let expected_dlugosc_geograficzna =
            &Float64Array::from(vec![15.9121240698886, 15.911799807186908]);
        let dlugosc_geograficzna: &Float64Array = &arrow_batch
            .column_by_name("dlugosc_geograficzna")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&dlugosc_geograficzna, &expected_dlugosc_geograficzna);
        let expected_szerokosc_geograficzna =
            &Float64Array::from(vec![51.92977532639213, 51.92997049675426]);
        let szerokosc_geograficzna: &Float64Array = &arrow_batch
            .column_by_name("szerokosc_geograficzna")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&szerokosc_geograficzna, &expected_szerokosc_geograficzna);
    }

    #[test]
    fn test_address_parser_2021_zip_csv() {
        let sample_file_path = "fixtures/PRG-punkty_adresowe.zip";
        let teryt_file_path = "fixtures/TERC_Urzedowy_2025-11-18.zip";
        let f = std::fs::File::open(&sample_file_path)
            .expect(format!("Failed to open file: `{}`.", &sample_file_path).as_str());
        let mut archive = ZipArchive::new(f)
            .expect(format!("Failed to decompress ZIP file: `{}`.", &sample_file_path).as_str());
        let parser = get_address_parser_2021_zip(
            &mut archive,
            &1,
            &OutputFormat::CSV,
            false,
            &None,
            &None,
            &Some(PathBuf::from(teryt_file_path)),
            1,
            &CRS::Epsg4326,
            crate::common::SCHEMA_CSV.clone(),
            &PointType::new(
                geoarrow::datatypes::Dimension::XY,
                Arc::new(geoarrow::datatypes::Metadata::new(
                    geoarrow::datatypes::Crs::from_srid("4326".to_string()),
                    None,
                )),
            ),
        );
        let batches: Vec<arrow::array::RecordBatch> = parser
            .expect("Something wrong while creating parser object.")
            .into_iter()
            .collect();
        let arrow_batch = concat_batches(&crate::common::SCHEMA_CSV.clone(), &batches)
            .expect("Error in concatenating batches");
        assert_eq!(arrow_batch.num_rows(), 3);
        assert_eq!(arrow_batch.num_columns(), 24);
        let expected_przestrzen_nazw =
            &StringArray::from(vec!["PL.PZGIK.200", "PL.PZGIK.200", "PL.PZGIK.200"]);
        let przestrzen_nazw: &StringArray = &arrow_batch
            .column_by_name("przestrzen_nazw")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&przestrzen_nazw, &expected_przestrzen_nazw);
        let expected_lokalny_id = &StringArray::from(vec![
            "7343b2d2-c2ac-4951-ae9a-fe1932ffecfb",
            "07bcb481-4975-4c77-ab58-c8e4b9e05362",
            "e4ed4971-15f6-473d-b9a4-e9e12e602f6e",
        ]);
        let lokalny_id: &StringArray = &arrow_batch
            .column_by_name("lokalny_id")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&lokalny_id, &expected_lokalny_id);
        //
        let expected_wersja_id =
            &TimestampMillisecondArray::from(vec![1760443546000, 1762434168000, 1492090215000])
                .with_timezone(Arc::from("UTC"));
        let wersja_id: &TimestampMillisecondArray = &arrow_batch
            .column_by_name("wersja_id")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&wersja_id, &expected_wersja_id);
        let expected_poczatek_wersji_obiektu =
            &TimestampMillisecondArray::from(vec![1760443546000, 1762434168000, 1492090215000])
                .with_timezone(Arc::from("UTC"));
        let poczatek_wersji_obiektu: &TimestampMillisecondArray = &arrow_batch
            .column_by_name("poczatek_wersji_obiektu")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&poczatek_wersji_obiektu, &expected_poczatek_wersji_obiektu);
        //
        let expected_wazny_od_lub_data_nadania = &Date32Array::from(vec![15457, 18695, 15457]);
        let wazny_od_lub_data_nadania: &Date32Array = &arrow_batch
            .column_by_name("wazny_od_lub_data_nadania")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(
            &wazny_od_lub_data_nadania,
            &expected_wazny_od_lub_data_nadania
        );
        let expected_wazny_do = &Date32Array::from(vec![None, None, None]);
        let wazny_do: &Date32Array = &arrow_batch
            .column_by_name("wazny_do")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&wazny_do, &expected_wazny_do);
        //
        let expected_teryt_wojewodztwo = &StringArray::from(vec!["08", "08", "08"]);
        let teryt_wojewodztwo: &StringArray = &arrow_batch
            .column_by_name("teryt_wojewodztwo")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_wojewodztwo, &expected_teryt_wojewodztwo);
        let expected_wojewodztwo = &StringArray::from(vec!["lubuskie", "lubuskie", "lubuskie"]);
        let wojewodztwo: &StringArray = &arrow_batch
            .column_by_name("wojewodztwo")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&wojewodztwo, &expected_wojewodztwo);
        let expected_teryt_powiat = &StringArray::from(vec!["0807", "0805", "0807"]);
        let teryt_powiat: &StringArray = &arrow_batch
            .column_by_name("teryt_powiat")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_powiat, &expected_teryt_powiat);
        let expected_powiat = &StringArray::from(vec!["sulęciński", "słubicki", "sulęciński"]);
        let powiat: &StringArray = &arrow_batch
            .column_by_name("powiat")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&powiat, &expected_powiat);
        let expected_teryt_gmina = &StringArray::from(vec!["0807043", "0805043", "0807023"]);
        let teryt_gmina: &StringArray = &arrow_batch
            .column_by_name("teryt_gmina")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_gmina, &expected_teryt_gmina);
        let expected_gmina = &StringArray::from(vec!["Sulęcin", "Rzepin", "Lubniewice"]);
        let gmina: &StringArray = &arrow_batch
            .column_by_name("gmina")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&gmina, &expected_gmina);
        let expected_teryt_miejscowosc = &StringArray::from(vec!["0188009", "0935682", "0182969"]);
        let teryt_miejscowosc: &StringArray = &arrow_batch
            .column_by_name("teryt_miejscowosc")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_miejscowosc, &expected_teryt_miejscowosc);
        let expected_miejscowosc = &StringArray::from(vec!["Żubrów", "Rzepin", "Lubniewice"]);
        let miejscowosc: &StringArray = &arrow_batch
            .column_by_name("miejscowosc")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&miejscowosc, &expected_miejscowosc);
        let expected_czesc_miejscowosci =
            &StringArray::from(vec![None, None, None] as Vec<Option<String>>);
        let czesc_miejscowosci: &StringArray = &arrow_batch
            .column_by_name("czesc_miejscowosci")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&czesc_miejscowosci, &expected_czesc_miejscowosci);
        let expected_teryt_ulica = &StringArray::from(vec![None, Some("06921"), Some("08173")]);
        let teryt_ulica: &StringArray = &arrow_batch
            .column_by_name("teryt_ulica")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_ulica, &expected_teryt_ulica);
        let expected_ulica = &StringArray::from(vec![
            None,
            Some("Inwalidów Wojennych"),
            Some("Plac Kasztanowy"),
        ]);
        let ulica: &StringArray = &arrow_batch
            .column_by_name("ulica")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&ulica, &expected_ulica);
        let expected_numer_porzadkowy = &StringArray::from(vec!["21A", "1A", "2A"]);
        let numer_porzadkowy: &StringArray = &arrow_batch
            .column_by_name("numer_porzadkowy")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&numer_porzadkowy, &expected_numer_porzadkowy);
        let expected_kod_pocztowy = &StringArray::from(vec!["69-200", "69-110", "69-210"]);
        let kod_pocztowy: &StringArray = &arrow_batch
            .column_by_name("kod_pocztowy")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&kod_pocztowy, &expected_kod_pocztowy);
        let expected_status = &StringArray::from(vec![None, None, None] as Vec<Option<String>>);
        let status: &StringArray = &arrow_batch
            .column_by_name("status")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&status, &expected_status);
        //
        let expected_x_epsg_2180 = &Float64Array::from(vec![238651.83, 216691.39, 245250.11]);
        let x_epsg_2180: &Float64Array = &arrow_batch
            .column_by_name("x_epsg_2180")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&x_epsg_2180, &expected_x_epsg_2180);
        let expected_y_epsg_2180 = &Float64Array::from(vec![519741.27, 505645.69, 522957.46]);
        let y_epsg_2180: &Float64Array = &arrow_batch
            .column_by_name("y_epsg_2180")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&y_epsg_2180, &expected_y_epsg_2180);
        let expected_dlugosc_geograficzna = &Float64Array::from(vec![
            15.149797186509767,
            14.839103470789498,
            15.24431221852159,
        ]);
        let dlugosc_geograficzna: &Float64Array = &arrow_batch
            .column_by_name("dlugosc_geograficzna")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&dlugosc_geograficzna, &expected_dlugosc_geograficzna);
        let expected_szerokosc_geograficzna =
            &Float64Array::from(vec![52.48080576032958, 52.3434219342925, 52.51278706040695]);
        let szerokosc_geograficzna: &Float64Array = &arrow_batch
            .column_by_name("szerokosc_geograficzna")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&szerokosc_geograficzna, &expected_szerokosc_geograficzna);
    }
}
