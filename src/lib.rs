use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use quick_xml::Reader;
use zip::ZipArchive;
use zip::read::ZipFile;

pub mod terc;
use terc::Terc;
#[cfg(feature = "download")]
use terc::download_terc_mapping;
use terc::get_terc_mapping;
pub mod common;
mod model2012;
use model2012::AddressParser2012;
mod model2021;
use model2021::AddressParser2021;

/// Order of the two numbers inside a `<gml:pos>` element.
///
/// `XY` is easting first, `YX` is northing first. EPSG:2180's official axis
/// order is northing, easting, so `YX` is what a file gets by following the
/// standard; `XY` is the order the 2021-schema files used until 2026-10-01.
/// See [`common::coord_order_for_srs_name`] for how a file's own declaration
/// picks between them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CoordOrder {
    XY,
    YX,
}

#[derive(Clone, Copy)]
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

#[derive(Clone, Copy)]
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

#[derive(Clone, Copy)]
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

fn get_xml_reader_from_uncompressed_file(
    path: &PathBuf,
) -> anyhow::Result<Reader<BufReader<File>>> {
    let mut reader = Reader::from_file(path)
        .with_context(|| format!("Failed to open file: `{}`.", &path.display()))?;
    reader.config_mut().expand_empty_elements = true; // makes it easier to process empty tags (<x/>)
    Ok(reader)
}

/// `coordinate_order` forces the order of every `<gml:pos>`; `None` reads each
/// point in the order its `srsName` declares (see
/// [`common::coord_order_for_srs_name`]). The same holds for all four
/// `get_address_parser_*` functions.
pub fn get_address_parser_2012_uncompressed(
    file_path: &PathBuf,
    batch_size: &usize,
    coordinate_order: Option<CoordOrder>,
) -> anyhow::Result<AddressParser2012<std::io::BufReader<File>>> {
    let reader = get_xml_reader_from_uncompressed_file(file_path)?;
    println!("Building dictionaries...");
    let dict = model2012::build_dictionaries(reader);
    let reader = get_xml_reader_from_uncompressed_file(file_path)?;
    Ok(AddressParser2012::new(
        reader,
        *batch_size,
        dict,
        coordinate_order,
    ))
}

pub fn get_address_parser_2012_zip<'a>(
    archive: &'a mut ZipArchive<File>,
    batch_size: &usize,
    zip_file_index: usize,
    coordinate_order: Option<CoordOrder>,
) -> anyhow::Result<AddressParser2012<std::io::BufReader<ZipFile<'a, File>>>> {
    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true;
    println!("Building dictionaries...");
    let dict = model2012::build_dictionaries(reader);

    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true;

    Ok(AddressParser2012::new(
        reader,
        *batch_size,
        dict,
        coordinate_order,
    ))
}

pub fn get_teryt_mapping(
    download_teryt: bool,
    teryt_api_username: &Option<String>,
    teryt_api_password: &Option<String>,
    teryt_file_path: &Option<PathBuf>,
) -> anyhow::Result<HashMap<String, Terc>> {
    if download_teryt {
        #[cfg(feature = "download")]
        {
            download_terc_mapping(
                teryt_api_username.as_deref().unwrap(),
                teryt_api_password.as_deref().unwrap(),
            )
        }
        #[cfg(not(feature = "download"))]
        {
            let _ = (teryt_api_username, teryt_api_password);
            anyhow::bail!(
                "This build was compiled without the `download` feature; downloading TERYT is unavailable. Provide a TERYT file via --teryt-path."
            )
        }
    } else {
        get_terc_mapping(teryt_file_path.as_ref().unwrap())
    }
}

pub fn get_address_parser_2021_uncompressed(
    file_path: &PathBuf,
    batch_size: &usize,
    teryt_mapping: &Arc<HashMap<String, Terc>>,
    coordinate_order: Option<CoordOrder>,
) -> anyhow::Result<AddressParser2021<std::io::BufReader<File>>> {
    let reader = get_xml_reader_from_uncompressed_file(file_path)?;
    println!("Building dictionaries...");
    let dict = model2021::build_dictionaries(reader);
    let reader = get_xml_reader_from_uncompressed_file(file_path)?;
    Ok(AddressParser2021::new(
        reader,
        *batch_size,
        dict,
        teryt_mapping.clone(),
        coordinate_order,
    ))
}

pub fn get_address_parser_2021_zip<'a>(
    archive: &'a mut ZipArchive<File>,
    batch_size: &usize,
    teryt_mapping: &Arc<HashMap<String, Terc>>,
    zip_file_index: usize,
    coordinate_order: Option<CoordOrder>,
) -> anyhow::Result<AddressParser2021<std::io::BufReader<ZipFile<'a, File>>>> {
    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true;
    println!("Building dictionaries...");
    let dict = model2021::build_dictionaries(reader);

    let zip_file = archive
        .by_index(zip_file_index)
        .with_context(|| "Could not decompress file from ZIP archive.")?;
    let buf_reader = BufReader::new(zip_file);
    let mut reader = Reader::from_reader(buf_reader);
    reader.config_mut().expand_empty_elements = true;

    Ok(AddressParser2021::new(
        reader,
        *batch_size,
        dict,
        teryt_mapping.clone(),
        coordinate_order,
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

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
        let parser = get_address_parser_2012_zip(&mut archive, &1, 0, None);
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
            &Float64Array::from(vec![15.912124069888604, 15.911799807186908]);
        let dlugosc_geograficzna: &Float64Array = &arrow_batch
            .column_by_name("dlugosc_geograficzna")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&dlugosc_geograficzna, &expected_dlugosc_geograficzna);
        let expected_szerokosc_geograficzna =
            &Float64Array::from(vec![51.929775327307524, 51.92997049766966]);
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
        let teryt_mapping = Arc::new(
            get_teryt_mapping(false, &None, &None, &Some(PathBuf::from(teryt_file_path))).unwrap(),
        );
        let f = std::fs::File::open(&sample_file_path)
            .expect(format!("Failed to open file: `{}`.", &sample_file_path).as_str());
        let mut archive = ZipArchive::new(f)
            .expect(format!("Failed to decompress ZIP file: `{}`.", &sample_file_path).as_str());
        let parser = get_address_parser_2021_zip(&mut archive, &1, &teryt_mapping, 1, None);
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
            &TimestampMillisecondArray::from(vec![1760443546000, 1762437768000, 1492090215000])
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
            15.244312218521593,
        ]);
        let dlugosc_geograficzna: &Float64Array = &arrow_batch
            .column_by_name("dlugosc_geograficzna")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&dlugosc_geograficzna, &expected_dlugosc_geograficzna);
        let expected_szerokosc_geograficzna = &Float64Array::from(vec![
            52.48080576124044,
            52.343421935204525,
            52.51278706131754,
        ]);
        let szerokosc_geograficzna: &Float64Array = &arrow_batch
            .column_by_name("szerokosc_geograficzna")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&szerokosc_geograficzna, &expected_szerokosc_geograficzna);
    }

    #[test]
    fn test_address_parser_2012_xml_csv() {
        let file_path = PathBuf::from("fixtures/sample_model2012.xml");
        let parser = get_address_parser_2012_uncompressed(&file_path, &100_000, None);
        let batches: Vec<arrow::array::RecordBatch> = parser
            .expect("Something wrong while creating parser object.")
            .into_iter()
            .collect();
        let arrow_batch = concat_batches(&crate::common::SCHEMA_CSV.clone(), &batches)
            .expect("Error in concatenating batches");
        assert_eq!(arrow_batch.num_rows(), 2);
        assert_eq!(arrow_batch.num_columns(), 24);
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
        let expected_teryt_wojewodztwo = &StringArray::from(vec!["08", "08"]);
        let teryt_wojewodztwo: &StringArray = &arrow_batch
            .column_by_name("teryt_wojewodztwo")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_wojewodztwo, &expected_teryt_wojewodztwo);
    }

    #[test]
    fn test_address_parser_2021_xml_csv() {
        let file_path = PathBuf::from("fixtures/sample_model2021.xml");
        let teryt_file_path = "fixtures/TERC_Urzedowy_2025-11-18.zip";
        let teryt_mapping = Arc::new(
            get_teryt_mapping(false, &None, &None, &Some(PathBuf::from(teryt_file_path))).unwrap(),
        );
        let parser =
            get_address_parser_2021_uncompressed(&file_path, &100_000, &teryt_mapping, None);
        let batches: Vec<arrow::array::RecordBatch> = parser
            .expect("Something wrong while creating parser object.")
            .into_iter()
            .collect();
        let arrow_batch = concat_batches(&crate::common::SCHEMA_CSV.clone(), &batches)
            .expect("Error in concatenating batches");
        assert_eq!(arrow_batch.num_rows(), 3);
        assert_eq!(arrow_batch.num_columns(), 24);
        let expected_teryt_gmina = &StringArray::from(vec!["0807043", "0805043", "0807023"]);
        let teryt_gmina: &StringArray = &arrow_batch
            .column_by_name("teryt_gmina")
            .unwrap()
            .as_any()
            .downcast_ref()
            .unwrap();
        assert_eq!(&teryt_gmina, &expected_teryt_gmina);
    }

    #[test]
    fn test_address_parser_2012_zip_canonical() {
        let sample_file_path = "fixtures/PRG-punkty_adresowe.zip";
        let f = std::fs::File::open(&sample_file_path)
            .expect(format!("Failed to open file: `{}`.", &sample_file_path).as_str());
        let mut archive = ZipArchive::new(f)
            .expect(format!("Failed to decompress ZIP file: `{}`.", &sample_file_path).as_str());
        let parser = get_address_parser_2012_zip(&mut archive, &100_000, 0, None);
        let batches: Vec<arrow::array::RecordBatch> = parser
            .expect("Something wrong while creating parser object.")
            .into_iter()
            .collect();
        let arrow_batch = concat_batches(&crate::common::SCHEMA_CSV.clone(), &batches)
            .expect("Error in concatenating batches");
        assert_eq!(arrow_batch.num_rows(), 2);
        assert_eq!(arrow_batch.num_columns(), 24);
        let x = arrow_batch
            .column_by_name("x_epsg_2180")
            .expect("Expected x_epsg_2180 column");
        assert_eq!(x.null_count(), 0);
    }

    #[test]
    fn test_address_parser_2021_zip_canonical() {
        let sample_file_path = "fixtures/PRG-punkty_adresowe.zip";
        let teryt_file_path = "fixtures/TERC_Urzedowy_2025-11-18.zip";
        let teryt_mapping = Arc::new(
            get_teryt_mapping(false, &None, &None, &Some(PathBuf::from(teryt_file_path))).unwrap(),
        );
        let f = std::fs::File::open(&sample_file_path)
            .expect(format!("Failed to open file: `{}`.", &sample_file_path).as_str());
        let mut archive = ZipArchive::new(f)
            .expect(format!("Failed to decompress ZIP file: `{}`.", &sample_file_path).as_str());
        let parser = get_address_parser_2021_zip(&mut archive, &100_000, &teryt_mapping, 1, None);
        let batches: Vec<arrow::array::RecordBatch> = parser
            .expect("Something wrong while creating parser object.")
            .into_iter()
            .collect();
        let arrow_batch = concat_batches(&crate::common::SCHEMA_CSV.clone(), &batches)
            .expect("Error in concatenating batches");
        assert_eq!(arrow_batch.num_rows(), 3);
        assert_eq!(arrow_batch.num_columns(), 24);
        let x = arrow_batch
            .column_by_name("x_epsg_2180")
            .expect("Expected x_epsg_2180 column");
        assert_eq!(x.null_count(), 0);
    }

    #[test]
    fn test_address_parser_2012_zip_csv_multi_batch() {
        let sample_file_path = "fixtures/PRG-punkty_adresowe.zip";
        let f = std::fs::File::open(&sample_file_path)
            .expect(format!("Failed to open file: `{}`.", &sample_file_path).as_str());
        let mut archive = ZipArchive::new(f)
            .expect(format!("Failed to decompress ZIP file: `{}`.", &sample_file_path).as_str());
        let parser = get_address_parser_2012_zip(&mut archive, &1, 0, None);
        let batches: Vec<arrow::array::RecordBatch> = parser
            .expect("Something wrong while creating parser object.")
            .into_iter()
            .collect();
        assert_eq!(batches.len(), 2);
        assert_eq!(batches[0].num_rows(), 1);
        assert_eq!(batches[1].num_rows(), 1);
    }

    // --- coordinate order ---

    const SRS_SHORT: &str = r#"srsName="EPSG:2180""#;
    const SRS_URN: &str = r#"srsName="urn:ogc:def:crs:EPSG::2180""#;

    /// The same document with the two numbers of every `<gml:pos>` exchanged.
    fn exchange_gml_pos(xml: &str) -> String {
        const OPEN: &str = "<gml:pos>";
        let mut out = String::with_capacity(xml.len());
        let mut rest = xml;
        while let Some(start) = rest.find(OPEN) {
            let body_start = start + OPEN.len();
            let body_end = body_start + rest[body_start..].find("</gml:pos>").unwrap();
            out.push_str(&rest[..body_start]);
            let mut numbers = rest[body_start..body_end].split_whitespace();
            let (first, second) = (numbers.next().unwrap(), numbers.next().unwrap());
            out.push_str(&format!("{second} {first}"));
            rest = &rest[body_end..];
        }
        out.push_str(rest);
        out
    }

    /// The four coordinate columns as raw bits, so equality means the very
    /// same `f64`s and not merely close ones.
    fn coordinate_bits(batch: &arrow::array::RecordBatch) -> Vec<Vec<Option<u64>>> {
        [
            "x_epsg_2180",
            "y_epsg_2180",
            "dlugosc_geograficzna",
            "szerokosc_geograficzna",
        ]
        .iter()
        .map(|name| {
            let column: &Float64Array = batch
                .column_by_name(name)
                .unwrap()
                .as_any()
                .downcast_ref()
                .unwrap();
            column.iter().map(|v| v.map(f64::to_bits)).collect()
        })
        .collect()
    }

    fn write_temp_xml(xml: &str) -> tempfile::NamedTempFile {
        let file = tempfile::Builder::new().suffix(".xml").tempfile().unwrap();
        std::fs::write(file.path(), xml).unwrap();
        file
    }

    fn parse_2021(xml: &str, coordinate_order: Option<CoordOrder>) -> arrow::array::RecordBatch {
        let file = write_temp_xml(xml);
        let teryt_mapping = Arc::new(
            get_teryt_mapping(
                false,
                &None,
                &None,
                &Some(PathBuf::from("fixtures/TERC_Urzedowy_2025-11-18.zip")),
            )
            .unwrap(),
        );
        let batches: Vec<arrow::array::RecordBatch> = get_address_parser_2021_uncompressed(
            &file.path().to_path_buf(),
            &100_000,
            &teryt_mapping,
            coordinate_order,
        )
        .unwrap()
        .collect();
        concat_batches(&crate::common::SCHEMA_CSV.clone(), &batches).unwrap()
    }

    fn parse_2012(xml: &str, coordinate_order: Option<CoordOrder>) -> arrow::array::RecordBatch {
        let file = write_temp_xml(xml);
        let batches: Vec<arrow::array::RecordBatch> = get_address_parser_2012_uncompressed(
            &file.path().to_path_buf(),
            &100_000,
            coordinate_order,
        )
        .unwrap()
        .collect();
        concat_batches(&crate::common::SCHEMA_CSV.clone(), &batches).unwrap()
    }

    /// `[x, y, lon, lat]` with x and y exchanged, lon/lat left alone -- for
    /// comparing the EPSG:2180 columns of a mirrored parse.
    fn with_x_and_y_exchanged(bits: &[Vec<Option<u64>>]) -> Vec<Vec<Option<u64>>> {
        vec![bits[1].clone(), bits[0].clone()]
    }

    /// GUGiK's 2021-schema files changed on 2026-10-01 from
    /// `srsName="EPSG:2180"` with easting first to the URN form with northing
    /// first. Read in `auto`, both layouts must give the very same positions.
    #[test]
    fn test_2021_urn_layout_parses_to_the_same_positions_as_the_short_layout() {
        let original = std::fs::read_to_string("fixtures/sample_model2021.xml").unwrap();
        assert!(original.contains(SRS_SHORT));
        let october = exchange_gml_pos(&original.replace(SRS_SHORT, SRS_URN));
        assert_ne!(original, october);

        let expected = coordinate_bits(&parse_2021(&original, None));
        assert!(expected[0].iter().all(Option::is_some));
        assert_eq!(coordinate_bits(&parse_2021(&october, None)), expected);
    }

    #[test]
    fn test_2021_point_without_srs_name_is_read_northing_first() {
        let original = std::fs::read_to_string("fixtures/sample_model2021.xml").unwrap();
        let undeclared = exchange_gml_pos(&original.replace(SRS_SHORT, ""));
        assert!(!undeclared.contains("srsName"));

        assert_eq!(
            coordinate_bits(&parse_2021(&undeclared, None)),
            coordinate_bits(&parse_2021(&original, None))
        );
    }

    #[test]
    fn test_2021_forced_order_overrides_the_declaration() {
        let original = std::fs::read_to_string("fixtures/sample_model2021.xml").unwrap();
        let auto = coordinate_bits(&parse_2021(&original, None));

        // The fixture declares easting first, so forcing XY changes nothing...
        assert_eq!(
            coordinate_bits(&parse_2021(&original, Some(CoordOrder::XY))),
            auto
        );
        // ...and forcing YX reads every point mirrored.
        let mirrored = coordinate_bits(&parse_2021(&original, Some(CoordOrder::YX)));
        assert_eq!(mirrored[..2], with_x_and_y_exchanged(&auto)[..]);
        assert_ne!(mirrored[2], auto[2]);

        // A file whose declaration is wrong is what the override is for: the
        // URN form over easting-first numbers reads right only when forced.
        let misdeclared = original.replace(SRS_SHORT, SRS_URN);
        assert_ne!(coordinate_bits(&parse_2021(&misdeclared, None)), auto);
        assert_eq!(
            coordinate_bits(&parse_2021(&misdeclared, Some(CoordOrder::XY))),
            auto
        );
    }

    /// The same rule in the 2012 parser, which used to hardcode northing
    /// first: its fixture declares the URN form, and the short form flips it.
    #[test]
    fn test_2012_order_follows_srs_name_and_can_be_forced() {
        let original = std::fs::read_to_string("fixtures/sample_model2012.xml").unwrap();
        assert!(original.contains(SRS_URN));
        let auto = coordinate_bits(&parse_2012(&original, None));
        assert!(auto[0].iter().all(Option::is_some));

        assert_eq!(
            coordinate_bits(&parse_2012(&original, Some(CoordOrder::YX))),
            auto
        );
        let mirrored = coordinate_bits(&parse_2012(&original, Some(CoordOrder::XY)));
        assert_eq!(mirrored[..2], with_x_and_y_exchanged(&auto)[..]);

        let short_form = exchange_gml_pos(&original.replace(SRS_URN, SRS_SHORT));
        assert_eq!(coordinate_bits(&parse_2012(&short_form, None)), auto);
    }
}
