use std::fs::File;
#[cfg(feature = "download")]
use std::io::Seek;
use std::path::PathBuf;

use anyhow::Context;
use clap::ArgAction;
use glob::glob;
use parquet::basic::BrotliLevel;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterVersion;
#[cfg(feature = "download")]
use tempfile::NamedTempFile;
use zip::ZipArchive;

use prg_convert::CRS;
use prg_convert::FileType;
use prg_convert::OutputFormat;
use prg_convert::SchemaVersion;

pub const DEFAULT_BATCH_SIZE: usize = 100_000;

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum OutputFormatArg {
    Csv,
    Geoparquet,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum SchemaVersionArg {
    #[value(name = "2012")]
    V2012,
    #[value(name = "2021")]
    V2021,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum ParquetCompressionArg {
    Zstd,
    Snappy,
    Brotli,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum ParquetVersionArg {
    #[value(name = "v1")]
    V1,
    #[value(name = "v2")]
    V2,
}

#[derive(Clone, Copy, clap::ValueEnum)]
pub enum CrsEpsgArg {
    #[value(name = "2180")]
    Epsg2180,
    #[value(name = "4326")]
    Epsg4326,
}

#[derive(clap::Parser)]
pub struct RawArgs {
    #[arg(
        long = "input-paths",
        help = "Input XML or ZIP file path(s). Can be multiple paths separated with space. Can use glob patterns (e.g. `data/*.xml`). If ZIP file path is provided then flag --schema-version will determine which files inside will be read (2012: *.xml, 2021: *.gml).",
        value_delimiter = ' ',
        num_args = 1..,
    )]
    input_paths: Vec<String>,
    #[arg(long = "download-data", num_args = 0..=1, default_missing_value = "", help = "Download PRG address data from the official GUGiK URL instead of providing --input-paths. Optionally provide a file path to save the downloaded file to (e.g. --download-data /tmp/prg.zip). If no path is given, a temporary file is used. URL: https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml")]
    download_data: Option<String>,
    #[arg(long = "output-path", help = "Output file path.")]
    output_path: std::path::PathBuf,
    #[arg(
        long = "output-format",
        ignore_case = true,
        help = "Output file format."
    )]
    output_format: OutputFormatArg,
    #[arg(long = "schema-version", help = "Schema version.")]
    schema_version: SchemaVersionArg,
    #[arg(
        long = "teryt-path",
        help = "Path of XML file with TERYT dictionary unpacked from archive downloaded from: https://eteryt.stat.gov.pl/eTeryt/rejestr_teryt/udostepnianie_danych/baza_teryt/uzytkownicy_indywidualni/pobieranie/pliki_pelne.aspx?contrast=default (TERC, podstawowa). Required for --schema-version 2021."
    )]
    teryt_path: Option<std::path::PathBuf>,
    #[arg(long = "download-teryt", action = ArgAction::SetTrue, help = "Download TERYT dictionary file from official API. (Requires authentication info, see: https://api.stat.gov.pl/Home/TerytApi , relevant flags: teryt-api-username, teryt-api-password)")]
    teryt_download: Option<bool>,
    #[arg(
        long = "teryt-api-username",
        help = "(Optional) Username to use when authenticating to TERYT API if it's used (`download-teryt` flag is used). If not provided env variable: TERYT_API_USERNAME will be used."
    )]
    teryt_api_username: Option<String>,
    #[arg(
        long = "teryt-api-password",
        help = "(Optional) Password to use when authenticating to TERYT API if it's used (`download-teryt` flag is used). If not provided env variable: TERYT_API_PASSWORD will be used."
    )]
    teryt_api_password: Option<String>,
    #[arg(
        long = "batch-size",
        help = format!("(Optional) How many rows are kept in memory before writing to output (default: {}).", DEFAULT_BATCH_SIZE),
    )]
    batch_size: Option<usize>,
    #[arg(
        long = "parquet-compression",
        ignore_case = true,
        help = "(Optional) Compression to use when writing parquet file (default: zstd)."
    )]
    parquet_compression: Option<ParquetCompressionArg>,
    #[arg(
        long = "compression-level",
        help = "(Optional) What level of compression to use when writing parquet file (if compression algorithm supports it)."
    )]
    compression_level: Option<i32>,
    #[arg(
        long = "parquet-row-group-size",
        help = "(Optional) What's the max row group size when writing parquet file (default: same as batch-size)."
    )]
    parquet_row_group_size: Option<usize>,
    #[arg(
        long = "parquet-version",
        ignore_case = true,
        help = "(Optional) Version of parquet standard to use (default: v2)."
    )]
    parquet_version: Option<ParquetVersionArg>,
    #[arg(
        long = "crs-epsg",
        help = "(Optional) EPSG code of Coordinate Reference System for geometry data written to geoparquet (default: 2180). Does not affect CSV format which includes coordinates in both."
    )]
    crs_epsg: Option<CrsEpsgArg>,
}

pub struct CompressedFile {
    pub index: usize,
    pub name: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub to_be_parsed: bool,
}

pub struct FileRecord {
    pub file_type: FileType,
    pub path: PathBuf,
    pub size_in_bytes: u64,
    pub compressed_files: Option<Vec<CompressedFile>>, // only for FileType::ZIP
    pub decompressed_size: Option<u128>,               // only for FileType::ZIP
}

pub(crate) fn parse_input_paths(
    input_paths: &Vec<String>,
    schema_version: &SchemaVersion,
) -> anyhow::Result<Vec<FileRecord>> {
    let mut paths: Vec<FileRecord> = Vec::new();
    for raw_path in input_paths {
        let globbed_paths = glob(&raw_path)
            .with_context(|| format!("Failed to parse glob pattern: `{}`", &raw_path))?;
        for potential_path in globbed_paths {
            let path = potential_path?;
            let file_metadata = std::fs::metadata(&path).with_context(|| {
                format!("could not get metadata for file `{}`", &path.display())
            })?;
            if file_metadata.is_dir() {
                anyhow::bail!(
                    "input path `{}` is a directory, expected a file",
                    &path.display()
                );
            }
            let file_type = match path
                .extension()
                .expect("Could not read file extension.")
                .to_string_lossy()
                .to_lowercase()
                .as_str()
            {
                "zip" => FileType::ZIP,
                "xml" | "gml" => FileType::XML,
                _ => {
                    anyhow::bail!("File extension not one of: zip, xml, gml.")
                }
            };
            let mut compressed_files = None;
            let mut decompressed_size = None;
            if let FileType::ZIP = file_type {
                let mut cf: Vec<CompressedFile> = Vec::new();
                let f = File::open(&path)
                    .with_context(|| format!("Failed to open ZIP file: `{}`.", &path.display()))?;
                let mut archive = ZipArchive::new(f).with_context(|| {
                    format!("Failed to decompress ZIP file: `{}`.", &path.display())
                })?;
                decompressed_size = archive.decompressed_size();
                for idx in 0..archive.len() {
                    let entry = archive
                        .by_index(idx)
                        .with_context(|| "Could not access file inside ZIP archive")?;
                    let name = entry
                        .enclosed_name()
                        .with_context(|| "Could not read file name inside ZIP archive.")?;
                    // for now we'll determine if the file inside zip should be processed based on extension
                    let file_extension = &name
                        .extension()
                        .expect("Could not read file extension.")
                        .to_string_lossy()
                        .to_lowercase();
                    let to_be_parsed = match schema_version {
                        SchemaVersion::Model2012 => file_extension == "xml",
                        SchemaVersion::Model2021 => file_extension == "gml",
                    };
                    cf.push(CompressedFile {
                        index: idx,
                        name: name.to_string_lossy().to_string(),
                        compressed_size: entry.compressed_size(),
                        uncompressed_size: entry.size(),
                        to_be_parsed: to_be_parsed,
                    });
                }
                compressed_files = Some(cf);
            }
            paths.push(FileRecord {
                file_type: file_type,
                path: path,
                size_in_bytes: file_metadata.len(),
                compressed_files: compressed_files,
                decompressed_size,
            });
        }
    }
    if paths.is_empty() {
        anyhow::bail!("Could not read input files. Do the files exist? Are the paths correct?");
    }
    Ok(paths)
}

pub const PRG_DOWNLOAD_URL: &str =
    "https://integracja.gugik.gov.pl/PRG/pobierz.php?adresy_zbiorcze_gml";

#[cfg(feature = "download")]
pub fn download_prg_data(
    save_path: Option<&std::path::Path>,
) -> anyhow::Result<Option<NamedTempFile>> {
    let client = reqwest::blocking::Client::builder()
        .timeout(std::time::Duration::from_secs(3600))
        .build()
        .with_context(|| "Failed to build HTTP client.")?;
    println!("Sending download request to: {}", PRG_DOWNLOAD_URL);
    let mut response = client
        .get(PRG_DOWNLOAD_URL)
        .send()
        .with_context(|| format!("Failed to send download request to: {}", PRG_DOWNLOAD_URL))?;
    if !response.status().is_success() {
        anyhow::bail!("Download request failed with status: {}", response.status());
    }
    if let Some(path) = save_path {
        println!("Download started, saving to: {}", path.display());
        let mut file = File::create(path)
            .with_context(|| format!("Failed to create file: {}", path.display()))?;
        std::io::copy(&mut response, &mut file)
            .with_context(|| format!("Failed to stream download to: {}", path.display()))?;
        println!("Download complete.");
        Ok(None)
    } else {
        let mut temp_file = tempfile::Builder::new()
            .suffix(".zip")
            .tempfile()
            .with_context(|| "Failed to create temporary file for download.")?;
        println!("Download started, saving to temporary file...");
        std::io::copy(&mut response, &mut temp_file)
            .with_context(|| "Failed to stream download to temporary file.")?;
        println!("Download complete.");
        temp_file
            .seek(std::io::SeekFrom::Start(0))
            .with_context(|| "Failed to seek to start of temporary file after download.")?;
        Ok(Some(temp_file))
    }
}

pub struct ParsedArgs {
    pub input_paths: Vec<String>,
    pub parsed_paths: Vec<FileRecord>,
    pub download_data: bool,
    pub download_data_path: Option<PathBuf>,
    pub output_path: PathBuf,
    pub download_teryt: bool,
    pub teryt_api_username: Option<String>,
    pub teryt_api_password: Option<String>,
    pub teryt_path: Option<std::path::PathBuf>,
    pub batch_size: usize,
    pub schema_version: SchemaVersion,
    pub output_format: OutputFormat,
    pub compression_level: Option<i32>,
    pub parquet_compression: parquet::basic::Compression,
    pub parquet_row_group_size: usize,
    pub parquet_version: parquet::file::properties::WriterVersion,
    pub crs: CRS,
}

pub fn print_parsed_args(parsed_args: &ParsedArgs) {
    println!("⚙️  Parameters:");
    if parsed_args.download_data {
        println!("  Input: download from URL: {}", PRG_DOWNLOAD_URL);
        match &parsed_args.download_data_path {
            Some(path) => println!("  Download save path: {}", path.display()),
            None => println!("  Download save path: temporary file"),
        }
    } else {
        println!("  Input paths/patterns:");
        for path in &parsed_args.input_paths {
            println!("    - {}", path);
        }
        println!("  Input:");
        for file in &parsed_args.parsed_paths {
            match file.file_type {
                FileType::XML => {
                    println!(
                        "    - {} (XML), size: {:.2} MB",
                        file.path.display(),
                        (file.size_in_bytes as f64 / 1024.0 / 1024.0)
                    );
                }
                FileType::ZIP => {
                    let decompressed_size_str = match file.decompressed_size {
                        Some(size) => format!("{:.2} MB", size as f64 / 1024.0 / 1024.0),
                        None => "unknown".to_string(),
                    };
                    println!(
                        "    - {} (ZIP), size compressed: {:.2} MB, size uncompressed: {}",
                        file.path.display(),
                        (file.size_in_bytes as f64 / 1024.0 / 1024.0),
                        decompressed_size_str
                    );
                    let compressed_files = file
                        .compressed_files
                        .as_ref()
                        .expect("No files inside ZIP.");
                    for compressed_file in compressed_files {
                        let status_emoji = match compressed_file.to_be_parsed {
                            true => "✅",
                            false => "⛔️",
                        };
                        println!(
                            "        - {} idx: {}, {}, size compressed: {:.2} MB, size uncompressed: {:.2} MB",
                            status_emoji,
                            compressed_file.index,
                            compressed_file.name,
                            (compressed_file.compressed_size as f64 / 1024.0 / 1024.0),
                            (compressed_file.uncompressed_size as f64 / 1024.0 / 1024.0)
                        );
                    }
                }
            };
        }
    }
    println!("  Output file: {}", parsed_args.output_path.display());
    println!("  Output file format: {}", parsed_args.output_format);
    println!("  Schema version: {}", parsed_args.schema_version);
    match parsed_args.schema_version {
        SchemaVersion::Model2012 => {}
        SchemaVersion::Model2021 => {
            println!("  Download TERYT from API: {}", parsed_args.download_teryt);
            if parsed_args.download_teryt {
                println!(
                    "  TERYT API Username: {}",
                    parsed_args.teryt_api_username.as_ref().unwrap()
                );
            } else {
                println!(
                    "  TERYT file: {}",
                    &parsed_args.teryt_path.as_ref().unwrap().display()
                );
            }
        }
    }
    println!("  Batch size: {}", parsed_args.batch_size);
    if let OutputFormat::GeoParquet = parsed_args.output_format {
        println!("  Parquet compression: {}", parsed_args.parquet_compression);
        if parsed_args.compression_level.is_some() {
            println!(
                "  Compression level: {}",
                parsed_args.compression_level.unwrap()
            );
        }
        println!(
            "  Parquet max row group size: {}",
            parsed_args.parquet_row_group_size
        );
        match parsed_args.parquet_version {
            WriterVersion::PARQUET_1_0 => {
                println!("  Parquet file format version: v1")
            }
            WriterVersion::PARQUET_2_0 => {
                println!("  Parquet file format version: v2")
            }
        };
        println!("  CRS: {}", parsed_args.crs);
    };
    println!("----------------------------------------");
}

impl TryFrom<RawArgs> for ParsedArgs {
    type Error = anyhow::Error;

    fn try_from(value: RawArgs) -> anyhow::Result<ParsedArgs> {
        let batch_size = value.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        let download_data = value.download_data.is_some();
        let download_data_path = value
            .download_data
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(PathBuf::from);
        let has_input_paths = !value.input_paths.is_empty();
        if has_input_paths && download_data {
            anyhow::bail!("Provide either --input-paths or --download-data, but not both.");
        }
        if !has_input_paths && !download_data {
            anyhow::bail!("Either --input-paths or --download-data must be provided.");
        }
        let download_teryt_flag = {
            let mut flag = value.teryt_download.unwrap_or(false);
            if matches!(value.schema_version, SchemaVersionArg::V2012) && flag {
                println!(
                    "Warning: teryt-download was set to true but schema was set to 2012 which is not compatible. teryt-download will be treated as false."
                );
                flag = false;
            }
            flag
        };
        if matches!(value.schema_version, SchemaVersionArg::V2021)
            && value.teryt_path.is_none()
            && !download_teryt_flag
        {
            anyhow::bail!(
                "Chosen schema 2021 but provided neither teryt file path nor teryt-download flag. PRG schema 2021 does not contain names of administrative units so they need to be read from external source."
            )
        }
        let teryt_api_username = value
            .teryt_api_username
            .unwrap_or(std::env::var("TERYT_API_USERNAME").unwrap_or_default());
        let teryt_api_password = value
            .teryt_api_password
            .unwrap_or(std::env::var("TERYT_API_PASSWORD").unwrap_or_default());
        if download_teryt_flag && (teryt_api_username.is_empty() || teryt_api_password.is_empty()) {
            anyhow::bail!(
                "When teryt-download flag is used then either the env variables need to be set or credentials needs to be provided via parameters."
            )
        }
        let schema_version = match value.schema_version {
            SchemaVersionArg::V2012 => SchemaVersion::Model2012,
            SchemaVersionArg::V2021 => SchemaVersion::Model2021,
        };
        let output_format = match value.output_format {
            OutputFormatArg::Csv => OutputFormat::CSV,
            OutputFormatArg::Geoparquet => OutputFormat::GeoParquet,
        };
        let compression_level = match value.parquet_compression {
            None | Some(ParquetCompressionArg::Zstd) => Some(value.compression_level.unwrap_or(11)),
            Some(ParquetCompressionArg::Brotli) => Some(value.compression_level.unwrap_or(6)),
            Some(ParquetCompressionArg::Snappy) => None,
        };
        let parquet_compression = match value.parquet_compression {
            None | Some(ParquetCompressionArg::Zstd) => {
                Compression::ZSTD(ZstdLevel::try_new(compression_level.unwrap())?)
            }
            Some(ParquetCompressionArg::Snappy) => Compression::SNAPPY,
            Some(ParquetCompressionArg::Brotli) => Compression::BROTLI(BrotliLevel::try_new(
                compression_level.unwrap().cast_unsigned(),
            )?),
        };
        let parquet_row_group_size = value.parquet_row_group_size.unwrap_or(batch_size);
        let parquet_version = match value.parquet_version {
            None | Some(ParquetVersionArg::V2) => WriterVersion::PARQUET_2_0,
            Some(ParquetVersionArg::V1) => WriterVersion::PARQUET_1_0,
        };
        let crs = match value.crs_epsg {
            None | Some(CrsEpsgArg::Epsg2180) => CRS::Epsg2180,
            Some(CrsEpsgArg::Epsg4326) => CRS::Epsg4326,
        };
        let parsed_paths = if download_data {
            vec![]
        } else {
            parse_input_paths(&value.input_paths, &schema_version)?
        };
        Ok(ParsedArgs {
            input_paths: value.input_paths,
            parsed_paths: parsed_paths,
            download_data,
            download_data_path,
            output_path: value.output_path,
            download_teryt: download_teryt_flag,
            teryt_api_username: if teryt_api_username.is_empty() {
                None
            } else {
                Some(teryt_api_username)
            },
            teryt_api_password: if teryt_api_password.is_empty() {
                None
            } else {
                Some(teryt_api_password)
            },
            teryt_path: value.teryt_path,
            batch_size: batch_size,
            schema_version: schema_version,
            output_format: output_format,
            compression_level: compression_level,
            parquet_compression: parquet_compression,
            parquet_row_group_size: parquet_row_group_size,
            parquet_version: parquet_version,
            crs: crs,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    fn make_base_raw_args() -> RawArgs {
        RawArgs {
            input_paths: vec!["fixtures/sample_model2012.xml".to_string()],
            download_data: None,
            output_path: PathBuf::from("/tmp/test_output.csv"),
            output_format: OutputFormatArg::Csv,
            schema_version: SchemaVersionArg::V2012,
            teryt_path: None,
            teryt_download: None,
            teryt_api_username: None,
            teryt_api_password: None,
            batch_size: None,
            parquet_compression: None,
            compression_level: None,
            parquet_row_group_size: None,
            parquet_version: None,
            crs_epsg: None,
        }
    }

    #[test]
    fn test_parse_input_paths_xml_file() {
        let result = parse_input_paths(
            &vec!["fixtures/sample_model2012.xml".to_string()],
            &prg_convert::SchemaVersion::Model2012,
        );
        let records = result.expect("Expected Ok result");
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0].file_type, prg_convert::FileType::XML));
        assert!(records[0].compressed_files.is_none());
    }

    #[test]
    fn test_parse_input_paths_zip_file_model2012() {
        let result = parse_input_paths(
            &vec!["fixtures/PRG-punkty_adresowe.zip".to_string()],
            &prg_convert::SchemaVersion::Model2012,
        );
        let records = result.expect("Expected Ok result");
        assert_eq!(records.len(), 1);
        assert!(matches!(records[0].file_type, prg_convert::FileType::ZIP));
        let compressed_files = records[0]
            .compressed_files
            .as_ref()
            .expect("Expected compressed files");
        let xml_files: Vec<_> = compressed_files
            .iter()
            .filter(|f| f.name.ends_with(".xml"))
            .collect();
        let gml_files: Vec<_> = compressed_files
            .iter()
            .filter(|f| f.name.ends_with(".gml"))
            .collect();
        assert!(!xml_files.is_empty());
        assert!(xml_files.iter().all(|f| f.to_be_parsed));
        assert!(!gml_files.is_empty());
        assert!(gml_files.iter().all(|f| !f.to_be_parsed));
    }

    #[test]
    fn test_parse_input_paths_zip_file_model2021() {
        let result = parse_input_paths(
            &vec!["fixtures/PRG-punkty_adresowe.zip".to_string()],
            &prg_convert::SchemaVersion::Model2021,
        );
        let records = result.expect("Expected Ok result");
        assert_eq!(records.len(), 1);
        let compressed_files = records[0]
            .compressed_files
            .as_ref()
            .expect("Expected compressed files");
        let xml_files: Vec<_> = compressed_files
            .iter()
            .filter(|f| f.name.ends_with(".xml"))
            .collect();
        let gml_files: Vec<_> = compressed_files
            .iter()
            .filter(|f| f.name.ends_with(".gml"))
            .collect();
        assert!(!gml_files.is_empty());
        assert!(gml_files.iter().all(|f| f.to_be_parsed));
        assert!(!xml_files.is_empty());
        assert!(xml_files.iter().all(|f| !f.to_be_parsed));
    }

    #[test]
    fn test_parse_input_paths_empty_glob_result() {
        let result = parse_input_paths(
            &vec!["fixtures/nonexistent_*.xml".to_string()],
            &prg_convert::SchemaVersion::Model2012,
        );
        assert!(result.is_err());
        let err_str = format!("{}", result.err().unwrap());
        assert!(err_str.contains("Do the files exist"));
    }

    #[test]
    fn test_try_into_schema_2021_missing_teryt() {
        let args = RawArgs {
            schema_version: SchemaVersionArg::V2021,
            teryt_path: None,
            teryt_download: Some(false),
            ..make_base_raw_args()
        };
        let result: anyhow::Result<ParsedArgs> = args.try_into();
        assert!(result.is_err());
        let err_str = format!("{}", result.err().unwrap());
        assert!(err_str.contains("2021") || err_str.contains("teryt") || err_str.contains("TERYT"));
    }

    #[test]
    fn test_try_into_both_input_paths_and_download_data() {
        let args = RawArgs {
            download_data: Some(String::new()),
            ..make_base_raw_args() // make_base_raw_args has non-empty input_paths
        };
        let result: anyhow::Result<ParsedArgs> = args.try_into();
        assert!(result.is_err());
        let err_str = format!("{}", result.err().unwrap());
        assert!(
            err_str.contains("input-paths") || err_str.contains("download-data"),
            "Error message was: {}",
            err_str
        );
    }

    #[test]
    fn test_try_into_neither_input_paths_nor_download_data() {
        let args = RawArgs {
            input_paths: vec![],
            download_data: None,
            ..make_base_raw_args()
        };
        let result: anyhow::Result<ParsedArgs> = args.try_into();
        assert!(result.is_err());
        let err_str = format!("{}", result.err().unwrap());
        assert!(
            err_str.contains("input-paths") || err_str.contains("download-data"),
            "Error message was: {}",
            err_str
        );
    }

    #[test]
    fn test_try_into_download_teryt_missing_credentials() {
        // Safety: test-only modification of env vars; tests run single-threaded within this process
        unsafe {
            std::env::remove_var("TERYT_API_USERNAME");
            std::env::remove_var("TERYT_API_PASSWORD");
        }
        let args = RawArgs {
            schema_version: SchemaVersionArg::V2021,
            teryt_path: None,
            teryt_download: Some(true),
            ..make_base_raw_args()
        };
        let result: anyhow::Result<ParsedArgs> = args.try_into();
        assert!(result.is_err());
        let err_str = format!("{}", result.err().unwrap());
        assert!(
            err_str.contains("credentials")
                || err_str.contains("env")
                || err_str.contains("teryt-download")
        );
    }

    // --- download_data with optional path ---

    #[test]
    fn test_try_into_download_data_flag_without_path() {
        let args = RawArgs {
            input_paths: vec![],
            download_data: Some(String::new()),
            ..make_base_raw_args()
        };
        let result: anyhow::Result<ParsedArgs> = args.try_into();
        let parsed = result.expect("Expected Ok result");
        assert!(parsed.download_data);
        assert!(parsed.download_data_path.is_none());
    }

    #[test]
    fn test_try_into_download_data_flag_with_path() {
        let args = RawArgs {
            input_paths: vec![],
            download_data: Some("/tmp/prg.zip".to_string()),
            ..make_base_raw_args()
        };
        let result: anyhow::Result<ParsedArgs> = args.try_into();
        let parsed = result.expect("Expected Ok result");
        assert!(parsed.download_data);
        assert_eq!(
            parsed.download_data_path,
            Some(PathBuf::from("/tmp/prg.zip"))
        );
    }

    #[test]
    fn test_try_into_download_data_with_path_and_input_paths() {
        let args = RawArgs {
            download_data: Some("/tmp/prg.zip".to_string()),
            ..make_base_raw_args() // has non-empty input_paths
        };
        let result: anyhow::Result<ParsedArgs> = args.try_into();
        assert!(result.is_err());
        let err_str = format!("{}", result.err().unwrap());
        assert!(
            err_str.contains("input-paths") || err_str.contains("download-data"),
            "Error message was: {}",
            err_str
        );
    }

    // --- happy path tests ---

    #[test]
    fn test_try_into_valid_model2012() {
        let args = make_base_raw_args();
        let result: anyhow::Result<ParsedArgs> = args.try_into();
        let parsed = result.expect("Expected Ok result");
        assert!(matches!(parsed.schema_version, SchemaVersion::Model2012));
        assert!(matches!(parsed.output_format, OutputFormat::CSV));
        assert!(!parsed.download_data);
        assert!(parsed.download_data_path.is_none());
        assert!(!parsed.download_teryt);
        assert_eq!(parsed.batch_size, DEFAULT_BATCH_SIZE);
    }

    #[test]
    fn test_try_into_valid_model2021_with_teryt_path() {
        let args = RawArgs {
            schema_version: SchemaVersionArg::V2021,
            teryt_path: Some(PathBuf::from("fixtures/TERC_Urzedowy_2025-11-18.xml")),
            ..make_base_raw_args()
        };
        let result: anyhow::Result<ParsedArgs> = args.try_into();
        let parsed = result.expect("Expected Ok result");
        assert!(matches!(parsed.schema_version, SchemaVersion::Model2021));
        assert!(!parsed.download_teryt);
        assert_eq!(
            parsed.teryt_path,
            Some(PathBuf::from("fixtures/TERC_Urzedowy_2025-11-18.xml"))
        );
    }

    // --- invalid parquet/crs options ---

    #[test]
    fn test_parse_rejects_invalid_crs_epsg() {
        let result = RawArgs::try_parse_from([
            "prg_convert",
            "--output-path",
            "/tmp/o.parquet",
            "--schema-version",
            "2012",
            "--output-format",
            "geoparquet",
            "--crs-epsg",
            "3857",
        ]);
        assert!(result.is_err());
    }

    // --- download-teryt with schema 2012 warning ---

    #[test]
    fn test_try_into_download_teryt_with_schema_2012_is_downgraded() {
        let args = RawArgs {
            schema_version: SchemaVersionArg::V2012,
            teryt_download: Some(true),
            ..make_base_raw_args()
        };
        let result: anyhow::Result<ParsedArgs> = args.try_into();
        let parsed = result.expect("Expected Ok result");
        assert!(!parsed.download_teryt);
    }

    // --- parse_input_paths edge cases ---

    #[test]
    fn test_parse_input_paths_directory() {
        let result = parse_input_paths(
            &vec!["fixtures".to_string()],
            &prg_convert::SchemaVersion::Model2012,
        );
        assert!(result.is_err());
        let err_str = format!("{}", result.err().unwrap());
        assert!(
            err_str.contains("directory"),
            "Error message was: {}",
            err_str
        );
    }

    #[test]
    fn test_parse_input_paths_unsupported_extension() {
        let result = parse_input_paths(
            &vec!["Cargo.toml".to_string()],
            &prg_convert::SchemaVersion::Model2012,
        );
        assert!(result.is_err());
        let err_str = format!("{}", result.err().unwrap());
        assert!(
            err_str.contains("extension"),
            "Error message was: {}",
            err_str
        );
    }

    #[test]
    fn test_parse_rejects_invalid_output_format() {
        let result = RawArgs::try_parse_from([
            "prg_convert",
            "--output-path",
            "/tmp/o.csv",
            "--schema-version",
            "2012",
            "--output-format",
            "excel",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rejects_invalid_schema_version() {
        let result = RawArgs::try_parse_from([
            "prg_convert",
            "--output-path",
            "/tmp/o.csv",
            "--schema-version",
            "9999",
            "--output-format",
            "csv",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rejects_invalid_parquet_compression() {
        let result = RawArgs::try_parse_from([
            "prg_convert",
            "--output-path",
            "/tmp/o.parquet",
            "--schema-version",
            "2012",
            "--output-format",
            "geoparquet",
            "--parquet-compression",
            "lz4",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_rejects_invalid_parquet_version() {
        let result = RawArgs::try_parse_from([
            "prg_convert",
            "--output-path",
            "/tmp/o.parquet",
            "--schema-version",
            "2012",
            "--output-format",
            "geoparquet",
            "--parquet-version",
            "v3",
        ]);
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_accepts_uppercase_output_format() {
        // ignore_case = true keeps the old case-insensitive behaviour
        let result = RawArgs::try_parse_from([
            "prg_convert",
            "--output-path",
            "/tmp/o.csv",
            "--schema-version",
            "2012",
            "--output-format",
            "CSV",
        ]);
        assert!(result.is_ok());
    }
}
