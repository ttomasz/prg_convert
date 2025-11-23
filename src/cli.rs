use std::convert::TryInto;
use std::fs::File;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use glob::glob;
use parquet::basic::BrotliLevel;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterVersion;

use prg_convert::FileType;
use prg_convert::OutputFormat;
use prg_convert::SCHEMA_CSV;
use prg_convert::SCHEMA_GEOPARQUET;
use prg_convert::SchemaVersion;
use zip::ZipArchive;

pub const DEFAULT_BATCH_SIZE: usize = 100_000;

#[derive(clap::Parser)]
pub struct RawArgs {
    #[arg(
        long = "input-paths",
        help = "Input XML file path(s). Can be multiple paths separated with space. Can use glob patterns (e.g. `data/*.xml`).",
        value_delimiter = ' ',
        num_args = 1..,
    )]
    input_paths: Vec<String>,
    #[arg(long = "output-path", help = "Output file path.")]
    output_path: std::path::PathBuf,
    #[arg(
        long = "output-format",
        help = "Output file format (one of: csv, geoparquet)."
    )]
    output_format: String,
    #[arg(long = "schema-version", help = "Schema version (one of: 2012, 2021).")]
    schema_version: String,
    #[arg(
        long = "teryt-path",
        help = "Path of XML file with teryt dictionary unpacked from archive downloaded from: https://eteryt.stat.gov.pl/eTeryt/rejestr_teryt/udostepnianie_danych/baza_teryt/uzytkownicy_indywidualni/pobieranie/pliki_pelne.aspx?contrast=default (TERC, podstawowa). Required for schema 2021."
    )]
    teryt_path: Option<std::path::PathBuf>,
    #[arg(
        long = "batch-size",
        help = format!("(Optional) How many rows are kept in memory before writing to output (default: {}).", DEFAULT_BATCH_SIZE),
    )]
    batch_size: Option<usize>,
    #[arg(
        long = "parquet-compression",
        help = "(Optional) What type of compression to use when writing parquet file (one of: zstd, snappy,) (default: zstd)."
    )]
    parquet_compression: Option<String>,
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
        help = "(Optional) Version of parquet standard to use (one of: v1, v2,) (default: v2)."
    )]
    parquet_version: Option<String>,
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

fn parse_input_paths(
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
    Ok(paths)
}

pub struct ParsedArgs {
    pub input_paths: Vec<String>,
    pub parsed_paths: Vec<FileRecord>,
    pub output_path: PathBuf,
    pub teryt_path: Option<std::path::PathBuf>,
    pub batch_size: usize,
    pub schema_version: SchemaVersion,
    pub output_format: OutputFormat,
    pub schema: Arc<arrow::datatypes::Schema>,
    pub compression_level: Option<i32>,
    pub parquet_compression: parquet::basic::Compression,
    pub parquet_row_group_size: usize,
    pub parquet_version: parquet::file::properties::WriterVersion,
}

pub fn print_parsed_args(parsed_args: &ParsedArgs) {
    println!("⚙️  Parameters:");
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
                println!(
                    "    - {} (ZIP), size compressed: {:.2} MB, size uncompressed: {:.2} MB",
                    file.path.display(),
                    (file.size_in_bytes as f64 / 1024.0 / 1024.0),
                    (file.decompressed_size.unwrap() as f64 / 1024.0 / 1024.0)
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
    println!("  Output file: {}", parsed_args.output_path.display());
    println!("  Output file format: {}", parsed_args.output_format);
    println!("  Schema version: {}", parsed_args.schema_version);
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
    };
    println!("----------------------------------------");
}

impl TryInto<ParsedArgs> for RawArgs {
    type Error = anyhow::Error;

    fn try_into(self) -> anyhow::Result<ParsedArgs> {
        let batch_size = self.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
        if self.schema_version.to_lowercase() == "2021" && self.teryt_path.is_none() {
            anyhow::bail!(
                "Chosen schema 2021 but did not provide teryt file path. PRG schema 2021 does not contain names of administrative units so they need to be read from external source."
            )
        }
        let schema_version = match self.schema_version.to_lowercase().as_str() {
            "2012" => SchemaVersion::Model2012,
            "2021" => SchemaVersion::Model2021,
            _ => {
                anyhow::bail!(
                    "unsupported schema version `{}`, expected one of: 2012, 2021",
                    &self.schema_version
                );
            }
        };
        let (output_format, schema) = match self.output_format.to_lowercase().as_str() {
            "csv" => (OutputFormat::CSV, SCHEMA_CSV.clone()),
            "geoparquet" => (OutputFormat::GeoParquet, SCHEMA_GEOPARQUET.clone()),
            _ => {
                anyhow::bail!(
                    "unsupported format `{}`, expected one of: csv, geoparquet",
                    &self.output_format
                );
            }
        };
        let compression_level = match &self.parquet_compression.as_deref() {
            None | Some("zstd") => Some(self.compression_level.unwrap_or(11)),
            Some("brotli") => Some(self.compression_level.unwrap_or(6)),
            _ => None,
        };
        let parquet_compression = match &self.parquet_compression.as_deref() {
            None | Some("zstd") => {
                Compression::ZSTD(ZstdLevel::try_new(compression_level.unwrap())?)
            }
            Some("snappy") => Compression::SNAPPY,
            Some("brotli") => Compression::BROTLI(BrotliLevel::try_new(
                compression_level.unwrap().cast_unsigned(),
            )?),
            _ => {
                anyhow::bail!(
                    "Unexpected compression type for parquet writer: `{:?}`",
                    &self.parquet_compression
                )
            }
        };
        let parquet_row_group_size = self.parquet_row_group_size.unwrap_or(batch_size);
        let parquet_version = match &self.parquet_version.as_deref() {
            None | Some("v2") => WriterVersion::PARQUET_2_0,
            Some("v1") => WriterVersion::PARQUET_1_0,
            _ => {
                anyhow::bail!(
                    "Unexpected version for parquet writer: `{:?}`",
                    &self.parquet_version
                )
            }
        };
        let paths = parse_input_paths(&self.input_paths, &schema_version);
        Ok(ParsedArgs {
            input_paths: self.input_paths,
            parsed_paths: paths?,
            output_path: self.output_path,
            teryt_path: self.teryt_path,
            batch_size: batch_size,
            schema_version: schema_version,
            output_format: output_format,
            schema: schema,
            compression_level: compression_level,
            parquet_compression: parquet_compression,
            parquet_row_group_size: parquet_row_group_size,
            parquet_version: parquet_version,
        })
    }
}
