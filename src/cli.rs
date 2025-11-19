use std::convert::TryInto;
use std::path::PathBuf;
use std::sync::Arc;

use glob::glob;
use parquet::basic::BrotliLevel;
use parquet::basic::Compression;
use parquet::basic::ZstdLevel;
use parquet::file::properties::WriterVersion;

use prg_convert::OutputFormat;
use prg_convert::SCHEMA_CSV;
use prg_convert::SCHEMA_GEOPARQUET;
use prg_convert::SchemaVersion;

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

pub struct ParsedArgs {
    pub input_paths: Vec<String>,
    pub parsed_paths: Vec<PathBuf>,
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
    if parsed_args.parsed_paths.len() == 1 {
        println!("  Input file: {}", &parsed_args.parsed_paths[0].display());
    } else {
        println!(
            "  Input patterns: {}",
            &parsed_args.input_paths.clone().join(" ")
        );
        println!("  Input files:");
        for path in &parsed_args.parsed_paths {
            println!("    - {}", path.display());
        }
    }
    println!("  Output file: {}", parsed_args.output_path.display());
    println!("  Output file format: {}", parsed_args.output_format);
    println!("  Schema version: {}", parsed_args.schema_version);
    println!("  Batch size: {}", parsed_args.batch_size);
    match parsed_args.output_format {
        OutputFormat::GeoParquet => {
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
        }
        _ => {}
    };
    println!("----------------------------------------");
}

impl TryInto<ParsedArgs> for RawArgs {
    type Error = anyhow::Error;

    fn try_into(self) -> Result<ParsedArgs, anyhow::Error> {
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
                Compression::ZSTD(ZstdLevel::try_new(compression_level.unwrap()).unwrap())
            }
            Some("snappy") => Compression::SNAPPY,
            Some("brotli") => Compression::BROTLI(
                BrotliLevel::try_new(compression_level.unwrap().cast_unsigned()).unwrap(),
            ),
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
        let paths: Vec<PathBuf> = self
            .input_paths
            .clone()
            .into_iter()
            .flat_map(|p| glob(&p).expect("Failed to read glob pattern"))
            .map(|p| p.unwrap())
            .collect();
        Ok(ParsedArgs {
            input_paths: self.input_paths,
            parsed_paths: paths,
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
