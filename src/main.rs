use std::{fs::File, path::PathBuf};

use anyhow::{Context, Result};
use arrow::csv::writer::WriterBuilder;
use parquet::{arrow::arrow_writer::ArrowWriter, basic::{BrotliLevel, Compression, ZstdLevel}, file::properties::{WriterProperties, WriterVersion}};
use clap::Parser;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions};
use glob::glob;
use quick_xml::reader::Reader;

use prg_convert::{AddressParser, OutputFormat, build_dictionaries, SCHEMA_CSV, SCHEMA_GEOPARQUET};

const DEFAULT_BATCH_SIZE: usize = 100_000;

struct Writer {
    csv: Option<arrow::csv::writer::Writer<File>>,
    geoparquet: Option<parquet::arrow::arrow_writer::ArrowWriter<File>>,
}

#[derive(Parser)]
struct Cli {
    #[arg(
        long = "input-paths",
        help = "Input XML file path(s). Can be multiple paths separated with space. Can use glob patterns (e.g. `data/*.xml`).",
        value_delimiter = ' ',
        num_args = 1..,
    )]
    input_paths: Vec<String>,
    #[arg(
        long = "output-path",
        help = "Output file path.",
    )]
    output_path: std::path::PathBuf,
    #[arg(
        long = "output-format",
        help = "Output file format (one of: csv, geoparquet).",
    )]
    output_format: String,
    #[arg(
        long = "schema-version",
        help = "Schema version (one of: 2012, 2021).",
    )]
    schema_version: String,
    #[arg(
        long = "batch-size",
        help = format!("(Optional) How many rows are kept in memory before writing to output (default: {}).", DEFAULT_BATCH_SIZE),
    )]
    batch_size: Option<usize>,
    #[arg(
        long = "parquet-compression",
        help = "(Optional) What type of compression to use when writing parquet file (one of: zstd, snappy,) (default: zstd).",
    )]
    parquet_compression: Option<String>,
    #[arg(
        long = "compression-level",
        help = "(Optional) What level of compression to use when writing parquet file (if compression algorithm supports it).",
    )]
    compression_level: Option<i32>,
    #[arg(
        long = "parquet-row-group-size",
        help = "(Optional) What's the max row group size when writing parquet file (default: same as batch-size).",
    )]
    parquet_row_group_size: Option<usize>,
    #[arg(
        long = "parquet-version",
        help = "(Optional) Version of parquet standard to use (one of: v1, v2,) (default: v2).",
    )]
    parquet_version: Option<String>,
}

fn get_xml_reader(path: &PathBuf) -> Result<Reader<std::io::BufReader<std::fs::File>>> {
    let mut reader = Reader::from_file(&path)
        .with_context(|| format!("could not read XML from file `{}`", &path.display()))?;
    reader.config_mut().expand_empty_elements = true;
    return Ok(reader);
}

fn main() -> Result<()> {
    let start_time = std::time::Instant::now();
    let args = Cli::parse();
    let batch_size = args.batch_size.unwrap_or(DEFAULT_BATCH_SIZE);
    if &args.schema_version != "2012" && &args.schema_version != "2021" {
        anyhow::bail!("unsupported schema version `{}`, expected one of: 2012, 2021", &args.schema_version);
    }
    if &args.schema_version == "2021" {
        anyhow::bail!("Schema version 2021 is not implemented yet.")
    }
    let (output_format, schema) = match args.output_format.to_lowercase().as_str() {
        "csv" => { (OutputFormat::CSV, SCHEMA_CSV.clone()) },
        "geoparquet" => { (OutputFormat::GeoParquet, SCHEMA_GEOPARQUET.clone()) },
        _ => { anyhow::bail!("unsupported format `{}`, expected one of: csv, geoparquet", &args.output_format); }
    };
    let compression_level = match &args.parquet_compression.as_deref() {
        None | Some("zstd") => { Some(args.compression_level.unwrap_or(11)) },
        Some("brotli") => { Some(args.compression_level.unwrap_or(6)) },
        _ => { None },
    };
    let parquet_compression = match &args.parquet_compression.as_deref() {
        None | Some("zstd") => { Compression::ZSTD(ZstdLevel::try_new(compression_level.unwrap()).unwrap()) },
        Some("snappy") => { Compression::SNAPPY },
        Some("brotli") => { Compression::BROTLI(BrotliLevel::try_new(compression_level.unwrap().cast_unsigned()).unwrap()) },
        _ => {anyhow::bail!("Unexpected compression type for parquet writer: `{:?}`", &args.parquet_compression)},
    };
    let parquet_row_group_size = args.parquet_row_group_size.unwrap_or(batch_size);
    let parquet_version = match &args.parquet_version.as_deref() {
        None | Some("v2") => { WriterVersion::PARQUET_2_0 },
        Some("v1") => { WriterVersion::PARQUET_1_0 },
        _ => { anyhow::bail!("Unexpected version for parquet writer: `{:?}`", &args.parquet_version) }
    };
    let mut counter = 1;
    let mut total_count = 0;
    let mut total_size = 0;

    let paths: Vec<PathBuf> = args.input_paths.clone()
        .into_iter()
        .flat_map(|p| {glob(&p).expect("Failed to read glob pattern")})
        .map(|p| { p.unwrap() })
        .collect();

    println!("⚙️  Parameters:");
    if paths.len() == 1 {
        println!("  Input file: {}", &paths[0].display());
    } else {
        println!("  Input patterns: {}", &args.input_paths.clone().join(" "));
        println!("  Input files:");
        for path in &paths {
            println!("    - {}", path.display());
        }
    }
    println!("  Output file: {}", args.output_path.display());
    println!("  Output file format: {}", args.output_format);
    println!("  Schema version: {}", args.schema_version);
    println!("  Batch size: {}", batch_size);
    match output_format  {
        OutputFormat::GeoParquet => {
            println!("  Parquet compression: {}", parquet_compression);
            if compression_level.is_some() {
                println!("  Compression level: {}", compression_level.unwrap());
            }
            println!("  Parquet max row group size: {}", parquet_row_group_size);
            match parquet_version {
                WriterVersion::PARQUET_1_0 => { println!("  Parquet file format version: v1") },
                WriterVersion::PARQUET_2_0 => { println!("  Parquet file format version: v2") },
            };
        },
        _ => {},
    };
    println!("----------------------------------------");

    let output_file = std::fs::File::create(&args.output_path)
        .with_context(|| format!("could not create output file `{}`", &args.output_path.display()))?;
    let (mut writer, mut gpq_encoder)  = match &output_format {
        OutputFormat::CSV => {
            (
                Writer{ csv: Some(WriterBuilder::new().with_header(true).build(output_file)), geoparquet: None },
                None,
            )
        },
        OutputFormat::GeoParquet => {
            let props = WriterProperties::builder()
                .set_max_row_group_size(parquet_row_group_size)
                .set_writer_version(parquet_version)
                .set_compression(parquet_compression)
                .build();
            let gpq_encoder = GeoParquetRecordBatchEncoder::try_new(&schema, &GeoParquetWriterOptions::default()).unwrap();
            (
                Writer{ csv: None, geoparquet: Some(ArrowWriter::try_new(output_file, gpq_encoder.target_schema(), Some(props)).unwrap()) },
                Some(gpq_encoder),
            )
        }
    };

    let num_files_to_process = &paths.len();
    for path in &paths {
        let input_file_metadata = std::fs::metadata(&path)
            .with_context(|| format!("could not get metadata for file `{}`", &path.display()))?;
        if input_file_metadata.is_dir() {
            anyhow::bail!("input path `{}` is a directory, expected a file", &path.display());
        }
        let input_file_size = input_file_metadata.len();
        total_size += &input_file_size;
        let mut reader = get_xml_reader(&path).unwrap();
        if counter == 1 {
            println!("⚙️  XML reader configuration: {:#?}", reader.config());
            println!("----------------------------------------");
        }

        println!("🪓 Processing file ({}/{}): `{}`, size: {:.2}MB.", &counter, &num_files_to_process, &path.display(), (input_file_size as f64 / 1024.0 / 1024.0));
        println!("Building dictionaries...");
        let dict = build_dictionaries(reader);
        reader = get_xml_reader(&path).unwrap();
        println!("Parsing data...");
        AddressParser::new(reader, batch_size, dict, output_format.clone()).for_each(|batch| {
            total_count += batch.num_rows();
            println!("Read batch of {} addresses.", batch.num_rows());
            match &output_format {
                OutputFormat::CSV => {
                    writer.csv.as_mut().unwrap().write(&batch).expect("Failed to write batch.");
                },
                OutputFormat::GeoParquet => {
                    let encoded_batch = gpq_encoder.as_mut().unwrap().encode_record_batch(&batch).unwrap();
                    writer.geoparquet.as_mut().unwrap().write(&encoded_batch).unwrap();
                },
            }
        });
        counter += 1;
    }
    if matches!(output_format, OutputFormat::GeoParquet) {
        let kv_metadata = gpq_encoder.unwrap().into_keyvalue().unwrap();
        let parquet_writer = writer.geoparquet.as_mut().unwrap();
        parquet_writer.append_key_value_metadata(kv_metadata);
        parquet_writer.finish().expect("Failed to write geoparquet metadata.");
    }
    let duration = start_time.elapsed();
    println!("----------------------------------------");
    println!("📊 Total addresses read {}. Duration: {:#?}. Data size: {:.2}MB.", total_count, duration, (total_size as f64 / 1024.0 / 1024.0));

    Ok(())
}
