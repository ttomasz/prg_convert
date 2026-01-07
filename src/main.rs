use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::{array::RecordBatch, csv::writer::WriterBuilder};
use clap::Parser;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions};
use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};

mod cli;
use prg_convert::{
    FileType, OutputFormat, SchemaVersion, Writer, get_address_parser_2012_uncompressed,
    get_address_parser_2012_zip, get_address_parser_2021_uncompressed, get_address_parser_2021_zip,
};
use zip::ZipArchive;

use crate::cli::CompressedFile;

fn write_batch(
    output_format: &OutputFormat,
    writer: &mut Writer,
    geoparquet_encoder: &mut Option<GeoParquetRecordBatchEncoder>,
    batch: RecordBatch,
) {
    match output_format {
        OutputFormat::CSV => {
            writer
                .csv
                .as_mut()
                .unwrap()
                .write(&batch)
                .expect("Failed to write batch.");
        }
        OutputFormat::GeoParquet => {
            let encoded_batch = geoparquet_encoder
                .as_mut()
                .unwrap()
                .encode_record_batch(&batch)
                .expect("Failed to encode batch.");
            writer
                .geoparquet
                .as_mut()
                .unwrap()
                .write(&encoded_batch)
                .expect("Failed to write batch.");
        }
    }
}

fn parse_file(
    file_type: &FileType,
    parsed_args: &cli::ParsedArgs,
    file_path: &PathBuf,
    mut writer: &mut Writer,
    mut geoparquet_encoder: &mut Option<GeoParquetRecordBatchEncoder>,
    zip_file_index: &Option<usize>,
) -> anyhow::Result<usize> {
    let mut processed_rows = 0;
    match (&file_type, &parsed_args.schema_version) {
        (FileType::XML, SchemaVersion::Model2012) => {
            get_address_parser_2012_uncompressed(
                &file_path,
                &parsed_args.batch_size,
                &parsed_args.output_format,
                &parsed_args.crs,
                parsed_args.arrow_schema.clone(),
                &parsed_args.geoarrow_geom_type,
            )?
            .for_each(|batch| {
                processed_rows += batch.num_rows();
                println!("Read batch of {} addresses.", batch.num_rows());
                write_batch(
                    &parsed_args.output_format,
                    &mut writer,
                    &mut geoparquet_encoder,
                    batch,
                );
            });
        }
        (FileType::ZIP, SchemaVersion::Model2012) => {
            let f = std::fs::File::open(&file_path)
                .with_context(|| format!("Failed to open file: `{}`.", &file_path.display()))?;
            let mut archive = ZipArchive::new(f).with_context(|| {
                format!("Failed to decompress ZIP file: `{}`.", &file_path.display())
            })?;
            get_address_parser_2012_zip(
                &mut archive,
                &parsed_args.batch_size,
                &parsed_args.output_format,
                zip_file_index.unwrap(),
                &parsed_args.crs,
                parsed_args.arrow_schema.clone(),
                &parsed_args.geoarrow_geom_type,
            )?
            .for_each(|batch| {
                processed_rows += batch.num_rows();
                println!("Read batch of {} addresses.", batch.num_rows());
                write_batch(
                    &parsed_args.output_format,
                    &mut writer,
                    &mut geoparquet_encoder,
                    batch,
                );
            });
        }
        (FileType::XML, SchemaVersion::Model2021) => {
            get_address_parser_2021_uncompressed(
                &file_path,
                &parsed_args.batch_size,
                &parsed_args.output_format,
                &parsed_args.teryt_path.as_ref().unwrap(),
                &parsed_args.crs,
                parsed_args.arrow_schema.clone(),
                &parsed_args.geoarrow_geom_type,
            )?
            .for_each(|batch| {
                processed_rows += batch.num_rows();
                println!("Read batch of {} addresses.", batch.num_rows());
                write_batch(
                    &parsed_args.output_format,
                    &mut writer,
                    &mut geoparquet_encoder,
                    batch,
                );
            });
        }
        (FileType::ZIP, SchemaVersion::Model2021) => {
            let f = std::fs::File::open(&file_path)
                .with_context(|| format!("Failed to open file: `{}`.", &file_path.display()))?;
            let mut archive = ZipArchive::new(f).with_context(|| {
                format!("Failed to decompress ZIP file: `{}`.", &file_path.display())
            })?;
            get_address_parser_2021_zip(
                &mut archive,
                &parsed_args.batch_size,
                &parsed_args.output_format,
                &parsed_args.teryt_path.as_ref().unwrap(),
                zip_file_index.unwrap(),
                &parsed_args.crs,
                parsed_args.arrow_schema.clone(),
                &parsed_args.geoarrow_geom_type,
            )?
            .for_each(|batch| {
                processed_rows += batch.num_rows();
                println!("Read batch of {} addresses.", batch.num_rows());
                write_batch(
                    &parsed_args.output_format,
                    &mut writer,
                    &mut geoparquet_encoder,
                    batch,
                );
            });
        }
    }
    Ok(processed_rows)
}

fn main() -> Result<()> {
    let start_time = std::time::Instant::now();
    let args = cli::RawArgs::parse();
    let parsed_args: cli::ParsedArgs = args.try_into().expect("Could not parse args.");

    cli::print_parsed_args(&parsed_args);

    let mut file_counter = 1;
    let mut total_row_count = 0;
    let mut total_file_size = 0;

    let output_file = std::fs::File::create(&parsed_args.output_path).with_context(|| {
        format!(
            "could not create output file `{}`",
            &parsed_args.output_path.to_string_lossy()
        )
    })?;
    let (mut writer, mut geoparquet_encoder) = match &parsed_args.output_format {
        OutputFormat::CSV => (
            Writer {
                csv: Some(WriterBuilder::new().with_header(true).build(output_file)),
                geoparquet: None,
            },
            None,
        ),
        OutputFormat::GeoParquet => {
            let props = WriterProperties::builder()
                .set_max_row_group_size(parsed_args.parquet_row_group_size)
                .set_writer_version(parsed_args.parquet_version)
                .set_compression(parsed_args.parquet_compression)
                .build();
            let gpq_encoder = GeoParquetRecordBatchEncoder::try_new(
                &parsed_args.arrow_schema,
                &GeoParquetWriterOptions::default(),
            )
            .expect("Could not create GeoParquet encoder.");
            (
                Writer {
                    csv: None,
                    geoparquet: Some(
                        ArrowWriter::try_new(output_file, gpq_encoder.target_schema(), Some(props))
                            .expect("Could not create GeoParquet writer."),
                    ),
                },
                Some(gpq_encoder),
            )
        }
    };

    let num_files_to_process = &parsed_args.parsed_paths.len();
    for file in &parsed_args.parsed_paths {
        total_file_size += &file.size_in_bytes;

        println!(
            "🪓 Processing file ({}/{})({}): `{}`, size: {:.2}MB.",
            &file_counter,
            &num_files_to_process,
            &file.file_type,
            &file.path.display(),
            (file.size_in_bytes as f64 / 1024.0 / 1024.0)
        );
        println!("Parsing data...");
        match file.file_type {
            FileType::XML => {
                let processed_rows = parse_file(
                    &file.file_type,
                    &parsed_args,
                    &file.path,
                    &mut writer,
                    &mut geoparquet_encoder,
                    &None,
                )?;
                total_row_count += processed_rows;
            }
            FileType::ZIP => {
                let files_to_parse: Vec<&CompressedFile> = file
                    .compressed_files
                    .as_ref()
                    .unwrap()
                    .iter()
                    .filter(|f| f.to_be_parsed)
                    .collect();
                for compressed_file in files_to_parse {
                    println!("Decompressing file: {}", compressed_file.name);
                    let processed_rows = parse_file(
                        &file.file_type,
                        &parsed_args,
                        &file.path,
                        &mut writer,
                        &mut geoparquet_encoder,
                        &Some(compressed_file.index),
                    )?;
                    total_row_count += processed_rows;
                }
            }
        }

        file_counter += 1;
    }
    if matches!(parsed_args.output_format, OutputFormat::GeoParquet) {
        let kv_metadata = geoparquet_encoder
            .unwrap()
            .into_keyvalue()
            .expect("Could not create GeoParquet K/V metadata.");
        let parquet_writer = writer.geoparquet.as_mut().unwrap();
        parquet_writer.append_key_value_metadata(kv_metadata);
        parquet_writer
            .finish()
            .expect("Failed to write geoparquet metadata.");
    }
    let duration = start_time.elapsed();
    println!("----------------------------------------");
    println!(
        "📊 Total addresses read {}. Duration: {:.1}s. Input data size: {:.2}MB.",
        total_row_count,
        duration.as_secs_f64(),
        (total_file_size as f64 / 1024.0 / 1024.0)
    );

    let _ = &parsed_args.output_path.metadata().inspect(|f| {
        let output_file_size_mb = f.len() as f64 / 1024.0 / 1024.0;
        println!(
            "💾 Output file: {} size: {:.2}MB",
            &parsed_args.output_path.to_string_lossy(),
            output_file_size_mb
        );
    });

    Ok(())
}
