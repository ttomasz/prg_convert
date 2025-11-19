use anyhow::{Context, Result};
use arrow::csv::writer::WriterBuilder;
use clap::Parser;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions};
use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};

mod cli;
use prg_convert::{
    OutputFormat, SchemaVersion, Writer, get_address_parser_2012, get_address_parser_2021,
};

fn main() -> Result<()> {
    let start_time = std::time::Instant::now();
    let args = cli::RawArgs::parse();
    let parsed_args: cli::ParsedArgs = args.try_into().expect("Could not parse args.");

    cli::print_parsed_args(&parsed_args);

    let mut file_counter = 1;
    let mut total_row_count = 0;
    let mut total_file_size = 0;

    let output_file = std::fs::File::create(&parsed_args.output_path)
        .with_context(|| {
            format!(
                "could not create output file `{}`",
                &parsed_args.output_path.to_string_lossy()
            )
        })
        .unwrap();
    let (mut writer, mut gpq_encoder) = match &parsed_args.output_format {
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
                &parsed_args.schema,
                &GeoParquetWriterOptions::default(),
            )
            .unwrap();
            (
                Writer {
                    csv: None,
                    geoparquet: Some(
                        ArrowWriter::try_new(output_file, gpq_encoder.target_schema(), Some(props))
                            .unwrap(),
                    ),
                },
                Some(gpq_encoder),
            )
        }
    };

    let num_files_to_process = &parsed_args.parsed_paths.len();
    for path in &parsed_args.parsed_paths {
        let input_file_metadata = std::fs::metadata(&path)
            .with_context(|| format!("could not get metadata for file `{}`", &path.display()))?;
        if input_file_metadata.is_dir() {
            anyhow::bail!(
                "input path `{}` is a directory, expected a file",
                &path.display()
            );
        }
        let input_file_size = input_file_metadata.len();
        total_file_size += &input_file_size;

        println!(
            "🪓 Processing file ({}/{}): `{}`, size: {:.2}MB.",
            &file_counter,
            &num_files_to_process,
            &path.display(),
            (input_file_size as f64 / 1024.0 / 1024.0)
        );
        println!("Parsing data...");
        match parsed_args.schema_version {
            SchemaVersion::Model2012 => {
                get_address_parser_2012(
                    &path,
                    &parsed_args.batch_size,
                    &parsed_args.output_format,
                    file_counter == 1,
                )
                .for_each(|batch| {
                    total_row_count += batch.num_rows();
                    println!("Read batch of {} addresses.", batch.num_rows());
                    match &parsed_args.output_format {
                        OutputFormat::CSV => {
                            writer
                                .csv
                                .as_mut()
                                .unwrap()
                                .write(&batch)
                                .expect("Failed to write batch.");
                        }
                        OutputFormat::GeoParquet => {
                            let encoded_batch = gpq_encoder
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
                });
            }
            SchemaVersion::Model2021 => {
                get_address_parser_2021(
                    &path,
                    &parsed_args.batch_size,
                    &parsed_args.output_format,
                    file_counter == 1,
                    &parsed_args.teryt_path.clone().unwrap(),
                )
                .for_each(|batch| {
                    total_row_count += batch.num_rows();
                    println!("Read batch of {} addresses.", batch.num_rows());
                    match &parsed_args.output_format {
                        OutputFormat::CSV => {
                            writer
                                .csv
                                .as_mut()
                                .unwrap()
                                .write(&batch)
                                .expect("Failed to write batch.");
                        }
                        OutputFormat::GeoParquet => {
                            let encoded_batch = gpq_encoder
                                .as_mut()
                                .unwrap()
                                .encode_record_batch(&batch)
                                .unwrap();
                            writer
                                .geoparquet
                                .as_mut()
                                .unwrap()
                                .write(&encoded_batch)
                                .unwrap();
                        }
                    }
                });
            }
        }
        file_counter += 1;
    }
    if matches!(parsed_args.output_format, OutputFormat::GeoParquet) {
        let kv_metadata = gpq_encoder.unwrap().into_keyvalue().unwrap();
        let parquet_writer = writer.geoparquet.as_mut().unwrap();
        parquet_writer.append_key_value_metadata(kv_metadata);
        parquet_writer
            .finish()
            .expect("Failed to write geoparquet metadata.");
    }
    let duration = start_time.elapsed();
    println!("----------------------------------------");
    println!(
        "📊 Total addresses read {}. Duration: {:#?}. Data size: {:.2}MB.",
        total_row_count,
        duration,
        (total_file_size as f64 / 1024.0 / 1024.0)
    );

    Ok(())
}
