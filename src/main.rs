use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};

use anyhow::{Context, Result};
use arrow::array::{Array, ArrayRef, Float64Array, RecordBatch};
use arrow::csv::writer::WriterBuilder;
use arrow::datatypes::Schema;
use clap::Parser;
use geoarrow::array::{GeoArrowArray, PointBuilder};
use geoarrow::datatypes::PointType;
use geoparquet::writer::{GeoParquetRecordBatchEncoder, GeoParquetWriterOptions};
use parquet::{arrow::arrow_writer::ArrowWriter, file::properties::WriterProperties};
use prg_convert::CRS;

mod cli;
use prg_convert::{
    FileType, OutputFormat, SchemaVersion, get_address_parser_2012_uncompressed,
    get_address_parser_2012_zip, get_address_parser_2021_uncompressed, get_address_parser_2021_zip,
    get_teryt_mapping, terc::Terc,
};
use zip::ZipArchive;

use crate::cli::CompressedFile;

enum OutputWriter {
    Csv(arrow::csv::writer::Writer<std::fs::File>),
    GeoParquet {
        writer: ArrowWriter<std::fs::File>,
        encoder: GeoParquetRecordBatchEncoder,
        crs: CRS,
        geom_type: PointType,
        geoparquet_schema: Arc<Schema>,
    },
}

impl OutputWriter {
    fn write_batch(&mut self, batch: &RecordBatch) -> anyhow::Result<()> {
        match self {
            OutputWriter::Csv(w) => {
                w.write(batch).context("Failed to write CSV batch.")?;
            }
            OutputWriter::GeoParquet {
                writer,
                encoder,
                crs,
                geom_type,
                geoparquet_schema,
            } => {
                let geo_batch =
                    canonical_to_geoparquet_batch(batch, crs, geom_type, geoparquet_schema)?;
                let encoded = encoder
                    .encode_record_batch(&geo_batch)
                    .context("Failed to encode GeoParquet batch.")?;
                writer
                    .write(&encoded)
                    .context("Failed to write GeoParquet batch.")?;
            }
        }
        Ok(())
    }

    fn finish(self) -> anyhow::Result<()> {
        match self {
            OutputWriter::Csv(_) => Ok(()),
            OutputWriter::GeoParquet {
                mut writer,
                encoder,
                ..
            } => {
                let kv_metadata = encoder
                    .into_keyvalue()
                    .context("Could not create GeoParquet K/V metadata.")?;
                writer.append_key_value_metadata(kv_metadata);
                writer
                    .finish()
                    .context("Failed to write GeoParquet metadata.")?;
                Ok(())
            }
        }
    }
}

/// Convert a canonical (SCHEMA_CSV-shaped) batch into a GeoParquet batch:
/// build a `geometry` point column from the coordinate columns selected by `crs`,
/// drop `x_epsg_2180`/`y_epsg_2180`, and reorder to match `geoparquet_schema`.
fn canonical_to_geoparquet_batch(
    batch: &RecordBatch,
    crs: &CRS,
    geom_type: &PointType,
    geoparquet_schema: &Arc<Schema>,
) -> anyhow::Result<RecordBatch> {
    let (x_name, y_name) = match crs {
        CRS::Epsg2180 => ("x_epsg_2180", "y_epsg_2180"),
        CRS::Epsg4326 => ("dlugosc_geograficzna", "szerokosc_geograficzna"),
    };
    let xs = batch
        .column_by_name(x_name)
        .context("canonical batch missing x column")?
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("x column is not Float64")?;
    let ys = batch
        .column_by_name(y_name)
        .context("canonical batch missing y column")?
        .as_any()
        .downcast_ref::<Float64Array>()
        .context("y column is not Float64")?;
    let points: Vec<Option<geo_types::Point>> = (0..batch.num_rows())
        .map(|i| {
            if xs.is_null(i) || ys.is_null(i) {
                None
            } else {
                Some(geo_types::point!(x: xs.value(i), y: ys.value(i)))
            }
        })
        .collect();
    let geometry =
        PointBuilder::from_nullable_points(points.iter().map(Option::as_ref), geom_type.clone())
            .finish();

    let mut columns: Vec<ArrayRef> = Vec::with_capacity(geoparquet_schema.fields().len());
    for field in geoparquet_schema.fields() {
        if field.name() == "geometry" {
            columns.push(geometry.to_array_ref());
        } else {
            let col = batch
                .column_by_name(field.name())
                .with_context(|| format!("canonical batch missing column `{}`", field.name()))?;
            columns.push(col.clone());
        }
    }
    Ok(RecordBatch::try_new(geoparquet_schema.clone(), columns)?)
}

fn parse_file(
    file_type: &FileType,
    parsed_args: &cli::ParsedArgs,
    file_path: &PathBuf,
    output_writer: &mut OutputWriter,
    zip_file_index: &Option<usize>,
    teryt_mapping: &Option<HashMap<String, Terc>>,
) -> anyhow::Result<usize> {
    let mut processed_rows = 0;
    match (&file_type, &parsed_args.schema_version) {
        (FileType::XML, SchemaVersion::Model2012) => {
            for batch in get_address_parser_2012_uncompressed(&file_path, &parsed_args.batch_size)?
            {
                processed_rows += batch.num_rows();
                println!("Read batch of {} addresses.", batch.num_rows());
                output_writer.write_batch(&batch)?;
            }
        }
        (FileType::ZIP, SchemaVersion::Model2012) => {
            let f = std::fs::File::open(&file_path)
                .with_context(|| format!("Failed to open file: `{}`.", &file_path.display()))?;
            let mut archive = ZipArchive::new(f).with_context(|| {
                format!("Failed to decompress ZIP file: `{}`.", &file_path.display())
            })?;
            for batch in get_address_parser_2012_zip(
                &mut archive,
                &parsed_args.batch_size,
                zip_file_index.unwrap(),
            )? {
                processed_rows += batch.num_rows();
                println!("Read batch of {} addresses.", batch.num_rows());
                output_writer.write_batch(&batch)?;
            }
        }
        (FileType::XML, SchemaVersion::Model2021) => {
            for batch in get_address_parser_2021_uncompressed(
                &file_path,
                &parsed_args.batch_size,
                teryt_mapping.as_ref().unwrap(),
            )? {
                processed_rows += batch.num_rows();
                println!("Read batch of {} addresses.", batch.num_rows());
                output_writer.write_batch(&batch)?;
            }
        }
        (FileType::ZIP, SchemaVersion::Model2021) => {
            let f = std::fs::File::open(&file_path)
                .with_context(|| format!("Failed to open file: `{}`.", &file_path.display()))?;
            let mut archive = ZipArchive::new(f).with_context(|| {
                format!("Failed to decompress ZIP file: `{}`.", &file_path.display())
            })?;
            for batch in get_address_parser_2021_zip(
                &mut archive,
                &parsed_args.batch_size,
                teryt_mapping.as_ref().unwrap(),
                zip_file_index.unwrap(),
            )? {
                processed_rows += batch.num_rows();
                println!("Read batch of {} addresses.", batch.num_rows());
                output_writer.write_batch(&batch)?;
            }
        }
    }
    Ok(processed_rows)
}

fn main() -> Result<()> {
    let start_time = std::time::Instant::now();
    let args = cli::RawArgs::parse();
    let mut parsed_args: cli::ParsedArgs = args.try_into().expect("Could not parse args.");

    cli::print_parsed_args(&parsed_args);

    // Download data if requested, keeping the temp file alive for the duration of processing
    let _temp_file;
    let files_to_process: Vec<cli::FileRecord>;
    if parsed_args.download_data {
        println!("⬇️  Downloading PRG data...");
        let temp = cli::download_prg_data(parsed_args.download_data_path.as_deref())?;
        let download_path = match (&parsed_args.download_data_path, &temp) {
            (Some(path), _) => path.to_string_lossy().to_string(),
            (None, Some(t)) => t.path().to_string_lossy().to_string(),
            _ => unreachable!(),
        };
        files_to_process =
            cli::parse_input_paths(&vec![download_path], &parsed_args.schema_version)?;
        _temp_file = temp;
    } else {
        files_to_process = std::mem::take(&mut parsed_args.parsed_paths);
        _temp_file = None;
    }

    let mut file_counter = 1;
    let mut total_row_count = 0;
    let mut total_file_size = 0;

    let output_file = std::fs::File::create(&parsed_args.output_path).with_context(|| {
        format!(
            "could not create output file `{}`",
            &parsed_args.output_path.to_string_lossy()
        )
    })?;
    let mut output_writer = match &parsed_args.output_format {
        OutputFormat::CSV => {
            OutputWriter::Csv(WriterBuilder::new().with_header(true).build(output_file))
        }
        OutputFormat::GeoParquet => {
            let props = WriterProperties::builder()
                .set_max_row_group_row_count(Some(parsed_args.parquet_row_group_size))
                .set_writer_version(parsed_args.parquet_version)
                .set_compression(parsed_args.parquet_compression)
                .build();
            let encoder = GeoParquetRecordBatchEncoder::try_new(
                &parsed_args.arrow_schema,
                &GeoParquetWriterOptions::default(),
            )
            .expect("Could not create GeoParquet encoder.");
            let writer = ArrowWriter::try_new(output_file, encoder.target_schema(), Some(props))
                .expect("Could not create GeoParquet writer.");
            OutputWriter::GeoParquet {
                writer,
                encoder,
                crs: parsed_args.crs.clone(),
                geom_type: parsed_args.geoarrow_geom_type.clone(),
                geoparquet_schema: parsed_args.arrow_schema.clone(),
            }
        }
    };

    let num_files_to_process = &files_to_process.len();
    let teryt_mapping = match &parsed_args.schema_version {
        SchemaVersion::Model2012 => None,
        SchemaVersion::Model2021 => Some(get_teryt_mapping(
            parsed_args.download_teryt,
            &parsed_args.teryt_api_username,
            &parsed_args.teryt_api_password,
            &parsed_args.teryt_path,
        )?),
    };
    for file in &files_to_process {
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
                    &mut output_writer,
                    &None,
                    &teryt_mapping,
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
                        &mut output_writer,
                        &Some(compressed_file.index),
                        &teryt_mapping,
                    )?;
                    total_row_count += processed_rows;
                }
            }
        }

        file_counter += 1;
    }
    output_writer.finish()?;
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
