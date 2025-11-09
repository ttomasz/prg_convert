use std::path::PathBuf;

use anyhow::{Context, Result};
use arrow::csv::writer::WriterBuilder;
use clap::Parser;
use glob::glob;
use quick_xml::reader::Reader;

use prg_convert::{AddressParser, build_dictionaries};

const DEFAULT_BATCH_SIZE: usize = 100_000;

#[derive(Parser)]
struct Cli {
    #[arg(
        long = "input-paths",
        help = "Input XML file path(s). Can be multiple paths separated with space. Can use glob patterns (e.g. `data/*.xml`).",
        value_delimiter = ' ',
        num_args = 1..,
    )]
    input_paths: Vec<String>,
    #[arg(long = "output-path", help = "Output file path.")]
    output_path: std::path::PathBuf,
    #[arg(long = "output-format", help = "Output file format (one of: csv, geoparquet).")]
    output_format: String,
    #[arg(long = "schema-version", help = "Schema version (one of: 2012, 2021).")]
    schema_version: String,
    #[arg(long = "batch-size", help = format!("How many rows are kept in memory before writing to output (default: {}).", DEFAULT_BATCH_SIZE))]
    batch_size: Option<usize>,
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
    if &args.output_format != "csv" && &args.output_format != "geoparquet" {
        anyhow::bail!("unsupported format `{}`, expected one of: csv, geoparquet", &args.output_format);
    }
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
    println!("----------------------------------------");

    let output_file = std::fs::File::create(&args.output_path)
        .with_context(|| format!("could not create output file `{}`", &args.output_path.display()))?;
    if &args.output_format == "geoparquet" {
        anyhow::bail!("geoparquet format is not yet implemented");
    }
    let mut writer = WriterBuilder::new().with_header(true).build(output_file);

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
        AddressParser::new(reader, batch_size, dict).for_each(|batch| {
            total_count += batch.num_rows();
            println!("Read batch of {} addresses.", batch.num_rows());
            writer.write(&batch).expect("Failed to write CSV batch.");
        });
        counter += 1;
    }
    let duration = start_time.elapsed();
    println!("----------------------------------------");
    println!("📊 Total addresses read {}. Duration: {:#?}. Data size: {:.2}MB.", total_count, duration, (total_size as f64 / 1024.0 / 1024.0));

Ok(())
}
