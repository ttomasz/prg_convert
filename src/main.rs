use anyhow::{Context, Result};
use arrow::csv::writer::WriterBuilder;
use clap::Parser;
use quick_xml::reader::Reader;

use prg_convert::AddressParser;

const BATCH_SIZE: usize = 100_000;

#[derive(Parser)]
struct Cli {
    #[arg(long = "input", help = "Input XML file path")]
    input: std::path::PathBuf,
    #[arg(long = "output", help = "Output file path")]
    output: std::path::PathBuf,
    #[arg(long = "batch-size", help = format!("How many rows are kept in memory before writing to output (default: {})", BATCH_SIZE))]
    batch_size: Option<usize>,
}

fn main() -> Result<()> {
    let args = Cli::parse();
    let batch_size = args.batch_size.unwrap_or(BATCH_SIZE);
    let mut total_count = 0;

    println!("⚙️  Parameters:");
    println!("  Input file: {}", args.input.display());
    println!("  Output file: {}", args.output.display());
    println!("  Batch size: {}", batch_size);
    println!("----------------------------------------");

    let mut reader = Reader::from_file(&args.input)
        .with_context(|| format!("could not read XML from file `{}`", &args.input.display()))?;
    reader.config_mut().expand_empty_elements = true;
    println!("⚙️  XML reader configuration: {:#?}", reader.config());
    println!("----------------------------------------");

    let output_file = std::fs::File::create(&args.output)
        .with_context(|| format!("could not create output file `{}`", &args.output.display()))?;
    let mut csv_writer = WriterBuilder::new().with_header(true).build(output_file);

    let input_file_metadata = std::fs::metadata(&args.input)
        .with_context(|| format!("could not get metadata for file `{}`", &args.input.display()))?;
    if input_file_metadata.is_dir() {
        anyhow::bail!("input path `{}` is a directory, expected a file", &args.input.display());
    }
    let input_file_size = input_file_metadata.len();
    let input_file_type = &args.input.extension().unwrap().to_string_lossy().to_uppercase();
    println!("📁  Input file ({}) size: {:.2} MB", input_file_type, (input_file_size as f64 / 1024.0 / 1024.0));
    println!("----------------------------------------");

    let start_time = std::time::Instant::now();
    AddressParser::new(reader, batch_size).for_each(|batch| {
        total_count += batch.num_rows();
        println!("Read batch of {} addresses.", batch.num_rows());
        csv_writer.write(&batch).expect("Failed to write CSV batch");
    });
    let duration = start_time.elapsed();
    println!("📊  Total addresses read {}. Duration: {:#?}", total_count, duration);

Ok(())
}
