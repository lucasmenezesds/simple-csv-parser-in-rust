use std::fs;
use std::fs::{File, ReadDir};
use std::error::Error;
use std::io::{BufRead, BufReader};
use std::process;
use std::string::String;
use indicatif::{ProgressBar, ProgressStyle};

static INPUT_FOLDER: &str = "./data/input";
static OUTPUT_FOLDER: &str = "./data/output";


fn main() {
    if let Err(err) = run() {
        println!("{}", err);
        process::exit(1);
    }
}

fn retrieve_filename_to_parse() -> String {
    let dir_files: Result<ReadDir, _> = fs::read_dir(INPUT_FOLDER);

    dir_files.unwrap()
        .filter_map(|e| e.ok())
        .filter(|elem| !elem.path().ends_with(".gitkeep"))
        .nth(0)
        .unwrap()
        .file_name()
        .to_string_lossy()
        .into_owned()
}

fn run() -> Result<(), Box<dyn Error>> {
    let filename_to_parse = retrieve_filename_to_parse();

    let file_path_to_read = format!("{INPUT_FOLDER}/{filename_to_parse}");
    let file_path_to_write = format!("{OUTPUT_FOLDER}/processed-{filename_to_parse}");

    println!("File to be Processed: => {:?}", &file_path_to_read);

    let total_lines = count_lines_in_file(&file_path_to_read);
    println!("Total Lines: => {}", total_lines);

    let progress_bar = create_progress_bar(total_lines);


    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(true)
        .from_path(&file_path_to_read)
        .unwrap();

    let mut wtr = csv::WriterBuilder::new()
        .from_path(file_path_to_write)
        .unwrap();


    let headers = rdr.headers()?;

    // Headers Position
    let identity_pos = &headers.iter().position(|e| e == "id").unwrap();
    let email_pos = &headers.iter().position(|e| e == "email").unwrap();
    let first_name_pos = &headers.iter().position(|e| e == "first_name").unwrap();
    let last_name_pos = &headers.iter().position(|e| e == "last_name").unwrap();
    let country_pos = &headers.iter().position(|e| e == "country").unwrap();
    let created_at_pos = &headers.iter().position(|e| e == "created_at").unwrap();

    let headers_to_write = ["id", "email", "name", "location", "created_at"];

    wtr.write_record(headers_to_write)?;


    for result in rdr.records() {
        let record = result?;

        let full_name = format!("{} {}", &record[*first_name_pos], &record[*last_name_pos]);
        let trimmed_full_name = full_name.trim();

        let data_to_write = [&record[*identity_pos], &record[*email_pos], &trimmed_full_name, &record[*country_pos], &record[*created_at_pos]];

        wtr.write_record(data_to_write)?;
        progress_bar.inc(1);
    }

    progress_bar.finish();
    println!("Time Spent: {:?}", progress_bar.elapsed());

    wtr.flush()?;
    Ok(())
}

fn create_progress_bar(total_lines: u64) -> ProgressBar {
    let progress_bar = ProgressBar::new(total_lines);
    progress_bar.set_style(ProgressStyle::default_bar()
        .template("ETA: {eta} | Elapsed: {elapsed_precise} | {bar:40.cyan/blue} {pos:>9}/{len:9} {msg}")
        .unwrap());

    progress_bar
}

fn count_lines_in_file(path: &String) -> u64 {
    let file_to_count = BufReader::new(File::open(path).expect("Unable to open file"));
    let mut total_lines: u64 = 0;

    for _line in file_to_count.lines() {
        total_lines += 1;
    }

    total_lines
}