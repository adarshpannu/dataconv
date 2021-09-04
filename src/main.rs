#![allow(warnings)]
use arrow2::datatypes;
use arrow2::error::Result;
use arrow2::io::csv::read::{self, ByteRecord};
use arrow2::record_batch::RecordBatch;

use std::env;
use std::fs::File;

#[macro_use]
extern crate clap;
use clap::App;

use arrow2::io::parquet::write::{write_file, to_parquet_schema, Compression, Encoding, Version, WriteOptions};
use arrow2::io::parquet::write::{RowGroupIter, RowGroupIterator};

struct CSV {
    filename: String,
    has_header: bool,
    delimiter: u8,
    reader: read::Reader<File>,
    schema: datatypes::Schema,
    rows: Vec<ByteRecord>,
}

impl CSV {
    fn new(filename: String, has_header: bool, delimiter: u8) -> CSV {
        // Create a CSV reader. This is typically created on the thread that reads the file and
        // thus owns the read head.
        let mut reader = read::ReaderBuilder::new()
            .delimiter(delimiter)
            .from_path(filename.clone())
            .unwrap();

        // Infers the schema using the default inferer. The inferer is just a function that maps a string
        // to a `DataType`.
        let schema = read::infer_schema(&mut reader, None, true, &read::infer).unwrap();

        // allocate space to read from CSV to. The size of this vec denotes how many rows are read.
        let mut rows = vec![read::ByteRecord::default(); 100];

        CSV {
            filename,
            has_header,
            delimiter: delimiter,
            reader,
            schema,
            rows,
        }
    }
}

impl Iterator for CSV {
    type Item = Result<RecordBatch>;
    fn next(&mut self) -> Option<Self::Item> {
        // skip 0 (excluding the header) and read up to 100 rows.
        // this is IO-intensive and performs minimal CPU work. In particular,
        // no deserialization is performed.
        self.rows.clear();
        let rows_read = read::read_rows(&mut self.reader, 0, &mut self.rows).unwrap();
        let rows = &self.rows[..rows_read];

        if rows_read > 0 {
            // parse the batches into a `RecordBatch`. This is CPU-intensive, has no IO,
            // and can be performed on a different thread by passing `rows` through a channel.
            Some(read::deserialize_batch(
                rows,
                self.schema.fields(),
                None,
                0,
                read::deserialize_column,
            ))
        } else {
            None
        }
    }
}

enum Filetype {
    UNKNOWN,
    CSV,
    PARQUET,
}

fn get_filetype_from_name(filename: &str) -> Filetype {
    let ix = filename.rfind(".");
    if let Some(ix) = ix {
        let file_ext = &filename[ix + 1..];
        match file_ext.to_lowercase().as_str() {
            "csv" | "CSV" => Filetype::CSV,
            "parquet" => Filetype::PARQUET,
            _ => Filetype::UNKNOWN,
        }
    } else {
        Filetype::UNKNOWN
    }
}

fn main() -> Result<()> {
    //let args: Vec<String> = env::args().collect();
    let arg_str = "fconv 
        -f /Users/adarshrp/Projects/flare/data/emp.csv \
        -t /tmp/emp.parquet 
        -d , 
        -h";

    let arg_vec: Vec<String> = arg_str
        .split(' ')
        .map(|e| e.to_owned())
        .filter(|e| e.len() > 0 && e != "\n")
        .collect();

    //dbg!(&arg_vec);

    // The YAML file is found relative to the current file, similar to how modules are found
    let yaml = load_yaml!("clap.yml");
    let matches = App::from_yaml(yaml).get_matches_from(arg_vec);

    let filetype = get_filetype_from_name(matches.value_of("from").unwrap());

    let iter = match filetype {
        Filetype::CSV => {
            let delimiter = matches.value_of("delimiter").unwrap_or(",");
            if delimiter.len() != 1 {
                panic!("Invalid delimiter specified: >{}<.", delimiter)
            }
            let delimiter: u8 = delimiter.as_bytes()[0];

            let has_header = matches.value_of("header");
            let has_header = match has_header {
                Some("Y") | Some("y") => true,
                _ => false,
            };

            let filename = matches.value_of("from").unwrap().to_string();
            let csv = CSV::new(filename, has_header, delimiter);
            Box::new(csv)
        }
        _ => {
            panic!("Unknown filetype: {:?}", matches.value_of("from"))
        }
    };

    /*
    for batch in from_iter {
        println!("{:?}", batch);
        //break;
    }
    */
    // Create a new empty file
    let output_filename = matches.value_of("to").unwrap();

    let mut file = File::create(output_filename)?;
    let options = WriteOptions {
        write_statistics: true,
        compression: Compression::Uncompressed,
        version: Version::V2,
    };
    let encodings = vec![Encoding::Plain];
    let schema = &iter.schema.clone();
    let parquet_schema = to_parquet_schema(&iter.schema)?;

    let iter: Box<dyn Iterator<Item = Result<RecordBatch>>> = iter;

    let row_groups = RowGroupIterator::try_new(iter, schema, options, encodings)?;

    // Write the file. Note that, at present, any error results in a corrupted file.
    let _ = write_file(
        &mut file,
        row_groups,
        &schema,
        parquet_schema,
        options,
        None,
    )?;
    Ok(())
}
