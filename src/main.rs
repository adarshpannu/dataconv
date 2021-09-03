#![allow(warnings)]
use arrow2::datatypes;
use arrow2::error::Result;
use arrow2::io::csv::read::{self, ByteRecord};
use arrow2::record_batch::RecordBatch;

use std::env;
use std::fs::File;
extern crate clap;
use clap::{App, Arg, SubCommand};

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
    type Item = RecordBatch;
    fn next(&mut self) -> Option<Self::Item> {
        // skip 0 (excluding the header) and read up to 100 rows.
        // this is IO-intensive and performs minimal CPU work. In particular,
        // no deserialization is performed.
        let rows_read = read::read_rows(&mut self.reader, 0, &mut self.rows).unwrap();
        let rows = &self.rows[..rows_read];

        if rows_read == 0 {
            return None;
        }
        // parse the batches into a `RecordBatch`. This is CPU-intensive, has no IO,
        // and can be performed on a different thread by passing `rows` through a channel.
        read::deserialize_batch(
            rows,
            self.schema.fields(),
            None,
            0,
            read::deserialize_column,
        )
        .ok()
    }
}

fn setup_clp() -> App<'static, 'static> {
    let app = App::new("fconv: file converter")
        .version("1.0")
        .author("Adarsh Pannu")
        .about("Converts files from one format to another")
        .subcommand(
            SubCommand::with_name("from")
                .about("specify input file details")
                .arg(
                    Arg::with_name("name")
                        .long("name")
                        .short("n")
                        .help("specify filename")
                        .takes_value(true)
                        .required(true),
                )
                .arg(
                    Arg::with_name("type")
                        .long("type")
                        .short("t")
                        .help("specify file type (csv | [parquet)")
                        .takes_value(true),
                ),
        );
    app
}

fn main() -> Result<()> {
    //let args: Vec<String> = env::args().collect();
    let arg_str = "fconv 
        from --name /Users/adarshrp/Projects/flare/data/emp.csv \
        to --name /tmp/emp.csv";

    let arg_vec: Vec<String> = arg_str
        .split(' ')
        .map(|e| e.to_owned())
        .filter(|e| e.len() > 0 && e != "\n")
        .collect();

    let matches = setup_clp().get_matches_from(arg_vec);

    println!("{:?}", matches);

    /*
    let from_iter: Box<dyn Iterator<Item = RecordBatch>> = match clpr.get("from_type") {
        Some("csv") => {
            let delimiter = clpr.get("from_delimiter").unwrap_or("|");
            if delimiter.len() != 1 {
                panic!("Invalid from_delimiter specified.")
            }
            let delimiter: u8 = delimiter.as_bytes()[0];

            let has_header = clpr.get("from_has_header");
            let has_header = match has_header {
                Some("Y") | Some("y") => true,
                _ => false,
            };

            let filename = clpr.get("from").unwrap().to_string();
            let csv = CSV::new(filename, has_header, delimiter);
            Box::new(csv)
        }
        _ => {
            panic!("Invalid from_type: {:?}", clpr.get("from_type"))
        }
    };

    for batch in from_iter {
        println!("{:?}", batch);
        //break;
    }
    */

    Ok(())
}
