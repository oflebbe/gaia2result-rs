extern crate flate2;
extern crate tar;
extern crate num_cpus;

use flate2::read::GzDecoder;
use std::fs::File;
use std::io::prelude::*;
use std::mem;
use std::slice;

use std::io::Read;
use tar::Archive;
use std::time::Instant;

use serde::Deserialize;
use tar::EntryType;

use crossbeam_channel::bounded;
use crossbeam_channel::Receiver;
#[derive(Debug, Deserialize)]
struct Record {
    ra: f32,
    dec: f32,
    parallax: f32,
}

#[derive(Debug, Deserialize)]
struct Result {
    x: f32,
    y: f32,
    z: f32,
}

fn handle_file( buffer: Vec<u8>) -> Vec<Result> {
    let decoder = GzDecoder::new(buffer.as_slice());
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',')
        .from_reader(decoder);

    let mut buffer = Vec::new(); 
    for record in reader.deserialize() {
        let r: Record = match record {
            Ok(x) => x,
            Err(_) => continue,
        };
        if r.parallax <= 0.0 {
            continue;
        }
        // println!("{:?}", r);
        let (sra, cra) = (r.ra * std::f32::consts::PI / 180.0).sin_cos();
        let (sdec, cdec) = (r.dec * std::f32::consts::PI / 180.0).sin_cos();
        let r = 1.58125074e-5 / (r.parallax / (1000.0 * 3600.0) * std::f32::consts::PI / 180.0);
        let x = r * cra * cdec;
        let y = r * sra * cdec;
        let z = r * sdec;
        let rr = Result{ x: x, y: y, z: z};
        buffer.push( rr);
    }
    buffer
}

fn writer( rcv: Receiver< Vec<Result>>, num_files: usize) -> (usize, usize) {
    
    let mut fs = File::create("result.dat").unwrap();
    let mut count : usize  = 0;
    let mut num_stars = 0;
    let start = Instant::now();
    for res in rcv {
        count += 1;
        let stars = res.len();
        num_stars += stars;
        let p: *const [Result] = res.as_slice();
        let p: *const u8 = p as *const u8;  // convert between pointer types
        let bytes: &[u8] = unsafe {
            slice::from_raw_parts(p, mem::size_of::<Result>()*stars)
        };
        let res = fs.write_all( bytes);
        if let Err(y) = res {
            panic!("{}", y);
        }
        let progress = start.elapsed().as_secs_f64();
        let remaining = (progress / (count as f64 )) * (num_files as f64 - count as f64);
        println!("{}/{} : remaing {} min :{} stars", count, num_files, remaining/ 60.0, num_stars);
    }
    (count, num_stars)
}

fn count_tar( filename: &str) -> usize {
    let file = File::open(filename).unwrap();
    let mut archive = Archive::new(file);
    archive.entries().unwrap().into_iter().filter_map( |file| file.ok()).
    filter(|file| file.header().entry_type() == EntryType::Regular).count()
}

fn handle_tar(filename: &str) {
    let num_files = 61242 ; //= count_tar( filename); Workaround for being too slow
    let file = File::open(filename).unwrap();
    let mut archive = Archive::new(file);

    
    let (s1, r1 ) = bounded::<Vec<u8>>(0);
    let (s2, r2 ) = bounded::<Vec<Result>>(0);

    // Iterate over tar file
    std::thread::spawn( move || {
        archive.entries().unwrap().into_iter().filter_map( |file| file.ok()).
        filter(|file| file.header().entry_type() == EntryType::Regular ).
        for_each( |file| {
            let mut buffer = Vec::new();
            let mut file = file;
            // read the whole file
            file.read_to_end(&mut buffer).unwrap();
            s1.send(buffer).unwrap();
        });
    });
    for _ in 0..num_cpus::get() {
        let receiver = r1.clone();  // clone for this thread
        let sender = s2.clone();
        std::thread::spawn(move || {
            receiver.into_iter().map( |byte_buf| handle_file(byte_buf.to_vec())).
               for_each( |buffer| sender.send(buffer).unwrap());
            }
        );
    }
    drop(s2);

    let (count, stars) = writer( r2, num_files);
    println!("{} {}", count, stars);
}


fn main() {
    handle_tar("gaia.tar")
}
