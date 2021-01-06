extern crate flate2;
extern crate tar;
extern crate rayon;

use flate2::read::GzDecoder;
use std::fs::File;
use std::io::prelude::*;
use std::mem;
use std::slice;

use std::io::Read;
use tar::Archive;

use serde::Deserialize;
use tar::EntryType;

use rayon::iter::ParallelBridge;
use rayon::prelude::ParallelIterator;

//use crossbeam_channel::bounded;
use std::sync::mpsc::sync_channel;
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
        if r.parallax < 0.0 {
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

fn writer( fs : std::fs::File, res : Vec<Result>) -> std::fs::File {
    
    let mut fs = fs;
    let count = res.len();

    let p: *const [Result] = res.as_slice();
    let p: *const u8 = p as *const u8;  // convert between pointer types
    let bytes: &[u8] = unsafe {
        slice::from_raw_parts(p, mem::size_of::<Result>()*count)
    };
    let res = fs.write_all( bytes);
    if let Err(y) = res {
        panic!("{}", y);
    }
    fs
}

fn handle_tar(filename: &str) {
    let file = File::open(filename).unwrap();
    let mut archive = Archive::new(file);

    
    let (s, r ) = sync_channel(5);
    
    let output_file = File::create("result.dat").unwrap();
    let c = r.into_iter().par_brige().
    for_each(  | buffer| print!("1"));

    /*let c = r.into_iter().par_brige().map( |byte_buf| handle_file(byte_buf.to_vec())).
    for_each(  | buffer| writer( output_file, buffer) );*/

    
    let v = archive.entries().unwrap().into_iter().filter_map( |file| file.ok()).
    filter(|file| file.header().entry_type() == EntryType::Regular ).
    fold( s, |s,  file| {
        let mut buffer = Vec::new();
        let mut file = file;
        // read the whole file
        file.read_to_end(&mut buffer).unwrap();
        s.send(buffer).unwrap();
        s });
        print!("huhu");
    
}

fn main() {
    handle_tar("gaia.tar")
}
