extern crate libflate;
extern crate tar;

use libflate::gzip::Decoder;
use std::fs::File;
use std::io::prelude::*;
use std::time::Duration;
use std::thread;
use std::mem;
use std::slice;

use std::io::Read;
use tar::Archive;

use serde::Deserialize;
use std::sync::mpsc::channel;
use std::sync::mpsc::Sender;
use std::sync::mpsc::Receiver;

use tar::EntryType;
use threadpool::ThreadPool;


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

fn handle_file(sender: Sender<Vec<Result>>, buffer: Vec<u8>)  {
    let decoder = Decoder::new(buffer.as_slice()).unwrap();
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
    let res = sender.send(buffer);
    if let Err(x) = res {
        panic!("{}", x);
    }
}

fn receive( recv : Receiver<Vec<Result>>) {
    let mut file = File::create("result.dat").unwrap();
    let mut count = 0;
    let mut num = 0;
    for r in recv.iter() {
        count += r.len();
        num += 1;
        let p: *const [Result] = r.as_slice();
        let p: *const u8 = p as *const u8;  // convert between pointer types
        let bytes: &[u8] = unsafe {
            slice::from_raw_parts(p, mem::size_of::<Result>()*r.len())
        };
        let res = file.write_all( bytes);
        if let Err(y) = res {
            panic!("{}", y);
        }
    }
    drop( file);
    println!("{} files {} stars", num, count);
}


fn handle_tar(filename: &str) {
    let file = File::open(filename).unwrap();
    let mut archive = Archive::new(file);

    let pool = threadpool::Builder::new().build();
    let (sender, receiver) = channel();

    pool.execute( move || receive( receiver));

    for file in archive.entries().unwrap() {
        let mut f = match file {
            Ok(x) => x,
            Err(y) => { println!("Ignore {:?}\n", y); continue }
        };

        if f.header().entry_type() != EntryType::Regular {
            continue;
        }
        let mut buffer = Vec::new();
        // read the whole file
        f.read_to_end(&mut buffer).unwrap();
       
        let sender_thread = sender.clone();
        pool.execute(move || handle_file(sender_thread, buffer));
    }
    drop(sender);
    while pool.active_count() > 0 {
        println!("{}", pool.active_count());
        let ten_millis = Duration::from_millis(100);


        thread::sleep(ten_millis);
    }
    
    

}

fn main() {
    handle_tar("gaia.tar")
}
