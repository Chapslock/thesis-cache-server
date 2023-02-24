#![deny(warnings)]

use std::fs::File;
use std::io::{Read, Cursor};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

//use futures::io::Cursor;
use rocksdb::{Options, DB};
//use tokio::fs::File;

//use tokio_util::codec::{BytesCodec, FramedRead};

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Result, Server, StatusCode};

pub static NOTFOUND: &[u8] = b"Not Found";
pub static CACHE_DATABASE_PATH: &str = "./database";
pub static SERVER_PORT: u16 = 8080;
pub static PATH_TO_TEST_FILES: &str = "/var/www/html/";
//static PATH_TO_TEST_FILES: &str = "C:\\Users\\Charl.Kivioja\\Desktop\\http-test-server\\testFiles\\";
pub static TEST_FILES: [&str; 4] = ["500KB.html", "1MB.html", "10MB.html", "100MB.html"];
//static TEST_FILES: [&str; 1] = ["500KB.html"];

#[tokio::main]
async fn main() {
    pretty_env_logger::init();
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_use_direct_reads(true);
    opts.set_use_direct_io_for_flush_and_compaction(true);
    opts.set_max_background_jobs(2);
    opts.set_compaction_readahead_size(2 * 1024 * 1024); //2MB
    opts.set_writable_file_max_buffer_size(1024 * 1024); //1MB
    let db = Arc::new(DB::open(&opts, CACHE_DATABASE_PATH).unwrap());
    populate_database(db.clone()).expect("Populating the database failed!");

    let make_service = make_service_fn(move |_| {
        let db = db.clone();
        async {
            Ok::<_, hyper::Error>(service_fn(move |req| {
                response_examples(req, db.to_owned())
            }))
        }
    });

    let addr = SocketAddr::from(([127, 0, 0, 1], SERVER_PORT));
    let server = Server::bind(&addr).serve(make_service);
    println!("Listening on http://{}", addr);
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

async fn response_examples(req: Request<Body>, db: Arc<DB>) -> Result<Response<Body>> {
    match req.method() {
        &Method::GET => {
            let path: PathBuf = req.uri().path().parse().unwrap();
            let file_name = path.file_name().unwrap().to_str().unwrap();
            simple_file_send(file_name, db).await
        },
        _ => Ok(not_found()),
    }
}

/// HTTP status code 404
fn not_found() -> Response<Body> {
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .body(NOTFOUND.into())
        .unwrap()
}

async fn simple_file_send(filename: &str, db: Arc<DB>) -> Result<Response<Body>> {
    // Serve a file by asynchronously reading it by chunks using tokio-util crate.
    let chunk_size = 1024 * 1024; //1MB
    if let Ok(Some(file)) = db.get(filename.as_bytes()) {
        //let stream = futures_util::stream::iter(file);
        //let stream = FramedRead::new(file, BytesCodec::new());
        //let body = Body::wrap_stream(stream);

        let stream = StreamChunks::new(Cursor::new(file), chunk_size);
        let body = Body::wrap_stream(futures::stream::iter(stream));
        return Ok(Response::new(body));
    }

    Ok(not_found())
}

struct StreamChunks<R: Read> {
    reader: R,
    chunk_size: usize,
}

impl<R: Read> StreamChunks<R> {
    fn new(reader: R, chunk_size: usize) -> Self {
        StreamChunks { reader, chunk_size }
    }
}

impl<R: Read> Iterator for StreamChunks<R> {
    type Item = Result<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = vec![0u8; self.chunk_size];
        let num_bytes = self.reader.read(&mut chunk).unwrap();
        if num_bytes > 0 {
            chunk.truncate(num_bytes);
            Some(Ok(chunk))
        } else {
            None
        }
    }
}

fn populate_database(db: Arc<DB>) -> std::io::Result<()> {
    for file_name in &TEST_FILES {
        let mut file_path: String = PATH_TO_TEST_FILES.to_owned();
        file_path.push_str(&file_name);
        println!("{}",&file_path);
        let mut file = File::open(file_path)?;
        let mut file_contents = String::new();
        file.read_to_string(&mut file_contents).expect("Failed to read from file!");
        db.put(file_name, &file_contents).expect("Failed to add value to database!");
    }
    return Ok(());
}