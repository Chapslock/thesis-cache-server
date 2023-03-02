use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;

use std::path::PathBuf;

use conf::{CHUNK_SIZE, NOTFOUND, TEST_FILES};
use hyper::header::CONTENT_LENGTH;
use hyper::http::HeaderValue;
use rocksdb::{Options, DB};
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Result, Server, StatusCode};

use crate::conf::{CACHE_DATABASE_PATH, PATH_TO_TEST_FILES, SERVER_IP, SERVER_PORT};

mod conf;

#[tokio::main]
async fn main() {
    pretty_env_logger::init(); //For logging
    let db = init_database();
    populate_database(db.clone()).expect("Populating the database failed!"); // Adds the initial files to the database

    // Creates a service function that handles all requests made to the server
    let make_service = make_service_fn(move |_| {
        let db = db.clone();
        async {
            Ok::<_, hyper::Error>(service_fn(move |req| handle_requests(req, db.to_owned())))
        }
    });

    // Initialise the HTTP server
    let addr = SocketAddr::from((SERVER_IP, SERVER_PORT));
    let server = Server::bind(&addr).serve(make_service);
    println!("Listening on http://{}", addr);
    if let Err(e) = server.await {
        eprintln!("server error: {}", e);
    }
}

async fn handle_requests(req: Request<Body>, db: Arc<DB>) -> Result<Response<Body>> {
    match req.method() {
        &Method::GET => {
            let path: PathBuf = req.uri().path().parse().unwrap();
            let file_name = path.file_name().unwrap().to_str().unwrap();
            cache_file_send(file_name, db).await
        }
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

async fn cache_file_send(filename: &str, db: Arc<DB>) -> Result<Response<Body>> {
    let doc_len = db
        .get_pinned(filename.as_bytes())
        .expect("Does not exist!")
        .unwrap()
        .len();
    let content_length = HeaderValue::from_str(&doc_len.to_string()).unwrap();

    let filename = filename.clone().to_owned();
    // We create an async stream, that reads the values from the DBPinnable slice object in chunks and send them over network
    let make_stream = async_stream::stream! {
        let value = db
            .get_pinned(filename.as_bytes())
            .expect("Entry does not exist!")
            .unwrap();
        let mut written: u64 = 0;
        while written < value.len().try_into().unwrap() {
            let start = written;
            let end = std::cmp::min(CHUNK_SIZE + written, value.len().try_into().unwrap());
            let buffer = &value[start.try_into().unwrap()..end.try_into().unwrap()];
            written += buffer.len() as u64;
            let res: Result<Vec<u8>> = Ok(buffer.to_vec());
            yield res;
        }
    };

    let body = Body::wrap_stream(make_stream);
    let response = Response::builder()
        .header(CONTENT_LENGTH, content_length)
        .body(body)
        .unwrap();
    return Ok(response);
}

fn populate_database(db: Arc<DB>) -> std::io::Result<()> {
    for file_name in &TEST_FILES {
        let mut file_path: String = PATH_TO_TEST_FILES.to_owned();
        file_path.push_str(&file_name);
        println!("{}", &file_path);
        let mut file = File::open(file_path)?;
        let mut file_contents = String::new();
        file.read_to_string(&mut file_contents)
            .expect("Failed to read from file!");
        db.put(file_name, &file_contents)
            .expect("Failed to add value to database!");
    }
    return Ok(());
}

fn init_database() -> Arc<DB> {
    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.set_use_direct_reads(true);
    opts.set_use_direct_io_for_flush_and_compaction(true);
    opts.set_max_background_jobs(2);
    opts.set_compaction_readahead_size(2 * 1024 * 1024); //2MB
    opts.set_writable_file_max_buffer_size(1024 * 1024); //1MB
    return Arc::new(DB::open(&opts, CACHE_DATABASE_PATH).unwrap());
}