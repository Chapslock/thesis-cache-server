use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;

use std::path::PathBuf;

use hyper::header::CONTENT_LENGTH;
use hyper::http::HeaderValue;
use rocksdb::{Options, DB};
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Result, Server, StatusCode};

pub static NOTFOUND: &[u8] = b"Not Found";
pub static CACHE_DATABASE_PATH: &str = "./database";
pub static CHUNK_SIZE: u64 = 10 * 1024 * 1024; // 10MB

//*
pub static SERVER_IP: [u8; 4] = [0, 0, 0, 0];
pub static SERVER_PORT: u16 = 80;
pub static PATH_TO_TEST_FILES: &str = "/var/www/html/";
pub static TEST_FILES: [&str; 4] = ["500KB.html", "1MB.html", "10MB.html", "100MB.html"];
// */
/*
pub static SERVER_IP: [u8; 4] = [127, 0, 0, 1];
pub static SERVER_PORT: u16 = 3000;
static PATH_TO_TEST_FILES: &str = "C:\\Users\\Charl.Kivioja\\Desktop\\http-test-server\\testFiles\\";
static TEST_FILES: [&str; 2] = ["500KB.html", "100MB.html"];
// */

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
            Ok::<_, hyper::Error>(service_fn(move |req| response_examples(req, db.to_owned())))
        }
    });

    let addr = SocketAddr::from((SERVER_IP, SERVER_PORT));
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

async fn simple_file_send(filename: &str, db: Arc<DB>) -> Result<Response<Body>> {
    let value = db
        .get_pinned(filename.as_bytes())
        .expect("Entry does not exist!")
        .unwrap()
        .as_ref()
        .to_owned();

    let doc_len = &value.len();
    let make_stream = async_stream::stream! {
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
    let content_length = HeaderValue::from_str(&doc_len.to_string()).unwrap();
    let response = Response::builder()
        .header(CONTENT_LENGTH, content_length)
        .body(body)
        .unwrap();

    Ok(response)
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
