use std::net::SocketAddr;

use std::path::PathBuf;

use conf::{NOTFOUND, LOGGING_ENABLED};
use database::delete::delete_from_cache;
use database::get::get_file_from_cache;
use database::save::save_to_cache;

use rocksdb::{Options, DB};
use std::sync::Arc;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Result, Server, StatusCode};

use crate::conf::{CACHE_DATABASE_PATH, SERVER_IP, SERVER_PORT};

use crate::database::util::populate_database_with_dummy_data;

mod conf;
mod database;


#[tokio::main]
async fn main() {
    pretty_env_logger::init(); //For logging
    let db = init_database();
    populate_database_with_dummy_data(db.clone()).expect("Populating the database failed!"); // Adds the initial files to the database

    // Creates a service function that handles all requests made to the server
    let make_service = make_service_fn(move |_| {
        let db = db.clone();
        async { Ok::<_, hyper::Error>(service_fn(move |req| handle_requests(req, db.to_owned()))) }
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
            if LOGGING_ENABLED {
                println!("Fetching file: {}", file_name);
            }
            get_file_from_cache(file_name, db).await
        }
        &Method::POST => save_to_cache(req, db).await,
        &Method::DELETE => delete_from_cache(req, db).await,
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
