use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;

use http_body_util::{Full};
use hyper::body::{Bytes, Incoming};
use hyper::server::conn::http1;
use hyper::service::Service;
use hyper::{Request, Response};
use tokio::net::TcpListener;
use std::fs::File;
use std::io::prelude::*;

use crate::cache::DBOperations;

mod cache;

static CACHE_DATABASE_PATH: &str = "./database";
static SERVER_PORT: u16 = 80;
static PATH_TO_TEST_FILES: &str = "/var/www/html/";
//static PATH_TO_TEST_FILES: &str = "C:\\Users\\Charl.Kivioja\\Desktop\\http-test-server\\testFiles\\";
static TEST_FILES: [&str; 4] = ["500KB.html", "1MB.html", "10MB.html", "100MB.html"];
//static TEST_FILES: [&str; 1] = ["500KB.html"];
static IS_POPULATING_OF_DATABASE_NEEDED: bool = true;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if IS_POPULATING_OF_DATABASE_NEEDED {
        populate_database().expect("Populating the database failed !");
    }

    let addr = SocketAddr::from(([0, 0, 0, 0], SERVER_PORT));

    // We create a TcpListener and bind it to 127.0.0.1:3000
    let listener = TcpListener::bind(addr).await?;
    println!("Listening on http://{}", addr);

    // We start a loop to continuously accept incoming connections
    loop {
        let (stream, _) = listener.accept().await?;

        // Spawn a tokio task to serve multiple connections concurrently
        tokio::task::spawn(async move {
            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(
                    stream,
                    RequestHandlerService {
                        db: cache::DBOperations::init(CACHE_DATABASE_PATH),
                    },
                )
                .await
            {
                println!("Error serving connection: {:?}", err);
            }
        });
    }
}

fn populate_database() -> std::io::Result<()> {
    let db: cache::Database = cache::DBOperations::init(CACHE_DATABASE_PATH);
    println!("Populating database with:");

    for file_name in &TEST_FILES {
        let mut file_path: String = PATH_TO_TEST_FILES.to_owned();
        file_path.push_str(&file_name);
        println!("{}",&file_path);
        let mut file = File::open(file_path)?;
        let mut file_contents = String::new();
        file.read_to_string(&mut file_contents).expect("Failed to read from file!");
        println!("Key : {}\nValue : {}", file_name, &file_contents);
        db.save(file_name, &file_contents);
    }
    return Ok(());
}

struct RequestHandlerService {
    db: cache::Database,
}

impl Service<Request<Incoming>> for RequestHandlerService {
    type Response = Response<Full<Bytes>>;
    type Error = hyper::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&mut self, req: Request<Incoming>) -> Self::Future {
        fn create_response(s: String) -> Result<Response<Full<Bytes>>, hyper::Error> {
            return Ok(Response::builder().body(Full::new(Bytes::from(s))).unwrap());
        }

        let str_path = req.uri().path();
        let requested_file_name = str_path.trim_matches('/');
        let cache_result = self.db.find(requested_file_name);

        if cache_result.is_some() {
            return Box::pin(async { create_response(cache_result.unwrap()) });
        } else {
            return Box::pin(async { create_response("Requested resource not found!".into()) });
        }
    }
}
