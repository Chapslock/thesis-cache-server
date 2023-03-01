
use std::fs::File;
use std::io::Read;
use std::net::SocketAddr;

use std::path::PathBuf;

use std::sync::{Arc};

use async_stream::stream;
use futures::channel::mpsc;
use futures::{pin_mut, Stream, StreamExt, TryStreamExt};
use hyper::header::CONTENT_LENGTH;
use hyper::http::HeaderValue;
//use futures::io::Cursor;
use rocksdb::{IteratorMode, Options, WriteBatch, DB};
//use tokio::fs::File;

//use tokio_util::codec::{BytesCodec, FramedRead};

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Result, Server, StatusCode};

pub static NOTFOUND: &[u8] = b"Not Found";
pub static CACHE_DATABASE_PATH: &str = "./database";
//pub static SERVER_IP: [i8; 4] = [0, 0, 0, 0];
pub static SERVER_IP: [u8; 4] = [127, 0, 0, 1];
//pub static SERVER_PORT: u16 = 80;
pub static SERVER_PORT: u16 = 3000;
//pub static PATH_TO_TEST_FILES: &str = "/var/www/html/";
static PATH_TO_TEST_FILES: &str = "C:\\Users\\Charl.Kivioja\\Desktop\\http-test-server\\testFiles\\";
//pub static TEST_FILES: [&str; 4] = ["500KB.html", "1MB.html", "10MB.html", "100MB.html"];
static TEST_FILES: [&str; 2] = ["500KB.html", "100MB.html"];
pub static CHUNK_SIZE: u64 = 10 * 1024 * 1024; // 10MB

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
    // Serve a file by asynchronously reading it by chunks using tokio-util crate.
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

    //pin_mut!(make_stream);

    let body = Body::wrap_stream(make_stream);
    let content_length = HeaderValue::from_str(&doc_len.to_string()).unwrap();
    let response = Response::builder()
        .header(CONTENT_LENGTH, content_length)
        .body(body)
        .unwrap();

    Ok(response)

    //Ok(not_found())

    /*
    let files_cf_handle = db.cf_handle("files").expect("No column family handle called files found!");
    let offset_bytes = db.get_cf(files_cf_handle, filename.as_bytes()).expect("Could not get_cf from db!");
    let arr: [u8; 8] = match offset_bytes.unwrap().try_into() {
        Ok(array) => array,
        Err(vec) => panic!("Expected a Vec of length 8 but got {:?}", vec),
    };
    let offset = i64::from_le_bytes(arr);
    let mut buffer = vec![0; CHUNK_SIZE.try_into().unwrap()];
    let mut position = 0;

    let (sender, body) = Body::channel();
    let (parts,mut body) = Response::builder()
    .status(200)
    .header("Content-Type", "application/octet-stream")
    .body(body).unwrap().into_parts();
    let (sender, receiver) = mpsc::channel(16);

    loop {
        let bytes_to_read = std::cmp::min(CHUNK_SIZE, (position + CHUNK_SIZE) - offset as u64);

        if bytes_to_read == 0 {
            break;
        }
        let bytes_read = db.get_cf(
            db.cf_handle("data").expect("No column family handle called data found!"),
            (offset + position as i64).to_le_bytes()
        ).expect("failed to do db cf_handle")
        .map(|value| {
            let bytes_read = std::cmp::min(bytes_to_read, value.len().try_into().unwrap());
            buffer[..bytes_read as usize].copy_from_slice(&value[..bytes_read as usize]);
            bytes_read
        }).unwrap_or_default();
        let chunk = &buffer[..bytes_read as usize];

        //send the data here!!
        //sender.send_data(hyper::body::Bytes::from(chunk));
        body = Body::wrap_stream(futures::stream::iter(chunk.into_iter().map(Ok)));


        position += bytes_read as u64;
    }
    let body = Body::wrap_stream(receiver.map_ok(|result| result.map))
    Ok(Response::from_parts(parts, body))

    //Ok(not_found())


    // */

    /*

    let mut offset = 0;
    let mut buffer = vec![0u8; 1024*1024]; //read 1MB at a time
    let mut key = filename.to_string();
    key.push_str("0");
    let iter = db.iterator(IteratorMode::From(key.as_bytes(), rocksdb::Direction::Forward));
    for item in iter {
        let (key, value) = item.unwrap();
        let key_str = std::str::from_utf8(&key).unwrap();

        if !key_str.contains(filename) {
            break;
        }
        //TODO: Maybe
    }
    return Ok(Response::builder().status(200)
    .header("Content-Type", "application/octet-stream").header("Content-Length", iter.count().mul(CHUNK_SIZE)).body(Body::wrap_stream(iter)).unwrap())

    // */

    /*
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
    // */
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
        println!("{}", &file_path);
        let mut file = File::open(file_path)?;
        let mut file_contents = String::new();
        file.read_to_string(&mut file_contents)
            .expect("Failed to read from file!");
        db.put(file_name, &file_contents)
            .expect("Failed to add value to database!");
    }
    return Ok(());

    /*
    let mut batch = WriteBatch::default();

    for file_name in &TEST_FILES {
        let mut path = PATH_TO_TEST_FILES.to_owned();
        path.push_str(&file_name);
        let mut file = File::open(file_name)?;
        let key = String::from(file_name.to_owned());

        let mut buffer = vec![0; CHUNK_SIZE.try_into().unwrap()];
        let mut position = 0;

        while let Ok(bytes_read) = file.read(&mut buffer) {
            if bytes_read == 0 {
                break;
            }

            let value = &buffer[..bytes_read];
            let offset = position as i64;
            let files_cf_handle = db.cf_handle("files").expect("No column family handle called files found!");
            let data_cf_handle = db.cf_handle("data").expect("No column family handle called data found!");
            batch.put_cf(files_cf_handle, key.as_bytes(), &offset.to_le_bytes());
            db.put_cf(
                data_cf_handle,
                offset.to_le_bytes(),
                &value
            ).expect("inserting to the database failed!");

            position += bytes_read as u64;

        }


    }

    db.write(batch).expect("Writing of batch failed!");

    Ok(())
    // */
}
