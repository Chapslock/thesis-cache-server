use std::sync::Arc;

use hyper::{header::CONTENT_LENGTH, http::HeaderValue};
use hyper::{Body, Response, Result};
use rocksdb::DB;

use crate::conf::CHUNK_SIZE;

pub async fn get_file_from_cache(filename: &str, db: Arc<DB>) -> Result<Response<Body>> {
    let mut opts = rocksdb::ReadOptions::default();
    opts.fill_cache(false);
    opts.set_readahead_size(2 * 1024 * 1024);

    let doc_len = db
        .get_pinned_opt(filename.as_bytes(), &opts)
        .expect("Does not exist!")
        .unwrap()
        .len();
    let content_length = HeaderValue::from_str(&doc_len.to_string()).unwrap();

    let filename = filename.clone().to_owned();
    // We create an async stream, that reads the values from the DBPinnable slice object in chunks and send them over network
    let make_stream = async_stream::stream! {
        let value = db
            .get_pinned_opt(filename.as_bytes(), &opts)
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
