use std::{path::PathBuf, sync::Arc};

use hyper::{Body, Request, Response, Result, StatusCode};
use rocksdb::DB;

use crate::conf::LOGGING_ENABLED;

pub async fn delete_from_cache(req: Request<Body>, db: Arc<DB>) -> Result<Response<Body>> {
    let path: PathBuf = req.uri().path().parse().unwrap();
    let file_name = path.file_name().unwrap().to_str().unwrap();

    return match db.delete(file_name.as_bytes()) {
        Ok(()) => {
        let mut message: String = String::from("file sucessfully deleted! ");
        message.push_str(file_name);
        if LOGGING_ENABLED {
            println!("deleting file: {}", file_name);
        }
        Ok(Response::builder()
            .status(StatusCode::OK)
            .body(message.into())
            .unwrap())
        },
        Err(error) => {
            let mut message = String::from("Problem occured while deleting file: \n");
            message.push_str(error.into_string().as_str());
            if LOGGING_ENABLED {
                println!("{}", message);
            }
            Ok(Response::builder()
            .status(StatusCode::INTERNAL_SERVER_ERROR)
            .body(message.into())
            .unwrap())
        }
    };
}
