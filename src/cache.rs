use rocksdb::{DB, Options, DBCompactionStyle};
use std::sync::Arc;

pub trait DBOperations {
    fn init(file_path: &str) -> Self;
    fn save(&self, k: &str, v: &str) -> bool;
    fn find(&self, k: &str) -> Option<String>;
    fn delete(&self, k: &str) -> bool;
}

#[derive(Clone)]
pub struct Database {
    inst: Arc<DB>,
}

impl DBOperations for Database {
    fn init(file_path: &str) -> Self {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.set_max_open_files(100);
        opts.increase_parallelism(4);
        opts.set_bytes_per_sync(8388608);
        return Database {         
            inst: Arc::new(DB::open(&opts, file_path).unwrap()),
        };
    }

    fn save(&self, k: &str, v: &str) -> bool {
        return self.inst.put(k.as_bytes(), v.as_bytes()).is_ok();
    }

    fn find(&self, k: &str) -> Option<String> {
        match self.inst.get(k.as_bytes()) {
            Ok(Some(v)) => {
                let result = String::from_utf8(v).unwrap();
                println!("Cache hit for key : {} \nReturning value!", k);
                return Some(result);
            }
            Ok(None) => {
                println!("Cache miss for key : {} \nReturning None", k);
                return None;
            }
            Err(e) => {
                println!(
                    "Error occured while retrieving value with key: {} \n Error: {}",
                    k, e
                );
                return None;
            }
        }
    }

    fn delete(&self, k: &str) -> bool {
        return self.inst.delete(k.as_bytes()).is_ok();
    }
}
